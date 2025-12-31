from __future__ import annotations

import hashlib
from typing import Any, Dict, Iterable, List, Optional, Tuple

from entities import Company, Product, Warehouse
from services.master_data.geo_data import WarehouseGeo, readable, slugify
from services.master_data.product_seeder import PRODUCT_CATEGORIES
from database.odoo_client import IdempotentStore, OdooClient
from services.master_data.company_seeder import CompanySeeder
from services.master_data.partner_seeder import PartnerSeeder
from services.master_data.product_seeder import ProductSeeder
from services.master_data.warehouse_seeder import WarehouseSeeder


def _stable_int_seed(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


class MasterSeeder:
    def __init__(self, client: OdooClient, *, dataset_key: str, dry_run: bool = False):
        self.client = client
        self.dataset_key = dataset_key
        self.dry_run = dry_run
        self.store = IdempotentStore()
        self._dry_wh_codes: set[str] = set()

        # Initialize service classes
        self.company_seeder = CompanySeeder(self)
        self.partner_seeder = PartnerSeeder(self)
        self.product_seeder = ProductSeeder(self)
        self.warehouse_seeder = WarehouseSeeder(self)

    def _fake_id(self, model: str, key: str) -> int:
        return int(_stable_int_seed(f"{model}:{key}") % 900_000_000 + 100_000_000)

    # Delegate methods to service classes for backward compatibility
    def ensure_country_id(self, country_code: str) -> int:
        return self.company_seeder.ensure_country_id(country_code)

    def ensure_company(self, name: str, *, country_code: str) -> int:
        return self.company_seeder.ensure_company(name, country_code=country_code)

    def ensure_partner(self, name: str, *, country_code: str, is_vendor: bool, company_id: int | None = None) -> int:
        return self.partner_seeder.ensure_partner(name, country_code=country_code, is_vendor=is_vendor, company_id=company_id)

    def ensure_product_category(self, name: str) -> int:
        return self.product_seeder.ensure_product_category(name)

    def ensure_uom(self, *, kind: str) -> tuple[int, str]:
        return self.product_seeder.ensure_uom(kind=kind)

    def ensure_warehouse(self, *, company_id: int, company_name: str, wh_name: str) -> Warehouse:
        return self.warehouse_seeder.ensure_warehouse(company_id=company_id, company_name=company_name, wh_name=wh_name)

    def ensure_internal_location(
        self,
        *,
        company_id: int,
        parent_location_id: int,
        name: str,
    ) -> int:
        return self.warehouse_seeder.ensure_internal_location(
            company_id=company_id, parent_location_id=parent_location_id, name=name
        )

    def ensure_product(self, *, default_code: str, name: str, categ_id: int, uom_id: int, uom_po_id: int) -> Product:
        return self.product_seeder.ensure_product(
            default_code=default_code, name=name, categ_id=categ_id, uom_id=uom_id, uom_po_id=uom_po_id
        )

    def set_prices(self, *, product_tmpl_id: int, company_id: int, standard_cost: float, list_price: float) -> None:
        return self.product_seeder.set_prices(
            product_tmpl_id=product_tmpl_id, company_id=company_id, standard_cost=standard_cost, list_price=list_price
        )

    def ensure_supplierinfo(
        self,
        *,
        product_tmpl_id: int,
        vendor_id: int,
        company_id: int,
        price: float,
        delay_days: int,
    ) -> int:
        return self.product_seeder.ensure_supplierinfo(
            product_tmpl_id=product_tmpl_id,
            vendor_id=vendor_id,
            company_id=company_id,
            price=price,
            delay_days=delay_days,
        )

    def seed_companies_warehouses_locations(
        self,
        *,
        company_name: str,
        country_code: str,
        geo: list[WarehouseGeo],
    ) -> Company:
        return self.company_seeder.seed_companies_warehouses_locations(
            company_name=company_name, country_code=country_code, geo=geo
        )

    def seed_products_and_vendors(
        self,
        *,
        company: Company,
        min_products: int = 80,
        max_products: int = 120,
    ) -> tuple[list[Product], dict[str, list[int]]]:
        return self.product_seeder.seed_products_and_vendors(
            company=company, min_products=min_products, max_products=max_products
        )

    def load_company_assets(
        self,
        *,
        company_name: str,
        country_code: str,
    ) -> tuple[Company, list[Product], dict[str, list[int]]]:
        if self.dry_run:
            raise RuntimeError("Cannot load existing data in dry-run mode.")

        company_id = self._find_company_id(company_name)
        customer_id = self._find_seed_customer(company_id, company_name)
        warehouses = self._load_warehouses(company_id)
        locations = self._load_locations(company_id, warehouses)
        company = Company(
            company_id=company_id,
            name=company_name,
            country_code=country_code,
            customer_id=customer_id,
            warehouses=warehouses,
            locations=locations,
        )
        products = self._load_seed_products(company_id)
        vendors_by_cat = self._load_vendor_ids_by_category(company_id, products)
        return company, products, vendors_by_cat

    def _find_company_id(self, company_name: str) -> int:
        recs = self.client.search_read(
            "res.company",
            [["name", "=", company_name]],
            fields=["id", "name"],
            limit=1,
        )
        if not recs:
            raise RuntimeError(f"Company not found: {company_name}")
        return int(recs[0]["id"])

    def _find_seed_customer(self, company_id: int, company_name: str) -> int:
        target_name = f"Seed Customer - {company_name}"
        recs = self.client.search_read(
            "res.partner",
            [["name", "=", target_name], ["company_id", "=", company_id]],
            fields=["id", "name"],
            limit=1,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        if recs:
            return int(recs[0]["id"])
        fallback = self.client.search_read(
            "res.partner",
            [["customer_rank", ">", 0], ["company_id", "in", [False, company_id]]],
            fields=["id", "name"],
            limit=1,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        return int(fallback[0]["id"]) if fallback else 0

    def _load_warehouses(self, company_id: int) -> list[Warehouse]:
        recs = self.client.search_read(
            "stock.warehouse",
            [["company_id", "=", company_id]],
            fields=[
                "id",
                "name",
                "code",
                "view_location_id",
                "lot_stock_id",
                "in_type_id",
                "int_type_id",
                "out_type_id",
            ],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        if not recs:
            raise RuntimeError(f"No warehouses found for company_id={company_id}")
        warehouses: list[Warehouse] = []
        for rec in recs:
            warehouses.append(
                Warehouse(
                    warehouse_id=int(rec["id"]),
                    name=str(rec["name"]),
                    code=str(rec["code"]),
                    view_location_id=int(rec["view_location_id"][0]) if rec["view_location_id"] else 0,
                    stock_location_id=int(rec["lot_stock_id"][0]) if rec["lot_stock_id"] else 0,
                    picking_type_in_id=int(rec["in_type_id"][0]) if rec["in_type_id"] else 0,
                    picking_type_internal_id=int(rec["int_type_id"][0]) if rec["int_type_id"] else 0,
                    picking_type_out_id=int(rec["out_type_id"][0]) if rec["out_type_id"] else 0,
                )
            )
        return warehouses

    def _load_locations(self, company_id: int, warehouses: list[Warehouse]) -> dict[str, dict[str, int]]:
        loc_map: dict[str, dict[str, int]] = {}
        kinds = {"GOOD", "TRANSIT", "DAMAGED"}
        for wh in warehouses:
            if not wh.view_location_id:
                continue
            wh_slug = slugify(wh.name)
            loc_map.setdefault(wh.code, {})
            recs = self.client.search_read(
                "stock.location",
                [["location_id", "=", wh.view_location_id], ["usage", "=", "internal"]],
                fields=["id", "name"],
                allowed_company_ids=[company_id],
                company_id=company_id,
            )
            for rec in recs:
                name = str(rec.get("name") or "")
                if not name.startswith(f"{wh_slug}-"):
                    continue
                parts = name.split("-")
                if len(parts) < 3:
                    continue
                kind = parts[1]
                if kind not in kinds:
                    continue
                base_slug = "-".join(parts[2:])
                loc_map[wh.code][f"{kind}::{base_slug}"] = int(rec["id"])
        return loc_map

    def _load_seed_products(self, company_id: int) -> list[Product]:
        prefixes = [f"{slugify(cat)[:5]}-" for cat in PRODUCT_CATEGORIES]
        conds = [["default_code", "ilike", f"{prefix}%"] for prefix in prefixes]
        if not conds:
            raise RuntimeError("No category prefixes available for product lookup.")
        or_domain: list[Any] = ["|"] * (len(conds) - 1) + conds
        domain: list[Any] = ["&", ["active", "=", True]] + or_domain
        recs = self.client.search_read(
            "product.product",
            domain,
            fields=["id", "default_code", "name", "product_tmpl_id", "uom_id", "categ_id"],
            limit=1000,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        products: list[Product] = []
        for rec in recs:
            categ = rec.get("categ_id") or []
            category = str(categ[1]) if categ else ""
            if category not in PRODUCT_CATEGORIES:
                continue
            tmpl = rec.get("product_tmpl_id") or []
            uom = rec.get("uom_id") or []
            products.append(
                Product(
                    product_tmpl_id=int(tmpl[0]) if tmpl else 0,
                    product_id=int(rec["id"]),
                    default_code=str(rec.get("default_code") or ""),
                    name=str(rec.get("name") or ""),
                    category=category,
                    uom_id=int(uom[0]) if uom else 0,
                    uom_name=str(uom[1]) if len(uom) > 1 else "",
                )
            )
        if not products:
            raise RuntimeError("No seeded products found (default_code prefixes not detected).")
        return products

    def _load_vendor_ids_by_category(self, company_id: int, products: list[Product]) -> dict[str, list[int]]:
        vendor_ids_by_category: dict[str, list[int]] = {c: [] for c in PRODUCT_CATEGORIES}
        tmpl_to_cat = {p.product_tmpl_id: p.category for p in products if p.product_tmpl_id}
        tmpl_ids = list(tmpl_to_cat.keys())
        if not tmpl_ids:
            return vendor_ids_by_category
        recs = self.client.search_read(
            "product.supplierinfo",
            [["product_tmpl_id", "in", tmpl_ids], ["company_id", "in", [False, company_id]]],
            fields=["partner_id", "product_tmpl_id"],
            limit=2000,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        for rec in recs:
            tmpl = rec.get("product_tmpl_id") or []
            vendor = rec.get("partner_id") or []
            if not tmpl or not vendor:
                continue
            cat = tmpl_to_cat.get(int(tmpl[0]))
            if not cat:
                continue
            vendor_ids_by_category[cat].append(int(vendor[0]))
        for cat in vendor_ids_by_category:
            vendor_ids_by_category[cat] = sorted(set(vendor_ids_by_category[cat]))
        return vendor_ids_by_category
