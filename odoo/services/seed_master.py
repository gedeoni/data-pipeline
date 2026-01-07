from __future__ import annotations

import hashlib
import logging
import random
from typing import Any, Dict, Iterable, List, Optional, Tuple

from entities import Company, Product, Warehouse
from services.master_data.geo_data import WarehouseGeo, readable, slugify
from services.master_data.product_seeder import PRODUCT_CATEGORIES
from database.odoo_client import IdempotentStore, OdooClient
from services.master_data.company_seeder import CompanySeeder
from services.master_data.partner_seeder import PartnerSeeder
from services.master_data.product_seeder import ProductSeeder
from services.master_data.warehouse_seeder import WarehouseSeeder

_logger = logging.getLogger(__name__)


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

    def apply_cost_drifts(
        self,
        *,
        company_id: int,
        products: list[Product],
        rng: random.Random,
    ) -> None:
        """Introduce controlled cost drift so anomaly marts have signal."""
        if self.dry_run or not products:
            return
        sample_n = max(1, int(len(products) * 0.1))
        sample = rng.sample(products, k=min(sample_n, len(products)))
        tmpl_ids = [p.product_tmpl_id for p in sample if p.product_tmpl_id]
        if not tmpl_ids:
            return
        records = self.client.read(
            "product.template",
            tmpl_ids,
            fields=["id", "standard_price", "list_price"],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        for rec in records:
            base = float(rec.get("standard_price") or 0.0)
            if base <= 0:
                base = float(rec.get("list_price") or 10.0) * 0.6
            factor = rng.uniform(0.5, 1.8)
            new_cost = max(0.01, base * factor)
            vals: dict[str, Any] = {"standard_price": new_cost}
            if rec.get("list_price"):
                vals["list_price"] = max(0.01, float(rec["list_price"]) * rng.uniform(0.8, 1.2))
            self.client.write(
                "product.template",
                [int(rec["id"])],
                vals,
                allowed_company_ids=[company_id],
                company_id=company_id,
            )

    def ensure_product_prices(
        self,
        *,
        company_id: int,
        products: list[Product],
        rng: random.Random,
    ) -> None:
        """Ensure products have a usable standard/list price for analytics."""
        if self.dry_run:
            return
        tmpl_ids = {int(p.product_tmpl_id) for p in products if p.product_tmpl_id}
        # Ensure pricing for every template in the company, not only those in the seed list.
        all_ids = self.client.search(
            "product.template",
            [],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        tmpl_ids.update(all_ids)
        if not tmpl_ids:
            return
        chunk = []
        for tmpl_id in tmpl_ids:
            chunk.append(tmpl_id)
            if len(chunk) < 200:
                continue
            self._set_prices_for_templates(company_id=company_id, tmpl_ids=chunk, rng=rng)
            chunk = []
        if chunk:
            self._set_prices_for_templates(company_id=company_id, tmpl_ids=chunk, rng=rng)

    def _set_prices_for_templates(
        self,
        *,
        company_id: int,
        tmpl_ids: list[int],
        rng: random.Random,
    ) -> None:
        records = self.client.read(
            "product.template",
            list({int(tid) for tid in tmpl_ids}),
            fields=["id", "standard_price", "list_price"],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        for rec in records:
            standard_price = float(rec.get("standard_price") or 0.0)
            list_price = float(rec.get("list_price") or 0.0)
            if standard_price > 0 and list_price > 0:
                continue
            if list_price <= 0:
                list_price = rng.uniform(12.0, 120.0)
            if standard_price <= 0:
                standard_price = max(0.01, list_price * rng.uniform(0.5, 0.85))
            self.client.write(
                "product.template",
                [int(rec["id"])],
                {"standard_price": standard_price, "list_price": list_price},
                allowed_company_ids=[company_id],
                company_id=company_id,
            )

    def ensure_initial_stock(
        self,
        *,
        company: Company,
        products: list[Product],
        rng: random.Random,
        min_qty: float = 40.0,
        max_qty: float = 200.0,
    ) -> None:
        """Seed on-hand quantities in GOOD locations so reservations can succeed."""
        if self.dry_run or not products:
            return
        stock_locations = [int(w.stock_location_id) for w in company.warehouses if w.stock_location_id]
        if not stock_locations:
            _logger.warning("No warehouse stock locations found; skipping initial stock seeding.")
            return

        fields = self.client.call_kw(
            "stock.quant",
            "fields_get",
            args=[[]],
            kwargs={"attributes": ["type"]},
        )
        if "inventory_quantity" in fields:
            inv_field = "inventory_quantity"
        elif "inventory_quantity_set" in fields:
            inv_field = "inventory_quantity_set"
        else:
            _logger.warning("stock.quant inventory field not found; skipping initial stock seeding.")
            return

        for loc_id in stock_locations:
            for prod in products:
                if not prod.product_id:
                    continue
                qty = float(rng.uniform(min_qty, max_qty))
                existing = self.client.search_read(
                    "stock.quant",
                    [["location_id", "=", loc_id], ["product_id", "=", int(prod.product_id)]],
                    fields=["id"],
                    limit=1,
                    allowed_company_ids=[company.company_id],
                    company_id=company.company_id,
                )
                if existing:
                    quant_id = int(existing[0]["id"])
                    self.client.write(
                        "stock.quant",
                        [quant_id],
                        {inv_field: qty},
                        allowed_company_ids=[company.company_id],
                        company_id=company.company_id,
                    )
                else:
                    quant_id = self.client.create(
                        "stock.quant",
                        {
                            "product_id": int(prod.product_id),
                            "location_id": loc_id,
                            "company_id": company.company_id,
                            inv_field: qty,
                        },
                        allowed_company_ids=[company.company_id],
                        company_id=company.company_id,
                    )
                try:
                    self.client.call_kw(
                        "stock.quant",
                        "action_apply_inventory",
                        args=[[quant_id]],
                        allowed_company_ids=[company.company_id],
                        company_id=company.company_id,
                    )
                except Exception as exc:
                    _logger.warning(
                        "Failed to apply inventory for product %s at location %s: %s",
                        prod.product_id,
                        loc_id,
                        exc,
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
