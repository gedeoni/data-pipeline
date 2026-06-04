from __future__ import annotations

from entities import Company, Warehouse
from services.master_data.geo_data import WarehouseGeo, slugify
from services.interfaces.master_data_protocol import MasterDataProtocol


class CompanySeeder:
    """Encapsulates company and country seeding logic."""

    def __init__(self, master_seeder: MasterDataProtocol):
        self.master = master_seeder

    def ensure_country_id(self, country_code: str) -> int:
        key = f"country:{country_code.lower()}"
        cached = self.master.store.get("res.country", key)
        if cached:
            return cached
        if self.master.dry_run:
            cid = self.master._fake_id("res.country", key)
            self.master.store.set("res.country", key, cid)
            return cid
        recs = self.master.client.search_read(
            "res.country",
            [["code", "=", country_code.upper()]],
            fields=["id", "name", "code"],
            limit=1,
        )
        if not recs:
            raise RuntimeError(f"Could not find country for code={country_code}")
        cid = int(recs[0]["id"])
        self.master.store.set("res.country", key, cid)
        return cid

    def ensure_company(self, name: str, *, country_code: str) -> int:
        key = f"company:{name}"
        cached = self.master.store.get("res.company", key)
        if cached:
            return cached
        if self.master.dry_run:
            cid = self.master._fake_id("res.company", key)
            self.master.store.set("res.company", key, cid)
            return cid
        recs = self.master.client.search_read("res.company", [["name", "=", name]], fields=["id", "name"], limit=1)
        if recs:
            cid = int(recs[0]["id"])
            self.master.store.set("res.company", key, cid)
            return cid
        country_id = self.ensure_country_id(country_code)
        cid = self.master.client.create("res.company", {"name": name, "country_id": country_id})
        try:
            self.master.client.write("res.users", [self.master.client.uid], {"company_ids": [(4, cid)]})
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("Could not automatically assign company %s to active user: %s", name, e)
        self.master.store.set("res.company", key, cid)
        return cid

    def seed_companies_warehouses_locations(
        self,
        *,
        company_name: str,
        country_code: str,
        geo: list[WarehouseGeo],
    ) -> Company:
        company_id = self.ensure_company(company_name, country_code=country_code)

        from services.master_data.partner_seeder import PartnerSeeder
        from services.master_data.warehouse_seeder import WarehouseSeeder

        partner_seeder = PartnerSeeder(self.master)
        customer_id = partner_seeder.ensure_partner(
            f"Seed Customer - {company_name}",
            country_code=country_code,
            is_vendor=False,
            company_id=company_id,
        )

        warehouse_seeder = WarehouseSeeder(self.master)
        warehouses: list[Warehouse] = []
        loc_map: dict[str, dict[str, int]] = {}
        for wh in geo:
            wh_ref = warehouse_seeder.ensure_warehouse(
                company_id=company_id,
                company_name=company_name,
                wh_name=wh.warehouse_name,
            )
            warehouses.append(wh_ref)
            loc_map.setdefault(wh_ref.code, {})
            parent = wh_ref.view_location_id
            for base in wh.base_unit_names:
                base_slug = slugify(base)
                wh_slug = wh.warehouse_slug
                good_name = f"{wh_slug}-GOOD-{base_slug}"
                tran_name = f"{wh_slug}-TRANSIT-{base_slug}"
                dmg_name = f"{wh_slug}-DAMAGED-{base_slug}"
                good_id = warehouse_seeder.ensure_internal_location(
                    company_id=company_id,
                    parent_location_id=parent,
                    name=good_name,
                )
                tran_id = warehouse_seeder.ensure_internal_location(
                    company_id=company_id,
                    parent_location_id=parent,
                    name=tran_name,
                )
                dmg_id = warehouse_seeder.ensure_internal_location(
                    company_id=company_id,
                    parent_location_id=parent,
                    name=dmg_name,
                )
                loc_map[wh_ref.code][f"GOOD::{base_slug}"] = good_id
                loc_map[wh_ref.code][f"TRANSIT::{base_slug}"] = tran_id
                loc_map[wh_ref.code][f"DAMAGED::{base_slug}"] = dmg_id

        return Company(
            company_id=company_id,
            name=company_name,
            country_code=country_code,
            customer_id=customer_id,
            warehouses=warehouses,
            locations=loc_map,
        )
