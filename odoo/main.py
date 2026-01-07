from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import logging
import random
from typing import Any
from collections import defaultdict

from dotenv import load_dotenv

from services.master_data.geo_data import geo_plan
from database.odoo_client import OdooClient, OdooConfig
from services.seed_master import MasterSeeder
from services.seed_movements import MovementSeeder
from services.seed_orders import OrderSeeder


COUNTRY_COMPANY = {
    "rw": "Rwanda",
    "ug": "Uganda",
    "ke": "Kenya",
}


def _stable_int_seed(value: str) -> int:
    import hashlib
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed realistic inventory + 180 days stock movements into Odoo 17")
    p.add_argument("--base-url", default=os.getenv("ODOO_URL", "http://localhost:8069"))
    p.add_argument("--db", default=os.getenv("ODOO_DB", "odoo"))
    p.add_argument("--user", default=os.getenv("ODOO_USER", "odoo@gmail.com"))
    p.add_argument("--password", default=os.getenv("ODOO_PASSWORD", "odoo"))

    p.add_argument("--days", type=int, default=180)
    p.add_argument("--scale", choices=["small", "medium", "large"], default="medium")
    p.add_argument("--countries", default="rw,ug,ke", help="Comma-separated: rw,ug,ke")
    p.add_argument("--full-geo", action="store_true", help="Create more base-unit locations per warehouse")
    p.add_argument("--dry-run", action="store_true", help="Generate CSV + logs without calling Odoo")
    p.add_argument("--orders", action="store_true", help="Generate Purchase/Sales orders instead of direct stock moves")
    p.add_argument("--no-master-data", action="store_true", help="Skip master data creation and reuse existing records")
    partition_group = p.add_mutually_exclusive_group()
    partition_group.add_argument("--orders-only", action="store_true", help="Only seed Purchase/Sales orders (no partitioning)")
    partition_group.add_argument("--movements-only", action="store_true", help="Only seed direct stock movements (no partitioning)")
    p.add_argument("--out-dir", default=os.path.join(os.getcwd(), "seed_output"))
    return p.parse_args(argv)


def check_modules(client: OdooClient, require_orders: bool = False) -> None:
    required = ["stock"]
    if require_orders:
        required.extend(["purchase", "sale", "sale_stock", "purchase_stock"])
    mods = client.search_read(
        "ir.module.module",
        [["name", "in", required]],
        fields=["name", "state"],
        limit=50,
    )
    by_name = {m["name"]: m["state"] for m in mods}
    missing = [n for n in required if by_name.get(n) not in ("installed", "to upgrade")]
    if missing:
        raise SystemExit(f"Missing required Odoo modules: {missing}. Install Inventory (stock) in the UI first.")


def _run_orders_mode(
    args: argparse.Namespace,
    company: Any,
    products: list[Any],
    vendors_by_cat: dict,
    end_date: dt.date,
    order_seeder: OrderSeeder,
    mover: MovementSeeder,
) -> dict:
    if args.days < 100:
        return order_seeder.seed_orders(
            company=company,
            products=products,
            vendor_ids_by_category=vendors_by_cat,
            days=args.days,
            scale=args.scale,
        )

    orders_days = args.days // 2
    moves_days = args.days - orders_days
    orders_end = end_date
    moves_end = end_date - dt.timedelta(days=orders_days)

    orders_summary = order_seeder.seed_orders(
        company=company,
        products=products,
        vendor_ids_by_category=vendors_by_cat,
        days=orders_days,
        scale=args.scale,
        end_date=orders_end,
    )
    moves_summary = mover.seed_movements(
        company=company,
        products=products,
        vendor_ids_by_category=vendors_by_cat,
        days=moves_days,
        scale=args.scale,
        end_date=moves_end,
    )

    combined_counts = dict(moves_summary.get("picking_counts", {}))
    for k, v in orders_summary.get("picking_counts", {}).items():
        combined_counts[k] = combined_counts.get(k, 0) + v

    combined_outbound = defaultdict(int)
    for sku, qty in moves_summary.get("top_outbound_skus", []):
        combined_outbound[sku] += qty
    for sku, qty in orders_summary.get("top_outbound_skus", []):
        combined_outbound[sku] += qty

    return {
        "pickings_csv": moves_summary.get("pickings_csv", "N/A"),
        "moves_csv": moves_summary.get("moves_csv", "N/A"),
        "picking_counts": combined_counts,
        "top_outbound_skus": sorted(combined_outbound.items(), key=lambda x: x[1], reverse=True)[:10],
        "lowest_days_of_cover": moves_summary.get("lowest_days_of_cover", []),
    }


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv()
    args = parse_args(argv)
    countries = [c.strip().lower() for c in args.countries.split(",") if c.strip()]
    # convert this into a validation private function for countries
    for c in countries:
        if c not in COUNTRY_COMPANY:
            raise SystemExit(f"Unsupported country code: {c} (expected rw, ug, ke)")

    end_date = dt.date.today()
    dataset_key = f"{end_date.isoformat()}_{args.days}d"

    client = OdooClient(
        OdooConfig(
            base_url=args.base_url,
            db=args.db,
            login=args.user,
            password=args.password,
        )
    )
    if not args.dry_run:
        client.authenticate()
        check_modules(client, require_orders=args.orders)
    elif args.no_master_data:
        raise SystemExit("--no-master-data requires live Odoo access (disable --dry-run).")

    master = MasterSeeder(client, dataset_key=dataset_key, dry_run=args.dry_run)
    mover = MovementSeeder(client, dataset_key=dataset_key, dry_run=args.dry_run, out_dir=args.out_dir)
    order_seeder = OrderSeeder(client, dataset_key=dataset_key, dry_run=args.dry_run, out_dir=args.out_dir)

    print(f"Dataset key: {dataset_key}")
    print(f"Dry-run: {args.dry_run}")
    print(f"Output dir: {args.out_dir}")

    summaries: list[tuple[str, dict]] = []
    for country_code in countries:
        company_name = COUNTRY_COMPANY[country_code]
        geo = geo_plan(country_code, scale=args.scale, full_geo=args.full_geo)
        if args.no_master_data:
            company, products, vendors_by_cat = master.load_company_assets(
                company_name=company_name,
                country_code=country_code,
            )
        else:
            company = master.seed_companies_warehouses_locations(company_name=company_name, country_code=country_code, geo=geo)
            products, vendors_by_cat = master.seed_products_and_vendors(company=company)

        price_rng = random.Random(_stable_int_seed(f"{dataset_key}:{company_name}:prices"))
        master.ensure_product_prices(company_id=company.company_id, products=products, rng=price_rng)

        stock_rng = random.Random(_stable_int_seed(f"{dataset_key}:{company_name}:stock"))
        master.ensure_initial_stock(company=company, products=products, rng=stock_rng)

        drift_rng = random.Random(_stable_int_seed(f"{dataset_key}:{company_name}:cost_drift"))
        master.apply_cost_drifts(company_id=company.company_id, products=products, rng=drift_rng)

        if args.movements_only:
            summary = mover.seed_movements(
                company=company,
                products=products,
                vendor_ids_by_category=vendors_by_cat,
                days=args.days,
                scale=args.scale,
            )
        elif args.orders_only:
            summary = order_seeder.seed_orders(
                company=company,
                products=products,
                vendor_ids_by_category=vendors_by_cat,
                days=args.days,
                scale=args.scale,
            )
        else:
            # Default to orders mode to ensure PO-based analytics have signal.
            summary = _run_orders_mode(
                args, company, products, vendors_by_cat, end_date, order_seeder, mover
            )
        summaries.append((company_name, summary))

    print("\nSummary")
    for company_name, s in summaries:
        print(f"- {company_name}:")
        print(f"  - CSV pickings: {s['pickings_csv']}")
        print(f"  - CSV moves: {s['moves_csv']}")
        print(f"  - Picking counts: {s['picking_counts']}")
        print(f"  - Top outbound SKUs: {s['top_outbound_skus']}")
        print("  - Lowest days-of-cover (approx):")
        for doc, sku, stock, rate in s["lowest_days_of_cover"]:
            print(f"    - {sku}: {doc:.1f} days (stock={stock:.1f}, avg_out/day={rate:.2f})")

    all_anomalies = mover.anomalies + order_seeder.anomalies
    if all_anomalies:
        print("\nAnomalies injected")
        for a in all_anomalies:
            print(f"- {a.company} {a.kind} {a.date.isoformat()}: {a.detail}")

    print("\nOdoo modules")
    print("- Required: Inventory (stock)")
    print("- Optional: Purchase (purchase), Sales (sale)\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
