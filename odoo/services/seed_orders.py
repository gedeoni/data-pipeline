from __future__ import annotations

import datetime as dt
import heapq
import logging
import random
from dataclasses import dataclass
from collections import defaultdict
from typing import Callable

from database.odoo_client import OdooClient
from entities import Company, Product
from dto import AnomalyEvent

_logger = logging.getLogger(__name__)


def _date_range(end_date: dt.date, days: int) -> list[dt.date]:
    if days <= 0:
        return []
    start = end_date - dt.timedelta(days=days - 1)
    return [start + dt.timedelta(days=i) for i in range(days)]


def _dt_at(day: dt.date, *, hour: int, minute: int) -> str:
    return dt.datetime(day.year, day.month, day.day, hour, minute, 0).isoformat(sep=" ")


class OrderSeeder:
    def __init__(self, client: OdooClient, dataset_key: str, dry_run: bool = False, out_dir: str = "."):
        self.client = client
        self.dataset_key = dataset_key
        self.dry_run = dry_run
        self.out_dir = out_dir
        self.rng = random.Random()
        self.customers: list[int] = []
        self.pending_actions: list[tuple[dt.date, int, Callable[[dt.date], None]]] = []
        self._pending_seq = 0
        self.anomalies: list[AnomalyEvent] = []
        self._move_line_done_field: str | None = None

    def _get_or_create_customer(self) -> int:
        if self.customers:
            return self.rng.choice(self.customers)

        if not self.dry_run:
            # Try to find existing customers
            domain = [["customer_rank", ">", 0]]
            existing = self.client.search_read("res.partner", domain, ["id"], limit=10)
            if existing:
                self.customers = [e["id"] for e in existing]
                return self.rng.choice(self.customers)

            # Create a generic customer
            cid = self.client.create("res.partner", {"name": "Generic Customer", "customer_rank": 1})
            self.customers = [cid]
            return cid
        return 0

    def _log_ctx(self, company: Company | None = None) -> str:
        if company:
            return f"[orders][{company.country_code}][{company.name}][{self.dataset_key}]"
        return f"[orders][{self.dataset_key}]"

    def _get_move_line_done_field(self) -> str:
        """Return the done qty field for stock.move.line (Odoo 17 uses `quantity`)."""
        if self._move_line_done_field:
            return self._move_line_done_field
        if self.dry_run:
            self._move_line_done_field = "quantity"
            return self._move_line_done_field

        fields = self.client.call_kw(
            "stock.move.line",
            "fields_get",
            args=[[]],
            kwargs={"attributes": ["type"]},
        )
        if "quantity" in fields:
            self._move_line_done_field = "quantity"
        elif "qty_done" in fields:
            self._move_line_done_field = "qty_done"
        else:
            raise RuntimeError("Unsupported Odoo stock.move.line done qty field.")
        return self._move_line_done_field

    def _process_pending_actions(self, current_date: dt.date) -> None:
        while self.pending_actions and self.pending_actions[0][0] <= current_date:
            _, _, action = heapq.heappop(self.pending_actions)
            if not self.dry_run:
                action(current_date)

    def _schedule_action(self, due_date: dt.date, action: Callable[[dt.date], None]) -> None:
        self._pending_seq += 1
        heapq.heappush(self.pending_actions, (due_date, self._pending_seq, action))

    def _generate_anomalies(self, company_name: str, days_list: list[dt.date]) -> None:
        if len(days_list) < 60:
            return

        # 1. Supplier Delay (e.g., Port strike)
        if self.rng.random() < 0.4:
            start_idx = self.rng.randint(10, len(days_list) - 30)
            duration = self.rng.randint(10, 20)
            start_date = days_list[start_idx]
            end_date = days_list[start_idx + duration]
            self.anomalies.append(AnomalyEvent(
                company=company_name,
                kind="SUPPLIER_DELAY",
                date=start_date,
                detail=f"Vendor lead times +15 days until {end_date}",
                end_date=end_date,
            ))

        # 2. Controlled Stockout (e.g., Cash flow issue, stop buying)
        if self.rng.random() < 0.3:
            start_idx = self.rng.randint(10, len(days_list) - 20)
            duration = self.rng.randint(7, 14)
            start_date = days_list[start_idx]
            end_date = days_list[start_idx + duration]
            self.anomalies.append(AnomalyEvent(
                company=company_name,
                kind="STOCKOUT",
                date=start_date,
                detail=f"Purchasing halted until {end_date}",
                end_date=end_date,
            ))

        # 3. Shrinkage Event (e.g., Warehouse leak)
        if self.rng.random() < 0.5:
            date = self.rng.choice(days_list[20:-20])
            self.anomalies.append(AnomalyEvent(
                company=company_name,
                kind="SHRINKAGE",
                date=date,
                detail="Sudden inventory loss (Scrap)",
            ))

    def _load_product_prices(self, company_id: int, products: list[Product]) -> dict[int, dict[str, float]]:
        if self.dry_run:
            return {}
        product_ids = [p.product_id for p in products if p.product_id]
        if not product_ids:
            return {}
        records = self.client.read(
            "product.product",
            product_ids,
            fields=["id", "list_price", "standard_price"],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        price_by_product: dict[int, dict[str, float]] = {}
        for r in records:
            pid = int(r["id"])
            price_by_product[pid] = {
                "list_price": float(r.get("list_price") or 0.0),
                "standard_price": float(r.get("standard_price") or 0.0),
            }
        return price_by_product

    def _price_for_product(self, price_by_product: dict[int, dict[str, float]], product: Product, *, kind: str) -> float:
        base = price_by_product.get(product.product_id, {})
        list_price = float(base.get("list_price") or 0.0)
        standard_price = float(base.get("standard_price") or 0.0)
        if kind == "sale":
            if list_price > 0:
                return list_price
            if standard_price > 0:
                return standard_price * 1.35
            return 10.0
        if standard_price > 0:
            return standard_price
        if list_price > 0:
            return list_price * 0.8
        return 10.0

    def seed_orders(
        self,
        company: Company,
        products: list[Product],
        vendor_ids_by_category: dict,
        days: int,
        scale: str,
        end_date: dt.date | None = None,
    ) -> dict:
        vol_map = {"small": 5, "medium": 20, "large": 100}
        daily_vol = vol_map.get(scale, 20)

        stats = {"po_count": 0, "so_count": 0, "po_lines": 0, "so_lines": 0}
        sku_outbound_counts = defaultdict(int)

        _logger.info("%s Seeding orders for %s", self._log_ctx(company), company.name)

        if not products:
            _logger.warning("%s No products provided; skipping order seeding.", self._log_ctx(company))
            return {
                "pickings_csv": "N/A (Orders Mode)",
                "moves_csv": "N/A (Orders Mode)",
                "picking_counts": {"purchase": 0, "sale": 0},
                "top_outbound_skus": [],
                "lowest_days_of_cover": [],
            }

        if not self.dry_run:
            self._get_or_create_customer()

        end_date = end_date or dt.date.today()
        days_list = _date_range(end_date, days)
        if not days_list:
            _logger.warning("No days requested for order seeding.")
            return {
                "pickings_csv": "N/A (Orders Mode)",
                "moves_csv": "N/A (Orders Mode)",
                "picking_counts": {"purchase": 0, "sale": 0},
                "top_outbound_skus": [],
                "lowest_days_of_cover": [],
            }

        _logger.info("%s Seeding orders from %s to %s...", self._log_ctx(company), days_list[0], days_list[-1])

        # Fetch warehouse details (IDs, Input Picking Types, Stock Locations)
        warehouses = []
        if not self.dry_run:
            wh_ids = [w.warehouse_id for w in company.warehouses]
            if wh_ids:
                warehouses = self.client.read("stock.warehouse", wh_ids, ["id", "name", "in_type_id", "lot_stock_id"])
        else:
            # Mock warehouses for dry-run
            warehouses = [
                {"id": w.warehouse_id, "name": w.code, "in_type_id": [1, "Receipts"], "lot_stock_id": [1, "Stock"]}
                for w in company.warehouses
            ]

        price_by_product = self._load_product_prices(company.company_id, products)
        self._generate_anomalies(company.name, days_list)
        if self.anomalies:
            _logger.info("%s Anomalies: %s", self._log_ctx(company), [a.kind for a in self.anomalies])

        for current_date in days_list:
            # Process pending actions (receipts/deliveries)
            self._process_pending_actions(current_date)

            # Check active anomalies
            is_stockout = False
            delay_add = 0
            for a in self.anomalies:
                if a.company != company.name:
                    continue
                if a.kind == "STOCKOUT" and a.end_date and a.date <= current_date <= a.end_date:
                    is_stockout = True
                elif a.kind == "SUPPLIER_DELAY" and a.end_date and a.date <= current_date <= a.end_date:
                    delay_add = 15
                elif a.kind == "SHRINKAGE" and a.date == current_date:
                    _logger.debug("%s Shrinkage day: %s", self._log_ctx(company), current_date)
                    self._plan_shrinkage(company, warehouses, products, current_date)

            if not self.dry_run:
                # 1. Purchases (Replenishment) - 40% chance per day
                if not is_stockout and self.rng.random() < 0.4:
                    self._plan_purchase(
                        company,
                        warehouses,
                        products,
                        vendor_ids_by_category,
                        current_date,
                        stats,
                        price_by_product,
                        delay_add=delay_add,
                    )
                elif is_stockout:
                    _logger.debug("%s Stockout window active: %s", self._log_ctx(company), current_date)

                # 2. Sales
                num_sales = self.rng.randint(0, int(daily_vol))
                _logger.debug("%s Sales planned: %s on %s", self._log_ctx(company), num_sales, current_date)
                for _ in range(num_sales):
                    self._plan_sale(company, warehouses, products, current_date, stats, sku_outbound_counts, price_by_product)

        # Flush remaining actions to ensure stock moves are completed
        while self.pending_actions:
            due_date, _, action = heapq.heappop(self.pending_actions)
            if not self.dry_run:
                action(max(due_date, end_date))

        _logger.info(
            "%s Completed orders: POs=%s (lines=%s), SOs=%s (lines=%s)",
            self._log_ctx(company),
            stats["po_count"],
            stats["po_lines"],
            stats["so_count"],
            stats["so_lines"],
        )

        return {
            "pickings_csv": "N/A (Orders Mode)",
            "moves_csv": "N/A (Orders Mode)",
            "picking_counts": {"purchase": stats["po_count"], "sale": stats["so_count"]},
            "top_outbound_skus": sorted(sku_outbound_counts.items(), key=lambda x: x[1], reverse=True)[:5],
            "lowest_days_of_cover": [],
        }

    def _plan_purchase(
        self,
        company: Company,
        warehouses: list[dict],
        products: list[Product],
        vendors: dict,
        date: dt.date,
        stats: dict[str, int],
        price_by_product: dict[int, dict[str, float]],
        delay_add: int = 0,
    ) -> None:
        if not vendors or not products or not warehouses:
            return
        cat_id = self.rng.choice(list(vendors.keys()))
        vendor_ids = vendors[cat_id]
        if not vendor_ids:
            return
        vendor_id = self.rng.choice(vendor_ids)

        wh = self.rng.choice(warehouses)

        po_vals = {
            "partner_id": vendor_id,
            "company_id": company.company_id,
            "date_order": date.isoformat(),
            "order_line": [],
        }

        num_lines = self.rng.randint(1, 5)
        subset = self.rng.sample(products, min(len(products), num_lines))
        if not subset:
            return
        for p in subset:
            qty = self.rng.randint(10, 100)
            price = self._price_for_product(price_by_product, p, kind="purchase")
            po_vals["order_line"].append((0, 0, {
                "product_id": p.product_id,
                "product_qty": qty,
                "price_unit": price,
                "date_planned": date.isoformat(),
            }))
            stats["po_lines"] += 1

        # Set picking type to target specific warehouse
        if wh.get("in_type_id"):
            po_vals["picking_type_id"] = wh["in_type_id"][0]

        try:
            po_id = self.client.create("purchase.order", po_vals, allowed_company_ids=[company.company_id], company_id=company.company_id)
            self.client.call_kw("purchase.order", "button_confirm", args=[[po_id]], allowed_company_ids=[company.company_id], company_id=company.company_id)
        except Exception as exc:
            _logger.exception("%s Purchase order creation/confirmation failed: %s", self._log_ctx(company), exc)
            return

        stats["po_count"] += 1

        # Schedule Receipt with random lead time
        lead_time = self.rng.randint(1, 7) + delay_add
        receipt_date = date + dt.timedelta(days=lead_time)

        def receive_action(act_date):
            _logger.debug("%s Receiving PO pickings for %s", self._log_ctx(company), act_date)
            for picking_id in self._order_pickings("purchase.order", po_id, company.company_id):
                self._validate_picking(company, picking_id, act_date)

        self._schedule_action(receipt_date, receive_action)
        _logger.debug("%s Scheduled PO receipt for %s", self._log_ctx(company), receipt_date)

    def _plan_sale(
        self,
        company: Company,
        warehouses: list[dict],
        products: list[Product],
        date: dt.date,
        stats: dict[str, int],
        sku_counts: dict,
        price_by_product: dict[int, dict[str, float]],
    ) -> None:
        if not products or not warehouses:
            return
        customer_id = self._get_or_create_customer()
        wh = self.rng.choice(warehouses)

        so_vals = {
            "partner_id": customer_id,
            "company_id": company.company_id,
            "date_order": date.isoformat(),
            "order_line": [],
        }

        num_lines = self.rng.randint(1, 3)
        subset = self.rng.sample(products, min(len(products), num_lines))
        if not subset:
            return
        for p in subset:
            qty = self.rng.randint(1, 10)
            so_vals["order_line"].append((0, 0, {
                "product_id": p.product_id,
                "product_uom_qty": qty,
                "price_unit": self._price_for_product(price_by_product, p, kind="sale"),
            }))
            stats["so_lines"] += 1
            sku_counts[p.default_code or p.product_id] += qty

        # Set warehouse for the sales order
        so_vals["warehouse_id"] = wh["id"]

        try:
            so_id = self.client.create("sale.order", so_vals, allowed_company_ids=[company.company_id], company_id=company.company_id)
            self.client.call_kw("sale.order", "action_confirm", args=[[so_id]], allowed_company_ids=[company.company_id], company_id=company.company_id)
        except Exception as exc:
            _logger.exception("%s Sales order creation/confirmation failed: %s", self._log_ctx(company), exc)
            return

        stats["so_count"] += 1

        def deliver_action(act_date):
            _logger.debug("%s Delivering SO pickings for %s", self._log_ctx(company), act_date)
            for picking_id in self._order_pickings("sale.order", so_id, company.company_id):
                self._validate_picking(company, picking_id, act_date)

        scheduled = date + dt.timedelta(days=self.rng.randint(0, 3))
        self._schedule_action(scheduled, deliver_action)
        _logger.debug("%s Scheduled SO delivery for %s", self._log_ctx(company), scheduled)

    def _plan_shrinkage(self, company: Company, warehouses: list[dict], products: list[Product], date: dt.date) -> None:
        if not products or not warehouses:
            return

        wh = self.rng.choice(warehouses)
        stock_loc_id = wh.get("lot_stock_id")
        if not stock_loc_id:
            return

        p = self.rng.choice(products)
        qty = self.rng.randint(5, 20)

        scrap_vals = {
            "product_id": p.product_id,
            "scrap_qty": qty,
            "location_id": stock_loc_id[0],
            "company_id": company.company_id,
            # Odoo 17 scrap doesn't always take a date field on create, but we backdate after
        }
        try:
            scrap_id = self.client.create("stock.scrap", scrap_vals, allowed_company_ids=[company.company_id], company_id=company.company_id)
            self.client.call_kw("stock.scrap", "action_validate", args=[[scrap_id]], allowed_company_ids=[company.company_id], company_id=company.company_id)
            # Attempt to backdate the scrap record and its move
            self.client.write("stock.scrap", [scrap_id], {"date_done": date.isoformat()}, allowed_company_ids=[company.company_id], company_id=company.company_id)
        except Exception as exc:
            _logger.exception("%s Shrinkage (Scrap) failed: %s", self._log_ctx(company), exc)

    def _order_pickings(self, model: str, order_id: int, company_id: int) -> list[int]:
        if self.dry_run:
            return []
        records = self.client.read(
            model,
            [order_id],
            fields=["picking_ids"],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        if not records:
            return []
        picking_ids = records[0].get("picking_ids") or []
        return [int(pid) for pid in picking_ids]

    def _ensure_move_lines_done(self, company_id: int, picking_id: int) -> None:
        if self.dry_run:
            return
        done_field = self._get_move_line_done_field()
        moves = self.client.search_read(
            "stock.move",
            [["picking_id", "=", picking_id]],
            fields=["id", "product_id", "product_uom", "product_uom_qty", "location_id", "location_dest_id"],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        for mv in moves:
            qty_done = float(mv.get("product_uom_qty") or 0.0)
            if qty_done <= 0:
                continue
            move_id = int(mv["id"])
            existing = self.client.search_read(
                "stock.move.line",
                [["move_id", "=", move_id]],
                fields=["id", done_field],
                limit=1,
                allowed_company_ids=[company_id],
                company_id=company_id,
            )
            if existing:
                self.client.write(
                    "stock.move.line",
                    [int(existing[0]["id"])],
                    {done_field: qty_done},
                    allowed_company_ids=[company_id],
                    company_id=company_id,
                )
            else:
                self.client.create(
                    "stock.move.line",
                    {
                        "picking_id": picking_id,
                        "move_id": move_id,
                        "product_id": int(mv["product_id"][0]),
                        "product_uom_id": int(mv["product_uom"][0]),
                        done_field: qty_done,
                        "location_id": int(mv["location_id"][0]),
                        "location_dest_id": int(mv["location_dest_id"][0]),
                        "company_id": company_id,
                    },
                    allowed_company_ids=[company_id],
                    company_id=company_id,
                )

    def _validate_picking(self, company: Company, picking_id: int, date: dt.date) -> None:
        company_id = company.company_id
        try:
            self.client.call_kw(
                "stock.picking",
                "action_confirm",
                args=[[picking_id]],
                allowed_company_ids=[company_id],
                company_id=company_id,
            )
            self.client.call_kw(
                "stock.picking",
                "action_assign",
                args=[[picking_id]],
                allowed_company_ids=[company_id],
                company_id=company_id,
            )
            self._ensure_move_lines_done(company_id, picking_id)
            res = self.client.call_kw(
                "stock.picking",
                "button_validate",
                args=[[picking_id]],
                kwargs={},
                context={"force_period_date": date.isoformat()},
                allowed_company_ids=[company_id],
                company_id=company_id,
            )
            if isinstance(res, dict) and res.get("res_model") and res.get("res_id"):
                model = str(res["res_model"])
                rid = int(res["res_id"])
                if model == "stock.immediate.transfer":
                    self.client.call_kw(
                        model,
                        "process",
                        args=[[rid]],
                        allowed_company_ids=[company_id],
                        company_id=company_id,
                    )
                elif model == "stock.backorder.confirmation":
                    self.client.call_kw(
                        model,
                        "process_cancel_backorder",
                        args=[[rid]],
                        allowed_company_ids=[company_id],
                        company_id=company_id,
                    )

            done_dt = _dt_at(date, hour=16, minute=30)
            try:
                self.client.write(
                    "stock.picking",
                    [picking_id],
                    {"date_done": done_dt},
                    allowed_company_ids=[company_id],
                    company_id=company_id,
                )
                line_ids = self.client.search(
                    "stock.move.line",
                    [["picking_id", "=", picking_id]],
                    allowed_company_ids=[company_id],
                    company_id=company_id,
                )
                if line_ids:
                    self.client.write(
                        "stock.move.line",
                        line_ids,
                        {"date": done_dt},
                        allowed_company_ids=[company_id],
                        company_id=company_id,
                    )
            except Exception:
                # Backdating is best-effort; not all configs allow it.
                pass
        except Exception as exc:
            _logger.exception("%s Picking validation failed %s: %s", self._log_ctx(company), picking_id, exc)
