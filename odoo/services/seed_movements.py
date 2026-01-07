from __future__ import annotations

import datetime as dt
import hashlib
import os
import random
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

from entities import Company, Product, Warehouse
from entities import StockMove, StockPicking
from dto import WarehouseProfile, AnomalyEvent
from services.master_data.geo_data import slugify
from database.odoo_client import OdooClient
from services.stock_movement.inbound_seeder import InboundSeeder
from services.stock_movement.internal_seeder import InternalSeeder
from services.stock_movement.damage_seeder import DamageSeeder
from services.stock_movement.outbound_seeder import OutboundSeeder
from services.stock_movement.reporting import Reporting



def _stable_int_seed(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _date_range(end_date: dt.date, days: int) -> list[dt.date]:
    start = end_date - dt.timedelta(days=days - 1)
    return [start + dt.timedelta(days=i) for i in range(days)]


def _dt_at(day: dt.date, *, hour: int, minute: int) -> str:
    return dt.datetime(day.year, day.month, day.day, hour, minute, 0).isoformat(sep=" ")


class StockLedger:
    """Simple in-memory ledger for availability decisions + summaries.

    Ledger is per location+product. This is separate from Odoo, but we keep it consistent
    across idempotent runs by reading existing pickings when we detect them.
    """

    def __init__(self):
        self.qty: dict[tuple[int, int], float] = defaultdict(float)

    def add(self, location_id: int, product_id: int, delta: float) -> None:
        self.qty[(location_id, product_id)] += float(delta)

    def get(self, location_id: int, product_id: int) -> float:
        return float(self.qty.get((location_id, product_id), 0.0))


@dataclass
class SimulationContext:
    company: Company
    days_list: list[dt.date]
    rng: random.Random
    wh_meta: dict[str, WarehouseProfile]
    vendor_ids_by_category: dict[str, list[int]]

    # Anomalies
    stockout_window: set[dt.date] = field(default_factory=set)
    stockout_products: list[Product] = field(default_factory=list)
    shrink_window: set[dt.date] = field(default_factory=set)
    shrink_wh_code: str | None = None
    spike_days: set[dt.date] = field(default_factory=set)

    # Accumulators
    picking_rows: list[StockPicking] = field(default_factory=list)
    move_rows: list[StockMove] = field(default_factory=list)
    picking_counts: Counter = field(default_factory=Counter)
    outbound_qty_by_sku: Counter = field(default_factory=Counter)
    seq_counter: dict[tuple[str, str, dt.date], int] = field(default_factory=lambda: defaultdict(int))

    # Dormant SKUs to guarantee liquidation candidates.
    dormant_products: set[int] = field(default_factory=set)
    dormant_start: dt.date | None = None


class MovementSeeder:
    def __init__(
        self,
        client: OdooClient,
        *,
        dataset_key: str,
        dry_run: bool,
        out_dir: str,
    ):
        self.client = client
        self.dataset_key = dataset_key
        self.dry_run = dry_run
        self.out_dir = out_dir
        self.ledger = StockLedger()
        self.anomalies: list[AnomalyEvent] = []

        self._location_supplier_id: int | None = None
        self._location_customer_id: int | None = None
        self._move_line_done_field: str | None = None
        self._stock_move_fields: set[str] | None = None

        self.MIN_ACTIVE_PRODUCTS = 12

        # Service objects for seeded flows and reporting.
        self.inbound = InboundSeeder(self)
        self.internal = InternalSeeder(self)
        self.damage = DamageSeeder(self)
        self.outbound = OutboundSeeder(self)
        self.reporting = Reporting(self)

    def _stock_move_has_field(self, field_name: str) -> bool:
        if self._stock_move_fields is None:
            if self.dry_run:
                self._stock_move_fields = set()
            else:
                fields = self.client.call_kw(
                    "stock.move",
                    "fields_get",
                    args=[[]],
                    kwargs={"attributes": ["type"]},
                )
                self._stock_move_fields = set(fields.keys())
        return field_name in self._stock_move_fields

    def _get_move_line_done_field(self) -> str:
        """Odoo field name for done qty on stock.move.line.

        Odoo versions/customizations vary: common names are `qty_done` (older) and `quantity` (newer).
        """
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
        if "qty_done" in fields:
            self._move_line_done_field = "qty_done"
        elif "quantity" in fields:
            self._move_line_done_field = "quantity"
        else:
            raise RuntimeError(
                "Unsupported Odoo stock.move.line done qty field; expected `qty_done` or `quantity`."
            )
        return self._move_line_done_field

    def _ensure_base_locations(self) -> tuple[int, int]:
        if self._location_supplier_id and self._location_customer_id:
            return self._location_supplier_id, self._location_customer_id

        if self.dry_run:
            self._location_supplier_id = 900000001
            self._location_customer_id = 900000002
            return self._location_supplier_id, self._location_customer_id

        supplier = self.client.search_read(
            "stock.location",
            # Ensure we get the standard virtual location (usually has no company_id)
            [["usage", "=", "supplier"], ["company_id", "=", False]],
            fields=["id", "name", "usage"],
            limit=1,
        )
        customer = self.client.search_read(
            "stock.location",
            [["usage", "=", "customer"], ["company_id", "=", False]],
            fields=["id", "name", "usage"],
            limit=1,
        )
        if not supplier or not customer:
            raise RuntimeError("Could not find default supplier/customer stock locations")
        self._location_supplier_id = int(supplier[0]["id"])
        self._location_customer_id = int(customer[0]["id"])
        return self._location_supplier_id, self._location_customer_id

    def _existing_picking_by_origin(self, *, company_id: int, origin: str) -> dict[str, Any] | None:
        recs = self.client.search_read(
            "stock.picking",
            [["origin", "=", origin], ["company_id", "=", company_id]],
            fields=["id", "name", "state", "origin", "move_line_ids"],
            limit=1,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        if not recs:
            return None
        return recs[0]

    def _apply_picking_to_ledger(self, *, company_id: int, picking_id: int) -> None:
        # Move lines have actual done quantities.
        done_field = self._get_move_line_done_field()
        lines = self.client.search_read(
            "stock.move.line",
            [["picking_id", "=", picking_id]],
            fields=["product_id", done_field, "location_id", "location_dest_id"],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        for l in lines:
            product_id = int(l["product_id"][0])
            qty_done = float(l.get(done_field) or 0.0)
            src = int(l["location_id"][0])
            dst = int(l["location_dest_id"][0])
            self.ledger.add(src, product_id, -qty_done)
            self.ledger.add(dst, product_id, qty_done)

    def _create_picking(
        self,
        *,
        company_id: int,
        picking_type_id: int,
        partner_id: int | None,
        location_id: int,
        location_dest_id: int,
        scheduled_dt: str,
        origin: str,
    ) -> int:
        if self.dry_run:
            return 0
        return self.client.create(
            "stock.picking",
            {
                "picking_type_id": picking_type_id,
                "partner_id": partner_id,
                "location_id": location_id,
                "location_dest_id": location_dest_id,
                "scheduled_date": scheduled_dt,
                "origin": origin,
                "company_id": company_id,
            },
            allowed_company_ids=[company_id],
            company_id=company_id,
        )

    def _create_move(
        self,
        *,
        company_id: int,
        picking_id: int,
        picking_type_id: int | None,
        warehouse_id: int | None,
        product_id: int,
        name: str,
        uom_id: int,
        qty: float,
        src: int,
        dst: int,
    ) -> int:
        if self.dry_run:
            return 0
        vals: dict[str, Any] = {
            "name": name,
            "picking_id": picking_id,
            "product_id": product_id,
            "product_uom": uom_id,
            "product_uom_qty": float(qty),
            "location_id": src,
            "location_dest_id": dst,
            "company_id": company_id,
        }
        # Some Odoo builds expose warehouse attribution on moves; set it when available.
        if picking_type_id and self._stock_move_has_field("picking_type_id"):
            vals["picking_type_id"] = int(picking_type_id)
        if warehouse_id and self._stock_move_has_field("warehouse_id"):
            vals["warehouse_id"] = int(warehouse_id)
        return self.client.create(
            "stock.move",
            vals,
            allowed_company_ids=[company_id],
            company_id=company_id,
        )

    def _ensure_move_lines_done(
        self,
        *,
        company_id: int,
        picking_id: int,
        quantities_done_by_product: dict[int, float],
    ) -> None:
        if self.dry_run:
            return
        done_field = self._get_move_line_done_field()
        # Create explicit move lines with qty_done.
        moves = self.client.search_read(
            "stock.move",
            [["picking_id", "=", picking_id]],
            fields=["id", "product_id", "product_uom", "location_id", "location_dest_id"],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )
        for mv in moves:
            product_id = int(mv["product_id"][0])
            qty_done = float(quantities_done_by_product.get(product_id, 0.0))
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
                        "product_id": product_id,
                        "product_uom_id": int(mv["product_uom"][0]),
                        done_field: qty_done,
                        "location_id": int(mv["location_id"][0]),
                        "location_dest_id": int(mv["location_dest_id"][0]),
                        "company_id": company_id,
                    },
                    allowed_company_ids=[company_id],
                    company_id=company_id,
                )

    def _validate_picking(
        self,
        *,
        company_id: int,
        picking_id: int,
        done_day: dt.date,
        quantities_done_by_product: dict[int, float] | None = None,
    ) -> None:
        if self.dry_run:
            return

        # 1. Confirm stock picking
        self.client.call_kw(
            "stock.picking",
            "action_confirm",
            args=[[picking_id]],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )

        # 2. Assign stock picking
        self.client.call_kw(
            "stock.picking",
            "action_assign",
            args=[[picking_id]],
            allowed_company_ids=[company_id],
            company_id=company_id,
        )

        # -  Ensure associated stock move lines are present
        if quantities_done_by_product:
            self._ensure_move_lines_done(
                company_id=company_id,
                picking_id=picking_id,
                quantities_done_by_product=quantities_done_by_product,
            )

        # 3. Validate stock picking, automatically validating stock moves + move lines.
        res = self.client.call_kw(
            "stock.picking",
            "button_validate",
            args=[[picking_id]],
            kwargs={},
            context={"force_period_date": done_day.isoformat()},
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
                # Keep dataset tidy: cancel backorder lines (stockout stays visible via partial done qty).
                self.client.call_kw(
                    model,
                    "process_cancel_backorder",
                    args=[[rid]],
                    allowed_company_ids=[company_id],
                    company_id=company_id,
                )

        # Best-effort backdate date_done + move line date.
        done_dt = _dt_at(done_day, hour=16, minute=30)
        try:
            self.client.write(
                "stock.picking",
                [picking_id],
                {"date_done": done_dt},
                allowed_company_ids=[company_id],
                company_id=company_id,
            )
            line_ids = self.client.search("stock.move.line", [["picking_id", "=", picking_id]], allowed_company_ids=[company_id], company_id=company_id)
            if line_ids:
                self.client.write(
                    "stock.move.line",
                    line_ids,
                    {"date": done_dt},
                    allowed_company_ids=[company_id],
                    company_id=company_id,
                )
        except Exception:
            # Not all configurations allow writing date_done; keep going.
            pass

    def _origin(self, *, company_code: str, warehouse_code: str, kind: str, day: dt.date, seq: int) -> str:
        return f"SEED17/{self.dataset_key}/{company_code.upper()}/{warehouse_code}/{kind}/{day.isoformat()}/{seq:04d}"

    def _warehouse_size(self, *, scale: str, rng: random.Random) -> tuple[str, float]:
        scale = scale.lower()
        if scale == "small":
            choices = [("small", 0.9), ("medium", 0.1)]
        elif scale == "medium":
            choices = [("small", 0.35), ("medium", 0.5), ("large", 0.15)]
        elif scale == "large":
            choices = [("medium", 0.45), ("large", 0.55)]
        else:
            raise ValueError("scale must be small|medium|large")
        sizes, probs = zip(*choices)
        s = rng.choices(sizes, weights=probs, k=1)[0]
        weight = {"small": 0.7, "medium": 1.0, "large": 1.6}[s]
        return str(s), float(weight)

    def _generate_warehouse_profiles(
        self,
        *,
        company: Company,
        products: list[Product],
        scale: str,
        rng: random.Random,
    ) -> dict[str, WarehouseProfile]:
        wh_meta: dict[str, WarehouseProfile] = {}
        for wh in company.warehouses:
            size_label, size_weight = self._warehouse_size(scale=scale, rng=rng)
            active_share = {"small": 0.35, "medium": 0.55, "large": 0.75}[size_label]
            active_n = max(self.MIN_ACTIVE_PRODUCTS, int(len(products) * active_share))
            active_products = rng.sample(products, k=min(active_n, len(products)))
            wh_meta[wh.code] = WarehouseProfile(
                size=size_label,
                weight=size_weight,
                active_products=active_products,
            )
        return wh_meta

    # sales quantity per product will be multiplied by 2.5x on spike days
    def _generate_demand_spikes(self, ctx: SimulationContext) -> None:
        ctx.spike_days = set(ctx.rng.sample(ctx.days_list, k=min(len(ctx.days_list), ctx.rng.randint(1, 3))))
        if ctx.spike_days:
            for d in sorted(ctx.spike_days):
                evt = AnomalyEvent(
                    kind="demand_spike",
                    company=ctx.company.name,
                    detail=f"Demand spike multiplier 2.5x on {d.isoformat()}",
                    date=d,
                )
                self.anomalies.append(evt)
                print(f"[anomaly] {evt.company} {evt.kind} {evt.date.isoformat()}: {evt.detail}")

    # the damage rate for the shrink warehouse on shrinkage days will be multiplied by 6x
    def _generate_shrinkage_event(self, ctx: SimulationContext) -> None:
        shrink_wh = ctx.rng.choice(ctx.company.warehouses)
        ctx.shrink_wh_code = shrink_wh.code
        shrink_start = ctx.rng.choice(ctx.days_list)
        shrink_len = ctx.rng.randint(3, 5)
        ctx.shrink_window = {
            shrink_start + dt.timedelta(days=i)
            for i in range(shrink_len)
            if shrink_start + dt.timedelta(days=i) in ctx.days_list
        }
        if ctx.shrink_window:
            evt = AnomalyEvent(
                kind="shrinkage_event",
                company=ctx.company.name,
                detail=f"Shrinkage event at {shrink_wh.code} for {len(ctx.shrink_window)} days starting {shrink_start.isoformat()}",
                date=shrink_start,
            )
            self.anomalies.append(evt)
            print(f"[anomaly] {evt.company} {evt.kind} {evt.date.isoformat()}: {evt.detail}")

    # demand for the stockout products is multiplied by 2.8x while supply is reduced by 35% on stockout days
    def _generate_controlled_stockouts(self, ctx: SimulationContext, products: list[Product]) -> None:
        ctx.stockout_products = ctx.rng.sample(products, k=min(4, len(products)))
        stockout_start = ctx.rng.choice(ctx.days_list)
        ctx.stockout_window = {
            stockout_start + dt.timedelta(days=i)
            for i in range(10)
            if stockout_start + dt.timedelta(days=i) in ctx.days_list
        }
        if ctx.stockout_window:
            codes = ",".join(p.default_code for p in ctx.stockout_products)
            evt = AnomalyEvent(
                kind="controlled_stockout",
                company=ctx.company.name,
                detail=f"Elevated outbound for SKUs [{codes}] for {len(ctx.stockout_window)} days from {stockout_start.isoformat()}",
                date=stockout_start,
            )
            self.anomalies.append(evt)
            print(f"[anomaly] {evt.company} {evt.kind} {evt.date.isoformat()}: {evt.detail}")

    def _pick_base_unit_location(self, ctx: SimulationContext, wh_code: str, kind: str) -> int:
        locs = ctx.company.locations.get(wh_code, {})
        keys = [k for k in locs.keys() if k.startswith(f"{kind}::")]
        if not keys:
            raise RuntimeError(f"No {kind} locations for warehouse {wh_code}")
        return int(locs[ctx.rng.choice(keys)])

    def _available_locations_for_product(self, ctx: SimulationContext, wh_code: str, product_id: int) -> list[int]:
        locs = ctx.company.locations.get(wh_code, {})
        good_locs = [loc_id for k, loc_id in locs.items() if k.startswith("GOOD::")]
        ctx.rng.shuffle(good_locs)
        return [lid for lid in good_locs if self.ledger.get(int(lid), product_id) > 0.01]

    def _eligible_products(
        self,
        ctx: SimulationContext,
        active_products: list[Product],
        day: dt.date,
    ) -> list[Product]:
        """Exclude dormant SKUs after a cutoff to create liquidation candidates."""
        if not ctx.dormant_start or day < ctx.dormant_start:
            return active_products
        return [
            p for p in active_products
            if int(p.product_id) not in ctx.dormant_products
        ]

    def _create_and_validate_picking(
        self,
        ctx: SimulationContext,
        *,
        wh: Warehouse,
        kind: str,
        day: dt.date,
        picking_type_id: int,
        partner_id: int | None,
        src_loc: int,
        dst_loc: int,
        lines: list[tuple[Product, float]],
        note: str = "",
    ) -> bool:
        ctx.seq_counter[(wh.code, kind, day)] += 1
        origin = self._origin(
            company_code=ctx.company.country_code,
            warehouse_code=wh.code,
            kind=kind,
            day=day,
            seq=ctx.seq_counter[(wh.code, kind, day)],
        )

        # Check for existing picking to ensure idempotency.
        existing = None if self.dry_run else self._existing_picking_by_origin(company_id=ctx.company.company_id, origin=origin)
        if existing:
            self._apply_picking_to_ledger(company_id=ctx.company.company_id, picking_id=int(existing["id"]))
            ctx.picking_counts[f"{kind}:existing"] += 1
            return True

        # Determine qty_done (partial delivery to simulate stockout/backorder).
        qty_done_by_product: dict[int, float] = {}
        for prod, qty_req in lines:
            qty_req = float(qty_req)
            if kind in ("IN", "INT", "DMG"):
                qty_done = qty_req
            else:
                avail = self.ledger.get(src_loc, int(prod.product_id))
                qty_done = min(qty_req, max(0.0, avail))
            qty_done_by_product[int(prod.product_id)] = qty_done

        if sum(qty_done_by_product.values()) <= 0.0:
            ctx.picking_counts[f"{kind}:skipped_no_qty"] += 1
            return False

        scheduled_dt = _dt_at(day, hour=int(ctx.rng.randint(8, 15)), minute=int(ctx.rng.choice([0, 15, 30, 45])))

        picking_id = self._create_picking(
            company_id=ctx.company.company_id,
            picking_type_id=picking_type_id,
            partner_id=partner_id,
            location_id=src_loc,
            location_dest_id=dst_loc,
            scheduled_dt=scheduled_dt,
            origin=origin,
        )

        for prod, qty_req in lines:
            qty_req = float(qty_req)
            qty_done = float(qty_done_by_product.get(int(prod.product_id), 0.0))

            if not self.dry_run:
                self._create_move(
                    company_id=ctx.company.company_id,
                    picking_id=picking_id,
                    picking_type_id=picking_type_id,
                    warehouse_id=getattr(wh, "warehouse_id", None),
                    product_id=int(prod.product_id),
                    name=str(prod.name),
                    uom_id=int(prod.uom_id),
                    qty=qty_req,
                    src=src_loc,
                    dst=dst_loc,
                )

            ctx.move_rows.append(
                StockMove(
                    origin=origin,
                    company=ctx.company.name,
                    warehouse=wh.code,
                    kind=kind,
                    scheduled_date=scheduled_dt,
                    product=prod.default_code,
                    product_name=prod.name,
                    category=prod.category,
                    qty_requested=qty_req,
                    qty_done=qty_done,
                    uom=prod.uom_name,
                    source_location_id=src_loc,
                    dest_location_id=dst_loc,
                    note=note,
                )
            )
            if kind == "OUT":
                ctx.outbound_qty_by_sku[str(prod.default_code)] += qty_done

        self._validate_picking(
            company_id=ctx.company.company_id,
            picking_id=picking_id,
            done_day=day,
            quantities_done_by_product=qty_done_by_product,
        )

        # Update ledger
        for prod, _ in lines:
            qty_done = float(qty_done_by_product.get(int(prod.product_id), 0.0))
            if qty_done <= 0:
                continue
            self.ledger.add(src_loc, int(prod.product_id), -qty_done)
            self.ledger.add(dst_loc, int(prod.product_id), qty_done)

        ctx.picking_rows.append(
            StockPicking(
                origin=origin,
                company=ctx.company.name,
                warehouse=wh.code,
                kind=kind,
                scheduled_date=scheduled_dt,
                source_location_id=src_loc,
                dest_location_id=dst_loc,
                lines=len(lines),
                note=note,
            )
        )
        ctx.picking_counts[kind] += 1
        return True


    def seed_movements(
        self,
        *,
        company: Company,
        products: list[Product],
        vendor_ids_by_category: dict[str, list[int]],
        days: int,
        scale: str,
        end_date: dt.date | None = None,
    ) -> dict[str, Any]:
        supplier_loc_id, customer_loc_id = self._ensure_base_locations()
        end_date = end_date or dt.date.today()
        days_list = _date_range(end_date, days)
        rng = random.Random(_stable_int_seed(f"{self.dataset_key}:{company.name}:moves"))

        # Assign size + active SKU sets per warehouse.
        warehouse_meta = self._generate_warehouse_profiles(
            company=company, products=products, scale=scale, rng=rng
        )

        ctx = SimulationContext(
            company=company,
            days_list=days_list,
            rng=rng,
            wh_meta=warehouse_meta,
            vendor_ids_by_category=vendor_ids_by_category,
        )

        if products and days_list:
            dormant_n = max(1, int(len(products) * 0.08))
            dormant_sample = rng.sample(products, k=min(dormant_n, len(products)))
            ctx.dormant_products = {int(p.product_id) for p in dormant_sample if p.product_id}
            ctx.dormant_start = days_list[int(len(days_list) * 0.6)]

        # Anomalies: adding demand spikes , controlled stockouts, and shrinkage windows info to the context.
        self._generate_demand_spikes(ctx)
        self._generate_shrinkage_event(ctx)
        self._generate_controlled_stockouts(ctx, products)


        # Seed movements.
        self.inbound.seed_inbound(ctx, supplier_loc_id)
        self.internal.seed_internal(ctx)
        self.damage.seed_damage(ctx)
        self.outbound.seed_outbound(ctx, customer_loc_id)

        os.makedirs(self.out_dir, exist_ok=True)
        pickings_csv = os.path.join(self.out_dir, f"pickings_{company.country_code}_{self.dataset_key}.csv")
        moves_csv = os.path.join(self.out_dir, f"moves_{company.country_code}_{self.dataset_key}.csv")

        self.reporting.write_csvs(ctx, pickings_csv=pickings_csv, moves_csv=moves_csv)

        return self.reporting.summarize(
            ctx=ctx,
            products=products,
            days_list=days_list,
            pickings_csv=pickings_csv,
            moves_csv=moves_csv,
        )
