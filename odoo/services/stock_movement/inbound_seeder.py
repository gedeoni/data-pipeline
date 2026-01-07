from __future__ import annotations

import datetime as dt
from typing import Any

from dto import AnomalyEvent, InboundWarehouseContext, WarehouseProfile
from services.stock_movement.seasonality import weekday_multiplier
from services.interfaces.movement_seeder_protocol import MovementSeederProtocol


class InboundSeeder:
    """Encapsulates inbound seeding logic."""

    def __init__(self, seeder: MovementSeederProtocol):
        self.seeder = seeder

    # ---- planning helpers -------------------------------------------------

    def _build_inbound_warehouse_context(
        self,
        ctx,
        wh,
        *,
        days_count: int,
        shipments_per_month_range: tuple[int, int],
        weekday_threshold: float,
        delay_month_multiplier: tuple[int, int],
    ) -> InboundWarehouseContext:
        profile: WarehouseProfile = ctx.wh_meta[wh.code]
        weight = profile.weight
        months = max(1, int(days_count / 30))

        per_month_min, per_month_max = shipments_per_month_range
        potential_inbound_shipments = ctx.rng.randint(per_month_min, per_month_max) * months
        actual_inbound_shipments = max(1, int(round(potential_inbound_shipments * weight)))

        candidate_days = [d for d in ctx.days_list if weekday_multiplier("inbound", d) > weekday_threshold]
        if not candidate_days:
            candidate_days = ctx.days_list
        receipt_days = ctx.rng.sample(candidate_days, k=min(actual_inbound_shipments, len(candidate_days)))

        delay_min_mult, delay_max_mult = delay_month_multiplier
        delayed_shipments_number = min(
            len(receipt_days),
            ctx.rng.randint(delay_min_mult * months, delay_max_mult * months),
        )
        delayed_days = (
            set(ctx.rng.sample(receipt_days, k=delayed_shipments_number)) if delayed_shipments_number else set()
        )

        return InboundWarehouseContext(
            warehouse=wh,
            profile=profile,
            weight=weight,
            months=months,
            receipt_days=receipt_days,
            delayed_days=delayed_days,
        )

    def _generate_inbound_lines_for_day(
        self,
        ctx,
        wh_ctx: InboundWarehouseContext,
        *,
        day: dt.date,
        line_count_range: tuple[int, int],
        category_qty_ranges: dict[str, tuple[float, float]],
        stockout_inbound_reduction: float,
    ) -> tuple[list[tuple[Any, float]], bool]:
        min_lines, max_lines = line_count_range
        active_products = wh_ctx.profile.active_products
        if not active_products:
            return [], False

        line_n = ctx.rng.randint(min_lines, max_lines)
        sample_size = min(line_n, len(active_products))
        had_stockout_reduction = False
        lines: list[tuple[Any, float]] = []

        candidates = self.seeder._eligible_products(ctx, active_products, day)
        if not candidates:
            return [], False
        for prod in ctx.rng.sample(candidates, k=min(sample_size, len(candidates))):
            low, high = category_qty_ranges.get(
                prod.category, category_qty_ranges["__default__"]
            )
            qty = ctx.rng.uniform(low, high) * wh_ctx.weight

            if day in ctx.stockout_window and prod in ctx.stockout_products:
                qty *= stockout_inbound_reduction
                had_stockout_reduction = True

            lines.append((prod, round(qty, 2)))

        return lines, had_stockout_reduction

    def _choose_vendor(self, ctx, lines: list[tuple[Any, float]]) -> int | None:
        if not lines:
            return None

        category_counts: dict[str, int] = {}
        for prod, _ in lines:
            category_counts[prod.category] = category_counts.get(prod.category, 0) + 1

        if not category_counts:
            return None

        max_count = max(category_counts.values())
        dominant_categories = [c for c, n in category_counts.items() if n == max_count]
        vendor_category = ctx.rng.choice(dominant_categories)
        vendor_candidates = ctx.vendor_ids_by_category.get(vendor_category, [])
        return ctx.rng.choice(vendor_candidates) if vendor_candidates else None


    def _process_inbound_receipt(
        self,
        ctx,
        wh,
        wh_ctx: InboundWarehouseContext,
        *,
        receipt_date: dt.date,
        supplier_loc_id: int,
        delay_days_range: tuple[int, int],
        line_count_range: tuple[int, int],
        category_qty_ranges: dict[str, tuple[float, float]],
        stockout_inbound_reduction: float,
    ) -> None:
        day = receipt_date
        note = ""

        if receipt_date in wh_ctx.delayed_days:
            min_delay, max_delay = delay_days_range
            delay = ctx.rng.randint(min_delay, max_delay)
            day = min(ctx.days_list[-1], receipt_date + dt.timedelta(days=delay))
            note = f"supplier_delay:+{delay}d"
            evt = AnomalyEvent(
                kind="supplier_delay",
                company=ctx.company.name,
                detail=f"Inbound delayed {delay}d for {wh.code} originally {receipt_date.isoformat()}",
                date=receipt_date,
            )
            self.seeder.anomalies.append(evt)
            print(f"[anomaly] {evt.company} {evt.kind} {evt.date.isoformat()}: {evt.detail}")

        lines, had_stockout_reduction = self._generate_inbound_lines_for_day(
            ctx,
            wh_ctx,
            day=day,
            line_count_range=line_count_range,
            category_qty_ranges=category_qty_ranges,
            stockout_inbound_reduction=stockout_inbound_reduction,
        )
        if not lines:
            return

        if had_stockout_reduction:
            note = (note + ";" if note else "") + "stockout_inbound_reduction"

        vendor_id = self._choose_vendor(ctx, lines)
        dest_good = self.seeder._pick_base_unit_location(ctx, wh.code, "GOOD")
        self.seeder._create_and_validate_picking(
            ctx,
            wh=wh,
            kind="IN",
            day=day,
            picking_type_id=wh.picking_type_in_id,
            partner_id=vendor_id,
            src_loc=supplier_loc_id,
            dst_loc=dest_good,
            lines=lines,
            note=note,
        )

    def seed_inbound(self, ctx, supplier_loc_id: int) -> None:
        SHIPMENTS_PER_MONTH_RANGE = (2, 6)
        WEEKDAY_THRESHOLD_INBOUND = 0.4
        DELAY_MONTH_MULTIPLIER_RANGE = (1, 2)
        DELIVERY_DELAY_DAYS_RANGE = (3, 10)
        LINES_PER_RECEIPT_RANGE = (3, 8)
        STOCKOUT_INBOUND_REDUCTION = 0.35
        CATEGORY_QTY_RANGES = {
            "Seeds": (150.0, 600.0),
            "Fertilizer": (150.0, 600.0),
            "Pesticides": (20.0, 80.0),
            "Tools": (5.0, 25.0),
            "Spare Parts": (5.0, 25.0),
            "__default__": (30.0, 120.0),
        }

        days_count = len(ctx.days_list)

        for wh in ctx.company.warehouses:
            wh_ctx = self._build_inbound_warehouse_context(
                ctx,
                wh,
                days_count=days_count,
                shipments_per_month_range=SHIPMENTS_PER_MONTH_RANGE,
                weekday_threshold=WEEKDAY_THRESHOLD_INBOUND,
                delay_month_multiplier=DELAY_MONTH_MULTIPLIER_RANGE,
            )

            for planned_day in wh_ctx.receipt_days:
                self._process_inbound_receipt(
                    ctx,
                    wh=wh,
                    wh_ctx=wh_ctx,
                    receipt_date=planned_day,
                    supplier_loc_id=supplier_loc_id,
                    delay_days_range=DELIVERY_DELAY_DAYS_RANGE,
                    line_count_range=LINES_PER_RECEIPT_RANGE,
                    category_qty_ranges=CATEGORY_QTY_RANGES,
                    stockout_inbound_reduction=STOCKOUT_INBOUND_REDUCTION,
                )
