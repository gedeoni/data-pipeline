from __future__ import annotations

import datetime as dt
from typing import Any

from dto import OutboundWarehouseContext, WarehouseProfile
from services.stock_movement.seasonality import demand_intensity, weekday_multiplier
from services.interfaces.movement_seeder_protocol import MovementSeederProtocol


class OutboundSeeder:
    """Encapsulates outbound seeding logic."""

    def __init__(self, seeder: MovementSeederProtocol):
        self.seeder = seeder


    def _build_outbound_warehouse_context(
        self,
        ctx,
        wh,
    ) -> OutboundWarehouseContext:
        profile: WarehouseProfile = ctx.wh_meta[wh.code]
        return OutboundWarehouseContext(
            warehouse=wh,
            profile=profile,
            weight=profile.weight,
        )

    def _determine_outbound_picking_count(
        self,
        ctx,
        wh_ctx: OutboundWarehouseContext,
        day: dt.date,
    ) -> int:
        if weekday_multiplier("outbound", day) < 0.35 and ctx.rng.random() < 0.75:
            return 0

        weight = wh_ctx.weight
        pick_n = 1 if ctx.rng.random() < min(0.85, 0.45 + 0.25 * weight) else 0
        if weight > 1.2 and ctx.rng.random() < 0.25:
            pick_n += 1
        return pick_n

    def _generate_outbound_lines(
        self,
        ctx,
        wh_ctx: OutboundWarehouseContext,
        day: dt.date,
        spike_mult: float,
        base_rates: dict[str, float],
        order_size_multipliers: dict[str, tuple[float, float]],
    ) -> tuple[list[tuple[Any, float]], str]:
        active_products = wh_ctx.profile.active_products
        weight = wh_ctx.weight
        line_n = ctx.rng.randint(2, 7)
        lines: list[tuple[Any, float]] = []
        note = ""

        candidates = self.seeder._eligible_products(ctx, active_products, day)
        if not candidates:
            return [], note
        for prod in ctx.rng.sample(candidates, k=min(line_n, len(candidates))):
            intensity = demand_intensity(ctx.company.country_code, prod.category, day, rng=ctx.rng) * spike_mult
            qty = base_rates[prod.category] * intensity * weight

            low, high = order_size_multipliers.get(prod.category, order_size_multipliers["__default__"])
            qty *= ctx.rng.uniform(low, high)

            if day in ctx.stockout_window and prod in ctx.stockout_products:
                qty *= 2.8
                note = "stockout_pressure"

            qty = round(max(0.0, qty), 2)
            if qty <= 0.0:
                continue
            lines.append((prod, qty))

        return lines, note


    def _post_outbound_picking(
        self,
        ctx,
        wh,
        day: dt.date,
        customer_loc_id: int,
        lines: list[tuple[Any, float]],
    ) -> None:
        candidate_srcs: list[int] = []
        for prod, _ in lines:
            candidate_srcs.extend(self.seeder._available_locations_for_product(ctx, wh.code, int(prod.product_id)))
        src_good = int(candidate_srcs[0]) if candidate_srcs else self.seeder._pick_base_unit_location(ctx, wh.code, "GOOD")

        filtered_lines: list[tuple[Any, float]] = []
        for prod, qty_req in lines:
            avail = self.seeder.ledger.get(src_good, int(prod.product_id))
            if avail <= 0.01:
                continue
            if day in ctx.stockout_window and prod in ctx.stockout_products:
                qty_req = max(qty_req, avail * 1.5)
            filtered_lines.append((prod, qty_req))

        if not filtered_lines:
            ctx.picking_counts["OUT:skipped_no_stock"] += 1
            return

        final_note = "" if day not in ctx.stockout_window else "stockout_window"
        self.seeder._create_and_validate_picking(
            ctx,
            wh=wh,
            kind="OUT",
            day=day,
            picking_type_id=wh.picking_type_out_id,
            partner_id=ctx.company.customer_id,
            src_loc=src_good,
            dst_loc=customer_loc_id,
            lines=filtered_lines,
            note=final_note,
        )

    def _process_outbound_warehouse(
        self,
        ctx,
        wh_ctx: OutboundWarehouseContext,
        *,
        day: dt.date,
        customer_loc_id: int,
        base_rates: dict[str, float],
        order_size_multipliers: dict[str, tuple[float, float]],
    ) -> None:
        spike_mult = 2.5 if day in ctx.spike_days else 1.0
        picking_number = self._determine_outbound_picking_count(ctx, wh_ctx, day)
        for _ in range(picking_number):
            lines, note = self._generate_outbound_lines(
                ctx, wh_ctx, day, spike_mult, base_rates, order_size_multipliers
            )
            if not lines:
                continue

            self._post_outbound_picking(ctx, wh_ctx.warehouse, day, customer_loc_id, lines)

    def seed_outbound(self, ctx, customer_loc_id: int) -> None:
        BASE_RATES = {
            "Seeds": 18.0,
            "Fertilizer": 22.0,
            "Pesticides": 2.8,
            "Tools": 0.45,
            "Spare Parts": 0.35,
            "Packaging": 6.0,
        }
        ORDER_SIZE_MULTIPLIERS = {
            "Seeds": (5.0, 18.0),
            "Fertilizer": (5.0, 18.0),
            "Pesticides": (1.0, 6.0),
            "Packaging": (1.0, 5.0),
            "__default__": (1.0, 4.0),
        }

        for wh in ctx.company.warehouses:
            wh_ctx = self._build_outbound_warehouse_context(ctx, wh)
            for day in ctx.days_list:
                self._process_outbound_warehouse(
                    ctx,
                    wh_ctx,
                    day=day,
                    customer_loc_id=customer_loc_id,
                    base_rates=BASE_RATES,
                    order_size_multipliers=ORDER_SIZE_MULTIPLIERS,
                )
