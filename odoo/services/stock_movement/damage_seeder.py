from __future__ import annotations

import datetime as dt
from typing import Any

from dto import DamageWarehouseContext, WarehouseProfile
from services.interfaces.movement_seeder_protocol import MovementSeederProtocol


class DamageSeeder:
    """Encapsulates damage/shrinkage seeding logic."""

    def __init__(self, seeder: MovementSeederProtocol):
        self.seeder = seeder


    def _build_damage_warehouse_context(
        self,
        ctx,
        wh,
        *,
        days_count: int,
    ) -> DamageWarehouseContext:
        profile: WarehouseProfile = ctx.wh_meta[wh.code]
        weight = profile.weight
        months = max(1, int(days_count / 30))

        events_count = max(months, int(round(months * 4 * (0.8 + 0.3 * weight))))
        step = max(1, int(len(ctx.days_list) / events_count))
        event_days = [ctx.days_list[i] for i in range(0, len(ctx.days_list), step)]

        return DamageWarehouseContext(
            warehouse=wh,
            profile=profile,
            weight=weight,
            event_days=event_days,
        )

    def _generate_damage_line(
        self,
        ctx,
        wh_code: str,
        prod,
        day: dt.date,
        src_loc: int,
        damage_rates: dict[str, tuple[float, float]],
        shrinkage_multiplier: float,
    ) -> tuple[float, str] | None:
        low, high = damage_rates.get(prod.category, (0.001, 0.005))
        rate = ctx.rng.uniform(low, high)

        is_shrinkage = day in ctx.shrink_window and wh_code == ctx.shrink_wh_code
        if is_shrinkage:
            rate *= shrinkage_multiplier

        base_stock = max(0.0, self.seeder.ledger.get(src_loc, int(prod.product_id)))
        qty = round(base_stock * rate, 2)

        if qty <= 0.0:
            return None

        note = "damage" + (";shrinkage" if is_shrinkage else "")
        return qty, note

    def _process_damage_events(
        self,
        ctx,
        wh_ctx: DamageWarehouseContext,
        *,
        day: dt.date,
        damage_rates: dict[str, tuple[float, float]],
        products_per_event_range: tuple[int, int],
        shrinkage_multiplier: float,
    ) -> None:
        wh = wh_ctx.warehouse
        active_products = wh_ctx.profile.active_products
        active_products = self.seeder._eligible_products(ctx, active_products, day)
        if not active_products:
            return
        good_loc = self.seeder._pick_base_unit_location(ctx, wh.code, "GOOD")
        dmg_loc = self.seeder._pick_base_unit_location(ctx, wh.code, "DAMAGED")

        min_p, max_p = products_per_event_range
        sample_k = min(ctx.rng.randint(min_p, max_p), len(active_products))

        for prod in ctx.rng.sample(active_products, k=sample_k):
            result = self._generate_damage_line(
                ctx, wh.code, prod, day, good_loc, damage_rates, shrinkage_multiplier
            )
            if not result:
                continue

            qty, note = result
            self.seeder._create_and_validate_picking(
                ctx,
                wh=wh,
                kind="DMG",
                day=day,
                picking_type_id=wh.picking_type_internal_id,
                partner_id=None,
                src_loc=good_loc,
                dst_loc=dmg_loc,
                lines=[(prod, qty)],
                note=note,
            )

    def seed_damage(self, ctx) -> None:
        DAMAGE_RATES = {
            "Seeds": (0.002, 0.008),
            "Fertilizer": (0.002, 0.008),
            "Pesticides": (0.001, 0.004),
            "Tools": (0.0005, 0.002),
            "Spare Parts": (0.0005, 0.002),
            "Packaging": (0.001, 0.004),
        }
        PRODUCTS_PER_EVENT_RANGE = (1, 3)
        SHRINKAGE_MULTIPLIER = 6.0

        days_count = len(ctx.days_list)
        for wh in ctx.company.warehouses:
            wh_ctx = self._build_damage_warehouse_context(
                ctx,
                wh,
                days_count=days_count,
            )

            for day in wh_ctx.event_days:
                self._process_damage_events(
                    ctx,
                    wh_ctx,
                    day=day,
                    damage_rates=DAMAGE_RATES,
                    products_per_event_range=PRODUCTS_PER_EVENT_RANGE,
                    shrinkage_multiplier=SHRINKAGE_MULTIPLIER,
                )
