from __future__ import annotations

import datetime as dt
from typing import Any

from dto import (
    InternalTransferDetails,
    InternalWarehouseContext,
    WarehouseProfile,
)
from services.stock_movement.seasonality import weekday_multiplier
from services.interfaces.movement_seeder_protocol import MovementSeederProtocol


class InternalSeeder:
    """Encapsulates internal transfer seeding logic."""

    def __init__(self, seeder: MovementSeederProtocol):
        self.seeder = seeder


    def _build_internal_warehouse_context(
        self,
        ctx,
        wh,
        *,
        days_count: int,
        transfer_count_range: tuple[int, int],
        weekday_threshold: float,
    ) -> InternalWarehouseContext:
        profile: WarehouseProfile = ctx.wh_meta[wh.code]
        weight = profile.weight
        months = max(1, int(days_count / 30))

        min_c, max_c = transfer_count_range
        base_count = ctx.rng.randint(min_c, max_c)
        transfer_count = int(round(base_count * months * (0.8 + 0.4 * weight)))

        candidate_days = [d for d in ctx.days_list if weekday_multiplier("internal", d) > weekday_threshold]
        if not candidate_days:
            candidate_days = ctx.days_list

        transfer_days = ctx.rng.sample(candidate_days, k=min(transfer_count, len(candidate_days)))

        return InternalWarehouseContext(
            warehouse=wh,
            profile=profile,
            weight=weight,
            transfer_days=transfer_days,
        )

    def _quantity_to_transfer(
        self,
        ctx,
        weight: float,
        prod,
        src_loc: int,
        qty_range_default: tuple[float, float],
        qty_range_small: tuple[float, float],
        availability_cap: float,
    ) -> float:
        if prod.category in ("Tools", "Spare Parts"):
            low, high = qty_range_small
        else:
            low, high = qty_range_default

        desired = ctx.rng.uniform(low, high) * weight
        avail = self.seeder.ledger.get(src_loc, int(prod.product_id))
        qty = min(desired, max(0.0, avail * availability_cap))
        return round(max(0.0, qty), 2)

    def _generate_internal_transfer_details(
        self,
        ctx,
        wh_ctx: InternalWarehouseContext,
        *,
        day: dt.date,
        qty_range_default: tuple[float, float],
        qty_range_small: tuple[float, float],
        availability_cap: float,
    ) -> InternalTransferDetails | None:
        wh = wh_ctx.warehouse
        active_products = wh_ctx.profile.active_products
        candidates = self.seeder._eligible_products(ctx, active_products, day)
        if not candidates:
            return None
        prod = ctx.rng.choice(candidates)

        src_candidates = self.seeder._available_locations_for_product(ctx, wh.code, int(prod.product_id))
        if not src_candidates:
            ctx.picking_counts["INT:skipped_no_stock"] += 1
            return None

        src_good = int(src_candidates[0])
        dst_transit = self.seeder._pick_base_unit_location(ctx, wh.code, "TRANSIT")

        dst_good = src_good
        for _ in range(5):
            candidate = self.seeder._pick_base_unit_location(ctx, wh.code, "GOOD")
            if candidate != src_good:
                dst_good = candidate
                break
        if dst_good == src_good:
            return None

        qty = self._quantity_to_transfer(
            ctx,
            wh_ctx.weight,
            prod,
            src_good,
            qty_range_default,
            qty_range_small,
            availability_cap,
        )

        if qty <= 0.0:
            ctx.picking_counts["INT:skipped_no_qty"] += 1
            return None

        return InternalTransferDetails(
            product=prod,
            qty=qty,
            src_loc_id=src_good,
            transit_loc_id=dst_transit,
            dst_loc_id=dst_good,
        )


    def _process_internal_transfers(
        self,
        ctx,
        wh_ctx: InternalWarehouseContext,
        *,
        day: dt.date,
        qty_range_default: tuple[float, float],
        qty_range_small: tuple[float, float],
        availability_cap: float,
    ) -> None:
        wh = wh_ctx.warehouse
        details = self._generate_internal_transfer_details(
            ctx,
            wh_ctx,
            day=day,
            qty_range_default=qty_range_default,
            qty_range_small=qty_range_small,
            availability_cap=availability_cap,
        )
        if not details:
            return

        ok = self.seeder._create_and_validate_picking(
            ctx,
            wh=wh,
            kind="INT",
            day=day,
            picking_type_id=wh.picking_type_internal_id,
            partner_id=None,
            src_loc=details.src_loc_id,
            dst_loc=details.transit_loc_id,
            lines=[(details.product, details.qty)],
            note="redistribution_step1",
        )
        if not ok:
            return

        d2 = day + dt.timedelta(days=1)
        if d2 > ctx.days_list[-1]:
            d2 = day
        self.seeder._create_and_validate_picking(
            ctx,
            wh=wh,
            kind="INT",
            day=d2,
            picking_type_id=wh.picking_type_internal_id,
            partner_id=None,
            src_loc=details.transit_loc_id,
            dst_loc=details.dst_loc_id,
            lines=[(details.product, details.qty)],
            note="redistribution_step2",
        )

    def seed_internal(self, ctx) -> None:
        TRANSFER_COUNT_RANGE = (12, 40)
        WEEKDAY_THRESHOLD_INTERNAL = 0.6
        QTY_RANGE_DEFAULT = (10.0, 120.0)
        QTY_RANGE_SMALL = (1.0, 12.0)
        AVAILABILITY_CAP = 0.85

        days_count = len(ctx.days_list)
        for wh in ctx.company.warehouses:
            wh_ctx = self._build_internal_warehouse_context(
                ctx,
                wh,
                days_count=days_count,
                transfer_count_range=TRANSFER_COUNT_RANGE,
                weekday_threshold=WEEKDAY_THRESHOLD_INTERNAL,
            )

            for day in wh_ctx.transfer_days:
                self._process_internal_transfers(
                    ctx,
                    wh_ctx,
                    day=day,
                    qty_range_default=QTY_RANGE_DEFAULT,
                    qty_range_small=QTY_RANGE_SMALL,
                    availability_cap=AVAILABILITY_CAP,
                )
