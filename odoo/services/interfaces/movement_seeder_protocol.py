from __future__ import annotations

from typing import Any, Protocol

from dto import AnomalyEvent
import datetime as dt


class MovementSeederProtocol(Protocol):
    ledger: Any
    anomalies: list[AnomalyEvent]

    def _pick_base_unit_location(self, ctx, wh_code: str, kind: str) -> int: ...

    def _available_locations_for_product(self, ctx, wh_code: str, product_id: int) -> list[int]: ...

    def _eligible_products(self, ctx, active_products: list[Any], day: dt.date) -> list[Any]: ...

    def _create_and_validate_picking(
        self,
        ctx,
        *,
        wh,
        kind: str,
        day: dt.date,
        picking_type_id: int,
        partner_id: int | None,
        src_loc: int,
        dst_loc: int,
        lines: list[tuple[Any, float]],
        note: str = "",
    ) -> None: ...
