from __future__ import annotations

import csv
import datetime as dt
from collections import defaultdict
from dataclasses import asdict, is_dataclass
from typing import Any

from services.interfaces.movement_seeder_protocol import MovementSeederProtocol


class Reporting:
    """Encapsulates reporting and CSV export logic."""

    def __init__(self, seeder: MovementSeederProtocol):
        self.seeder = seeder

    def _row_to_dict(self, row: Any) -> dict[str, Any]:
        if isinstance(row, dict):
            return row
        if is_dataclass(row):
            return asdict(row)
        return dict(row)

    def _write_pickings_csv(self, ctx, path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            first = self._row_to_dict(ctx.picking_rows[0]) if ctx.picking_rows else {"origin": ""}
            writer = csv.DictWriter(f, fieldnames=list(first.keys()))
            writer.writeheader()
            for row in ctx.picking_rows:
                writer.writerow(self._row_to_dict(row))

    def _write_moves_csv(self, ctx, path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            first = self._row_to_dict(ctx.move_rows[0]) if ctx.move_rows else {"origin": ""}
            writer = csv.DictWriter(f, fieldnames=list(first.keys()))
            writer.writeheader()
            for row in ctx.move_rows:
                writer.writerow(self._row_to_dict(row))

    def _daily_outbound(self, ctx) -> defaultdict[tuple[str, str], float]:
        daily_outbound: defaultdict[tuple[str, str], float] = defaultdict(float)
        for r in ctx.move_rows:
            row = self._row_to_dict(r)
            if row.get("kind") != "OUT":
                continue
            day = str(row["scheduled_date"])[:10]
            daily_outbound[(day, str(row["product"]))] += float(row["qty_done"])
        return daily_outbound

    def _avg_outbound_last_n(
        self, daily_outbound: dict[tuple[str, str], float], days_list: list[dt.date], *, window: int = 30
    ) -> dict[str, float]:
        avg_outbound: dict[str, float] = defaultdict(float)
        last_window = days_list[-window:] if len(days_list) >= window else days_list
        last_window_set = {d.isoformat() for d in last_window}
        for (day_s, sku), qty in daily_outbound.items():
            if day_s in last_window_set:
                avg_outbound[sku] += qty
        for sku in list(avg_outbound.keys()):
            avg_outbound[sku] /= max(1, len(last_window))
        return avg_outbound

    def _ending_stock_by_sku_from_ledger(self) -> dict[int, float]:
        ending: dict[int, float] = defaultdict(float)
        for (_loc_id, pid), qty in self.seeder.ledger.qty.items():
            if qty > 0:
                ending[int(pid)] += float(qty)
        return ending

    def _days_of_cover(
        self,
        *,
        products,
        ending_stock_by_sku: dict[int, float],
        avg_outbound: dict[str, float],
    ) -> list[tuple[float, str, float, float]]:
        days_of_cover: list[tuple[float, str, float, float]] = []
        for prod in products:
            sku = str(prod.default_code)
            stock = float(ending_stock_by_sku.get(int(prod.product_id), 0.0))
            rate = float(avg_outbound.get(sku, 0.0))
            if rate > 0:
                days_of_cover.append((stock / rate, sku, stock, rate))
        days_of_cover.sort(key=lambda x: x[0])
        return days_of_cover

    def summarize(
        self,
        *,
        ctx,
        products,
        days_list: list[dt.date],
        pickings_csv: str,
        moves_csv: str,
    ) -> dict[str, Any]:
        daily_outbound = self._daily_outbound(ctx)
        avg_outbound_30 = self._avg_outbound_last_n(daily_outbound, days_list, window=30)
        ending_stock_by_sku = self._ending_stock_by_sku_from_ledger()
        days_of_cover = self._days_of_cover(
            products=products,
            ending_stock_by_sku=ending_stock_by_sku,
            avg_outbound=avg_outbound_30,
        )

        return {
            "pickings_csv": pickings_csv,
            "moves_csv": moves_csv,
            "picking_counts": dict(ctx.picking_counts),
            "top_outbound_skus": ctx.outbound_qty_by_sku.most_common(10),
            "lowest_days_of_cover": days_of_cover[:10],
        }

    def write_csvs(self, ctx, *, pickings_csv: str, moves_csv: str) -> None:
        self._write_pickings_csv(ctx, pickings_csv)
        self._write_moves_csv(ctx, moves_csv)
