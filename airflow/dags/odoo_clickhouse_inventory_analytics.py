"""Airflow DAG: Odoo -> ClickHouse inventory analytics and BI marts.

Design notes:
- Extract once to raw ClickHouse tables, reuse for marts via SQL.
- Incremental extraction with per-model watermarks stored in Airflow Variables.
- Idempotent loads via ReplacingMergeTree on write_date for raw tables.
- Keep XCom payloads small (counts + watermark only).
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from datetime import date as dt_date
from urllib.parse import urlparse, urlunparse

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from clickhouse_driver import Client as ClickHouseClient

# -------------------- Constants --------------------

DAG_ID = "odoo_clickhouse_inventory_analytics"

# Connection IDs: These strings must match the 'Conn Id' configured in
# Airflow Admin -> Connections. Do not hardcode credentials here.
ODOO_CONN_ID = "ODOO_CONN_ID"
CLICKHOUSE_CONN_ID = "CLICKHOUSE_CONN_ID"
SUPERSET_CONN_ID = "SUPERSET_CONN_ID"
EMAIL_CONN_ID = "EMAIL_CONN_ID"

# Airflow Variable Keys: Configure the values for these keys in Airflow Admin -> Variables.
# The strings below are the keys, not the values.
VAR_DEAD_STOCK_DAYS = "dead_stock_days"
VAR_ANOMALY_PCT = "anomaly_pct"
VAR_OTIF_THRESHOLD = "otif_threshold"
VAR_ABC_A_PCT = "abc_a_pct"
VAR_ABC_B_PCT = "abc_b_pct"
VAR_ABC_C_PCT = "abc_c_pct"
VAR_FULL_REFRESH_MODELS = "full_refresh_models"
VAR_FORECAST_SCOPE = "forecast_top_n_or_only_class_a"
VAR_PROCUREMENT_EMAIL = "procurement_manager_email"
VAR_HELPDESK_TEAM_ID = "odoo_helpdesk_team_id"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "depends_on_past": False,
}

BATCH_SIZE = 5000
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2

RAW_TABLES = {
    "stock.move.line": "raw_stock_move_line",
    "stock.quant": "raw_stock_quant",
    "stock.move": "raw_stock_move",
    "purchase.order": "raw_purchase_order",
    "product.product": "raw_product_product",
    "product.template": "raw_product_template",
    "stock.valuation.layer": "raw_stock_valuation_layer",
    "stock.location": "raw_stock_location",
    "res.partner": "raw_res_partner",
}

STRING_FIELDS = {
    "state",
    "origin",
    "name",
    "usage",
    "default_code",
    "type",
}

MODEL_FIELD_ALIASES = {
    # Odoo 17 uses `quantity` and `date` on stock.move.line.
    "stock.move.line": {
        "quantity": "qty_done",
        "date": "date_done",
    },
    # Odoo 17 uses `quantity`, `date`, and `date_deadline` on stock.move.
    "stock.move": {
        "quantity": "quantity_done",
        "date": "date_done",
        "date_deadline": "date_expected",
    },
}

# -------------------- Odoo Client --------------------


@dataclass
class OdooConfig:
    url: str
    db: str
    username: str
    password: str
    api_path: str


class OdooClient:
    """Thin JSON-RPC Odoo client with retries, pagination, and safe backoff."""

    def __init__(self, config: OdooConfig):
        self.config = config
        self._uid: Optional[int] = None
        self._session = requests.Session()

    @property
    def endpoint(self) -> str:
        return f"{self.config.url.rstrip('/')}/{self.config.api_path.strip('/')}"

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._session.post(
                    self.endpoint,
                    json=payload,
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    raise RuntimeError(data["error"])
                return data
            except Exception as exc:
                if attempt >= MAX_RETRIES:
                    raise
                sleep_for = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                logging.warning("Odoo JSON-RPC retry %s/%s due to %s", attempt, MAX_RETRIES, exc)
                time.sleep(sleep_for)
        raise RuntimeError("Exceeded retries")

    def authenticate(self) -> int:
        if self._uid is not None:
            return self._uid
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "common",
                "method": "login",
                "args": [self.config.db, self.config.username, self.config.password],
            },
            "id": 1,
        }
        result = self._post(payload)
        self._uid = result.get("result")
        if not self._uid:
            raise RuntimeError("Odoo authentication failed")
        return self._uid

    def search_read(
        self,
        model: str,
        domain: List[Any],
        fields: List[str],
        limit: int,
        offset: int,
        order: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        uid = self.authenticate()
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "object",
                "method": "execute_kw",
                "args": [
                    self.config.db,
                    uid,
                    self.config.password,
                    model,
                    "search_read",
                    [domain],
                    {
                        "fields": fields,
                        "limit": limit,
                        "offset": offset,
                        "order": order or "id asc",
                    },
                ],
            },
            "id": 2,
        }
        result = self._post(payload)
        return result.get("result", [])

    def create(self, model: str, values: Dict[str, Any]) -> int:
        uid = self.authenticate()
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "object",
                "method": "execute_kw",
                "args": [self.config.db, uid, self.config.password, model, "create", [values]],
            },
            "id": 3,
        }
        result = self._post(payload)
        return result.get("result")

    def write(self, model: str, record_ids: List[int], values: Dict[str, Any]) -> bool:
        uid = self.authenticate()
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "object",
                "method": "execute_kw",
                "args": [
                    self.config.db,
                    uid,
                    self.config.password,
                    model,
                    "write",
                    [record_ids, values],
                ],
            },
            "id": 4,
        }
        result = self._post(payload)
        return bool(result.get("result"))

    def paginate(
        self,
        model: str,
        domain: List[Any],
        fields: List[str],
        batch_size: int,
        order: str,
    ) -> Iterable[List[Dict[str, Any]]]:
        """Yield batches to avoid large payloads and protect the API."""
        offset = 0
        while True:
            records = self.search_read(
                model=model,
                domain=domain,
                fields=fields,
                limit=batch_size,
                offset=offset,
                order=order,
            )
            if not records:
                break
            yield records
            offset += batch_size


# -------------------- ClickHouse Helpers --------------------


def get_clickhouse_client() -> ClickHouseClient:
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    return ClickHouseClient(
        host=conn.host,
        port=conn.port or 9000,
        user=conn.login or "default",
        password=conn.password or "",
        database=conn.schema or "default",
    )


def execute_sql(client: ClickHouseClient, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    for stmt in statements:
        if params:
            client.execute(stmt, params)
        else:
            client.execute(stmt)


def insert_rows(
    client: ClickHouseClient,
    table: str,
    rows: Sequence[Dict[str, Any]],
    batch_size: int = 10000,
) -> int:
    if not rows:
        return 0
    columns = sorted(rows[0].keys())
    total = 0
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        values = [[row.get(col) for col in columns] for row in chunk]
        client.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES",
            values,
        )
        total += len(chunk)
    return total


# -------------------- Odoo Property Helpers --------------------


def _chunked(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _fetch_ir_property_prices(
    client: OdooClient,
    company_id: Optional[int],
    model: str,
    record_ids: Sequence[int],
) -> Dict[int, float]:
    if not record_ids:
        return {}

    prices: Dict[int, Tuple[float, Optional[int]]] = {}
    fields = ["company_id", "res_id", "value_float"]
    for chunk in _chunked([f"{model},{record_id}" for record_id in record_ids], 1000):
        domain: List[Any] = [
            ["name", "=", "standard_price"],
            ["res_id", "in", list(chunk)],
        ]
        if company_id:
            domain = [
                "|",
                ["company_id", "=", company_id],
                ["company_id", "=", False],
            ] + domain

        for batch in client.paginate(
            model="ir.property",
            domain=domain,
            fields=fields,
            batch_size=BATCH_SIZE,
            order="id asc",
        ):
            for prop in batch:
                res_id = prop.get("res_id") or ""
                try:
                    _, record_id_str = res_id.split(",", 1)
                    record_id = int(record_id_str)
                except (ValueError, TypeError):
                    continue
                value = prop.get("value_float")
                if value is None:
                    continue
                prop_company_id = prop.get("company_id")
                if isinstance(prop_company_id, (list, tuple)) and prop_company_id:
                    prop_company_id = prop_company_id[0]
                if not prop_company_id:
                    prop_company_id = None
                if record_id not in prices or (company_id and prop_company_id == company_id):
                    prices[record_id] = (float(value), prop_company_id)

    return {record_id: price for record_id, (price, _) in prices.items()}


def _fetch_standard_prices_for_templates(
    client: OdooClient,
    company_id: Optional[int],
    template_ids: Sequence[int],
) -> Dict[int, float]:
    template_prices = _fetch_ir_property_prices(client, company_id, "product.template", template_ids)
    missing_templates = [template_id for template_id in template_ids if template_id not in template_prices]
    if not missing_templates:
        return template_prices

    product_template_map: Dict[int, int] = {}
    for batch in client.paginate(
        model="product.product",
        domain=[["product_tmpl_id", "in", missing_templates]],
        fields=["id", "product_tmpl_id"],
        batch_size=BATCH_SIZE,
        order="id asc",
    ):
        for product in batch:
            product_id = product.get("id")
            template_id = product.get("product_tmpl_id")
            if isinstance(template_id, (list, tuple)) and template_id:
                template_id = template_id[0]
            if not product_id or not template_id:
                continue
            product_template_map[int(product_id)] = int(template_id)

    if not product_template_map:
        return template_prices

    product_prices = _fetch_ir_property_prices(
        client,
        company_id,
        "product.product",
        list(product_template_map.keys()),
    )
    for product_id, template_id in product_template_map.items():
        if template_id in template_prices:
            continue
        price = product_prices.get(product_id)
        if price is not None:
            template_prices[template_id] = price

    return template_prices


# -------------------- SQL Definitions --------------------

RAW_TABLE_DDL = """
-- Raw tables use ReplacingMergeTree on write_date to dedupe incremental loads.
CREATE TABLE IF NOT EXISTS raw_stock_move_line (
    id UInt64,
    company_id UInt64,
    product_id UInt64,
    qty_done Float64,
    location_id UInt64,
    location_dest_id UInt64,
    state String,
    date_done DateTime,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
PARTITION BY toYYYYMM(date_done)
ORDER BY (company_id, product_id, id);

CREATE TABLE IF NOT EXISTS raw_stock_quant (
    id UInt64,
    company_id UInt64,
    product_id UInt64,
    location_id UInt64,
    quantity Float64,
    reserved_quantity Float64,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, product_id, id);

CREATE TABLE IF NOT EXISTS raw_stock_move (
    id UInt64,
    company_id UInt64,
    product_id UInt64,
    product_uom_qty Float64,
    quantity_done Float64,
    location_id UInt64,
    location_dest_id UInt64,
    state String,
    date_expected DateTime,
    date_done DateTime,
    origin String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
PARTITION BY toYYYYMM(date_done)
ORDER BY (company_id, product_id, id);

CREATE TABLE IF NOT EXISTS raw_purchase_order (
    id UInt64,
    company_id UInt64,
    partner_id UInt64,
    name String,
    date_order DateTime,
    date_planned DateTime,
    state String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
PARTITION BY toYYYYMM(date_order)
ORDER BY (company_id, partner_id, id);

CREATE TABLE IF NOT EXISTS raw_product_product (
    id UInt64,
    company_id UInt64,
    product_tmpl_id UInt64,
    default_code String,
    active UInt8,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, product_tmpl_id, id);

CREATE TABLE IF NOT EXISTS raw_product_template (
    id UInt64,
    company_id UInt64,
    name String,
    standard_price Float64,
    list_price Float64,
    type String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, id);

CREATE TABLE IF NOT EXISTS raw_stock_valuation_layer (
    id UInt64,
    company_id UInt64,
    product_id UInt64,
    quantity Float64,
    value Float64,
    stock_move_id UInt64,
    create_date DateTime,
    write_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
PARTITION BY toYYYYMM(create_date)
ORDER BY (company_id, product_id, id);

CREATE TABLE IF NOT EXISTS raw_stock_location (
    id UInt64,
    company_id UInt64,
    name String,
    usage String,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, id);

CREATE TABLE IF NOT EXISTS raw_res_partner (
    id UInt64,
    company_id UInt64,
    name String,
    supplier_rank UInt64,
    write_date DateTime,
    create_date DateTime
) ENGINE = ReplacingMergeTree(write_date)
ORDER BY (company_id, id);
"""

MART_TABLE_DDL = """
-- 1. Liquidation Candidates: Identifies 'dead stock'—products with no movement for a configurable period (e.g., 90 days).
-- Insight: Helps finance and ops teams decide what to liquidate to free up cash flow and warehouse space.
CREATE TABLE IF NOT EXISTS mart_liquidation_candidates (
    snapshot_date Date,
    company_id UInt64,
    product_id UInt64,
    last_movement_date Date,
    days_since_last_move UInt32,
    on_hand_qty Float64,
    standard_price Float64,
    value_at_risk Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (company_id, product_id, snapshot_date);

-- 2. Vendor Scorecard (OTIF): Tracks On-Time and In-Full performance for vendors.
-- Insight: Used for vendor negotiations and identifying supply chain risks.
CREATE TABLE IF NOT EXISTS mart_vendor_rating (
    month Date,
    company_id UInt64,
    partner_id UInt64,
    on_time_pct Float64,
    in_full_pct Float64,
    overall_score Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (company_id, partner_id, month);

-- 3. Warehouse Efficiency (Touch Ratio): Measures how many times an item is moved internally vs. shipped out.
-- Insight: High ratios indicate inefficient warehouse layout or excessive handling processes.
CREATE TABLE IF NOT EXISTS mart_warehouse_touch_ratio (
    month Date,
    company_id UInt64,
    product_id UInt64,
    internal_moves UInt64,
    outgoing_moves UInt64,
    touch_ratio Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (company_id, product_id, month);

-- 3b. Stockout Risk: Tracks unfulfilled demand by product.
CREATE TABLE IF NOT EXISTS mart_stockout_risk (
    month Date,
    company_id UInt64,
    product_id UInt64,
    demand_qty Float64,
    fulfilled_qty Float64,
    unmet_qty Float64,
    move_count UInt64,
    stockout_moves UInt64,
    stockout_rate Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (company_id, product_id, month);

-- 3c. Inventory Turnover: Outbound value moved relative to on-hand value.
CREATE TABLE IF NOT EXISTS mart_inventory_turnover (
    month Date,
    company_id UInt64,
    product_id UInt64,
    moved_qty Float64,
    moved_value Float64,
    on_hand_qty Float64,
    on_hand_value Float64,
    turnover_ratio Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (company_id, product_id, month);

-- 4. Cost Anomalies: Detects sudden spikes or drops in unit cost compared to a 30-day average.
-- Insight: Flags potential data entry errors or supplier pricing issues for immediate review.
CREATE TABLE IF NOT EXISTS mart_cost_anomalies (
    snapshot_date Date,
    company_id UInt64,
    product_id UInt64,
    unit_cost Float64,
    avg_30d_cost Float64,
    deviation_pct Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (company_id, product_id, snapshot_date);

-- 5. ABC Classification: Segments inventory by value usage (Pareto Principle: 80% value from 20% items).
-- Insight: Prioritizes cycle counting and forecasting efforts on Class A items.
CREATE TABLE IF NOT EXISTS mart_abc_classification (
    snapshot_date Date,
    company_id UInt64,
    product_id UInt64,
    total_value_moved Float64,
    cumulative_share Float64,
    abc_class String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (company_id, product_id, snapshot_date);

-- 6. Demand Forecast: Predicts future quantity requirements based on historical trends (Exponential Smoothing).
-- Insight: Supports purchasing decisions to prevent stockouts on high-value items.
CREATE TABLE IF NOT EXISTS mart_demand_forecast (
    forecast_month Date,
    company_id UInt64,
    product_id UInt64,
    forecast_qty Float64,
    lower_ci Float64,
    upper_ci Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(forecast_month)
ORDER BY (company_id, product_id, forecast_month);
"""

MART_SQL_LIQUIDATION = """
INSERT INTO mart_liquidation_candidates
SELECT
    toDate(now()) AS snapshot_date,
    lm.company_id,
    lm.product_id,
    lm.last_movement_date,
    dateDiff('day', lm.last_movement_date, toDate(now())) AS days_since_last_move,
    q.on_hand_qty,
    pt.standard_price,
    q.on_hand_qty * pt.standard_price AS value_at_risk
FROM (
    SELECT company_id, product_id, max(toDate(date_done)) AS last_movement_date
    FROM raw_stock_move_line
    WHERE state = 'done'
    GROUP BY company_id, product_id
) lm
LEFT JOIN (
    SELECT company_id, product_id, sum(quantity) AS on_hand_qty
    FROM raw_stock_quant
    LEFT JOIN raw_stock_location l
        ON raw_stock_quant.location_id = l.id
        AND (l.company_id = raw_stock_quant.company_id OR l.company_id = 0)
    WHERE l.usage = 'internal'
    GROUP BY company_id, product_id
) q ON lm.company_id = q.company_id AND lm.product_id = q.product_id
LEFT JOIN raw_product_product pp ON lm.product_id = pp.id
LEFT JOIN raw_product_template pt ON pp.product_tmpl_id = pt.id
HAVING days_since_last_move > %(dead_stock_days)s;
"""

MART_SQL_VENDOR = """
-- On-time/in-full is limited to incoming receipts based on supplier -> internal locations.
INSERT INTO mart_vendor_rating
SELECT
    toStartOfMonth(m.date_done) AS month,
    m.company_id,
    po.partner_id,
    avg(if(m.date_done <= po.date_planned, 1.0, 0.0)) AS on_time_pct,
    avg(if(m.quantity_done >= m.product_uom_qty, 1.0, 0.0)) AS in_full_pct,
    (avg(if(m.date_done <= po.date_planned, 1.0, 0.0)) * 0.5)
      + (avg(if(m.quantity_done >= m.product_uom_qty, 1.0, 0.0)) * 0.5) AS overall_score
FROM raw_stock_move m
LEFT JOIN raw_purchase_order po ON m.origin = po.name AND m.company_id = po.company_id
LEFT JOIN raw_stock_location src ON m.location_id = src.id AND m.company_id = src.company_id
LEFT JOIN raw_stock_location dst ON m.location_dest_id = dst.id AND m.company_id = dst.company_id
WHERE m.state = 'done'
  AND m.date_done IS NOT NULL
  AND src.usage = 'supplier'
  AND dst.usage = 'internal'
GROUP BY month, m.company_id, po.partner_id;
"""

MART_SQL_TOUCH_RATIO = """
INSERT INTO mart_warehouse_touch_ratio
SELECT
    toStartOfMonth(m.date_done) AS month,
    m.company_id,
    m.product_id,
    countIf(src.usage = 'internal' AND dst.usage = 'internal') AS internal_moves,
    countIf(dst.usage = 'customer') AS outgoing_moves,
    if(outgoing_moves = 0, 0.0, internal_moves / outgoing_moves) AS touch_ratio
FROM raw_stock_move m
LEFT JOIN raw_stock_location src
    ON m.location_id = src.id AND (src.company_id = m.company_id OR src.company_id = 0)
LEFT JOIN raw_stock_location dst
    ON m.location_dest_id = dst.id AND (dst.company_id = m.company_id OR dst.company_id = 0)
WHERE m.state = 'done'
GROUP BY month, m.company_id, m.product_id;
"""

MART_SQL_STOCKOUT_RISK = """
INSERT INTO mart_stockout_risk
SELECT
    toStartOfMonth(m.date_done) AS month,
    m.company_id,
    m.product_id,
    sum(m.product_uom_qty) AS demand_qty,
    sum(m.quantity_done) AS fulfilled_qty,
    sumIf(m.product_uom_qty - m.quantity_done, m.quantity_done < m.product_uom_qty) AS unmet_qty,
    count() AS move_count,
    countIf(m.quantity_done < m.product_uom_qty) AS stockout_moves,
    if(move_count = 0, 0.0, stockout_moves / move_count) AS stockout_rate
FROM raw_stock_move m
LEFT JOIN raw_stock_location src
    ON m.location_id = src.id AND (src.company_id = m.company_id OR src.company_id = 0)
LEFT JOIN raw_stock_location dst
    ON m.location_dest_id = dst.id AND (dst.company_id = m.company_id OR dst.company_id = 0)
WHERE m.state = 'done'
  AND m.date_done IS NOT NULL
  AND src.usage = 'internal'
  AND dst.usage = 'customer'
GROUP BY month, m.company_id, m.product_id;
"""

MART_SQL_INVENTORY_TURNOVER = """
INSERT INTO mart_inventory_turnover
WITH on_hand AS (
    SELECT
        company_id,
        product_id,
        sum(quantity) AS on_hand_qty
    FROM raw_stock_quant
    LEFT JOIN raw_stock_location l
        ON raw_stock_quant.location_id = l.id
        AND (l.company_id = raw_stock_quant.company_id OR l.company_id = 0)
    WHERE l.usage = 'internal'
    GROUP BY company_id, product_id
)
SELECT
    toStartOfMonth(m.date_done) AS month,
    m.company_id,
    m.product_id,
    sum(m.quantity_done) AS moved_qty,
    sum(m.quantity_done * pt.standard_price) AS moved_value,
    coalesce(on_hand.on_hand_qty, 0) AS on_hand_qty,
    coalesce(on_hand.on_hand_qty, 0) * pt.standard_price AS on_hand_value,
    if(on_hand_value = 0, 0.0, moved_value / on_hand_value) AS turnover_ratio
FROM raw_stock_move m
LEFT JOIN raw_stock_location src
    ON m.location_id = src.id AND (src.company_id = m.company_id OR src.company_id = 0)
LEFT JOIN raw_stock_location dst
    ON m.location_dest_id = dst.id AND (dst.company_id = m.company_id OR dst.company_id = 0)
LEFT JOIN raw_product_product pp
    ON m.product_id = pp.id AND (pp.company_id = m.company_id OR pp.company_id = 0)
LEFT JOIN raw_product_template pt
    ON pp.product_tmpl_id = pt.id AND (pt.company_id = m.company_id OR pt.company_id = 0)
LEFT JOIN on_hand
    ON on_hand.company_id = m.company_id AND on_hand.product_id = m.product_id
WHERE m.state = 'done'
  AND m.date_done IS NOT NULL
  AND src.usage = 'internal'
  AND dst.usage = 'customer'
GROUP BY month, m.company_id, m.product_id, on_hand_qty, pt.standard_price;
"""

MART_SQL_COST_ANOMALIES = """
INSERT INTO mart_cost_anomalies
SELECT
    toDate(now()) AS snapshot_date,
    company_id,
    product_id,
    today_cost AS unit_cost,
    avg_30d_cost,
    if(avg_30d_cost = 0, 0.0, (today_cost - avg_30d_cost) / avg_30d_cost) AS deviation_pct
FROM (
    SELECT
        company_id,
        product_id,
        avgIf(value / quantity, quantity > 0 AND toDate(create_date) = toDate(now())) AS today_cost,
        avgIf(value / quantity, quantity > 0 AND create_date >= now() - INTERVAL 30 DAY) AS avg_30d_cost
    FROM raw_stock_valuation_layer
    GROUP BY company_id, product_id
)
WHERE abs(deviation_pct) > %(anomaly_pct)s;
"""

MART_SQL_ABC = """
INSERT INTO mart_abc_classification
SELECT
    -- Logic: Rank products by total movement value (Cost * Qty) over the last 12 months.
    -- This implements the Pareto Principle (80/20 rule) based on inventory throughput, not sales revenue.
    toDate(now()) AS snapshot_date,
    company_id,
    product_id,
    total_value_moved,
    cumulative_share,
    multiIf(
        cumulative_share <= %(abc_a_pct)s, 'A',
        cumulative_share <= (%(abc_a_pct)s + %(abc_b_pct)s), 'B',
        'C'
    ) AS abc_class
FROM (
    SELECT
        company_id,
        product_id,
        total_value_moved,
        sum(total_value_moved) OVER (
            PARTITION BY company_id
            ORDER BY total_value_moved DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / sum(total_value_moved) OVER (PARTITION BY company_id) AS cumulative_share
    FROM (
        SELECT
            m.company_id AS company_id,
            m.product_id AS product_id,
            sum(m.quantity_done * pt.standard_price) AS total_value_moved
        FROM raw_stock_move m
        LEFT JOIN raw_product_product pp ON m.product_id = pp.id
        LEFT JOIN raw_product_template pt ON pp.product_tmpl_id = pt.id
        WHERE m.state = 'done'
          AND m.date_done >= now() - INTERVAL 12 MONTH
        GROUP BY m.company_id, m.product_id
    )
);
"""

# Demand forecasting uses Python for flexibility; results inserted into mart_demand_forecast.

# -------------------- DAG --------------------


def _get_odoo_config() -> OdooConfig:
    conn = BaseHook.get_connection(ODOO_CONN_ID)
    extras = conn.extra_dejson
    protocol = extras.get("protocol", "jsonrpc")
    api_path = extras.get("api_path", "/jsonrpc")
    if protocol != "jsonrpc":
        logging.warning("Protocol %s requested but JSON-RPC is enforced by design", protocol)
    host = conn.host or ""
    scheme = extras.get("scheme", "https")
    if not host.startswith("http"):
        host = f"{scheme}://{host}"
    port = extras.get("port") or conn.port
    parsed = urlparse(host)
    netloc = parsed.netloc
    if port and ":" not in netloc:
        netloc = f"{netloc}:{port}"
        host = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return OdooConfig(
        url=host,
        db=extras.get("db"),
        username=conn.login,
        password=conn.password,
        api_path=api_path,
    )


def _watermark_key(model: str) -> str:
    return f"odoo_watermark__{model}"


def _normalize_value(field: str, value: Any) -> Any:
    """Normalize Odoo search_read values into ClickHouse-friendly scalars."""
    if isinstance(value, (list, tuple)) and value:
        value = value[0]
    if field in STRING_FIELDS:
        if value is None or value is False:
            return ""
        if not isinstance(value, str):
            return str(value)
    if isinstance(value, bool):
        return int(value)
    if value is None:
        if field.endswith("_id") or field in {"company_id"}:
            return 0
        if field in {"quantity", "qty_done", "product_uom_qty", "quantity_done", "value", "standard_price", "list_price"}:
            return 0.0
        return None
    if field.endswith("_date") or field in {
        "date",
        "date_done",
        "date_deadline",
        "write_date",
        "create_date",
        "date_expected",
        "date_order",
        "date_planned",
    }:
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                try:
                    return datetime.combine(dt_date.fromisoformat(value), datetime.min.time())
                except ValueError:
                    return value
        return value
    return value


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md="""
# Odoo to ClickHouse Analytics

- Extract canonical Odoo entities to ClickHouse raw tables once per run.
- Build marts directly in ClickHouse SQL for Superset consumption.
- Uses incremental watermarks per model stored in Airflow Variables.
""",
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    @task
    def init_clickhouse_tables() -> None:
        client = get_clickhouse_client()
        execute_sql(client, RAW_TABLE_DDL)
        execute_sql(client, MART_TABLE_DDL)

    @task
    def fetch_active_companies() -> List[int]:
        """Fetch list of all active company IDs from Odoo, minus excluded ones."""
        config = _get_odoo_config()
        client = OdooClient(config)
        companies = client.search_read("res.company", [], ["id"], limit=100, offset=0)
        excluded_raw = Variable.get("excluded_company_ids", default_var="[]")
        try:
            excluded_ids = {int(cid) for cid in json.loads(excluded_raw)}
        except (ValueError, json.JSONDecodeError, TypeError):
            excluded_ids = set()
        return [c["id"] for c in companies if c["id"] not in excluded_ids]

    @task
    def extract_model_to_clickhouse(
        model: str,
        fields: List[str],
        domain: List[Any],
        order: str,
        required_fields: List[str],
        company_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Extract incrementally from Odoo API and insert into ClickHouse raw table.

        Uses write_date watermark for idempotent loads and stores it only after load succeeds.
        """
        config = _get_odoo_config()

        # If running per-company, force the Odoo context to that specific company.
        if company_id:
            config.company_id = company_id
            config.allowed_company_ids = [company_id]

        client = OdooClient(config)
        ch_client = get_clickhouse_client()

        watermark_key = _watermark_key(model)
        if company_id:
            watermark_key = f"{watermark_key}_{company_id}"
        full_refresh_raw = Variable.get(VAR_FULL_REFRESH_MODELS, default_var="[]")
        try:
            full_refresh_models = {m for m in json.loads(full_refresh_raw) if isinstance(m, str)}
        except (json.JSONDecodeError, TypeError):
            full_refresh_models = {m.strip() for m in str(full_refresh_raw).split(",") if m.strip()}

        if model in full_refresh_models:
            last_watermark = "1970-01-01 00:00:00"
            incremental_domain = list(domain)
        else:
            last_watermark = Variable.get(watermark_key, default_var="1970-01-01 00:00:00")
            incremental_domain = list(domain)
            incremental_domain.append(["write_date", ">", last_watermark])

        table = RAW_TABLES[model]
        aliases = MODEL_FIELD_ALIASES.get(model, {})
        total_inserted = 0
        max_write_date = last_watermark
        invalid_rows = 0

        for batch in client.paginate(
            model=model,
            domain=incremental_domain,
            fields=fields,
            batch_size=BATCH_SIZE,
            order=order,
        ):
            # Protect Odoo by rate limiting large scans.
            time.sleep(0.2)

            standard_price_overrides: Dict[int, float] = {}
            if model == "product.template":
                template_ids = [rec.get("id") for rec in batch if rec.get("id")]
                standard_price_overrides = _fetch_standard_prices_for_templates(
                    client,
                    company_id,
                    template_ids,
                )

            rows = []
            for rec in batch:
                write_date = rec.get("write_date") or last_watermark
                if write_date > max_write_date:
                    max_write_date = write_date

                # If running per-company, ensure the record is attributed to that company
                # even if Odoo returns False/0 (which happens for shared records like products).
                if company_id and not rec.get("company_id"):
                    rec["company_id"] = company_id

                # Basic data-quality guardrails to avoid null keys and dates in raw tables.
                normalized = {field: _normalize_value(field, rec.get(field)) for field in fields}
                for source_field, target_field in aliases.items():
                    if source_field in normalized:
                        normalized[target_field] = normalized.pop(source_field)
                if model == "product.template":
                    template_id = normalized.get("id")
                    if template_id in standard_price_overrides:
                        normalized["standard_price"] = standard_price_overrides[template_id]
                    if normalized.get("standard_price") in (0, 0.0) and normalized.get("list_price"):
                        normalized["standard_price"] = float(normalized["list_price"]) * 0.7
                if model in full_refresh_models and "write_date" in normalized:
                    # Force a fresh version so ReplacingMergeTree keeps the new values.
                    normalized["write_date"] = datetime.utcnow()
                if any(not normalized.get(field) for field in required_fields):
                    invalid_rows += 1
                    continue
                rows.append(normalized)

            inserted = insert_rows(ch_client, table, rows)
            total_inserted += inserted

        if total_inserted > 0 and model not in full_refresh_models:
            Variable.set(watermark_key, max_write_date)

        logging.info(
            "Model %s (company %s) inserted %s rows, invalid %s rows, watermark %s",
            model,
            company_id,
            total_inserted,
            invalid_rows,
            max_write_date,
        )
        # Keep XCom payloads tiny: only metadata, no row data.
        return {"model": model, "rows": total_inserted, "watermark": max_write_date}

    @task
    def run_mart_sql(sql: str, sql_params: Dict[str, Any]) -> None:
        client = get_clickhouse_client()
        logging.info("Running mart SQL with params %s" % sql_params)
        execute_sql(client, sql, sql_params)

    @task
    def fetch_dead_stock_days() -> int:
        return int(Variable.get(VAR_DEAD_STOCK_DAYS, default_var=90))

    @task
    def fetch_anomaly_pct() -> float:
        return float(Variable.get(VAR_ANOMALY_PCT, default_var=0.2))

    @task
    def fetch_abc_thresholds() -> Dict[str, float]:
        return {
            "abc_a_pct": float(Variable.get(VAR_ABC_A_PCT, default_var=0.80)),
            "abc_b_pct": float(Variable.get(VAR_ABC_B_PCT, default_var=0.15)),
            "abc_c_pct": float(Variable.get(VAR_ABC_C_PCT, default_var=0.05)),
        }

    @task.short_circuit
    def otif_below_threshold() -> bool:
        """Only notify if any vendor falls below the configured OTIF threshold."""
        threshold = float(Variable.get(VAR_OTIF_THRESHOLD, default_var=0.8))
        email = Variable.get(VAR_PROCUREMENT_EMAIL, default_var="")
        if not email:
            logging.info("No procurement email configured; skipping OTIF alert")
            return False
        client = get_clickhouse_client()
        rows = client.execute(
            """
            SELECT min(overall_score)
            FROM mart_vendor_rating
            WHERE month = toStartOfMonth(now())
            """
        )
        if not rows or rows[0][0] is None:
            return False
        return rows[0][0] < threshold

    @task
    def build_demand_forecast() -> int:
        """Forecast next month demand for Class A (or configured scope).

        Uses statsmodels if available; falls back to a moving-average baseline otherwise.
        """
        from collections import defaultdict

        client = get_clickhouse_client()
        scope = Variable.get(VAR_FORECAST_SCOPE, default_var="only_class_a")
        query = """
        SELECT
            m.company_id,
            m.product_id,
            toStartOfMonth(m.date_done) AS month,
            sum(m.quantity_done) AS qty
        FROM raw_stock_move m
        WHERE m.state = 'done'
          AND m.date_done >= now() - INTERVAL 12 MONTH
        GROUP BY company_id, product_id, month
        ORDER BY company_id, product_id, month
        """
        rows = client.execute(query)

        # Optional filter to class A to limit compute.
        class_a_set = set()
        top_n_set = set()
        if scope == "only_class_a":
            class_a_rows = client.execute(
                """
                SELECT company_id, product_id
                FROM mart_abc_classification
                WHERE snapshot_date = toDate(now()) AND abc_class = 'A'
                """
            )
            class_a_set = {(r[0], r[1]) for r in class_a_rows}
        if scope.startswith("top_n:"):
            try:
                n = int(scope.split(":", 1)[1])
            except ValueError:
                n = 0
            if n > 0:
                top_n_rows = client.execute(
                    """
                    SELECT company_id, product_id
                    FROM mart_abc_classification
                    WHERE snapshot_date = toDate(now())
                    ORDER BY total_value_moved DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": n},
                )
                top_n_set = {(r[0], r[1]) for r in top_n_rows}

        series = defaultdict(list)
        for company_id, product_id, month, qty in rows:
            if class_a_set and (company_id, product_id) not in class_a_set:
                continue
            if top_n_set and (company_id, product_id) not in top_n_set:
                continue
            series[(company_id, product_id)].append((month, qty))

        forecast_rows = []
        for (company_id, product_id), values in series.items():
            values = sorted(values, key=lambda x: x[0])
            qtys = [v[1] for v in values]
            if not qtys:
                continue
            forecast_month = (values[-1][0] + timedelta(days=32)).replace(day=1)
            avg = sum(qtys[-3:]) / max(1, len(qtys[-3:]))
            forecast_qty = avg
            try:
                from statsmodels.tsa.holtwinters import ExponentialSmoothing

                if len(qtys) >= 4:
                    model = ExponentialSmoothing(qtys, trend="add", seasonal=None, initialization_method="estimated")
                    fit = model.fit()
                    forecast_qty = float(fit.forecast(1)[0])
            except Exception as exc:
                logging.info("Statsmodels unavailable or failed (%s); using moving average", exc)
            forecast_rows.append(
                {
                    "forecast_month": forecast_month,
                    "company_id": company_id,
                    "product_id": product_id,
                    "forecast_qty": float(forecast_qty),
                    "lower_ci": float(forecast_qty * 0.85),
                    "upper_ci": float(forecast_qty * 1.15),
                }
            )

        inserted = insert_rows(client, "mart_demand_forecast", forecast_rows)
        logging.info("Inserted %s demand forecast rows", inserted)
        return inserted

    @task
    def reverse_etl_cost_anomalies() -> int:
        """Create Helpdesk tickets for cost anomalies; dedupe by product/day.

        If your Odoo model differs (helpdesk.ticket or mail.message), adjust here.
        """
        config = _get_odoo_config()
        client = OdooClient(config)
        ch_client = get_clickhouse_client()
        team_id = Variable.get(VAR_HELPDESK_TEAM_ID, default_var=None)
        if not team_id:
            logging.info("No helpdesk team configured; skipping ticket creation")
            return 0

        rows = ch_client.execute(
            """
            SELECT company_id, product_id, deviation_pct
            FROM mart_cost_anomalies
            WHERE snapshot_date = toDate(now())
            """
        )

        created = 0
        for company_id, product_id, deviation_pct in rows:
            subject = f"COGS anomaly product {product_id}"
            description = (
                f"Check item {product_id}. Cost deviated {deviation_pct:.2%} from 30-day average."
            )
            # Dedupe: check for existing ticket by name and day.
            existing = client.search_read(
                "helpdesk.ticket",
                [["name", "=", subject], ["create_date", ">=", datetime.utcnow().strftime("%Y-%m-%d")]],
                ["id"],
                limit=1,
                offset=0,
                order="id desc",
            )
            if existing:
                continue
            values = {
                "name": subject,
                "description": description,
                "team_id": int(team_id),
            }
            client.create("helpdesk.ticket", values)
            created += 1
            time.sleep(0.2)

        logging.info("Created %s helpdesk tickets", created)
        return created

    @task
    def reverse_etl_abc_classification() -> int:
        """Update Odoo product.template with ABC classification in batches."""
        config = _get_odoo_config()
        client = OdooClient(config)
        ch_client = get_clickhouse_client()

        rows = ch_client.execute(
            """
            SELECT pp.product_tmpl_id, a.abc_class
            FROM mart_abc_classification a
            LEFT JOIN raw_product_product FINAL pp ON a.product_id = pp.id
            WHERE a.snapshot_date = toDate(now())
            """
        )

        updated = 0
        for product_tmpl_id, abc_class in rows:
            if not product_tmpl_id:
                continue
            client.write("product.template", [int(product_tmpl_id)], {"x_abc_classification": abc_class})
            updated += 1
            time.sleep(0.1)

        logging.info("Updated %s product.template ABC classifications", updated)
        return updated

    with TaskGroup(group_id="extract_to_clickhouse_tg") as extract_to_clickhouse_tg:
        # Extract once from Odoo into raw ClickHouse tables; all marts reuse these tables.
        init_clickhouse_tables_task = init_clickhouse_tables()
        companies = fetch_active_companies()

        extract_stock_move_line = extract_model_to_clickhouse.partial(
            model="stock.move.line",
            fields=[
                "id",
                "company_id",
                "product_id",
                "quantity",
                "location_id",
                "location_dest_id",
                "state",
                "date",
                "write_date",
                "create_date",
            ],
            domain=[["state", "=", "done"]],
            order="write_date asc",
            required_fields=["id", "product_id", "date_done", "write_date"],
        ).expand(company_id=companies)

        extract_stock_quant = extract_model_to_clickhouse.partial(
            model="stock.quant",
            fields=[
                "id",
                "company_id",
                "product_id",
                "location_id",
                "quantity",
                "reserved_quantity",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "product_id", "write_date"],
        ).expand(company_id=companies)

        extract_stock_move = extract_model_to_clickhouse.partial(
            model="stock.move",
            fields=[
                "id",
                "company_id",
                "product_id",
                "product_uom_qty",
                "quantity",
                "location_id",
                "location_dest_id",
                "state",
                "date_deadline",
                "date",
                "origin",
                "write_date",
                "create_date",
            ],
            domain=[["state", "=", "done"]],
            order="write_date asc",
            required_fields=["id", "product_id", "date_done", "write_date"],
        ).expand(company_id=companies)

        extract_purchase_order = extract_model_to_clickhouse.partial(
            model="purchase.order",
            fields=[
                "id",
                "company_id",
                "partner_id",
                "name",
                "date_order",
                "date_planned",
                "state",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "partner_id", "date_order", "write_date"],
        ).expand(company_id=companies)

        extract_product_product = extract_model_to_clickhouse.partial(
            model="product.product",
            fields=[
                "id",
                "company_id",
                "product_tmpl_id",
                "default_code",
                "active",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "product_tmpl_id", "write_date"],
        ).expand(company_id=companies)

        extract_product_template = extract_model_to_clickhouse.partial(
            model="product.template",
            fields=[
                "id",
                "company_id",
                "name",
                "standard_price",
                "list_price",
                "type",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "name", "write_date"],
        ).expand(company_id=companies)

        extract_stock_valuation_layer = extract_model_to_clickhouse.partial(
            model="stock.valuation.layer",
            fields=[
                "id",
                "company_id",
                "product_id",
                "quantity",
                "value",
                "stock_move_id",
                "create_date",
                "write_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "product_id", "create_date", "write_date"],
        ).expand(company_id=companies)

        extract_stock_location = extract_model_to_clickhouse.partial(
            model="stock.location",
            fields=[
                "id",
                "company_id",
                "name",
                "usage",
                "write_date",
                "create_date",
            ],
            domain=[],
            order="write_date asc",
            required_fields=["id", "name", "write_date"],
        ).expand(company_id=companies)

        extract_res_partner = extract_model_to_clickhouse.partial(
            model="res.partner",
            fields=[
                "id",
                "company_id",
                "name",
                "supplier_rank",
                "write_date",
                "create_date",
            ],
            domain=[["supplier_rank", ">", 0]],
            order="write_date asc",
            required_fields=["id", "name", "write_date"],
        ).expand(company_id=companies)

        init_clickhouse_tables_task >> [
            extract_stock_move_line,
            extract_stock_quant,
            extract_stock_move,
            extract_purchase_order,
            extract_product_product,
            extract_product_template,
            extract_stock_valuation_layer,
            extract_stock_location,
            extract_res_partner,
        ]

    with TaskGroup(group_id="dead_stock_inventory_health_tg") as dead_stock_inventory_health_tg:
        dead_stock_days = fetch_dead_stock_days()
        load_dead_stock = run_mart_sql(MART_SQL_LIQUIDATION, {"dead_stock_days": dead_stock_days})

    with TaskGroup(group_id="otif_vendor_scorecard_tg") as otif_vendor_scorecard_tg:
        load_vendor_scorecard = run_mart_sql(MART_SQL_VENDOR, {})
        # should_notify = otif_below_threshold()
        # notify_procurement = EmailOperator(
        #     task_id="notify_procurement",
        #     email_on_failure=False,
        #     email_on_retry=False,
        #     to=Variable.get(VAR_PROCUREMENT_EMAIL, default_var=""),
        #     subject="OTIF score below threshold",
        #     html_content="OTIF score below threshold for latest month. Please review in Superset.",
        #     conn_id=EMAIL_CONN_ID,
        #     trigger_rule=TriggerRule.ALL_DONE,
        # )
        # load_vendor_scorecard >> should_notify >> notify_procurement

    with TaskGroup(group_id="warehouse_efficiency_tg") as warehouse_efficiency_tg:
        load_touch_ratio = run_mart_sql(MART_SQL_TOUCH_RATIO, {})

    with TaskGroup(group_id="stockout_risk_tg") as stockout_risk_tg:
        load_stockout_risk = run_mart_sql(MART_SQL_STOCKOUT_RISK, {})

    with TaskGroup(group_id="inventory_turnover_tg") as inventory_turnover_tg:
        load_inventory_turnover = run_mart_sql(MART_SQL_INVENTORY_TURNOVER, {})

    with TaskGroup(group_id="margin_cogs_anomaly_tg") as margin_cogs_anomaly_tg:
        anomaly_pct = fetch_anomaly_pct()
        load_anomalies = run_mart_sql(MART_SQL_COST_ANOMALIES, {"anomaly_pct": anomaly_pct})
        # create_tickets = reverse_etl_cost_anomalies()
        # load_anomalies >> create_tickets

    with TaskGroup(group_id="demand_forecast_abc_tg") as demand_forecast_abc_tg:
        abc_thresholds = fetch_abc_thresholds()
        load_abc = run_mart_sql(MART_SQL_ABC, abc_thresholds)
        forecast = build_demand_forecast()
        # update_abc = reverse_etl_abc_classification()
        # load_abc >> [forecast, update_abc]
        load_abc >> [forecast]

    # Superset cache refresh placeholder; wire auth via connection extras or custom headers as needed.
    refresh_superset = EmptyOperator(task_id="refresh_superset_cache", trigger_rule=TriggerRule.ALL_DONE)

    start >> extract_to_clickhouse_tg
    extract_to_clickhouse_tg >> [
        dead_stock_inventory_health_tg,
        otif_vendor_scorecard_tg,
        warehouse_efficiency_tg,
        stockout_risk_tg,
        inventory_turnover_tg,
        margin_cogs_anomaly_tg,
        demand_forecast_abc_tg,
    ]
    [
        dead_stock_inventory_health_tg,
        otif_vendor_scorecard_tg,
        warehouse_efficiency_tg,
        stockout_risk_tg,
        inventory_turnover_tg,
        margin_cogs_anomaly_tg,
        demand_forecast_abc_tg,
    ] >> refresh_superset
    refresh_superset >> end

if __name__ == "__main__":
    # This allows you to run 'python odoo_clickhouse_inventory_analytics.py' to test locally
    dag.test()
