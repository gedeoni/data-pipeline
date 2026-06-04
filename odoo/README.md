# Odoo 17 Inventory + Stock Movement Seeder

Production-quality Python seeder that generates realistic inventory master data and **180 days** of stock movement history and loads it into **Odoo 17** via **JSON-RPC**.

This dataset is designed for downstream ETL (e.g., Airflow) to compute stockout risk and reorder recommendations.

## Requirements

### Odoo

- Odoo version: **17** (local)
- Base URL: `http://localhost:8069`
- Database: `odoo`
- User: `odoo`
- Password: `odoo`
- Multi-company enabled with 3 companies: **Rwanda**, **Uganda**, **Kenya**

### Required Odoo modules

- Required: Inventory (`stock`)
- Optional (Required for `--orders`): Purchase (`purchase`), Sales (`sale`), Sales Stock (`sale_stock`), Purchase Stock (`purchase_stock`)

By default, the script uses **stock pickings** directly. If using `--orders`, full Purchase/Sales workflows are used.

### Python

- Python 3.10+ recommended

**Using `uv` (Recommended)**:
Simply run the following command to create the virtual environment and install all dependencies:
```bash
uv sync
```

**Using standard `pip`**:
Create virtual env and install:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## What it generates

### 1) Geography → Warehouses/Locations

Creates warehouses and internal locations per company:

- Rwanda: Districts → Warehouses (30; scale can use a subset)
- Kenya: Counties → Warehouses (47; scale can use a subset)
- Uganda: Districts → Warehouses (seed list; scale can use a subset)

For each warehouse, creates base-unit locations (synthetic, deterministic seed list by default; `--full-geo` increases density). For each base unit, creates 3 internal locations under the warehouse view location:

- `$WAREHOUSE-GOOD-$BASEUNIT`
- `$WAREHOUSE-TRANSIT-$BASEUNIT`
- `$WAREHOUSE-DAMAGED-$BASEUNIT`

Names are normalized using `slugify()` (uppercased, spaces to `_`, punctuation removed).

### 2) Master data

Creates/reuses:

- 6 product categories: Seeds, Fertilizer, Pesticides, Tools, Spare Parts, Packaging
- ~80–120 products total (category mix)
  - Unique `default_code` SKU
  - UoM: Seeds/Fertilizer mostly `kg`, Tools mostly `Unit(s)`
  - Realistic `standard_price` + `list_price` ranges by category
- Vendors: 5–10 vendors per country
  - Preferred vendor mapping per category via `product.supplierinfo`

### 3) Realistic seasonality & anomalies

Demand is not random noise; it uses:

- Two seasonal pulses per country (approx patterns)
- Category-specific lag + multiplier curves
- Weekday operational effects
- Cross-warehouse variability (small/medium/large throughput + SKU activity)
- Explainable anomalies (printed when injected):
  - Supplier delays for inbound receipts
  - Demand spikes
  - Shrinkage event (elevated damaged transfers)
  - Controlled stockout pressure window (outbound exceeds inbound for selected SKUs)

### 4) Movements (proper workflows)

Generates movements via correct Odoo stock workflows (confirm/assign/validate):

- Inbound receipts: Vendor → GOOD (using warehouse incoming picking type)
- Internal transfers: GOOD → TRANSIT → GOOD (using internal picking type)
- Damage/shrinkage: GOOD → DAMAGED (using internal picking type)
- Outbound consumption/sales: GOOD → Customer/outgoing (using outgoing picking type)

### 5) Orders (Optional via `--orders`)

Instead of direct stock movements, generates full business documents:
- **Purchase Orders**: Created, confirmed, and received after a random lead time.
- **Sales Orders**: Created, confirmed, and delivered.
This mode is useful for analyzing **Vendor Lead Time** and **Order-to-Delivery** metrics.

Pickings are created with realistic `scheduled_date` and are validated so quants update.

## How to run

### 1) Start Odoo

Ensure Odoo 17 is running locally and you can log in to the database.

### 2) Install Python deps

**Using `uv`**:
```bash
uv sync
```

**Using `pip`**:
```bash
pip install -r requirements.txt
```

### Seeding modes (defaults and overrides)

Default behavior:

- If you do not pass any mode flags, the script seeds **movements only** for `--days`.
- If you pass `--orders`:
  - `--days < 100`: **orders only**
  - `--days >= 100`: **partitioned**, with orders on the most recent half and movements on the older half (no overlap).
- Overrides:
  - `--orders-only`: always orders only (no partitioning)
  - `--movements-only`: always movements only (no partitioning)

Examples:

```bash
python3 main.py --scale medium --days 180 --countries rw,ug,ke
```

```bash
python3 main.py --orders --scale medium --days 180 --countries rw,ug,ke
```

```bash
python3 main.py --orders-only --scale medium --days 180 --countries rw,ug,ke
```

```bash
python3 main.py --movements-only --scale medium --days 180 --countries rw,ug,ke
```

### 3) Seed data into Odoo

Example (medium scale, 180 days, all 3 countries):

```bash
python3 main.py --scale medium --days 180 --countries rw,ug,ke
```

### Other useful options

- Full(er) location density per warehouse:

```bash
python3 main.py --scale large --days 180 --full-geo
```

- Dry-run (no Odoo API calls; still generates CSV + logs anomalies):

```bash
python3 main.py --dry-run --scale medium --days 180
```

- Custom output directory for CSV:

```bash
python3 main.py --out-dir ./seed_output --scale medium --days 180
```


```bash
python3 main.py --scale large --user odoo@gmail.com --password odoo --days 180 --full-geo --countries rw,ug,ke
```

Connection overrides (if needed):

```bash
python3 main.py --base-url http://localhost:8069 --db odoo --user odoo --password odoo
```

## Outputs

### CSV

Written to `--out-dir` (default `./seed_output`), per company:

- `pickings_<country>_<dataset_key>.csv`
- `moves_<country>_<dataset_key>.csv`

### Console summary

Per company:

- Picking counts by type (`IN`, `INT`, `DMG`, `OUT`)
- Top outbound SKUs
- Lowest days-of-cover SKUs (approx)
- Anomaly events injected (supplier delay, spikes, shrinkage, controlled stockout)

## Idempotency

- Master data is idempotent: the script searches and reuses existing records before creating new ones.
- Movements are idempotent *per dataset key*: pickings are keyed by a deterministic `origin` string.
  - Rerunning on the same day with the same `--days` produces the same `dataset_key` and will not duplicate pickings.
  - Running on a different day changes the `dataset_key` and creates a new 180-day window.

## Code layout

- `services/odoo_client.py` — JSON-RPC client with retries + helpers
- `services/geo_data.py` — geography seeds + normalization utilities
- `services/seasonality.py` — seasonality engine (multipliers, lags, weekday patterns)
- `services/seed_master.py` — companies, warehouses, locations, products, vendors
- `services/seed_movements.py` — pickings/moves generation + validation + CSV outputs
- `main.py` — CLI entrypoint
- `requirements.txt` — dependencies
