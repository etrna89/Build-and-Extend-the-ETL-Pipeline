"""Sari-sari store sales ETL.

Landing CSV -> raw data lake -> cleaned Parquet -> PostgreSQL star schema.
"""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

LANDING = Path("/opt/airflow/data/landing")
RAW = Path("/opt/airflow/datalake/raw/sales")
STAGED = Path("/opt/airflow/datalake/staged/sales")
DWH_CONN_ID = "dwh_postgres"

STG_COLUMNS = [
    "order_id",
    "order_date",
    "store_id",
    "store_name",
    "barangay",
    "city",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "line_total",
    "batch_id",
]

LOAD_DW_SQL = """
INSERT INTO dw.dim_date (
    date_key, full_date, day_of_week, is_weekend, month, month_name, quarter, year
)
SELECT DISTINCT
    TO_CHAR(order_date, 'YYYYMMDD')::INT,
    order_date,
    TRIM(TO_CHAR(order_date, 'Day')),
    EXTRACT(ISODOW FROM order_date) IN (6, 7),
    EXTRACT(MONTH FROM order_date)::INT,
    TRIM(TO_CHAR(order_date, 'Month')),
    EXTRACT(QUARTER FROM order_date)::INT,
    EXTRACT(YEAR FROM order_date)::INT
FROM staging.stg_sales
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO dw.dim_store (store_id, store_name, barangay, city)
SELECT DISTINCT ON (store_id) store_id, store_name, barangay, city
FROM staging.stg_sales
ORDER BY store_id
ON CONFLICT (store_id) DO UPDATE
SET store_name = EXCLUDED.store_name,
    barangay = EXCLUDED.barangay,
    city = EXCLUDED.city;

INSERT INTO dw.dim_product (product_id, product_name, category)
SELECT DISTINCT ON (product_id) product_id, product_name, category
FROM staging.stg_sales
ORDER BY product_id
ON CONFLICT (product_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    category = EXCLUDED.category;

INSERT INTO dw.fact_sales (
    order_id, date_key, store_key, product_key, quantity, unit_price, line_total, batch_id
)
SELECT
    s.order_id,
    TO_CHAR(s.order_date, 'YYYYMMDD')::INT,
    ds.store_key,
    dp.product_key,
    s.quantity,
    s.unit_price,
    s.line_total,
    s.batch_id
FROM staging.stg_sales AS s
JOIN dw.dim_store AS ds ON ds.store_id = s.store_id
JOIN dw.dim_product AS dp ON dp.product_id = s.product_id
ON CONFLICT (order_id, product_key) DO UPDATE
SET date_key = EXCLUDED.date_key,
    store_key = EXCLUDED.store_key,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    line_total = EXCLUDED.line_total,
    batch_id = EXCLUDED.batch_id;
"""


@dag(
    dag_id="sarisari_sales_etl",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["cselec1c", "etl"],
)
def sarisari_sales_etl():
    @task
    def extract() -> dict:
        """Copy each landing file unchanged into a uniquely named raw batch."""
        files = sorted(LANDING.glob("sales_*.csv"))
        if not files:
            raise FileNotFoundError(f"No sales_*.csv files found in {LANDING}")

        batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        target = RAW / f"ingest_date={batch_id[:8]}" / f"batch={batch_id}"
        target.mkdir(parents=True, exist_ok=True)

        copied = []
        for source in files:
            destination = target / source.name
            shutil.copy2(source, destination)
            copied.append(str(destination))

        print(f"Copied {len(copied)} file(s) to {target}")
        return {"batch_id": batch_id, "files": copied}

    @task
    def transform(raw: dict) -> dict:
        """Clean and type raw rows, then write the batch to staged Parquet."""
        frames = [pd.read_csv(path, dtype=str) for path in raw["files"]]
        df = pd.concat(frames, ignore_index=True)
        rows_in = len(df)

        expected_columns = {
            "order_id",
            "order_date",
            "store_id",
            "store_name",
            "barangay",
            "city",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
        }
        missing_columns = expected_columns.difference(df.columns)
        if missing_columns:
            raise ValueError(f"Sales CSV is missing columns: {sorted(missing_columns)}")

        df = df.apply(lambda column: column.str.strip())
        df["category"] = df["category"].str.title()
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

        required = ["order_id", "order_date", "store_id", "product_id", "quantity", "unit_price"]
        missing_rows = int(df[required].isna().any(axis=1).sum())
        missing_prices = int(df["unit_price"].isna().sum())
        complete = df.dropna(subset=required)
        invalid_values = (complete["quantity"] <= 0) | (complete["unit_price"] < 0)
        invalid_rows = int(invalid_values.sum())
        valid = complete.loc[~invalid_values]
        duplicate_rows = int(valid.duplicated(subset=["order_id", "product_id"]).sum())
        df = valid.drop_duplicates(subset=["order_id", "product_id"], keep="first")

        df["quantity"] = df["quantity"].astype(int)
        df["line_total"] = (df["quantity"] * df["unit_price"]).round(2)
        df["batch_id"] = raw["batch_id"]

        output = STAGED / f"batch={raw['batch_id']}" / "sales_clean.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        df[STG_COLUMNS].to_parquet(output, index=False)

        rejected = rows_in - len(df)
        print(
            f"Rows in: {rows_in}, rows kept: {len(df)}, rejected: {rejected} "
            f"(missing required fields: {missing_rows}; missing unit price: "
            f"{missing_prices}; invalid quantity/price: {invalid_rows}; "
            f"duplicate order/product: {duplicate_rows})"
        )
        return {
            "batch_id": raw["batch_id"],
            "path": str(output),
            "rows_in": rows_in,
            "rows_out": len(df),
        }

    @task
    def load(staged: dict) -> dict:
        """Load staged Parquet through staging into the warehouse star schema."""
        df = pd.read_parquet(staged["path"])
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

        hook.run("TRUNCATE staging.stg_sales;")
        rows = df[STG_COLUMNS].astype(object).itertuples(index=False, name=None)
        hook.insert_rows(
            table="staging.stg_sales",
            rows=rows,
            target_fields=STG_COLUMNS,
            commit_every=1000,
        )
        hook.run(LOAD_DW_SQL)

        fact_rows = hook.get_first("SELECT COUNT(*) FROM dw.fact_sales")[0]
        print(f"Loaded {len(df)} staged rows; fact_sales now has {fact_rows} rows")
        return {"batch_id": staged["batch_id"], "rows_loaded": len(df)}

    @task(retries=0)
    def quality_check(loaded: dict) -> None:
        """Fail the run if warehouse invariants are broken."""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        checks = {
            "fact table is not empty": (
                "SELECT COUNT(*) > 0 FROM dw.fact_sales",
                None,
            ),
            "every staged row reached the fact table": (
                "SELECT (SELECT COUNT(*) FROM staging.stg_sales) = "
                "(SELECT COUNT(*) FROM dw.fact_sales WHERE batch_id = %s)",
                (loaded["batch_id"],),
            ),
            "no non-positive quantities": (
                "SELECT COUNT(*) = 0 FROM dw.fact_sales WHERE quantity <= 0",
                None,
            ),
            "line_total equals quantity x unit_price": (
                "SELECT COUNT(*) = 0 FROM dw.fact_sales "
                "WHERE line_total <> ROUND(quantity * unit_price, 2)",
                None,
            ),
            "no batch quantity exceeds 100 units": (
                "SELECT COUNT(*) = 0 FROM dw.fact_sales "
                "WHERE batch_id = %s AND quantity > 100",
                (loaded["batch_id"],),
            ),
            "no sale date in the future": (
                "SELECT COUNT(*) = 0 FROM dw.fact_sales AS f "
                "JOIN dw.dim_date AS d USING (date_key) "
                "WHERE f.batch_id = %s AND d.full_date > CURRENT_DATE",
                (loaded["batch_id"],),
            ),
        }
        failed = [
            name
            for name, (sql, parameters) in checks.items()
            if not hook.get_first(sql, parameters=parameters)[0]
        ]
        if failed:
            raise ValueError(f"Data quality checks failed: {failed}")
        print(f"All {len(checks)} checks passed for batch {loaded['batch_id']}")

    quality_check(load(transform(extract())))


sarisari_sales_etl()
