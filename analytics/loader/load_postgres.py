from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

from deltalake import DeltaTable


# ------------------------------------------------------------------
# Project Paths
# ------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DELTA_DIR = PROJECT_ROOT / "delta"


# ------------------------------------------------------------------
# PostgreSQL Connection
# ------------------------------------------------------------------

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="ecommerce",
    user="admin",
    password="admin",
)

cursor = conn.cursor()

print("Connected to PostgreSQL")


# ------------------------------------------------------------------
# Delta Tables
# ------------------------------------------------------------------
print(f"Project Root : {PROJECT_ROOT}")
print(f"Delta Folder : {DELTA_DIR}")

assert DELTA_DIR.exists(), f"{DELTA_DIR} does not exist"

TABLES = {
    "events_per_minute": DELTA_DIR / "gold_events_per_minute",
    "category_metrics": DELTA_DIR / "gold_category_metrics",
}


# ------------------------------------------------------------------
# Load each Delta table
# ------------------------------------------------------------------

for table_name, delta_path in TABLES.items():

    print(f"\nLoading {table_name}")

    dt = DeltaTable(str(delta_path))

    arrow_table = dt.to_pyarrow_table()

    records = arrow_table.to_pylist()

    print(f"Rows to load: {len(records):,}")

    cursor.execute(f"TRUNCATE TABLE {table_name}")

    if table_name == "events_per_minute":

        rows = [
            (
                row["window"]["start"],
                row["window"]["end"],
                row["count"],
            )
            for row in records
        ]

        execute_values(
            cursor,
            """
            INSERT INTO events_per_minute
            (
                window_start,
                window_end,
                count
            )
            VALUES %s
            """,
            rows,
            page_size=10000,
        )

    else:

        rows = [
            (
                row["window"]["start"],
                row["window"]["end"],
                row["category"],
                row["count"],
            )
            for row in records
        ]

        execute_values(
            cursor,
            """
            INSERT INTO category_metrics
            (
                window_start,
                window_end,
                category,
                count
            )
            VALUES %s
            """,
            rows,
            page_size=10000,
        )

    conn.commit()

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")

    total = cursor.fetchone()[0]

    print(f"Loaded {total:,} rows")


print("\nFinished loading Gold layer into PostgreSQL.")

cursor.close()
conn.close()