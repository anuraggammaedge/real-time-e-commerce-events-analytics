import duckdb
from pathlib import Path
from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DELTA_DIR = PROJECT_ROOT / "delta"

DUCK_DB = PROJECT_ROOT / "analytics" / "analytics.duckdb"


conn = duckdb.connect(str(DUCK_DB))

print("Connected to DuckDB")


TABLES = {
    "events_per_minute": {
        "path": DELTA_DIR / "gold_events_per_minute",
        "query": """
            CREATE OR REPLACE TABLE events_per_minute AS
            SELECT
                "window".start AS window_start,
                "window".end   AS window_end,
                count
            FROM tmp_table
        """
    },

    "category_metrics": {
        "path": DELTA_DIR / "gold_category_metrics",
        "query": """
            CREATE OR REPLACE TABLE category_metrics AS
            SELECT
                "window".start AS window_start,
                "window".end   AS window_end,
                category,
                count
            FROM tmp_table
        """
    }
}

for table_name, config in TABLES.items():
    print(f"\n Loading {table_name}")

    dt = DeltaTable(str(config["path"])) 

    arrow_table = dt.to_pyarrow_table()

    conn.register("tmp_table", arrow_table)

    print(conn.execute("DESCRIBE tmp_table").fetchdf())

    conn.execute(config["query"])

    rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    print(f"Loaded {rows:,} rows")

print("\nFinished.")

conn.close()
