from pathlib import Path

# ------------------------------------------------------------------
# Project Paths
# ------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DELTA_DIR = PROJECT_ROOT / "delta"

# ------------------------------------------------------------------
# PostgreSQL Configuration
# ------------------------------------------------------------------

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "ecommerce",
    "user": "admin",
    "password": "admin",
}

# ------------------------------------------------------------------
# Gold Tables
# ------------------------------------------------------------------

TABLES = {
    "events_per_minute": {
        "delta_path": DELTA_DIR / "gold_events_per_minute",
        "columns": [
            "window_start",
            "window_end",
            "count",
        ],
        "insert_sql": """
            INSERT INTO events_per_minute
            (
                window_start,
                window_end,
                count
            )
            VALUES %s
        """,
    },
    "category_metrics": {
        "delta_path": DELTA_DIR / "gold_category_metrics",
        "columns": [
            "window_start",
            "window_end",
            "category",
            "count",
        ],
        "insert_sql": """
            INSERT INTO category_metrics
            (
                window_start,
                window_end,
                category,
                count
            )
            VALUES %s
        """,
    },
}