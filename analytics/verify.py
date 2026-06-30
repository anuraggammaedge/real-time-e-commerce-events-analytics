import duckdb

conn = duckdb.connect("analytics/analytics.duckdb")

print("\nTables")
print(
    conn.execute(
        "SHOW TABLES"
    ).fetchdf()
)

print("\nEvents Per Minute")

print(
    conn.execute("""
    DESCRIBE events_per_minute
    """).fetchdf()
) 

print(
    conn.execute(
        """
        SELECT *
        FROM events_per_minute
        ORDER BY window_start
        LIMIT 10
        """
    ).fetchdf()
)

print("\nCategory Metrics")

print(
    conn.execute(
        """
        DESCRIBE category_metrics
        """
    ).fetchdf()
)

print(
    conn.execute(
        """
        SELECT *
        FROM category_metrics
        ORDER BY window_start, category
        LIMIT 10
        """
    ).fetchdf()
)

print("\nCategory Totals")

print(
    conn.execute(
        """
        SELECT
            category,
            SUM(count) AS total_events
        FROM category_metrics
        GROUP BY category
        ORDER BY total_events DESC
        """
    ).fetchdf()
)

conn.close()