import psycopg2
from config import (
    POSTGRES_CONFIG,
    TABLES,
)
from deltalake import DeltaTable
from psycopg2.extras import execute_values


def get_connection():

    conn = psycopg2.connect(**POSTGRES_CONFIG)

    print("Connected to PostgreSQL")

    return conn


def read_delta_table(delta_path):

    print(f"Reading Delta Table : {delta_path}")

    dt = DeltaTable(str(delta_path))

    arrow_table = dt.to_pyarrow_table()

    records = arrow_table.to_pylist()

    for record in records:
        print(record["window"]["end"])

    print(f"Rows Found : {len(records):,}")

    return records


def get_last_loaded_timestamp(cursor, table_name):

    query = f"""
        SELECT MAX(window_end)
        FROM {table_name}
    """

    cursor.execute(query)

    last_timestamp = cursor.fetchone()[0]

    print(f"Last Loaded Timestamp : {last_timestamp}")

    return last_timestamp


def filter_new_records(records, last_timestamp):
    if last_timestamp is None:
        print("First load detected. Loading all records.")

        return records

    new_records = [
        record
        for record in records
        if record["window"]["end"].replace(tzinfo=None) > last_timestamp
    ]

    print(f"New Records : {len(new_records):,}")

    return new_records


def build_rows(table_name, records):

    rows = []

    for record in records:
        window_start = record["window"]["start"].replace(tzinfo=None)
        window_end = record["window"]["end"].replace(tzinfo=None)

        if table_name == "events_per_minute":
            rows.append(
                (
                    window_start,
                    window_end,
                    record["count"],
                )
            )

        elif table_name == "category_metrics":
            rows.append(
                (
                    window_start,
                    window_end,
                    record["category"],
                    record["count"],
                )
            )

    print(f"Rows Prepared : {len(rows):,}")

    return rows


def insert_rows(conn, cursor, table_name, table_config, rows):

    if not rows:
        print("No new rows to insert.")
        return

    execute_values(
        cursor,
        table_config["insert_sql"],
        rows,
        page_size=1000,
    )

    conn.commit()

    print(f"Inserted {len(rows):,} rows into {table_name}")


def main():

    conn = get_connection()

    cursor = conn.cursor()

    print()

    print("Gold Tables")

    print("-" * 40)

    for table_name, table_config in TABLES.items():
        print(f"\nLoading : {table_name}")

        last_timestamp = get_last_loaded_timestamp(cursor, table_name)
        print(type(last_timestamp))
        print(last_timestamp.tzinfo)
        records = read_delta_table(table_config["delta_path"])
        records = filter_new_records(records, last_timestamp)
        rows = build_rows(
            table_name,
            records,
        )
        insert_rows(
            conn,
            cursor,
            table_name,
            table_config,
            rows,
        )

        print(rows[:2])

        print(f"First Record : {records[0] if records else 'No Records'}")

        print("-" * 60)

    cursor.close()

    conn.close()

    print("\nDone")


if __name__ == "__main__":
    main()
