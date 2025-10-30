"""A tiny ETL pipeline that keeps every step as simple as possible."""

from __future__ import annotations

from Extract import extract_data
from Load import create_connection, load_core_tables, load_order_summary
from Transform import build_order_summary, prepare_relational_tables


def run_pipeline() -> None:
    """Run the three ETL steps and print friendly progress messages."""

    print("Step 1: Extracting the CSV files...")
    data = extract_data()

    print("Step 2: Preparing relational tables and the order summary...")
    tables = prepare_relational_tables(data)
    summary = build_order_summary(tables)
    print(summary.head())

    print("Step 3: Saving everything to PostgreSQL...")
    connection = create_connection()
    try:
        load_core_tables(connection, tables)
        load_order_summary(connection, summary)
    finally:
        connection.close()

    print("All done! You can now explore the order_summary table in PostgreSQL.")


if __name__ == "__main__":
    run_pipeline()
