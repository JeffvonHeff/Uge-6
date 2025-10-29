"""Entry point for the simple ETL pipeline."""
from __future__ import annotations

from Extract import extract_all
from Load import (
    build_connection_from_env,
    check_connection,
    load_dataframes,
    set_postgres_env,
)
from Transform import transform_dataframes


def run_pipeline() -> None:
    """Execute the ETL pipeline."""

    print("Starting extraction...")
    extracted = extract_all()
    print("Extraction complete.")

    print("Transforming data...")
    transformed = transform_dataframes(extracted)
    print("Transformation complete.")

    # Ensure sensible defaults so the demo pipeline can connect without manual setup.
    set_postgres_env()

    try:
        connection = build_connection_from_env()
    except ValueError as error:
        print(
            "Skipping load step because the database configuration is incomplete.\n"
            f"Details: {error}"
        )
        return

    if not check_connection(connection):
        print("Database connection test failed; aborting load step.")
        connection.close()
        return

    print("Loading data into PostgreSQL...")
    try:
        load_dataframes(connection, transformed)
        print("Load step finished.")
    finally:
        connection.close()


if __name__ == "__main__":
    run_pipeline()
