"""Entry point for the simple ETL pipeline."""
from __future__ import annotations

from Extract import extract_all
from Load import build_connection_from_env, load_dataframes
from Transform import transform_dataframes


def run_pipeline() -> None:
    """Execute the ETL pipeline."""

    extracted = extract_all()
    transformed = transform_dataframes(extracted)

    try:
        connection = build_connection_from_env()
    except ValueError as error:
        print(
            "Skipping load step because the database configuration is incomplete.\n"
            f"Details: {error}"
        )
        return

    try:
        load_dataframes(connection, transformed)
    finally:
        connection.close()


if __name__ == "__main__":
    run_pipeline()
