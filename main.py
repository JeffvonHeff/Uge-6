"""Entry point for the simple ETL pipeline."""

from __future__ import annotations

import argparse
from typing import Iterable, Optional

from Extract import extract_all
from Load import (
    build_connection_from_env,
    check_connection,
    load_dataframes,
    set_postgres_env,
)
from Transform import transform_dataframes
from postgres_env import (
    list_tables,
    postgres_connection,
    table_preview,
    table_row_count,
)
import pandas as pd


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

    print("\nDatabase snapshot after load:")
    command_overview(argparse.Namespace(limit=5))


def _ensure_env_defaults() -> None:
    """Populate any missing Postgres environment variables before connecting."""

    set_postgres_env()


def command_run(_args: argparse.Namespace) -> None:
    """Run the full ETL pipeline."""

    run_pipeline()


def command_tables(_args: argparse.Namespace) -> None:
    """Print tables and their row counts."""

    _ensure_env_defaults()
    with postgres_connection() as connection:
        tables = list_tables(connection)
        if not tables:
            print("Database contains no user tables.")
            return

        for table in tables:
            count = table_row_count(connection, table)
            print(f"{table}: {count} rows")


def command_preview(args: argparse.Namespace) -> None:
    """Print the first few rows of a table."""

    _ensure_env_defaults()
    with postgres_connection() as connection:
        rows = table_preview(connection, args.table, limit=args.limit)
        if not rows:
            print(f"Table '{args.table}' is empty.")
            return

        df = pd.DataFrame(rows)
        print(df.to_string(index=False))


def command_overview(args: argparse.Namespace) -> None:
    """Print an overview of all tables with row samples."""

    _ensure_env_defaults()
    with postgres_connection() as connection:
        tables = list_tables(connection)
        if not tables:
            print("Database contains no user tables.")
            return

        for table in tables:
            count = table_row_count(connection, table)
            print(f"\nTABLE: {table} ({count} rows)")
            if count == 0:
                continue

            rows = table_preview(connection, table, limit=args.limit)
            if not rows:
                print("  <no rows>")
                continue
            df = pd.DataFrame(rows)
            print(df.to_string(index=False))


def build_parser() -> argparse.ArgumentParser:
    """Construct the command-line parser shared by all entry points."""

    parser = argparse.ArgumentParser(
        description="Utility commands for the ETL pipeline and database inspection."
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = False

    run_parser = subparsers.add_parser("run", help="Execute the full ETL pipeline.")
    run_parser.set_defaults(func=command_run)

    tables_parser = subparsers.add_parser(
        "tables", help="List tables currently available in the database."
    )
    tables_parser.set_defaults(func=command_tables)

    preview_parser = subparsers.add_parser(
        "preview", help="Preview the first few rows of a specific table."
    )
    preview_parser.add_argument("table", help="Name of the table to preview.")
    preview_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of rows to display (default: 5).",
    )
    preview_parser.set_defaults(func=command_preview)

    overview_parser = subparsers.add_parser(
        "overview", help="Print row counts and sample data for every table."
    )
    overview_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of rows to display per table (default: 5).",
    )
    overview_parser.set_defaults(func=command_overview)

    parser.set_defaults(func=command_run)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    """Parse arguments and dispatch the requested command."""

    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
