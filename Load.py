"""Very small loader that writes the summary table into PostgreSQL."""

from __future__ import annotations

import os
from typing import Dict

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PGConnection

# Default connection details. Override them with POSTGRES_* environment variables
# if your database uses different values.
DEFAULT_SETTINGS: Dict[str, str] = {
    "POSTGRES_HOST": "127.0.0.1",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DATABASE": "etl_db",
    "POSTGRES_USER": "etl_user",
    "POSTGRES_PASSWORD": "etl_password",
}


def get_database_settings() -> Dict[str, str]:
    """Collect simple connection settings from the environment."""

    settings = {name: os.getenv(name, default) for name, default in DEFAULT_SETTINGS.items()}
    return settings


def create_connection() -> PGConnection:
    """Open a psycopg2 connection using the collected settings."""

    settings = get_database_settings()
    return psycopg2.connect(
        dbname=settings["POSTGRES_DATABASE"],
        user=settings["POSTGRES_USER"],
        password=settings["POSTGRES_PASSWORD"],
        host=settings["POSTGRES_HOST"],
        port=settings["POSTGRES_PORT"],
    )


def load_order_summary(connection: PGConnection, summary: pd.DataFrame) -> None:
    """Drop and recreate the order_summary table, then insert the rows."""

    rows = [
        (
            int(row.order_id),
            row.order_date.date(),
            int(row.customer_id),
            row.customer_name,
            float(row.order_total),
        )
        for row in summary.itertuples(index=False)
    ]

    with connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS order_summary;")
            cursor.execute(
                """
                CREATE TABLE order_summary (
                    order_id INTEGER PRIMARY KEY,
                    order_date DATE,
                    customer_id INTEGER,
                    customer_name TEXT,
                    order_total NUMERIC
                );
                """
            )
            cursor.executemany(
                """
                INSERT INTO order_summary (
                    order_id, order_date, customer_id, customer_name, order_total
                ) VALUES (%s, %s, %s, %s, %s);
                """,
                rows,
            )
            print(f"Saved {len(rows)} rows to the order_summary table.")


__all__ = ["create_connection", "load_order_summary", "get_database_settings"]
