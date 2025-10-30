"""Very small loader that writes the summary table into PostgreSQL."""

from __future__ import annotations

import os
from typing import Dict, Iterable, Sequence, Tuple

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import execute_values

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


TABLE_COLUMNS: Dict[str, Sequence[str]] = {
    "brands": ("brand_id", "brand_name"),
    "categories": ("category_id", "category_name"),
    "stores": (
        "store_id",
        "store_name",
        "phone",
        "email",
        "street",
        "city",
        "state",
        "zip_code",
    ),
    "customers": (
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "street",
        "city",
        "state",
        "zip_code",
    ),
    "products": (
        "product_id",
        "product_name",
        "brand_id",
        "category_id",
        "model_year",
        "list_price",
    ),
    "staffs": (
        "staff_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "active",
        "street",
        "store_id",
        "manager_id",
    ),
    "stocks": ("store_id", "product_id", "quantity"),
    "orders": (
        "order_id",
        "customer_id",
        "store_id",
        "staff_id",
        "order_status",
        "order_date",
        "required_date",
        "shipped_date",
    ),
    "order_items": (
        "order_id",
        "item_id",
        "product_id",
        "quantity",
        "list_price",
        "discount",
    ),
}


LOAD_ORDER: Sequence[str] = (
    "brands",
    "categories",
    "stores",
    "customers",
    "products",
    "staffs",
    "stocks",
    "orders",
    "order_items",
)


IDENTITY_TABLES: Dict[str, str] = {
    "stores": "store_id",
    "staffs": "staff_id",
}


def _iter_rows(frame: pd.DataFrame, columns: Sequence[str]) -> Iterable[Tuple]:
    for row in frame[columns].itertuples(index=False, name=None):
        yield tuple(None if pd.isna(value) else value for value in row)


def load_core_tables(connection: PGConnection, tables: Dict[str, pd.DataFrame]) -> None:
    """Truncate and reload all tables needed by the relational schema."""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE order_items, orders, stocks, staffs, products, "
                "customers, stores, categories, brands RESTART IDENTITY CASCADE;"
            )

            for table_name in LOAD_ORDER:
                frame = tables[table_name]
                columns = TABLE_COLUMNS[table_name]
                rows = list(_iter_rows(frame, columns))
                if not rows:
                    continue

                insert_sql = (
                    f"INSERT INTO {table_name} (" + ", ".join(columns) + ") VALUES %s"
                )
                execute_values(cursor, insert_sql, rows)
                print(f"Loaded {len(rows)} rows into {table_name}.")

            for table_name, column in IDENTITY_TABLES.items():
                cursor.execute(
                    "SELECT setval(pg_get_serial_sequence(%s, %s), "
                    "COALESCE(MAX(" + column + "), 0) + 1, false) FROM " + table_name + ";",
                    (f"public.{table_name}", column),
                )


__all__ = [
    "create_connection",
    "load_core_tables",
    "load_order_summary",
    "get_database_settings",
]
