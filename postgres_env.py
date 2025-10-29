"""Utility helpers for inspecting the Postgres database used by the ETL app."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, List

import psycopg2
from psycopg2 import sql

from Load import build_connection_from_env, check_connection

pos_POSTGRES_ENV_DEFAULTS: Dict[str, str] = {
    "POSTGRES_USER": "etl_user",
    "POSTGRES_PASSWORD": "etl_password",
    "POSTGRES_HOST": "127.0.0.1",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DATABASE": "etl_db",
}


@contextmanager
def postgres_connection():
    """Yield a connection using the same environment-based config as the ETL."""

    connection = build_connection_from_env()
    try:
        yield connection
    finally:
        connection.close()


def list_tables(connection) -> List[str]:
    """Return a sorted list of user table names in the current schema search path."""

    query = """
        select tablename
        from pg_catalog.pg_tables
        where schemaname not in ('pg_catalog', 'information_schema')
        order by tablename;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    return [row[0] for row in rows]


def table_row_count(connection, table_name: str) -> int:
    """Return the number of rows contained within the given table."""

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table_name))
        )
        count = cursor.fetchone()
    return count[0] if count else 0


def table_preview(
    connection,
    table_name: str,
    *,
    limit: int = 5,
) -> List[Dict[str, object]]:
    """Fetch the first few rows from a table."""

    if limit <= 0:
        raise ValueError("limit must be a positive integer")

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT * FROM {} LIMIT {}").format(
                sql.Identifier(table_name),
                sql.Literal(limit),
            )
        )
        column_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

    return [dict(zip(column_names, row)) for row in rows]


def print_database_overview(limit: int = 5) -> None:
    """Render a simple overview of each table and sample rows to stdout."""

    with postgres_connection() as connection:
        if not check_connection(connection):
            raise psycopg2.OperationalError("database connection test failed")

        tables = list_tables(connection)
        if not tables:
            print("Database contains no user tables.")
            return

        for table in tables:
            count = table_row_count(connection, table)
            print(f"\nTABLE: {table} ({count} rows)")
            if count == 0:
                continue

            for row in table_preview(connection, table, limit=limit):
                print("  ", row)


__all__ = [
    "postgres_connection",
    "list_tables",
    "table_row_count",
    "table_preview",
    "print_database_overview",
]
