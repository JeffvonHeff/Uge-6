"""Loading utilities for persisting transformed data to PostgreSQL."""
from __future__ import annotations

import os
from typing import Dict

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection


_DTYPE_TO_SQL: Dict[str, str] = {
    "int64": "BIGINT",
    "Int64": "BIGINT",
    "float64": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
}


def _column_sql_type(series: pd.Series) -> str:
    """Map a pandas series dtype to a simple PostgreSQL column type."""

    dtype_name = str(series.dtype)
    return _DTYPE_TO_SQL.get(dtype_name, "TEXT")


def build_postgres_connection(
    *,
    user: str,
    password: str,
    host: str,
    port: int,
    database: str,
) -> PGConnection:
    """Create a direct connection to a PostgreSQL database."""

    return psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )


def _ensure_table(cursor, table_name: str, dataframe: pd.DataFrame) -> None:
    """Create a table that matches the DataFrame's columns."""

    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table_name)))

    column_definitions = []
    for column in dataframe.columns:
        column_type = _column_sql_type(dataframe[column])
        column_definitions.append(
            sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(column_type))
        )

    create_statement = sql.SQL("CREATE TABLE {} ({});").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(column_definitions) if column_definitions else sql.SQL("id SERIAL PRIMARY KEY"),
    )
    cursor.execute(create_statement)


def _insert_rows(cursor, table_name: str, dataframe: pd.DataFrame) -> None:
    """Insert DataFrame rows into the database table."""

    if dataframe.empty:
        return

    columns = [sql.Identifier(column) for column in dataframe.columns]
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
    insert_statement = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns),
        placeholders,
    )

    values = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]
    cursor.executemany(insert_statement.as_string(cursor), values)


def load_dataframes(connection: PGConnection, tables: Dict[str, pd.DataFrame]) -> None:
    """Write each DataFrame to the configured PostgreSQL database."""

    with connection:
        with connection.cursor() as cursor:
            for name, dataframe in tables.items():
                _ensure_table(cursor, name, dataframe)
                _insert_rows(cursor, name, dataframe)
                print(f"Loaded DataFrame into table '{name}'")


def build_connection_from_env(prefix: str = "POSTGRES") -> PGConnection:
    """Create a PostgreSQL connection using ``{prefix}_*`` environment variables."""

    env = {
        key: os.getenv(f"{prefix}_{key}")
        for key in ["USER", "PASSWORD", "HOST", "PORT", "DATABASE"]
    }
    missing = [key for key, value in env.items() if value in (None, "")]
    if missing:
        missing_vars = ", ".join(f"{prefix}_{key}" for key in missing)
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    return build_postgres_connection(
        user=env["USER"],
        password=env["PASSWORD"],
        host=env["HOST"],
        port=int(env["PORT"]),
        database=env["DATABASE"],
    )


__all__ = ["build_postgres_connection", "load_dataframes", "build_connection_from_env"]
