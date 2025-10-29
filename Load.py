"""Loading utilities for persisting transformed data to PostgreSQL."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Mapping

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, connection as PGConnection
from postgres_env import pos_POSTGRES_ENV_DEFAULTS

_DTYPE_TO_SQL: Dict[str, str] = {
    "int64": "BIGINT",
    "Int64": "BIGINT",
    "float64": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
}

_POSTGRES_ENV_DEFAULTS = pos_POSTGRES_ENV_DEFAULTS


def set_postgres_env(
    vars_map: Mapping[str, str] | None = None,
    *,
    overwrite: bool = False,
) -> Dict[str, str]:
    """Populate missing Postgres-related environment variables for this process.

    Parameters
    ----------
    vars_map:
        Optional mapping that overrides the baked-in defaults.
    overwrite:
        When ``True`` existing environment values are replaced; otherwise only
        missing keys are set.

    Returns
    -------
    Dict[str, str]
        The key/value pairs that were written to ``os.environ``.
    """

    mapping: Dict[str, str] = dict(_POSTGRES_ENV_DEFAULTS)
    if vars_map:
        mapping.update(vars_map)

    applied: Dict[str, str] = {}
    for key, value in mapping.items():
        if overwrite or os.getenv(key) in (None, ""):
            os.environ[key] = value
            applied[key] = value
    return applied


def write_postgres_env(
    dotenv_path: Path | str = Path(".env"),
    vars_map: Mapping[str, str] | None = None,
) -> Path:
    """Write the Postgres environment variables to a dotenv-style file."""

    mapping: Dict[str, str] = dict(_POSTGRES_ENV_DEFAULTS)
    if vars_map:
        mapping.update(vars_map)

    set_postgres_env(mapping, overwrite=True)
    path = Path(dotenv_path)
    lines = [f"{key}={value}" for key, value in mapping.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


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

    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table_name))
    )

    column_definitions = []
    for column in dataframe.columns:
        column_type = _column_sql_type(dataframe[column])
        column_definitions.append(
            sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(column_type))
        )

    create_statement = sql.SQL("CREATE TABLE {} ({});").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(column_definitions)
        if column_definitions
        else sql.SQL("id SERIAL PRIMARY KEY"),
    )
    cursor.execute(create_statement)


def _normalize_cell(value):
    """Convert pandas-specific missing or datetime values to DB-friendly objects."""

    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.to_pydatetime()
    if pd.isna(value):
        return None
    return value


def check_connection(connection: PGConnection) -> bool:
    """Verify that the given connection can execute a trivial query."""

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return True
    except psycopg2.Error:
        return False


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

    values = [
        tuple(_normalize_cell(cell) for cell in row)
        for row in dataframe.itertuples(index=False, name=None)
    ]
    cursor.executemany(insert_statement.as_string(cursor), values)


def load_dataframes(connection: PGConnection, tables: Dict[str, pd.DataFrame]) -> None:
    """Write each DataFrame to the configured PostgreSQL database."""

    if not check_connection(connection):
        raise psycopg2.OperationalError("Database connection test failed")

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

    connection_params = {
        "user": env["USER"],
        "password": env["PASSWORD"],
        "host": env["HOST"],
        "port": int(env["PORT"]),
        "database": env["DATABASE"],
    }
    return _connect_with_database_creation(**connection_params)


def _connect_with_database_creation(
    *,
    user: str,
    password: str,
    host: str,
    port: int,
    database: str,
) -> PGConnection:
    """Connect to the database, creating it on the fly if it is missing."""

    try:
        return build_postgres_connection(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    except psycopg2.OperationalError as error:  # pragma: no cover - requires DB
        message = str(error).lower()
        if "does not exist" not in message:
            raise
        last_error = error

    try:
        _ensure_database_exists(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    except Exception as creation_error:  # pragma: no cover - requires DB
        raise last_error from creation_error

    return build_postgres_connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def _ensure_database_exists(
    *,
    user: str,
    password: str,
    host: str,
    port: int,
    database: str,
) -> None:
    """Create the target database if it is missing."""

    admin_connection = psycopg2.connect(
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port,
    )
    admin_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with admin_connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
            )
    except psycopg2.errors.DuplicateDatabase:
        # Another process created the database in the meantime; that's fine.
        pass
    finally:
        admin_connection.close()


__all__ = [
    "build_postgres_connection",
    "load_dataframes",
    "build_connection_from_env",
    "set_postgres_env",
    "write_postgres_env",
    "check_connection",
]
