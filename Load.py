"""Loading utilities for persisting transformed data to MySQL."""
from __future__ import annotations

import os
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def build_mysql_engine(
    *,
    user: str,
    password: str,
    host: str,
    port: int,
    database: str,
    echo: bool = False,
) -> Engine:
    """Create a SQLAlchemy engine for a MySQL database."""

    connection_uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_uri, echo=echo, future=True)


def load_dataframes(engine: Engine, tables: Dict[str, pd.DataFrame], *, if_exists: str = "replace") -> None:
    """Write each DataFrame to the configured MySQL database."""

    for name, dataframe in tables.items():
        dataframe.to_sql(name, engine, if_exists=if_exists, index=False)
        print(f"Loaded DataFrame into table '{name}'")


def build_engine_from_env(prefix: str = "MYSQL") -> Engine:
    """Create an engine using ``{prefix}_*`` environment variables."""

    env = {key: os.getenv(f"{prefix}_{key}") for key in ["USER", "PASSWORD", "HOST", "PORT", "DATABASE"]}
    missing = [key for key, value in env.items() if value in (None, "")]
    if missing:
        missing_vars = ", ".join(f"{prefix}_{key}" for key in missing)
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    return build_mysql_engine(
        user=env["USER"],
        password=env["PASSWORD"],
        host=env["HOST"],
        port=int(env["PORT"]),
        database=env["DATABASE"],
    )


__all__ = ["build_mysql_engine", "load_dataframes", "build_engine_from_env"]
