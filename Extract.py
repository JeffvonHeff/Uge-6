"""Simple data extraction utilities for the ETL pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
import requests


API_ENDPOINTS: Dict[str, str] = {
    "orders": "https://etl-server.fly.dev/orders",
    "order_items": "https://etl-server.fly.dev/order_items",
    "customers": "https://etl-server.fly.dev/customers",
}


def extract_all(output_dir: Path | str = ".") -> Dict[str, pd.DataFrame]:
    """Download all datasets and persist them as CSV files.

    Parameters
    ----------
    output_dir:
        Location where the CSV files will be written. The directory is
        created if it does not exist.

    Returns
    -------
    Dict[str, pd.DataFrame]
        A mapping between dataset name and the resulting DataFrame.
    """

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    dataframes: Dict[str, pd.DataFrame] = {}
    for name, url in API_ENDPOINTS.items():
        dataframes[name] = _fetch_dataset(name, url, output_path)

    return dataframes


def _fetch_dataset(name: str, url: str, output_path: Path) -> pd.DataFrame:
    """Fetch a dataset from the API, falling back to any cached CSV."""

    cache_path = output_path / f"{name}.csv"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        if cache_path.exists():
            print(
                f"Failed to download '{name}' ({error}); using cached file {cache_path}"
            )
            return pd.read_csv(cache_path)
        raise RuntimeError(
            f"Unable to download dataset '{name}' and no cache is available."
        ) from error

    df = pd.DataFrame(response.json())
    df.to_csv(cache_path, index=False)
    print(f"Extracted {name} data to {cache_path}")
    return df


__all__ = ["extract_all", "API_ENDPOINTS"]
