"""Transform utilities for the ETL pipeline."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def _prepare_orders(orders: pd.DataFrame) -> pd.DataFrame:
    cleaned = orders.copy()
    for column in ["order_date", "required_date", "shipped_date"]:
        if column in cleaned.columns:
            cleaned[column] = pd.to_datetime(
                cleaned[column], errors="coerce", dayfirst=False
            )
    return cleaned


def transform_dataframes(data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Perform a minimal set of transformations on the extracted data.

    Parameters
    ----------
    data:
        Dictionary containing the raw DataFrames returned by
        :func:`extract_all`.

    Returns
    -------
    Dict[str, pd.DataFrame]
        Dictionary containing the cleaned DataFrames. Currently the
        resulting dictionary contains:
        ``orders`` – cleaned orders data, and
        ``order_summary`` – aggregated order totals with customer
        information.
    """

    orders = _prepare_orders(data["orders"])
    order_items = data["order_items"].copy()
    customers = data["customers"].copy()

    order_items["line_total"] = (
        order_items["quantity"].astype(float)
        * order_items["list_price"].astype(float)
        * (1 - order_items["discount"].astype(float))
    )

    order_totals = (
        order_items.groupby("order_id", as_index=False)["line_total"]
        .sum()
        .rename(columns={"line_total": "order_total"})
    )

    order_summary = orders.merge(order_totals, on="order_id", how="left").merge(
        customers, on="customer_id", how="left", suffixes=("", "_customer")
    )

    return {
        "orders": orders,
        "order_summary": order_summary,
    }


__all__ = ["transform_dataframes"]
