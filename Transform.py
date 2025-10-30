"""Tiny transform helpers for the ETL demo."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def build_order_summary(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create a friendly summary table ready for loading into PostgreSQL."""

    orders = data["orders"].copy()
    items = data["order_items"].copy()
    customers = data["customers"].copy()

    # Convert string dates like "01/01/2016" into real datetime objects.
    orders["order_date"] = pd.to_datetime(orders["order_date"], format="%d/%m/%Y")

    # Work out how much each order is worth.
    items["line_total"] = (
        items["quantity"].astype(float)
        * items["list_price"].astype(float)
        * (1 - items["discount"].astype(float))
    )
    totals = (
        items.groupby("order_id", as_index=False)["line_total"].sum()
        .rename(columns={"line_total": "order_total"})
    )

    # Add the customer names to the orders table.
    customers["customer_name"] = (
        customers["first_name"].fillna("") + " " + customers["last_name"].fillna("")
    ).str.strip()
    customer_details = customers[["customer_id", "customer_name"]]

    summary = (
        orders.merge(totals, on="order_id", how="left")
        .merge(customer_details, on="customer_id", how="left")
        .fillna({"order_total": 0})
    )

    summary["order_total"] = summary["order_total"].astype(float)

    return summary[["order_id", "order_date", "customer_id", "customer_name", "order_total"]]


__all__ = ["build_order_summary"]
