import json
import pandas as pd

from collections import Counter
from datetime import datetime, timedelta, UTC
from pathlib import Path

from app.config import get_settings
from app.models.signal_models import VelocitySignals

from app.data_ingestion.dataset_mapper import map_dataset_row_to_normalized


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def check_velocity(
    customer_id: str,
    card_id: str,
    reference_timestamp: str,
    window_minutes: int = 10,
) -> VelocitySignals:
    """
    Analyze recent transaction velocity for a given customer/card
    using local mock transaction history.
    """
    settings = get_settings()
    tx_file: Path = settings.data_dir / "mock_cases" / f"{customer_id}_recent_transactions.json"

    if not tx_file.exists():
        raise FileNotFoundError(f"Recent transactions file not found: {tx_file}")

    with tx_file.open("r", encoding="utf-8") as f:
        transactions = json.load(f)

    ref_time = _parse_dt(reference_timestamp)
    window_start = ref_time - timedelta(minutes=window_minutes)

    in_window = [
        tx for tx in transactions
        if window_start <= _parse_dt(tx["timestamp"]) <= ref_time
    ]

    transactions_last_10m = len(in_window)
    declines_last_10m = sum(1 for tx in in_window if tx["status"] == "declined")
    distinct_merchants_last_10m = len({tx["merchant_id"] for tx in in_window})

    amount_counts = Counter(tx["amount"] for tx in in_window)
    repeated_same_amount = any(count >= 3 for count in amount_counts.values())

    velocity_breach = (
        transactions_last_10m >= 5
        or declines_last_10m >= 3
        or distinct_merchants_last_10m >= 4
    )

    return VelocitySignals(
        transactions_last_10m=transactions_last_10m,
        declines_last_10m=declines_last_10m,
        distinct_merchants_last_10m=distinct_merchants_last_10m,
        repeated_same_amount=repeated_same_amount,
        velocity_breach=velocity_breach,
    )
    
def check_velocity_from_dataset(
    df: pd.DataFrame,
    target_row: pd.Series,
    window_minutes: int = 10,
) -> VelocitySignals:
    """
    Compute velocity signals from Dataset rows for the same customer
    within a recent time window before and including the target transaction.
    """
    target_tx = map_dataset_row_to_normalized(target_row)
    customer_id = target_tx.customer_id
    ref_ts = pd.Timestamp(target_tx.timestamp)

    customer_df = df[df["user_id"].astype(str) == customer_id].copy()
    customer_df["transaction_time"] = pd.to_datetime(customer_df["transaction_time"], utc=True)

    window_start = ref_ts - pd.Timedelta(minutes=window_minutes)

    in_window = customer_df[
        (customer_df["transaction_time"] >= window_start)
        & (customer_df["transaction_time"] <= ref_ts)
    ].copy()

    transactions_last_10m = len(in_window)
    declines_last_10m = 0

    distinct_merchants_last_10m = (
        int(in_window["merchant_category"].nunique())
        if "merchant_category" in in_window.columns
        else 0
    )

    repeated_same_amount = False
    if not in_window.empty:
        amount_counts = in_window["amount"].value_counts()
        repeated_same_amount = bool((amount_counts >= 3).any())

    velocity_breach = (
        transactions_last_10m >= 5
        or declines_last_10m >= 3
        or distinct_merchants_last_10m >= 4
    )

    return VelocitySignals(
        transactions_last_10m=transactions_last_10m,
        declines_last_10m=declines_last_10m,
        distinct_merchants_last_10m=distinct_merchants_last_10m,
        repeated_same_amount=repeated_same_amount,
        velocity_breach=velocity_breach,
    )