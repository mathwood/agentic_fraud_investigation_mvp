from typing import Any

import pandas as pd

from app.data_ingestion.normalized_models import NormalizedTransaction


def _to_optional_str(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _to_optional_int(value: Any) -> int | None:
    if pd.isna(value):
        return None
    return int(value)


def _to_optional_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def _to_optional_bool(value: Any) -> bool | None:
    if pd.isna(value):
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False

    return None


def _normalize_status(is_fraud: Any) -> str:
    """
    Dataset 2 does not provide transaction authorization status.
    For MVP we keep status as 'unknown'.
    """
    return "unknown"


def map_dataset_row_to_normalized(row: pd.Series) -> NormalizedTransaction:
    """
    Map one Dataset row into a NormalizedTransaction.
    """
    timestamp = pd.to_datetime(row["transaction_time"], utc=True)

    transaction = NormalizedTransaction(
        transaction_id=str(row["transaction_id"]),
        customer_id=str(row["user_id"]),
        card_id=None,
        timestamp=timestamp.to_pydatetime(),
        amount=float(row["amount"]),
        currency="EUR",
        merchant_id=None,
        merchant_name=None,
        merchant_category=_to_optional_str(row.get("merchant_category")),
        merchant_country=None,
        status=_normalize_status(row.get("is_fraud")),
        channel=_to_optional_str(row.get("channel")),
        device_id=None,
        device_known=None,
        ip_address=None,
        ip_country=_to_optional_str(row.get("country")),
        billing_country=None,
        shipping_country=None,
        entry_mode=None,
        browser_fingerprint=None,
        account_age_days=_to_optional_int(row.get("account_age_days")),
        total_transactions_user=_to_optional_int(row.get("total_transactions_user")),
        avg_amount_user=_to_optional_float(row.get("avg_amount_user")),
        bin_country=_to_optional_str(row.get("bin_country")),
        promo_used=_to_optional_bool(row.get("promo_used")),
        avs_match=_to_optional_str(row.get("avs_match")),
        cvv_result=_to_optional_str(row.get("cvv_result")),
        three_ds_flag=_to_optional_bool(row.get("three_ds_flag")),
        shipping_distance_km=_to_optional_float(row.get("shipping_distance_km")),
        fraud_label=_to_optional_int(row.get("is_fraud")),
    )

    return transaction