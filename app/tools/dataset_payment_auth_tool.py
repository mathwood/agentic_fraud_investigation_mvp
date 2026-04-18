import pandas as pd

from app.data_ingestion.dataset_mapper import map_dataset_row_to_normalized
from app.models.signal_models import PaymentAuthSignals


def _is_avs_mismatch(value: str | None) -> bool:
    if value is None:
        return False
    text = value.strip().lower()
    return text in {"n", "no", "false", "mismatch", "not_match", "not_matched"}


def _is_cvv_failed(value: str | None) -> bool:
    if value is None:
        return False
    text = value.strip().lower()
    return text in {"n", "no_match", "fail", "failed", "false"}

def _is_negative_flag(value) -> bool:
    if value is None or pd.isna(value):
        return False

    if isinstance(value, (int, float, bool)):
        return int(value) == 0

    text = str(value).strip().lower()
    return text in {"0", "n", "no", "false", "mismatch", "not_match", "not_matched", "fail", "failed", "no_match"}

def _is_positive_flag(value) -> bool:
    if value is None or pd.isna(value):
        return False

    if isinstance(value, (int, float, bool)):
        return int(value) == 1

    text = str(value).strip().lower()
    return text in {"1", "y", "yes", "true", "match", "matched", "pass", "passed"}
    
def derive_payment_auth_signals_from_dataset(
    row: pd.Series,
) -> PaymentAuthSignals:
    tx = map_dataset_row_to_normalized(row)

    avs_mismatch = _is_negative_flag(tx.avs_match)
    cvv_failed = _is_negative_flag(tx.cvv_result)
    three_ds_missing = not _is_positive_flag(tx.three_ds_flag)
    
    if tx.fraud_label == 1:
        # optional: add synthetic boost
        cvv_failed = cvv_failed or False  # keep logic pure

    bin_country_mismatch = (
        bool(tx.bin_country)
        and bool(tx.ip_country)
        and tx.bin_country != tx.ip_country
    )

    excessive_shipping_distance = (
        tx.shipping_distance_km is not None and tx.shipping_distance_km >= 500
    )

    return PaymentAuthSignals(
        avs_mismatch=avs_mismatch,
        cvv_failed=cvv_failed,
        three_ds_missing=three_ds_missing,
        bin_country_mismatch=bin_country_mismatch,
        excessive_shipping_distance=excessive_shipping_distance,
    )