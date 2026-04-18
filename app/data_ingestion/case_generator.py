from datetime import timedelta, UTC

import pandas as pd

from app.data_ingestion.dataset_mapper import map_dataset_row_to_normalized
from app.models.case_models import Case, Context, Customer, Transaction
from app.models.history_models import (
    AmountRange,
    BehavioralBaseline,
    CustomerHistory,
    RecentActivity,
    RiskMarkers,
    TransactionSummary,
)


def generate_case_from_dataset_row(row: pd.Series) -> Case:
    tx = map_dataset_row_to_normalized(row)

    transaction = Transaction(
        transaction_id=tx.transaction_id,
        timestamp=tx.timestamp,
        amount=tx.amount,
        currency=tx.currency,
        channel="ecommerce",
        card_present=False,
        merchant_id=tx.merchant_id or f"M-{tx.transaction_id[-4:]}",
        merchant_name=tx.merchant_name,
        merchant_category=tx.merchant_category or "unknown",
        merchant_country=tx.merchant_country or (tx.ip_country or "UN"),
        status="authorized" if tx.fraud_label == 0 else "authorized",
    )

    customer = Customer(
        customer_id=tx.customer_id,
        account_id=f"ACC-{tx.customer_id}",
        card_id=tx.card_id or f"CARD-{tx.customer_id}",
        customer_since=(tx.timestamp.date() - timedelta(days=tx.account_age_days or 30)),
        kyc_level="full",
        segment="retail",
    )

    context = Context(
        device_id=tx.device_id,
        device_known=tx.device_known or False,
        ip_address=tx.ip_address,
        ip_country=tx.ip_country,
        billing_country=tx.bin_country,
        shipping_country=tx.ip_country,
        entry_mode="manual_card_entry" if tx.channel == "card_not_present" else tx.entry_mode,
        browser_fingerprint=tx.browser_fingerprint,
        email=None,
        phone=None,
    )

    case = Case(
        case_id=f"CASE-{tx.transaction_id}",
        alert_id=f"ALERT-{tx.transaction_id}",
        alert_type="suspicious_cnp_transaction",
        alert_created_at=tx.timestamp,
        transaction=transaction,
        customer=customer,
        context=context,
    )

    return case


def generate_customer_history_from_dataset(
    df: pd.DataFrame,
    target_row: pd.Series,
    lookback_days: int = 30,
) -> CustomerHistory:
    target_tx = map_dataset_row_to_normalized(target_row)
    customer_id = target_tx.customer_id

    customer_df = df[df["user_id"].astype(str) == customer_id].copy()
    customer_df["transaction_time"] = pd.to_datetime(customer_df["transaction_time"], utc=True)

    ref_ts = pd.Timestamp(target_tx.timestamp)
    window_start = ref_ts - pd.Timedelta(days=lookback_days)

    history_df = customer_df[
    	(customer_df["transaction_time"] < ref_ts)
    	& (customer_df["transaction_time"] >= window_start)
    ].copy()

    total_transactions = len(history_df)
    total_amount = float(history_df["amount"].sum()) if not history_df.empty else 0.0
    avg_amount = float(history_df["amount"].mean()) if not history_df.empty else float(target_tx.avg_amount_user or 0.0)
    max_amount = float(history_df["amount"].max()) if not history_df.empty else 0.0

    decline_count = 0
    chargeback_count = 0

    usual_countries = (
        history_df["country"].dropna().astype(str).value_counts().head(3).index.tolist()
        if "country" in history_df and not history_df.empty
        else ([target_tx.ip_country] if target_tx.ip_country else [])
    )

    usual_merchant_categories = (
        history_df["merchant_category"].dropna().astype(str).value_counts().head(5).index.tolist()
        if "merchant_category" in history_df and not history_df.empty
        else ([target_tx.merchant_category] if target_tx.merchant_category else [])
    )

    usual_transaction_hours = (
        sorted(history_df["transaction_time"].dt.hour.dropna().astype(int).unique().tolist())
        if not history_df.empty
        else [target_tx.timestamp.hour]
    )

    known_ip_countries = (
        history_df["country"].dropna().astype(str).tolist()
        if "country" in history_df and not history_df.empty
        else ([target_tx.ip_country] if target_tx.ip_country else [])
    )

    transactions_last_24h = 0
    transactions_last_7d = 0
    if not history_df.empty:
        last_24h_start = ref_ts - pd.Timedelta(hours=24)
        last_7d_start = ref_ts - pd.Timedelta(days=7)

        transactions_last_24h = int((history_df["transaction_time"] >= last_24h_start).sum())
        transactions_last_7d = int((history_df["transaction_time"] >= last_7d_start).sum())

    last_successful_transaction_at = None
    last_successful_transaction_country = None
    if not history_df.empty:
        latest_row = history_df.sort_values("transaction_time").iloc[-1]
        last_successful_transaction_at = latest_row["transaction_time"].to_pydatetime()
        last_successful_transaction_country = str(latest_row["country"]) if pd.notna(latest_row["country"]) else None

    history = CustomerHistory(
        customer_id=customer_id,
        lookback_days=lookback_days,
        transaction_summary=TransactionSummary(
            total_transactions=total_transactions,
            total_amount=round(total_amount, 2),
            avg_amount=round(avg_amount, 2),
            max_amount=round(max_amount, 2),
            decline_count=decline_count,
            chargeback_count=chargeback_count,
        ),
        behavioral_baseline=BehavioralBaseline(
            usual_countries=usual_countries,
            usual_merchant_categories=usual_merchant_categories,
            usual_transaction_hours=usual_transaction_hours,
            usual_amount_range=AmountRange(
                min=0.0 if history_df.empty else round(float(history_df["amount"].min()), 2),
                max=round(max_amount, 2),
            ),
            known_devices=[],
            known_ip_countries=known_ip_countries,
        ),
        recent_activity=RecentActivity(
            transactions_last_24h=transactions_last_24h,
            transactions_last_7d=transactions_last_7d,
            declines_last_24h=0,
            distinct_merchants_last_24h=0 if history_df.empty else int(
                history_df[
                    history_df["transaction_time"] >= (
                        ref_ts - pd.Timedelta(hours=24)
                    )
                ]["merchant_category"].nunique()
            ),
            last_successful_transaction_at=last_successful_transaction_at,
            last_successful_transaction_country=last_successful_transaction_country,
        ),
        risk_markers=RiskMarkers(
            previous_fraud_flags=0,
            previous_manual_reviews=0,
            account_takeover_history=False,
        ),
    )

    return history