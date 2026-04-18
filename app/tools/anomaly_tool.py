from app.models.case_models import Case
from app.models.history_models import CustomerHistory
from app.models.signal_models import (
    AmountSignals,
    BehavioralSignals,
    DeviceSignals,
    GeoSignals,
    MerchantSignals,
    PaymentAuthSignals,
)

from app.data_ingestion.case_generator import (
    generate_case_from_dataset_row,
    generate_customer_history_from_dataset,
)
from app.data_ingestion.dataset_loader import load_csv_dataset
from app.tools.velocity_tool import check_velocity_from_dataset


def check_anomalies(case: Case, history: CustomerHistory) -> dict:
    """
    Compare the current case against customer baseline and return
    deterministic anomaly signal groups.
    """
    tx = case.transaction
    ctx = case.context
    baseline = history.behavioral_baseline

    new_device = bool(ctx.device_id) and ctx.device_id not in baseline.known_devices
    device_change_recently = new_device

    browser_fingerprint_mismatch = False

    ip_country_mismatch = (
        bool(ctx.ip_country) and ctx.ip_country not in baseline.usual_countries
    )

    bin_ip_country_mismatch = (
        bool(ctx.ip_country)
        and bool(ctx.billing_country)
        and ctx.ip_country != ctx.billing_country
    )

    shipping_ip_mismatch = (
        bool(ctx.ip_country)
        and bool(ctx.shipping_country)
        and ctx.ip_country != ctx.shipping_country
    )

    impossible_travel = False

    above_customer_average = tx.amount > history.transaction_summary.avg_amount
    above_customer_max = tx.amount > history.transaction_summary.max_amount

    avg_amount = history.transaction_summary.avg_amount
    amount_deviation_ratio = tx.amount / avg_amount if avg_amount > 0 else 0.0

    new_merchant_for_customer = tx.merchant_category not in baseline.usual_merchant_categories
    merchant_category_unusual = tx.merchant_category not in baseline.usual_merchant_categories

    merchant_risk_level = "medium" if tx.merchant_category in {"electronics", "gift_cards"} else "low"

    tx_hour = tx.timestamp.hour
    unusual_transaction_hour = tx_hour not in baseline.usual_transaction_hours

    first_time_manual_entry = ctx.entry_mode == "manual_card_entry"
    
    avs_mismatch = False
    cvv_failed = False
    three_ds_missing = False
    bin_country_mismatch = False
    excessive_shipping_distance = False

    return {
        "device_signals": DeviceSignals(
            new_device=new_device,
            device_change_recently=device_change_recently,
            browser_fingerprint_mismatch=browser_fingerprint_mismatch,
        ),
        "geo_signals": GeoSignals(
            ip_country_mismatch=ip_country_mismatch,
            bin_ip_country_mismatch=bin_ip_country_mismatch,
            shipping_ip_mismatch=shipping_ip_mismatch,
            impossible_travel=impossible_travel,
        ),
        "amount_signals": AmountSignals(
            above_customer_average=above_customer_average,
            above_customer_max=above_customer_max,
            amount_deviation_ratio=round(amount_deviation_ratio, 2),
        ),
        "merchant_signals": MerchantSignals(
            new_merchant_for_customer=new_merchant_for_customer,
            merchant_category_unusual=merchant_category_unusual,
            merchant_risk_level=merchant_risk_level,
        ),
        "behavioral_signals": BehavioralSignals(
            unusual_transaction_hour=unusual_transaction_hour,
            first_time_manual_entry=first_time_manual_entry,
        ),
        "payment_auth_signals": PaymentAuthSignals(
            avs_mismatch=avs_mismatch,
            cvv_failed=cvv_failed,
            three_ds_missing=three_ds_missing,
            bin_country_mismatch=bin_country_mismatch,
            excessive_shipping_distance=excessive_shipping_distance,
        ),
    }
    
def investigate_case_from_dataset(file_path: str, row_index: int) -> InvestigationReport:
    """
    Run the full fraud investigation flow using Dataset and a selected row index.
    """
    init_db()

    df = load_csv_dataset(file_path)

    if df.empty:
        raise ValueError("Dataset is empty")

    if row_index < 0 or row_index >= len(df):
        raise IndexError(f"row_index {row_index} is out of range for dataset of size {len(df)}")

    target_row = df.iloc[row_index]

    case = generate_case_from_dataset_row(target_row)
    history = generate_customer_history_from_dataset(df, target_row, lookback_days=30)

    velocity_signals = check_velocity_from_dataset(
        df=df,
        target_row=target_row,
        window_minutes=10,
    )

    anomaly_outputs = check_anomalies(case, history)

    fraud_signals = merge_signals(
        case_id=case.case_id,
        velocity_signals=velocity_signals,
        device_signals=anomaly_outputs["device_signals"],
        geo_signals=anomaly_outputs["geo_signals"],
        amount_signals=anomaly_outputs["amount_signals"],
        merchant_signals=anomaly_outputs["merchant_signals"],
        behavioral_signals=anomaly_outputs["behavioral_signals"],
    )

    score = calculate_score(fraud_signals)
    decision = get_decision(score, fraud_signals)

    report = generate_llm_investigation_report(
        case=case,
        history=history,
        signals=fraud_signals,
        score=score,
        decision=decision,
    )

    save_investigation_report(report)

    return report