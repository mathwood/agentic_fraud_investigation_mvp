from app.agent.llm_reporter import generate_llm_investigation_report
from app.models.report_models import InvestigationReport
from app.tools.anomaly_tool import check_anomalies
from app.tools.case_loader import get_case
from app.tools.history_tool import get_customer_history
from app.tools.scoring_tool import merge_signals, calculate_score, get_decision
from app.storage.db import init_db
from app.storage.logs import save_investigation_report
from app.data_ingestion.case_generator import (
    generate_case_from_dataset_row,
    generate_customer_history_from_dataset,
)
from app.data_ingestion.dataset_loader import load_csv_dataset
from app.tools.velocity_tool import check_velocity, check_velocity_from_dataset
from app.tools.dataset_payment_auth_tool import derive_payment_auth_signals_from_dataset


def investigate_case(case_id: str) -> InvestigationReport:
    init_db()

    case = get_case(case_id)

    history = get_customer_history(
        customer_id=case.customer.customer_id,
        lookback_days=30,
    )

    velocity_signals = check_velocity(
        customer_id=case.customer.customer_id,
        card_id=case.customer.card_id,
        reference_timestamp=case.transaction.timestamp.isoformat(),
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
        payment_auth_signals=anomaly_outputs["payment_auth_signals"],
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


def investigate_case_from_dataset(file_path: str, row_index: int) -> InvestigationReport:
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
    payment_auth_signals = derive_payment_auth_signals_from_dataset(target_row)

    fraud_signals = merge_signals(
        case_id=case.case_id,
        velocity_signals=velocity_signals,
        device_signals=anomaly_outputs["device_signals"],
        geo_signals=anomaly_outputs["geo_signals"],
        amount_signals=anomaly_outputs["amount_signals"],
        merchant_signals=anomaly_outputs["merchant_signals"],
        behavioral_signals=anomaly_outputs["behavioral_signals"],
        payment_auth_signals=payment_auth_signals,
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


def investigate_case_from_dataset_structured(file_path: str, row_index: int) -> dict:
    """
    Deterministic, quiet dataset investigation path for dataset querying.
    No LLM report generation and no DB write.
    """
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
    payment_auth_signals = derive_payment_auth_signals_from_dataset(target_row)

    fraud_signals = merge_signals(
        case_id=case.case_id,
        velocity_signals=velocity_signals,
        device_signals=anomaly_outputs["device_signals"],
        geo_signals=anomaly_outputs["geo_signals"],
        amount_signals=anomaly_outputs["amount_signals"],
        merchant_signals=anomaly_outputs["merchant_signals"],
        behavioral_signals=anomaly_outputs["behavioral_signals"],
        payment_auth_signals=payment_auth_signals,
    )

    score = calculate_score(fraud_signals)
    decision = get_decision(score, fraud_signals)

    return {
        "case_id": case.case_id,
        "decision": decision.model_dump(),
        "risk_labels": fraud_signals.risk_labels,
    }
    
def investigate_case_from_preloaded_dataset_structured(df, row_index: int) -> dict:
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
    payment_auth_signals = derive_payment_auth_signals_from_dataset(target_row)

    fraud_signals = merge_signals(
        case_id=case.case_id,
        velocity_signals=velocity_signals,
        device_signals=anomaly_outputs["device_signals"],
        geo_signals=anomaly_outputs["geo_signals"],
        amount_signals=anomaly_outputs["amount_signals"],
        merchant_signals=anomaly_outputs["merchant_signals"],
        behavioral_signals=anomaly_outputs["behavioral_signals"],
        payment_auth_signals=payment_auth_signals,
    )

    score = calculate_score(fraud_signals)
    decision = get_decision(score, fraud_signals)

    return {
        "case_id": case.case_id,
        "decision": decision.model_dump(),
        "risk_labels": fraud_signals.risk_labels,
    }