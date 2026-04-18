from rich.console import Console

from app.models.case_models import Case
from app.models.history_models import CustomerHistory
from app.models.signal_models import FraudSignals, RiskScoreBreakdown
from app.models.report_models import InvestigationReport
from app.tools.case_loader import get_case
from app.tools.history_tool import get_customer_history
from app.tools.velocity_tool import check_velocity
from app.tools.anomaly_tool import check_anomalies
from app.tools.scoring_tool import merge_signals, calculate_score, get_decision
from app.agent.llm_reporter import generate_llm_investigation_report
from app.agent.orchestrator import investigate_case
from app.storage.db import get_db_connection
from app.data_ingestion.dataset_loader import load_csv_dataset
from app.data_ingestion.dataset_mapper import map_dataset_row_to_normalized
from app.agent.orchestrator import investigate_case, investigate_case_from_dataset
from app.data_ingestion.case_generator import (
    generate_case_from_dataset_row,
    generate_customer_history_from_dataset,
)

console = Console()


def test_case_model() -> None:
    sample_case = {
        "case_id": "CASE-1001",
        "alert_id": "ALERT-9001",
        "alert_type": "suspicious_cnp_transaction",
        "alert_created_at": "2026-04-18T10:15:00Z",
        "transaction": {
            "transaction_id": "TX-88421",
            "timestamp": "2026-04-18T10:13:21Z",
            "amount": 420.50,
            "currency": "EUR",
            "channel": "ecommerce",
            "card_present": False,
            "merchant_id": "M-2009",
            "merchant_name": "TechWorld",
            "merchant_category": "electronics",
            "merchant_country": "DE",
            "status": "authorized"
        },
        "customer": {
            "customer_id": "CUST-781",
            "account_id": "ACC-321",
            "card_id": "CARD-1122",
            "customer_since": "2023-06-10",
            "kyc_level": "full",
            "segment": "retail"
        },
        "context": {
            "device_id": "DEV-9A2",
            "device_known": False,
            "ip_address": "185.24.xx.xx",
            "ip_country": "NL",
            "billing_country": "DE",
            "shipping_country": "DE",
            "entry_mode": "manual_card_entry",
            "browser_fingerprint": "BFP-334455",
            "email": "m***@mail.com",
            "phone": "+49******22"
        }
    }

    case = Case.model_validate(sample_case)
    console.print("[bold green]Case model validated successfully[/bold green]")
    console.print(case.model_dump())


def test_history_model() -> None:
    sample_history = {
        "customer_id": "CUST-781",
        "lookback_days": 30,
        "transaction_summary": {
            "total_transactions": 42,
            "total_amount": 3180.20,
            "avg_amount": 75.72,
            "max_amount": 210.00,
            "decline_count": 3,
            "chargeback_count": 0
        },
        "behavioral_baseline": {
            "usual_countries": ["DE"],
            "usual_merchant_categories": ["groceries", "fashion", "utilities"],
            "usual_transaction_hours": [8, 9, 12, 18, 19, 20],
            "usual_amount_range": {
                "min": 8.50,
                "max": 180.00
            },
            "known_devices": ["DEV-1A1", "DEV-4B9"],
            "known_ip_countries": ["DE", "DE", "DE"]
        },
        "recent_activity": {
            "transactions_last_24h": 3,
            "transactions_last_7d": 11,
            "declines_last_24h": 1,
            "distinct_merchants_last_24h": 2,
            "last_successful_transaction_at": "2026-04-18T08:05:00Z",
            "last_successful_transaction_country": "DE"
        },
        "risk_markers": {
            "previous_fraud_flags": 0,
            "previous_manual_reviews": 1,
            "account_takeover_history": False
        }
    }

    history = CustomerHistory.model_validate(sample_history)
    console.print("[bold green]CustomerHistory model validated successfully[/bold green]")
    console.print(history.model_dump())
    

def test_signal_models() -> None:
    sample_signals = {
        "case_id": "CASE-1001",
        "velocity_signals": {
            "transactions_last_10m": 6,
            "declines_last_10m": 4,
            "distinct_merchants_last_10m": 5,
            "repeated_same_amount": True,
            "velocity_breach": True
        },
        "device_signals": {
            "new_device": True,
            "device_change_recently": True,
            "browser_fingerprint_mismatch": False
        },
        "geo_signals": {
            "ip_country_mismatch": True,
            "billing_ip_mismatch": True,
            "shipping_ip_mismatch": True,
            "impossible_travel": False
        },
        "amount_signals": {
            "above_customer_average": True,
            "above_customer_max": True,
            "amount_deviation_ratio": 5.55
        },
        "merchant_signals": {
            "new_merchant_for_customer": True,
            "merchant_category_unusual": True,
            "merchant_risk_level": "medium"
        },
        "behavioral_signals": {
            "unusual_transaction_hour": True,
            "first_time_manual_entry": True
        },
        "risk_labels": [
            "velocity_spike",
            "new_device",
            "geo_mismatch",
            "amount_outlier",
            "unusual_merchant"
        ]
    }

    sample_breakdown = {
        "velocity_score": 25,
        "device_score": 15,
        "geo_score": 16,
        "amount_score": 15,
        "merchant_score": 7,
        "behavioral_score": 8,
        "total_score": 86
    }

    signals = FraudSignals.model_validate(sample_signals)
    breakdown = RiskScoreBreakdown.model_validate(sample_breakdown)

    console.print("[bold green]FraudSignals model validated successfully[/bold green]")
    console.print(signals.model_dump())

    console.print("[bold green]RiskScoreBreakdown model validated successfully[/bold green]")
    console.print(breakdown.model_dump())
    
    
    
    
def test_report_model() -> None:
    sample_report = {
        "case_id": "CASE-1001",
        "generated_at": "2026-04-18T10:20:00Z",
        "decision": {
            "recommended_action": "manual_review",
            "risk_score": 82,
            "risk_level": "high"
        },
        "top_reasons": [
            "Transaction originates from a new device",
            "IP country differs from customer baseline",
            "High transaction velocity observed",
            "Amount exceeds historical maximum"
        ],
        "risk_labels": [
            "velocity_spike",
            "new_device",
            "geo_mismatch",
            "amount_outlier"
        ],
        "evidence_summary": {
            "customer_usual_countries": ["DE"],
            "current_ip_country": "NL",
            "customer_avg_amount": 75.72,
            "customer_max_amount": 210.00,
            "current_amount": 420.50,
            "transactions_last_10m": 6,
            "declines_last_10m": 4
        },
        "analyst_summary": "The transaction shows multiple deviations from the customer's normal behavior, including device, geography, velocity, and amount. While risk is elevated, manual review is recommended due to lack of confirmed prior fraud.",
        "next_steps": [
            "Contact customer for confirmation",
            "Check recent travel activity",
            "Review related transactions"
        ]
    }

    report = InvestigationReport.model_validate(sample_report)

    console.print("[bold green]InvestigationReport model validated successfully[/bold green]")
    console.print(report.model_dump())
    
    
def test_case_loader(case_id: str = "CASE-1001") -> None:
    case = get_case(case_id)
    console.print("[bold green]Case loader executed successfully[/bold green]")
    console.print(case.model_dump())
    
def test_history_loader(customer_id: str = "CUST-781", lookback_days: int = 30) -> None:
    history = get_customer_history(customer_id, lookback_days)
    console.print("[bold green]Customer history loader executed successfully[/bold green]")
    console.print(history.model_dump())
    
def test_velocity_tool() -> None:
    signals = check_velocity(
        customer_id="CUST-781",
        card_id="CARD-1122",
        reference_timestamp="2026-04-18T10:13:21Z",
        window_minutes=10,
    )
    console.print("[bold green]Velocity tool executed successfully[/bold green]")
    console.print(signals.model_dump())
    
def test_anomaly_tool() -> None:
    case = get_case("CASE-1001")
    history = get_customer_history("CUST-781", 30)

    anomaly_outputs = check_anomalies(case, history)

    console.print("[bold green]Anomaly tool executed successfully[/bold green]")
    for name, model in anomaly_outputs.items():
        console.print(f"[bold cyan]{name}[/bold cyan]")
        console.print(model.model_dump())
        
def test_scoring_tool() -> None:
    case = get_case("CASE-1001")
    history = get_customer_history("CUST-781", 30)

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
    )

    score = calculate_score(fraud_signals)
    decision = get_decision(score, fraud_signals)

    console.print("[bold green]Scoring tool executed successfully[/bold green]")

    console.print("[bold cyan]FraudSignals[/bold cyan]")
    console.print(fraud_signals.model_dump())

    console.print("[bold cyan]RiskScoreBreakdown[/bold cyan]")
    console.print(score.model_dump())

    console.print("[bold cyan]Decision[/bold cyan]")
    console.print(decision.model_dump())
    
    
def test_llm_reporter() -> None:
    case = get_case("CASE-1001")
    history = get_customer_history("CUST-781", 30)

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

    console.print("[bold green]LLM reporter executed successfully[/bold green]")
    console.print(report.model_dump())
    
    
def test_orchestrator(case_id: str = "CASE-1001") -> None:
    report = investigate_case(case_id)

    console.print("[bold green]Full investigation executed successfully[/bold green]")

    console.print("\n[bold cyan]Decision[/bold cyan]")
    console.print(report.decision.model_dump())

    console.print("\n[bold cyan]Top Reasons[/bold cyan]")
    for reason in report.top_reasons:
        console.print(f"- {reason}")

    console.print("\n[bold cyan]Analyst Summary[/bold cyan]")
    console.print(report.analyst_summary)

    console.print("\n[bold cyan]Next Steps[/bold cyan]")
    for step in report.next_steps:
        console.print(f"- {step}")
        
def test_list_saved_reports() -> None:
    conn = get_db_connection()
    cursor = conn.cursor()

    rows = cursor.execute(
        """
        SELECT case_id, generated_at, recommended_action, risk_score, risk_level
        FROM investigation_reports
        ORDER BY id DESC
        LIMIT 10
        """
    ).fetchall()

    console.print("[bold green]Saved investigation reports[/bold green]")
    for row in rows:
        console.print(dict(row))

    conn.close()
    
def inspect_dataset(file_path: str) -> None:
    df = load_csv_dataset(file_path)

    console.print("[bold green]Dataset loaded successfully[/bold green]")
    console.print(f"Shape: {df.shape}")
    console.print(f"Columns: {list(df.columns)}")

    preview = df.head(5)
    console.print("\n[bold cyan]Preview[/bold cyan]")
    console.print(preview.to_string(index=False))
    
def test_dataset_mapper(file_path: str) -> None:
    df = load_csv_dataset(file_path)

    if df.empty:
        raise ValueError("Dataset is empty")

    first_row = df.iloc[0]
    normalized = map_dataset_row_to_normalized(first_row)

    console.print("[bold green]Dataset 2 row mapped successfully[/bold green]")
    console.print(normalized.model_dump())
    
def test_case_generator_from_dataset(file_path: str) -> None:
    df = load_csv_dataset(file_path)

    if df.empty:
        raise ValueError("Dataset is empty")

    target_row = df.iloc[0]

    case = generate_case_from_dataset_row(target_row)
    history = generate_customer_history_from_dataset(df, target_row, lookback_days=30)

    console.print("[bold green]Dataset-backed Case generated successfully[/bold green]")
    console.print(case.model_dump())

    console.print("\n[bold green]Dataset-backed CustomerHistory generated successfully[/bold green]")
    console.print(history.model_dump())
    
def test_dataset_orchestrator(file_path: str, row_index: int = 0) -> None:
    report = investigate_case_from_dataset(file_path, row_index)

    console.print("[bold green]Dataset-backed full investigation executed successfully[/bold green]")

    console.print("\n[bold cyan]Decision[/bold cyan]")
    console.print(report.decision.model_dump())

    console.print("\n[bold cyan]Top Reasons[/bold cyan]")
    for reason in report.top_reasons:
        console.print(f"- {reason}")

    console.print("\n[bold cyan]Analyst Summary[/bold cyan]")
    console.print(report.analyst_summary)

    console.print("\n[bold cyan]Next Steps[/bold cyan]")
    for step in report.next_steps:
        console.print(f"- {step}")
        
def find_first_fraud_row(file_path: str) -> None:
    df = load_csv_dataset(file_path)

    fraud_rows = df[df["is_fraud"] == 1]
    if fraud_rows.empty:
        raise ValueError("No fraud-labeled rows found in dataset")

    idx = int(fraud_rows.index[0])
    console.print(f"[bold green]First fraud row index: {idx}[/bold green]")


def find_first_normal_row(file_path: str) -> None:
    df = load_csv_dataset(file_path)

    normal_rows = df[df["is_fraud"] == 0]
    if normal_rows.empty:
        raise ValueError("No non-fraud rows found in dataset")

    idx = int(normal_rows.index[0])
    console.print(f"[bold green]First normal row index: {idx}[/bold green]")