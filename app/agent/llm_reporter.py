from datetime import datetime, UTC
from typing import Any

from openai import OpenAI

from app.config import get_settings
from app.models.case_models import Case
from app.models.history_models import CustomerHistory
from app.models.signal_models import FraudSignals, RiskScoreBreakdown
from app.models.report_models import InvestigationReport, Decision


def generate_llm_investigation_report(
    case: Case,
    history: CustomerHistory,
    signals: FraudSignals,
    score: RiskScoreBreakdown,
    decision: Decision,
) -> InvestigationReport:
    """
    Use OpenAI to generate an analyst-friendly investigation report
    from deterministic fraud evidence.
    """
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key)

    system_prompt = """
You are a fraud investigation assistant for card-not-present ecommerce payments.

Your job is to write a concise, evidence-based investigation report for a fraud analyst.

Rules:
- Do not invent facts
- Use only the provided evidence
- Do not change the risk score or decision
- Explain the strongest suspicious patterns clearly
- Keep top_reasons short and concrete
- Keep analyst_summary practical and professional
- Suggest next_steps that a fraud analyst could actually take
"""

    user_input: dict[str, Any] = {
        "case": case.model_dump(mode="json"),
        "customer_history": history.model_dump(mode="json"),
        "fraud_signals": signals.model_dump(mode="json"),
        "risk_score_breakdown": score.model_dump(mode="json"),
        "decision": decision.model_dump(mode="json"),
    }

    response = client.responses.parse(
        model=settings.openai_model,
        input=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    "Generate an investigation report from this evidence:\n\n"
                    f"{user_input}"
                ),
            },
        ],
        text_format=InvestigationReport,
    )

    report = response.output_parsed

    # Keep system-owned fields authoritative
    report.case_id = case.case_id
    report.generated_at = datetime.now(UTC)
    report.decision = decision

    return report