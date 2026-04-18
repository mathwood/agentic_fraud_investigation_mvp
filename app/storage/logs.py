import json

from app.models.report_models import InvestigationReport
from app.storage.db import get_db_connection


def save_investigation_report(report: InvestigationReport) -> None:
    conn = get_db_connection()
    cursor = conn.cursor()

    report_json = json.dumps(report.model_dump(mode="json"))

    cursor.execute(
        """
        INSERT INTO investigation_reports (
            case_id,
            generated_at,
            recommended_action,
            risk_score,
            risk_level,
            report_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            report.case_id,
            report.generated_at.isoformat(),
            report.decision.recommended_action,
            report.decision.risk_score,
            report.decision.risk_level,
            report_json,
        ),
    )

    conn.commit()
    conn.close()