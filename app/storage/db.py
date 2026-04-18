import sqlite3
from pathlib import Path

from app.config import get_settings


def get_db_connection() -> sqlite3.Connection:
    settings = get_settings()
    db_path: Path = settings.db_path

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS investigation_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            recommended_action TEXT NOT NULL,
            risk_score INTEGER NOT NULL,
            risk_level TEXT NOT NULL,
            report_json TEXT NOT NULL
        )
        """
    )

    conn.commit()
    conn.close()