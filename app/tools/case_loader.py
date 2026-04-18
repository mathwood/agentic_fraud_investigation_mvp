import json
from pathlib import Path

from app.config import get_settings
from app.models.case_models import Case


def get_case(case_id: str) -> Case:
    """
    Load a fraud investigation case from local mock JSON storage
    and validate it against the Case schema.
    """
    settings = get_settings()
    case_file: Path = settings.data_dir / "mock_cases" / f"{case_id}.json"

    if not case_file.exists():
        raise FileNotFoundError(f"Case file not found: {case_file}")

    with case_file.open("r", encoding="utf-8") as f:
        raw_case = json.load(f)

    return Case.model_validate(raw_case)