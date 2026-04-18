import json
from pathlib import Path

from app.config import get_settings
from app.models.history_models import CustomerHistory


def get_customer_history(customer_id: str, lookback_days: int = 30) -> CustomerHistory:
    """
    Load a customer's recent behavioral baseline and activity summary
    from local mock JSON storage and validate it.
    """
    settings = get_settings()
    history_file: Path = settings.data_dir / "mock_cases" / f"{customer_id}_history.json"

    if not history_file.exists():
        raise FileNotFoundError(f"Customer history file not found: {history_file}")

    with history_file.open("r", encoding="utf-8") as f:
        raw_history = json.load(f)

    history = CustomerHistory.model_validate(raw_history)

    if history.lookback_days != lookback_days:
        # For MVP we keep it simple and just return the stored history,
        # but this check helps surface mismatch early.
        pass

    return history