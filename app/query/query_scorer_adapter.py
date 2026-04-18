from __future__ import annotations

from dataclasses import dataclass
import logging

import pandas as pd

from app.agent.orchestrator import investigate_case_from_preloaded_dataset_structured

logger = logging.getLogger(__name__)


class QueryScorerAdapterError(ValueError):
    pass


@dataclass
class QueryScorerAdapter:
    dataset_path: str

    def investigate_row(self, raw_df: pd.DataFrame, row_index: int) -> dict:
        try:
            result = investigate_case_from_preloaded_dataset_structured(
                df=raw_df,
                row_index=row_index,
            )
        except Exception as exc:
            raise QueryScorerAdapterError(f"Failed to score row {row_index}: {exc}") from exc

        decision = result.get("decision")
        if not decision:
            raise QueryScorerAdapterError(
                f"Row {row_index} investigation missing decision block."
            )

        return {
            "risk_score": decision["risk_score"],
            "recommended_action": decision["recommended_action"],
            "risk_level": decision["risk_level"],
        }