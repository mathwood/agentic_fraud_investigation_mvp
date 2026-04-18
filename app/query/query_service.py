from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import logging
import time

import pandas as pd

from app.query.analytics_engine import AnalyticsEngine
from app.query.query_parser import parse_question

logger = logging.getLogger(__name__)


class QueryServiceError(ValueError):
    pass


@dataclass
class DatasetQueryService:
    dataset_path: str
    scorer: Any
    scored_df_cache: pd.DataFrame | None = None
    log_every: int = 500
    max_rows: int | None = None

    def build_scored_dataframe(self, force_rebuild: bool = False) -> pd.DataFrame:
        if self.scored_df_cache is not None and not force_rebuild:
            logger.info("Using in-memory scored dataframe cache with %s rows", len(self.scored_df_cache))
            return self.scored_df_cache.copy()

        start_time = time.time()
        logger.info("Loading dataset from %s", self.dataset_path)
        raw_df = pd.read_csv(self.dataset_path)
        logger.info("Loaded dataset with %s rows", len(raw_df))

        if self.max_rows is not None:
            raw_df = raw_df.head(self.max_rows).copy()
            logger.warning("Limiting scoring to first %s rows for debugging", len(raw_df))

        scored_rows: list[dict[str, Any]] = []
        total_rows = len(raw_df)

        logger.info("Starting deterministic scoring pass for %s rows", total_rows)

        for row_index in range(total_rows):
            try:
                scored_row = self._score_single_row(raw_df, row_index)
                scored_rows.append(scored_row)
            except Exception as exc:
                logger.exception("Failed while scoring row %s", row_index)
                raise QueryServiceError(
                    f"Failed to score dataset row at index {row_index}: {exc}"
                ) from exc

            if (row_index + 1) % self.log_every == 0:
                elapsed = time.time() - start_time
                rate = (row_index + 1) / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "Scored %s/%s rows in %.2fs (%.2f rows/sec)",
                    row_index + 1,
                    total_rows,
                    elapsed,
                    rate,
                )

        scored_df = pd.DataFrame(scored_rows)
        elapsed = time.time() - start_time
        logger.info(
            "Completed scoring %s rows in %.2fs",
            len(scored_df),
            elapsed,
        )

        self.scored_df_cache = scored_df.copy()
        return scored_df

    def ask(self, question: str, force_rebuild: bool = False) -> dict[str, Any]:
        logger.info("Received dataset question: %s", question)
        scored_df = self.build_scored_dataframe(force_rebuild=force_rebuild)
        structured_query = parse_question(question)
        logger.info("Parsed structured query: %s", structured_query.model_dump())

        engine = AnalyticsEngine(scored_df)
        result = engine.run(structured_query)

        logger.info("Query execution finished with result type: %s", result.get("result_type"))

        return {
            "question": question,
            "structured_query": structured_query.model_dump(),
            "result": result,
        }

    def _score_single_row(self, raw_df: pd.DataFrame, row_index: int) -> dict[str, Any]:
        base_row = raw_df.iloc[row_index].to_dict()
        scored = self.scorer.investigate_row(raw_df=raw_df, row_index=row_index)

        if "risk_score" not in scored:
            raise QueryServiceError(f"Scorer output for row {row_index} is missing risk_score.")
        if "recommended_action" not in scored:
            raise QueryServiceError(
                f"Scorer output for row {row_index} is missing recommended_action."
            )

        merged = {
            **base_row,
            "risk_score": scored["risk_score"],
            "recommended_action": scored["recommended_action"],
        }

        if "risk_level" in scored:
            merged["risk_level"] = scored["risk_level"]

        return merged