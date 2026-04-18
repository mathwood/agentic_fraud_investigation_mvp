from __future__ import annotations

import logging

from app.agent.orchestrator import investigate_case_from_preloaded_dataset_structured
from app.storage.dataset_store import (
    clear_enriched_transactions,
    load_raw_transactions_dataframe,
    upsert_enriched_transactions_batch,
)

logger = logging.getLogger(__name__)


def build_enriched_dataset(log_every: int = 500, batch_size: int = 1000, limit: int | None = None) -> int:
    df = load_raw_transactions_dataframe()
    if df.empty:
        raise ValueError("raw_transactions table is empty. Run ingest-dataset first.")

    if limit is not None:
        df = df.head(limit).copy()

    total = len(df)
    written_total = 0
    batch_rows: list[dict] = []

    logger.info("Starting enriched dataset build for %s rows", total)
    logger.info("Using batch_size=%s", batch_size)

    clear_enriched_transactions()

    for row_index in range(total):
        result = investigate_case_from_preloaded_dataset_structured(df=df, row_index=row_index)
        decision = result["decision"]
        raw_row = df.iloc[row_index].to_dict()

        batch_rows.append(
            {
                **raw_row,
                "risk_score": decision["risk_score"],
                "risk_level": decision["risk_level"],
                "recommended_action": decision["recommended_action"],
            }
        )

        if len(batch_rows) >= batch_size:
            written = upsert_enriched_transactions_batch(batch_rows)
            written_total += written
            batch_rows = []
            logger.info("Committed batch. Total written so far: %s/%s", written_total, total)

        if (row_index + 1) % log_every == 0:
            logger.info("Processed %s/%s rows", row_index + 1, total)

    if batch_rows:
        written = upsert_enriched_transactions_batch(batch_rows)
        written_total += written
        logger.info("Committed final batch. Total written: %s/%s", written_total, total)

    logger.info("Finished enriched dataset build with %s written rows", written_total)
    return written_total