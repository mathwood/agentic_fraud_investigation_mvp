from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from app.query.query_contract import ActionFilter, DatasetQuery, QueryIntent


class AnalyticsExecutionError(ValueError):
    pass


@dataclass
class AnalyticsEngine:
    df: pd.DataFrame

    def run(self, query: DatasetQuery) -> dict[str, Any]:
        working_df = self._prepare_dataframe(self.df.copy())
        filtered_df = self._apply_filters(working_df, query)

        if query.intent == QueryIntent.NTH_HIGHEST_RISK_TRANSACTION:
            return self._run_nth_highest_risk_transaction(filtered_df, query)

        if query.intent == QueryIntent.TOP_K_HIGHEST_RISK_TRANSACTIONS:
            return self._run_top_k_highest_risk_transactions(filtered_df, query)

        if query.intent == QueryIntent.USER_WITH_MOST_SUSPICIOUS_TRANSACTIONS:
            return self._run_user_with_most_suspicious_transactions(filtered_df, query)

        if query.intent == QueryIntent.TOP_K_USERS_BY_SUSPICIOUS_TRANSACTION_COUNT:
            return self._run_top_k_users_by_suspicious_transaction_count(filtered_df, query)

        if query.intent == QueryIntent.COUNT_TRANSACTIONS:
            return self._run_count_transactions(filtered_df, query)

        raise AnalyticsExecutionError(f"Unsupported query intent: {query.intent}")

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        required_columns = {
            "transaction_id",
            "user_id",
            "amount",
            "country",
            "bin_country",
            "merchant_category",
            "is_fraud",
            "risk_score",
            "recommended_action",
        }
        missing = required_columns - set(df.columns)
        if missing:
            raise AnalyticsExecutionError(
                f"Input dataframe is missing required columns: {sorted(missing)}"
            )

        return df.reset_index().rename(columns={"index": "dataset_index"})

    def _apply_filters(self, df: pd.DataFrame, query: DatasetQuery) -> pd.DataFrame:
        filters = query.filters

        if filters.user_id is not None:
            df = df[df["user_id"] == filters.user_id]

        if filters.merchant_category is not None:
            df = df[df["merchant_category"].astype(str).str.lower() == filters.merchant_category.lower()]

        if filters.country is not None:
            df = df[df["country"].astype(str).str.lower() == filters.country.lower()]

        if filters.bin_country is not None:
            df = df[df["bin_country"].astype(str).str.lower() == filters.bin_country.lower()]

        if filters.is_fraud is not None:
            df = df[df["is_fraud"] == (1 if filters.is_fraud else 0)]

        if filters.recommended_action is not None:
            if filters.recommended_action == ActionFilter.SUSPICIOUS:
                df = df[df["recommended_action"].isin(["manual_review", "decline"])]
            else:
                df = df[df["recommended_action"] == filters.recommended_action.value]

        return df

    def _sort_transactions_by_risk(self, df: pd.DataFrame) -> pd.DataFrame:
        action_rank = {"decline": 3, "manual_review": 2, "approve": 1}
        sortable = df.copy()
        sortable["action_rank"] = sortable["recommended_action"].map(action_rank).fillna(0)

        sortable = sortable.sort_values(
            by=["risk_score", "action_rank", "dataset_index"],
            ascending=[False, False, True],
        )
        return sortable.drop(columns=["action_rank"])

    def _run_nth_highest_risk_transaction(self, df: pd.DataFrame, query: DatasetQuery) -> dict[str, Any]:
        sorted_df = self._sort_transactions_by_risk(df)
        if sorted_df.empty:
            raise AnalyticsExecutionError("No transactions matched the query filters.")

        rank = query.rank or 1
        if rank > len(sorted_df):
            raise AnalyticsExecutionError(
                f"Requested rank {rank} but only {len(sorted_df)} transaction(s) matched."
            )

        row = sorted_df.iloc[rank - 1]
        return {
            "query_intent": query.intent.value,
            "rank": rank,
            "result_type": "single_transaction",
            "transaction": self._transaction_record(row),
            "matched_count": len(sorted_df),
        }

    def _run_top_k_highest_risk_transactions(self, df: pd.DataFrame, query: DatasetQuery) -> dict[str, Any]:
        sorted_df = self._sort_transactions_by_risk(df)
        if sorted_df.empty:
            raise AnalyticsExecutionError("No transactions matched the query filters.")

        limit = query.limit or 5
        top_rows = sorted_df.head(limit)

        return {
            "query_intent": query.intent.value,
            "limit": limit,
            "result_type": "transaction_list",
            "transactions": [self._transaction_record(row) for _, row in top_rows.iterrows()],
            "matched_count": len(sorted_df),
        }

    def _run_user_with_most_suspicious_transactions(self, df: pd.DataFrame, query: DatasetQuery) -> dict[str, Any]:
        suspicious_df = df[df["recommended_action"].isin(["manual_review", "decline"])]
        if suspicious_df.empty:
            raise AnalyticsExecutionError("No suspicious transactions found.")

        user_stats = self._build_user_stats(suspicious_df)
        top_user = user_stats.iloc[0]

        return {
            "query_intent": query.intent.value,
            "result_type": "single_user",
            "user": {
                "user_id": int(top_user["user_id"]),
                "suspicious_transaction_count": int(top_user["suspicious_transaction_count"]),
                "declined_transaction_count": int(top_user["declined_transaction_count"]),
                "average_risk_score": float(top_user["average_risk_score"]),
                "max_risk_score": float(top_user["max_risk_score"]),
            },
            "matched_user_count": len(user_stats),
        }

    def _run_top_k_users_by_suspicious_transaction_count(self, df: pd.DataFrame, query: DatasetQuery) -> dict[str, Any]:
        suspicious_df = df[df["recommended_action"].isin(["manual_review", "decline"])]
        if suspicious_df.empty:
            raise AnalyticsExecutionError("No suspicious transactions found.")

        user_stats = self._build_user_stats(suspicious_df)
        limit = query.limit or 5
        top_users = user_stats.head(limit)

        return {
            "query_intent": query.intent.value,
            "limit": limit,
            "result_type": "user_list",
            "users": [
                {
                    "user_id": int(row["user_id"]),
                    "suspicious_transaction_count": int(row["suspicious_transaction_count"]),
                    "declined_transaction_count": int(row["declined_transaction_count"]),
                    "average_risk_score": float(row["average_risk_score"]),
                    "max_risk_score": float(row["max_risk_score"]),
                }
                for _, row in top_users.iterrows()
            ],
            "matched_user_count": len(user_stats),
        }

    def _run_count_transactions(self, df: pd.DataFrame, query: DatasetQuery) -> dict[str, Any]:
        return {
            "query_intent": query.intent.value,
            "result_type": "count",
            "count": int(len(df)),
            "applied_filters": query.filters.model_dump(),
        }

    def _build_user_stats(self, suspicious_df: pd.DataFrame) -> pd.DataFrame:
        grouped = (
            suspicious_df.groupby("user_id", as_index=False)
            .agg(
                suspicious_transaction_count=("user_id", "size"),
                declined_transaction_count=("recommended_action", lambda s: (s == "decline").sum()),
                average_risk_score=("risk_score", "mean"),
                max_risk_score=("risk_score", "max"),
            )
        )

        grouped = grouped.sort_values(
            by=[
                "suspicious_transaction_count",
                "declined_transaction_count",
                "average_risk_score",
                "user_id",
            ],
            ascending=[False, False, False, True],
        )
        return grouped

    def _transaction_record(self, row: pd.Series) -> dict[str, Any]:
        return {
            "dataset_index": int(row["dataset_index"]),
            "transaction_id": str(row["transaction_id"]),
            "user_id": int(row["user_id"]),
            "amount": float(row["amount"]),
            "country": str(row["country"]),
            "bin_country": str(row["bin_country"]),
            "merchant_category": str(row["merchant_category"]),
            "is_fraud": int(row["is_fraud"]),
            "risk_score": float(row["risk_score"]),
            "recommended_action": str(row["recommended_action"]),
        }