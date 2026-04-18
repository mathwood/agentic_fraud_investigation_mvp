from __future__ import annotations

from app.query.query_contract import (
    ActionFilter,
    DatasetQuery,
    QueryIntent,
    RankingMetric,
    TransactionQueryFilters,
)


SUPPORTED_QUERY_EXAMPLES = [
    {
        "question": "What is the highest-risk transaction?",
        "structured_query": DatasetQuery(
            intent=QueryIntent.NTH_HIGHEST_RISK_TRANSACTION,
            rank=1,
            metric=RankingMetric.RISK_SCORE,
        ),
    },
    {
        "question": "What is the 3rd most fraudulent transaction?",
        "structured_query": DatasetQuery(
            intent=QueryIntent.NTH_HIGHEST_RISK_TRANSACTION,
            rank=3,
            metric=RankingMetric.RISK_SCORE,
        ),
    },
    {
        "question": "Show top 5 highest-risk transactions",
        "structured_query": DatasetQuery(
            intent=QueryIntent.TOP_K_HIGHEST_RISK_TRANSACTIONS,
            limit=5,
            metric=RankingMetric.RISK_SCORE,
        ),
    },
    {
        "question": "Which user has the most suspicious transactions?",
        "structured_query": DatasetQuery(
            intent=QueryIntent.USER_WITH_MOST_SUSPICIOUS_TRANSACTIONS,
            metric=RankingMetric.SUSPICIOUS_TRANSACTION_COUNT,
        ),
    },
    {
        "question": "Show top 10 users by suspicious transaction count",
        "structured_query": DatasetQuery(
            intent=QueryIntent.TOP_K_USERS_BY_SUSPICIOUS_TRANSACTION_COUNT,
            limit=10,
            metric=RankingMetric.SUSPICIOUS_TRANSACTION_COUNT,
        ),
    },
    {
        "question": "How many suspicious transactions are there?",
        "structured_query": DatasetQuery(
            intent=QueryIntent.COUNT_TRANSACTIONS,
            filters=TransactionQueryFilters(
                recommended_action=ActionFilter.SUSPICIOUS
            ),
        ),
    },
    {
        "question": "How many declined transactions belong to user 32?",
        "structured_query": DatasetQuery(
            intent=QueryIntent.COUNT_TRANSACTIONS,
            filters=TransactionQueryFilters(
                user_id=32,
                recommended_action=ActionFilter.DECLINE,
            ),
        ),
    },
    {
        "question": "How many suspicious transactions are there in electronics?",
        "structured_query": DatasetQuery(
            intent=QueryIntent.COUNT_TRANSACTIONS,
            filters=TransactionQueryFilters(
                merchant_category="electronics",
                recommended_action=ActionFilter.SUSPICIOUS,
            ),
        ),
    },
]


UNSUPPORTED_QUERY_EXAMPLES = [
    {
        "question": "What will be the next fraudulent transaction?",
        "reason": "Forecasting is out of scope for the MVP query interface.",
    },
    {
        "question": "What would happen if AVS matched?",
        "reason": "Counterfactual simulation is not supported yet.",
    },
    {
        "question": "Which users are connected through shared addresses?",
        "reason": "Graph or entity-link analysis is not supported in this MVP.",
    },
    {
        "question": "Find transactions similar to this fraud case",
        "reason": "Similarity retrieval and vector search are not supported yet.",
    },
]