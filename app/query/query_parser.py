from __future__ import annotations

import re

from pydantic import ValidationError

from app.query.query_contract import (
    ActionFilter,
    DatasetQuery,
    QueryIntent,
    RankingMetric,
    TransactionQueryFilters,
)


class QueryParsingError(ValueError):
    pass


ORDINAL_WORDS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
}


def normalize_question(question: str) -> str:
    text = question.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def parse_ordinal(text: str) -> int | None:
    digit_match = re.search(r"\b(\d+)(st|nd|rd|th)\b", text)
    if digit_match:
        return int(digit_match.group(1))

    for word, value in ORDINAL_WORDS.items():
        if re.search(rf"\b{word}\b", text):
            return value

    return None


def parse_top_k(text: str) -> int | None:
    match = re.search(r"\btop\s+(\d+)\b", text)
    if match:
        return int(match.group(1))
    return None


def parse_user_id(text: str) -> int | None:
    patterns = [
        r"\buser\s+id\s+(\d+)\b",
        r"\buser\s+(\d+)\b",
        r"\bcustomer\s+id\s+(\d+)\b",
        r"\bcustomer\s+(\d+)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return None


def parse_action_filter(text: str) -> ActionFilter | None:
    if "suspicious" in text:
        return ActionFilter.SUSPICIOUS
    if "declined" in text or "decline" in text:
        return ActionFilter.DECLINE
    if "manual review" in text:
        return ActionFilter.MANUAL_REVIEW
    if "approved" in text or "approve" in text:
        return ActionFilter.APPROVE
    return None


def parse_merchant_category(text: str) -> str | None:
    patterns = [
        r"\bin\s+([a-zA-Z_]+)\b",
        r"\bin\s+the\s+([a-zA-Z_]+)\s+category\b",
    ]

    blocked_words = {
        "the",
        "dataset",
        "user",
        "country",
        "bin",
        "fraud",
        "decline",
        "declined",
        "suspicious",
        "transactions",
        "transaction",
        "category",
    }

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            token = match.group(1).strip().lower()
            if token not in blocked_words:
                return token

    return None


def parse_question(question: str) -> DatasetQuery:
    text = normalize_question(question)

    try:
        if (
            "transaction" in text
            and "top" in text
            and (
                "highest-risk" in text
                or "highest risk" in text
                or "most suspicious" in text
                or "most fraudulent" in text
            )
        ):
            limit = parse_top_k(text)
            if limit is None:
                raise QueryParsingError("Could not determine top-k limit.")

            return DatasetQuery(
                intent=QueryIntent.TOP_K_HIGHEST_RISK_TRANSACTIONS,
                limit=limit,
                metric=RankingMetric.RISK_SCORE,
            )

        if (
            "transaction" in text
            and (
                "highest-risk" in text
                or "highest risk" in text
                or "most suspicious" in text
                or "most fraudulent" in text
            )
        ):
            rank = parse_ordinal(text)
            if "highest-risk transaction" in text or "highest risk transaction" in text:
                rank = rank or 1
            if "what is the highest-risk transaction" in text or "what is the highest risk transaction" in text:
                rank = 1

            return DatasetQuery(
                intent=QueryIntent.NTH_HIGHEST_RISK_TRANSACTION,
                rank=rank or 1,
                metric=RankingMetric.RISK_SCORE,
            )

        if (
            ("which user" in text or "which customer" in text or "who" in text)
            and "most" in text
            and "suspicious" in text
            and "transaction" in text
        ):
            return DatasetQuery(
                intent=QueryIntent.USER_WITH_MOST_SUSPICIOUS_TRANSACTIONS,
                metric=RankingMetric.SUSPICIOUS_TRANSACTION_COUNT,
            )

        if (
            "top" in text
            and ("users" in text or "user" in text or "customers" in text or "customer" in text)
            and "suspicious" in text
            and "transaction" in text
        ):
            limit = parse_top_k(text)
            if limit is None:
                raise QueryParsingError("Could not determine top-k limit.")

            return DatasetQuery(
                intent=QueryIntent.TOP_K_USERS_BY_SUSPICIOUS_TRANSACTION_COUNT,
                limit=limit,
                metric=RankingMetric.SUSPICIOUS_TRANSACTION_COUNT,
            )

        if "how many" in text and "transaction" in text:
            return DatasetQuery(
                intent=QueryIntent.COUNT_TRANSACTIONS,
                filters=TransactionQueryFilters(
                    user_id=parse_user_id(text),
                    recommended_action=parse_action_filter(text),
                    merchant_category=parse_merchant_category(text),
                ),
            )

    except ValidationError as exc:
        raise QueryParsingError(f"Matched a supported pattern but failed validation: {exc}") from exc

    raise QueryParsingError(
        "Unsupported question. Supported MVP questions: ranked transactions, ranked users, and count queries."
    )