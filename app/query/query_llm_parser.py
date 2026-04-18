from __future__ import annotations

import json

from openai import OpenAI

from app.config import get_settings
from app.query.query_contract import DatasetQuery


class QueryLLMParserError(ValueError):
    pass


QUERY_DATASET_TOOL = {
    "type": "function",
    "name": "query_dataset",
    "description": (
        "Convert a user's natural-language question about the fraud dataset into a "
        "structured DatasetQuery object for deterministic execution."
    ),
    "strict": True,
    "parameters": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "intent": {
                "type": "string",
                "enum": [
                    "nth_highest_risk_transaction",
                    "top_k_highest_risk_transactions",
                    "user_with_most_suspicious_transactions",
                    "top_k_users_by_suspicious_transaction_count",
                    "count_transactions",
                    "top_category_by_transaction_count",
                ],
            },
            "rank": {
                "type": ["integer", "null"],
                "minimum": 1,
            },
            "limit": {
                "type": ["integer", "null"],
                "minimum": 1,
                "maximum": 100,
            },
            "metric": {
                "type": ["string", "null"],
                "enum": [
                    "risk_score",
                    "suspicious_transaction_count",
                    "declined_transaction_count",
                    None,
                ],
            },
            "filters": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "user_id": {"type": ["integer", "null"]},
                    "recommended_action": {
                        "type": ["string", "null"],
                        "enum": [
                            "approve",
                            "manual_review",
                            "decline",
                            "suspicious",
                            None,
                        ],
                    },
                    "merchant_category": {"type": ["string", "null"]},
                    "country": {"type": ["string", "null"]},
                    "bin_country": {"type": ["string", "null"]},
                    "is_fraud": {"type": ["boolean", "null"]},
                },
                "required": [
                    "user_id",
                    "recommended_action",
                    "merchant_category",
                    "country",
                    "bin_country",
                    "is_fraud",
                ],
            },
            "group_by": {
                "type": ["string", "null"],
                "enum": [
                    "merchant_category",
                    "country",
                    "channel",
                    None,
                ],
            },
            "sort_direction": {
                "type": ["string", "null"],
                "enum": [
                    "asc",
                    "desc",
                    None,
                ],
            },
        },
        "required": [
            "intent",
            "rank",
            "limit",
            "metric",
            "filters",
            "group_by",
            "sort_direction",
        ],
    },
}


SYSTEM_INSTRUCTIONS = """
You map user questions into a structured dataset query.

Supported intents:
- nth_highest_risk_transaction
- top_k_highest_risk_transactions
- user_with_most_suspicious_transactions
- top_k_users_by_suspicious_transaction_count
- count_transactions
- top_category_by_transaction_count

Definitions:
- suspicious transaction = recommended_action in {"manual_review", "decline"}
- highest-risk transaction = sorted by risk_score descending
- if the user says "most fraudulent" without clarifying, interpret it as highest-risk

Critical mapping rules:
- If the question asks about users or customers, use a user-based intent.
- If the question asks about merchant category, country, or channel, use top_category_by_transaction_count.
- "Show top N countries by suspicious transaction count" means:
  - intent = top_category_by_transaction_count
  - group_by = country
  - filters.recommended_action = suspicious
  - limit = N
  - sort_direction = desc
- "Show top N merchant categories by declined transaction count" means:
  - intent = top_category_by_transaction_count
  - group_by = merchant_category
  - filters.recommended_action = decline
  - limit = N
  - sort_direction = desc
- "Which channel has the fewest approved transactions?" means:
  - intent = top_category_by_transaction_count
  - group_by = channel
  - filters.recommended_action = approve
  - limit = 1
  - sort_direction = asc
- "Which user has the most suspicious transactions?" means:
  - intent = user_with_most_suspicious_transactions
- "Show top N users by suspicious transaction count" means:
  - intent = top_k_users_by_suspicious_transaction_count

Use filters only when explicitly stated or strongly implied.
For unsupported questions, do not call the tool.

Examples of supported questions:
- "Which merchant category has the most declined transactions?"
- "In which country most suspicious transactions are received?"
- "In which channel approved transactions are least?"
- "Show top 5 countries by suspicious transaction count"
- "Show top 3 merchant categories by declined transaction count"
- "Show top 10 users by suspicious transaction count"

Examples of unsupported questions:
- "Why is this dataset broken?"
- "What will be the next fraudulent transaction?"
- "Which users are connected through shared addresses?"
- "Find transactions similar to this fraud case"
- "What would happen if AVS matched?"
""".strip()


def parse_question_with_llm(question: str) -> DatasetQuery:
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key)

    response = client.responses.create(
        model=settings.openai_model,
        instructions=SYSTEM_INSTRUCTIONS,
        input=question,
        tools=[QUERY_DATASET_TOOL],
        tool_choice={
            "type": "allowed_tools",
            "mode": "auto",
            "tools": [{"type": "function", "name": "query_dataset"}],
        },
        temperature=0,
    )

    function_calls = [item for item in response.output if item.type == "function_call"]

    if not function_calls:
        raise QueryLLMParserError(
            "Unsupported question for the current dataset query interface."
        )

    call = function_calls[0]

    if call.name != "query_dataset":
        raise QueryLLMParserError(
            f"Unexpected tool call returned by model: {call.name}"
        )

    try:
        arguments = json.loads(call.arguments)
    except json.JSONDecodeError as exc:
        raise QueryLLMParserError(
            f"Tool arguments were not valid JSON: {call.arguments}"
        ) from exc

    try:
        query = DatasetQuery.model_validate(arguments)
        _validate_semantic_alignment(question, query)
        return query
    except Exception as exc:
        raise QueryLLMParserError(
            f"Tool arguments failed DatasetQuery validation: {exc}"
        ) from exc
        
def _validate_semantic_alignment(question: str, query: DatasetQuery) -> None:
    text = question.lower()

    mentions_country = "country" in text or "countries" in text
    mentions_channel = "channel" in text or "channels" in text
    mentions_merchant_category = "merchant category" in text or "merchant categories" in text

    if (mentions_country or mentions_channel or mentions_merchant_category):
        if query.intent in {
            query.intent.USER_WITH_MOST_SUSPICIOUS_TRANSACTIONS,
            query.intent.TOP_K_USERS_BY_SUSPICIOUS_TRANSACTION_COUNT,
        }:
            raise QueryLLMParserError(
                "The parsed query does not match the requested grouping field."
            )