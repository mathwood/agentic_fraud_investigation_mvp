from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class QueryIntent(str, Enum):
    NTH_HIGHEST_RISK_TRANSACTION = "nth_highest_risk_transaction"
    TOP_K_HIGHEST_RISK_TRANSACTIONS = "top_k_highest_risk_transactions"
    USER_WITH_MOST_SUSPICIOUS_TRANSACTIONS = "user_with_most_suspicious_transactions"
    TOP_K_USERS_BY_SUSPICIOUS_TRANSACTION_COUNT = "top_k_users_by_suspicious_transaction_count"
    COUNT_TRANSACTIONS = "count_transactions"
    TOP_CATEGORY_BY_TRANSACTION_COUNT = "top_category_by_transaction_count"


class ActionFilter(str, Enum):
    APPROVE = "approve"
    MANUAL_REVIEW = "manual_review"
    DECLINE = "decline"
    SUSPICIOUS = "suspicious"


class RankingMetric(str, Enum):
    RISK_SCORE = "risk_score"
    SUSPICIOUS_TRANSACTION_COUNT = "suspicious_transaction_count"
    DECLINED_TRANSACTION_COUNT = "declined_transaction_count"


class GroupByField(str, Enum):
    MERCHANT_CATEGORY = "merchant_category"
    COUNTRY = "country"
    CHANNEL = "channel"


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class TransactionQueryFilters(BaseModel):
    user_id: Optional[int] = None
    recommended_action: Optional[ActionFilter] = None
    merchant_category: Optional[str] = None
    country: Optional[str] = None
    bin_country: Optional[str] = None
    is_fraud: Optional[bool] = None


class DatasetQuery(BaseModel):
    intent: QueryIntent
    rank: Optional[int] = Field(default=None, ge=1)
    limit: Optional[int] = Field(default=None, ge=1, le=100)
    metric: Optional[RankingMetric] = None
    filters: TransactionQueryFilters = Field(default_factory=TransactionQueryFilters)
    group_by: Optional[GroupByField] = None
    sort_direction: Optional[SortDirection] = None