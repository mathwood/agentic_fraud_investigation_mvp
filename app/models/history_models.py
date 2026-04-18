from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class AmountRange(BaseModel):
    min: float = Field(..., ge=0)
    max: float = Field(..., ge=0)


class TransactionSummary(BaseModel):
    total_transactions: int = Field(..., ge=0)
    total_amount: float = Field(..., ge=0)
    avg_amount: float = Field(..., ge=0)
    max_amount: float = Field(..., ge=0)
    decline_count: int = Field(..., ge=0)
    chargeback_count: int = Field(..., ge=0)


class BehavioralBaseline(BaseModel):
    usual_countries: List[str] = Field(default_factory=list)
    usual_merchant_categories: List[str] = Field(default_factory=list)
    usual_transaction_hours: List[int] = Field(default_factory=list)
    usual_amount_range: AmountRange
    known_devices: List[str] = Field(default_factory=list)
    known_ip_countries: List[str] = Field(default_factory=list)


class RecentActivity(BaseModel):
    transactions_last_24h: int = Field(..., ge=0)
    transactions_last_7d: int = Field(..., ge=0)
    declines_last_24h: int = Field(..., ge=0)
    distinct_merchants_last_24h: int = Field(..., ge=0)
    last_successful_transaction_at: datetime | None = None
    last_successful_transaction_country: str | None = None


class RiskMarkers(BaseModel):
    previous_fraud_flags: int = Field(..., ge=0)
    previous_manual_reviews: int = Field(..., ge=0)
    account_takeover_history: bool = False


class CustomerHistory(BaseModel):
    customer_id: str
    lookback_days: int = Field(..., gt=0)
    transaction_summary: TransactionSummary
    behavioral_baseline: BehavioralBaseline
    recent_activity: RecentActivity
    risk_markers: RiskMarkers