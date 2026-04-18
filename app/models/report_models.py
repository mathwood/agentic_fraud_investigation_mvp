from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


RecommendedAction = Literal["approve", "manual_review", "decline"]
RiskLevel = Literal["low", "medium", "high"]


class Decision(BaseModel):
    recommended_action: RecommendedAction
    risk_score: int = Field(..., ge=0, le=100)
    risk_level: RiskLevel


class EvidenceSummary(BaseModel):
    customer_usual_countries: List[str] = Field(default_factory=list)
    current_ip_country: Optional[str] = None
    customer_avg_amount: Optional[float] = None
    customer_max_amount: Optional[float] = None
    current_amount: Optional[float] = None
    transactions_last_10m: Optional[int] = None
    declines_last_10m: Optional[int] = None


class InvestigationReport(BaseModel):
    case_id: str
    generated_at: datetime

    decision: Decision

    top_reasons: List[str] = Field(default_factory=list)
    risk_labels: List[str] = Field(default_factory=list)

    evidence_summary: EvidenceSummary

    analyst_summary: str

    next_steps: List[str] = Field(default_factory=list)