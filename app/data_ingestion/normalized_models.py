from datetime import datetime
from typing import Optional, Literal

from pydantic import BaseModel, Field


TransactionStatus = Literal["authorized", "declined", "reversed", "unknown"]


class NormalizedTransaction(BaseModel):
    transaction_id: str
    customer_id: str
    card_id: Optional[str] = None

    timestamp: datetime

    amount: float = Field(..., ge=0)
    currency: str = "EUR"

    merchant_id: Optional[str] = None
    merchant_name: Optional[str] = None
    merchant_category: Optional[str] = None
    merchant_country: Optional[str] = None

    status: TransactionStatus = "unknown"

    channel: Optional[str] = None

    device_id: Optional[str] = None
    device_known: Optional[bool] = None
    ip_address: Optional[str] = None
    ip_country: Optional[str] = None
    billing_country: Optional[str] = None
    shipping_country: Optional[str] = None
    entry_mode: Optional[str] = None
    browser_fingerprint: Optional[str] = None

    account_age_days: Optional[int] = None
    total_transactions_user: Optional[int] = None
    avg_amount_user: Optional[float] = None
    bin_country: Optional[str] = None
    promo_used: Optional[bool] = None
    avs_match: Optional[str] = None
    cvv_result: Optional[str] = None
    three_ds_flag: Optional[bool] = None
    shipping_distance_km: Optional[float] = None

    fraud_label: Optional[int] = None