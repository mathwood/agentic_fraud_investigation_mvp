from datetime import datetime, date
from typing import Literal, Optional

from pydantic import BaseModel, Field


TransactionStatus = Literal["authorized", "declined", "reversed"]
AlertType = Literal["suspicious_cnp_transaction"]
ChannelType = Literal["ecommerce"]
KYCLevel = Literal["minimal", "partial", "full"]
CustomerSegment = Literal["retail", "premium", "sme"]


class Transaction(BaseModel):
    transaction_id: str
    timestamp: datetime
    amount: float = Field(..., ge=0)
    currency: str = Field(..., min_length=3, max_length=3)
    channel: ChannelType
    card_present: bool = False
    merchant_id: str
    merchant_name: Optional[str] = None
    merchant_category: str
    merchant_country: str = Field(..., min_length=2, max_length=2)
    status: TransactionStatus


class Customer(BaseModel):
    customer_id: str
    account_id: str
    card_id: str
    customer_since: date
    kyc_level: KYCLevel
    segment: CustomerSegment


class Context(BaseModel):
    device_id: Optional[str] = None
    device_known: bool = False
    ip_address: Optional[str] = None
    ip_country: Optional[str] = Field(default=None, min_length=2, max_length=2)
    billing_country: Optional[str] = Field(default=None, min_length=2, max_length=2)
    shipping_country: Optional[str] = Field(default=None, min_length=2, max_length=2)
    entry_mode: Optional[str] = None
    browser_fingerprint: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None


class Case(BaseModel):
    case_id: str
    alert_id: str
    alert_type: AlertType
    alert_created_at: datetime
    transaction: Transaction
    customer: Customer
    context: Context