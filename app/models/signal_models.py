from typing import List, Literal

from pydantic import BaseModel, Field


MerchantRiskLevel = Literal["low", "medium", "high"]


class VelocitySignals(BaseModel):
    transactions_last_10m: int = Field(..., ge=0)
    declines_last_10m: int = Field(..., ge=0)
    distinct_merchants_last_10m: int = Field(..., ge=0)
    repeated_same_amount: bool = False
    velocity_breach: bool = False


class DeviceSignals(BaseModel):
    new_device: bool = False
    device_change_recently: bool = False
    browser_fingerprint_mismatch: bool = False


class GeoSignals(BaseModel):
    ip_country_mismatch: bool = False
    bin_ip_country_mismatch: bool = False
    shipping_ip_mismatch: bool = False
    impossible_travel: bool = False


class AmountSignals(BaseModel):
    above_customer_average: bool = False
    above_customer_max: bool = False
    amount_deviation_ratio: float = Field(..., ge=0)


class MerchantSignals(BaseModel):
    new_merchant_for_customer: bool = False
    merchant_category_unusual: bool = False
    merchant_risk_level: MerchantRiskLevel = "low"


class BehavioralSignals(BaseModel):
    unusual_transaction_hour: bool = False
    first_time_manual_entry: bool = False


class FraudSignals(BaseModel):
    case_id: str
    velocity_signals: VelocitySignals
    device_signals: DeviceSignals
    geo_signals: GeoSignals
    amount_signals: AmountSignals
    merchant_signals: MerchantSignals
    behavioral_signals: BehavioralSignals
    payment_auth_signals: PaymentAuthSignals
    risk_labels: List[str] = Field(default_factory=list)


class RiskScoreBreakdown(BaseModel):
    velocity_score: int = Field(..., ge=0, le=25)
    device_score: int = Field(..., ge=0, le=20)
    geo_score: int = Field(..., ge=0, le=20)
    amount_score: int = Field(..., ge=0, le=15)
    merchant_score: int = Field(..., ge=0, le=10)
    behavioral_score: int = Field(..., ge=0, le=10)
    payment_auth_score: int = Field(..., ge=0, le=35)
    total_score: int = Field(..., ge=0, le=100)
    
class PaymentAuthSignals(BaseModel):
    avs_mismatch: bool = False
    cvv_failed: bool = False
    three_ds_missing: bool = False
    bin_country_mismatch: bool = False
    excessive_shipping_distance: bool = False