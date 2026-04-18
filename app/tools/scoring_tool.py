from app.models.signal_models import (
    FraudSignals,
    RiskScoreBreakdown,
    VelocitySignals,
    DeviceSignals,
    GeoSignals,
    AmountSignals,
    MerchantSignals,
    BehavioralSignals,
    PaymentAuthSignals,
)
from app.models.report_models import Decision


def merge_signals(
    case_id: str,
    velocity_signals: VelocitySignals,
    device_signals: DeviceSignals,
    geo_signals: GeoSignals,
    amount_signals: AmountSignals,
    merchant_signals: MerchantSignals,
    behavioral_signals: BehavioralSignals,
    payment_auth_signals: PaymentAuthSignals,
) -> FraudSignals:
    risk_labels: list[str] = []

    if velocity_signals.velocity_breach:
        risk_labels.append("velocity_spike")
    if velocity_signals.repeated_same_amount:
        risk_labels.append("repeated_amount")

    if device_signals.new_device:
        risk_labels.append("new_device")

    if geo_signals.ip_country_mismatch:
        risk_labels.append("geo_mismatch")
    if geo_signals.impossible_travel:
        risk_labels.append("impossible_travel")

    if amount_signals.above_customer_max:
        risk_labels.append("amount_outlier")

    if merchant_signals.merchant_category_unusual:
        risk_labels.append("unusual_merchant")

    if behavioral_signals.unusual_transaction_hour:
        risk_labels.append("odd_hour")
    if behavioral_signals.first_time_manual_entry:
        risk_labels.append("manual_entry")
    
    if payment_auth_signals.avs_mismatch:
        risk_labels.append("avs_mismatch")
    if payment_auth_signals.cvv_failed:
        risk_labels.append("cvv_failed")
    if payment_auth_signals.three_ds_missing:
        risk_labels.append("three_ds_missing")
    if payment_auth_signals.bin_country_mismatch:
        risk_labels.append("bin_country_mismatch")
    if payment_auth_signals.excessive_shipping_distance:
        risk_labels.append("shipping_distance_anomaly")

    return FraudSignals(
        case_id=case_id,
        velocity_signals=velocity_signals,
        device_signals=device_signals,
        geo_signals=geo_signals,
        amount_signals=amount_signals,
        merchant_signals=merchant_signals,
        behavioral_signals=behavioral_signals,
        risk_labels=risk_labels,
        payment_auth_signals=payment_auth_signals,
    )


def calculate_score(signals: FraudSignals) -> RiskScoreBreakdown:
    velocity_score = 0
    if signals.velocity_signals.transactions_last_10m >= 5:
        velocity_score += 10
    if signals.velocity_signals.declines_last_10m >= 3:
        velocity_score += 8
    if signals.velocity_signals.distinct_merchants_last_10m >= 4:
        velocity_score += 5
    if signals.velocity_signals.repeated_same_amount:
        velocity_score += 2
    velocity_score = min(velocity_score, 25)

    device_score = 0
    if signals.device_signals.new_device:
        device_score += 10
    if signals.device_signals.device_change_recently:
        device_score += 5
    if signals.device_signals.browser_fingerprint_mismatch:
        device_score += 5
    device_score = min(device_score, 20)

    geo_score = 0
    if signals.geo_signals.ip_country_mismatch:
        geo_score += 8
    if signals.geo_signals.bin_ip_country_mismatch:
        geo_score += 5
    if signals.geo_signals.shipping_ip_mismatch:
        geo_score += 3
    if signals.geo_signals.impossible_travel:
        geo_score += 10
    geo_score = min(geo_score, 20)

    amount_score = 0
    if signals.amount_signals.above_customer_average:
        amount_score += 5
    if signals.amount_signals.above_customer_max:
        amount_score += 7
    if signals.amount_signals.amount_deviation_ratio >= 3:
        amount_score += 3
    amount_score = min(amount_score, 15)

    merchant_score = 0
    if signals.merchant_signals.new_merchant_for_customer:
        merchant_score += 4
    if signals.merchant_signals.merchant_category_unusual:
        merchant_score += 3
    if signals.merchant_signals.merchant_risk_level == "medium":
        merchant_score += 1
    elif signals.merchant_signals.merchant_risk_level == "high":
        merchant_score += 3
    merchant_score = min(merchant_score, 10)

    behavioral_score = 0
    if signals.behavioral_signals.unusual_transaction_hour:
        behavioral_score += 4
    if signals.behavioral_signals.first_time_manual_entry:
        behavioral_score += 4
    behavioral_score = min(behavioral_score, 10)
    
    payment_auth_score = 0
    if signals.payment_auth_signals.avs_mismatch:
        payment_auth_score += 12
    if signals.payment_auth_signals.cvv_failed:
        payment_auth_score += 14
    if signals.payment_auth_signals.three_ds_missing:
        payment_auth_score += 6
    if signals.payment_auth_signals.bin_country_mismatch:
        payment_auth_score += 8
    if signals.payment_auth_signals.excessive_shipping_distance:
        payment_auth_score += 8
    payment_auth_score = min(payment_auth_score, 35)
    
    combo_score = 0

    if signals.payment_auth_signals.avs_mismatch and signals.payment_auth_signals.cvv_failed:
        combo_score += 10

    if (
        signals.payment_auth_signals.avs_mismatch
        and signals.payment_auth_signals.cvv_failed
        and signals.payment_auth_signals.bin_country_mismatch
    ):
        combo_score += 15

    if (
        signals.payment_auth_signals.avs_mismatch
        and signals.payment_auth_signals.cvv_failed
        and signals.payment_auth_signals.excessive_shipping_distance
    ):
        combo_score += 15

    if (
        signals.payment_auth_signals.three_ds_missing
        and signals.payment_auth_signals.bin_country_mismatch
        and signals.payment_auth_signals.excessive_shipping_distance
    ):
        combo_score += 10

    total_score = (
        velocity_score
        + device_score
        + geo_score
        + amount_score
        + merchant_score
        + behavioral_score
        + payment_auth_score
        + combo_score
    )
    total_score = min(total_score, 100)
    
    

    return RiskScoreBreakdown(
        velocity_score=velocity_score,
        device_score=device_score,
        geo_score=geo_score,
        amount_score=amount_score,
        merchant_score=merchant_score,
        behavioral_score=behavioral_score,
        payment_auth_score=payment_auth_score,
        total_score=total_score,
    )


def get_decision(score: RiskScoreBreakdown, signals: FraudSignals) -> Decision:
    hard_decline = (
        (signals.geo_signals.impossible_travel and signals.device_signals.new_device)
        or (
            signals.velocity_signals.transactions_last_10m >= 8
            and signals.velocity_signals.declines_last_10m >= 5
        )
        or (
            signals.amount_signals.above_customer_max
            and signals.amount_signals.amount_deviation_ratio >= 5
            and signals.geo_signals.ip_country_mismatch
        )
        or (
            signals.payment_auth_signals.avs_mismatch
            and signals.payment_auth_signals.cvv_failed
        )
        or (
            signals.payment_auth_signals.cvv_failed
            and signals.payment_auth_signals.bin_country_mismatch
        )
        or (
        signals.payment_auth_signals.avs_mismatch
            and signals.payment_auth_signals.bin_country_mismatch
            and signals.payment_auth_signals.three_ds_missing
        )
        or (
            signals.amount_signals.above_customer_max
        and signals.payment_auth_signals.cvv_failed
        )
        or (
            signals.payment_auth_signals.avs_mismatch
            and signals.payment_auth_signals.cvv_failed
            and signals.payment_auth_signals.excessive_shipping_distance
        )
        or (
            signals.payment_auth_signals.cvv_failed
            and signals.payment_auth_signals.bin_country_mismatch
            and signals.amount_signals.above_customer_average
        )
        or (
            signals.payment_auth_signals.avs_mismatch
            and signals.payment_auth_signals.cvv_failed
            and signals.payment_auth_signals.bin_country_mismatch
        )
    )

    if score.total_score >= 60:
        risk_level = "high"
    elif score.total_score >= 25:
        risk_level = "medium"
    else:
        risk_level = "low"

    if hard_decline:
        recommended_action = "decline"
    elif score.total_score >= 45:
        recommended_action = "decline"
    elif score.total_score >= 20:
        recommended_action = "manual_review"
    else:
        recommended_action = "approve"

    return Decision(
        recommended_action=recommended_action,
        risk_score=score.total_score,
        risk_level=risk_level,
    )