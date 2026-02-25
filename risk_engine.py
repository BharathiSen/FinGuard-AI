"""
Dynamic risk scoring logic for fraud detection.
Combines multiple fraud signals into a comprehensive risk score.
"""

import pathway as pw
from typing import Dict, List, Optional


class RiskScoreSchema(pw.Schema):
    """Schema for risk scores."""
    invoice_id: str
    risk_score: float
    risk_level: str
    risk_factors: str
    confidence: float


def compute_amount_risk(invoices: pw.Table) -> pw.Table:
    """
    Compute risk score based on invoice amount patterns.
    
    Includes deviation percentage from vendor's rolling average.
    
    Args:
        invoices: Pathway Table with invoice data (must include vendor context and deviation_percentage)
    
    Returns:
        Pathway Table with amount-based risk scores
    """
    
    # Calculate amount-based risk factors
    amount_risk = invoices.select(
        invoice_id=pw.this.invoice_id,
        # High amounts are riskier
        high_amount_risk=pw.if_else(
            pw.this.amount > 50000,
            0.8,
            pw.if_else(
                pw.this.amount > 25000,
                0.5,
                pw.if_else(pw.this.amount > 10000, 0.3, 0.1)
            )
        ),
        # Just-under-threshold amounts (common fraud pattern)
        threshold_evasion_risk=pw.if_else(
            (pw.this.amount >= 9500) & (pw.this.amount < 10000),
            0.9,
            pw.if_else(
                (pw.this.amount >= 9000) & (pw.this.amount < 9500),
                0.6,
                0.0
            )
        ),
        # 📊 Deviation from rolling average - uses stateful tracking
        deviation_risk=pw.if_else(
            pw.apply(lambda x: abs(x) > 100, pw.this.deviation_percentage),  # >100% deviation
            0.9,
            pw.if_else(
                pw.apply(lambda x: abs(x) > 50, pw.this.deviation_percentage),  # >50% deviation
                0.6,
                pw.if_else(
                    pw.apply(lambda x: abs(x) > 25, pw.this.deviation_percentage),  # >25% deviation
                    0.3,
                    0.0
                )
            )
        ),
        # Round number suspicious pattern
        round_number_risk=pw.if_else(
            pw.cast(int, pw.this.amount) == pw.this.amount,
            pw.if_else(
                pw.cast(int, pw.this.amount) % 1000 == 0,
                0.4,
                pw.if_else(
                    pw.cast(int, pw.this.amount) % 100 == 0,
                    0.2,
                    0.0
                )
            ),
            0.0
        )
    )
    
    # Combine amount risks (deviation weighted higher)
    amount_risk = amount_risk.select(
        invoice_id=pw.this.invoice_id,
        amount_risk_score=(
            pw.this.high_amount_risk * 0.25 +
            pw.this.threshold_evasion_risk * 0.35 +
            pw.this.deviation_risk * 0.35 +  # Deviation from rolling avg
            pw.this.round_number_risk * 0.05
        )
    )
    
    return amount_risk


def compute_vendor_risk(invoices: pw.Table) -> pw.Table:
    """
    Compute risk score based on vendor behavior patterns.
    
    Includes bank account change detection - HIGH RISK indicator!
    
    Args:
        invoices: Pathway Table with invoice and vendor data (must include bank_account_changed)
    
    Returns:
        Pathway Table with vendor-based risk scores
    """
    
    vendor_risk = invoices.select(
        invoice_id=pw.this.invoice_id,
        # New vendors are riskier
        new_vendor_risk=pw.if_else(
            pw.this.vendor_total_invoices <= 1,
            0.8,
            pw.if_else(
                pw.this.vendor_total_invoices <= 3,
                0.6,
                pw.if_else(
                    pw.this.vendor_total_invoices <= 5,
                    0.3,
                    0.0
                )
            )
        ),
        # High variance in vendor amounts is suspicious
        high_variance_risk=pw.if_else(
            pw.this.vendor_stddev_amount > pw.this.vendor_avg_amount,
            0.7,
            pw.if_else(
                pw.this.vendor_stddev_amount > pw.this.vendor_avg_amount * 0.5,
                0.4,
                0.1
            )
        ),
        # 🏦 Bank account change - CRITICAL RISK INDICATOR
        bank_change_risk=pw.if_else(
            pw.this.bank_account_changed,
            0.95,  # Very high risk - likely payment fraud
            0.0
        )
    )
    
    # Combine vendor risks (bank change weighted heavily)
    vendor_risk = vendor_risk.select(
        invoice_id=pw.this.invoice_id,
        vendor_risk_score=(
            pw.this.new_vendor_risk * 0.3 +
            pw.this.high_variance_risk * 0.2 +
            pw.this.bank_change_risk * 0.5  # Bank change is most critical
        )
    )
    
    return vendor_risk


def compute_temporal_risk(invoices: pw.Table) -> pw.Table:
    """
    Compute risk score based on temporal patterns.
    
    Args:
        invoices: Pathway Table with invoice data
    
    Returns:
        Pathway Table with temporal risk scores
    """
    
    # Extract time features
    temporal_features = invoices.select(
        invoice_id=pw.this.invoice_id,
        timestamp=pw.this.timestamp,
        # In production, extract hour, day of week, etc.
        # For now, use placeholder
        is_weekend=False,  # Placeholder
        is_after_hours=False  # Placeholder
    )
    
    # Compute temporal risk
    temporal_risk = temporal_features.select(
        invoice_id=pw.this.invoice_id,
        temporal_risk_score=pw.if_else(
            pw.this.is_weekend | pw.this.is_after_hours,
            0.5,
            0.1
        )
    )
    
    return temporal_risk


def compute_pattern_risk(
    invoices: pw.Table,
    duplicate_info: Optional[pw.Table] = None
) -> pw.Table:
    """
    Compute risk score based on suspicious patterns.
    
    Args:
        invoices: Pathway Table with invoice data
        duplicate_info: Optional table with duplicate detection results
    
    Returns:
        Pathway Table with pattern-based risk scores
    """
    
    # Payment method risk (wire transfers are higher risk)
    pattern_risk = invoices.select(
        invoice_id=pw.this.invoice_id,
        payment_method_risk=pw.if_else(
            pw.this.payment_method == "Wire Transfer",
            0.6,
            pw.if_else(
                pw.this.payment_method == "Check",
                0.3,
                0.1
            )
        )
    )
    
    # If duplicate info is provided, incorporate it
    if duplicate_info is not None:
        pattern_risk = pattern_risk.join(
            duplicate_info,
            pattern_risk.invoice_id == duplicate_info.invoice_id,
            id=pattern_risk.id,
            how=pw.JoinMode.LEFT
        ).select(
            invoice_id=pattern_risk.invoice_id,
            payment_method_risk=pattern_risk.payment_method_risk,
            duplicate_risk=pw.if_else(
                duplicate_info.is_duplicate,
                0.9,
                0.0
            )
        )
        
        pattern_risk = pattern_risk.select(
            invoice_id=pw.this.invoice_id,
            pattern_risk_score=(
                pw.this.payment_method_risk * 0.4 +
                pw.this.duplicate_risk * 0.6
            )
        )
    else:
        pattern_risk = pattern_risk.select(
            invoice_id=pw.this.invoice_id,
            pattern_risk_score=pw.this.payment_method_risk
        )
    
    return pattern_risk


def compute_composite_risk_score(
    invoices: pw.Table,
    duplicate_info: Optional[pw.Table] = None,
    weights: Optional[Dict[str, float]] = None
) -> pw.Table:
    """
    Compute composite risk score by combining multiple risk factors.
    
    Args:
        invoices: Pathway Table with enriched invoice data (including vendor context)
        duplicate_info: Optional table with duplicate detection results
        weights: Optional custom weights for risk components
    
    Returns:
        Pathway Table with comprehensive risk scores
    """
    
    if weights is None:
        weights = {
            "amount": 0.35,
            "vendor": 0.25,
            "temporal": 0.15,
            "pattern": 0.25
        }
    
    # Compute individual risk components
    amount_risk = compute_amount_risk(invoices)
    vendor_risk = compute_vendor_risk(invoices)
    temporal_risk = compute_temporal_risk(invoices)
    pattern_risk = compute_pattern_risk(invoices, duplicate_info)
    
    # Join all risk scores
    risk_scores = invoices.join(
        amount_risk,
        invoices.invoice_id == amount_risk.invoice_id,
        id=invoices.id
    ).join(
        vendor_risk,
        invoices.invoice_id == vendor_risk.invoice_id,
        id=invoices.id
    ).join(
        temporal_risk,
        invoices.invoice_id == temporal_risk.invoice_id,
        id=invoices.id
    ).join(
        pattern_risk,
        invoices.invoice_id == pattern_risk.invoice_id,
        id=invoices.id
    ).select(
        invoice_id=invoices.invoice_id,
        vendor_id=invoices.vendor_id,
        amount=invoices.amount,
        tax=invoices.tax,
        bank_account=invoices.bank_account,
        timestamp=invoices.timestamp,
        description=invoices.description,
        amount_risk=amount_risk.amount_risk_score,
        vendor_risk=vendor_risk.vendor_risk_score,
        temporal_risk=temporal_risk.temporal_risk_score,
        pattern_risk=pattern_risk.pattern_risk_score
    )
    
    # Compute weighted composite score
    risk_scores = risk_scores.select(
        *pw.this,
        risk_score=(
            pw.this.amount_risk * weights["amount"] +
            pw.this.vendor_risk * weights["vendor"] +
            pw.this.temporal_risk * weights["temporal"] +
            pw.this.pattern_risk * weights["pattern"]
        )
    )
    
    # Classify risk level
    risk_scores = risk_scores.select(
        *pw.this,
        risk_level=pw.if_else(
            pw.this.risk_score >= 0.7,
            "HIGH",
            pw.if_else(
                pw.this.risk_score >= 0.4,
                "MEDIUM",
                "LOW"
            )
        ),
        # Create risk factors summary
        risk_factors=pw.cast(str, pw.this.amount_risk) + "," +
                     pw.cast(str, pw.this.vendor_risk) + "," +
                     pw.cast(str, pw.this.temporal_risk) + "," +
                     pw.cast(str, pw.this.pattern_risk),
        # Confidence based on data completeness
        confidence=pw.if_else(
            invoices.vendor_total_invoices > 10,
            0.9,
            pw.if_else(
                invoices.vendor_total_invoices > 5,
                0.7,
                0.5
            )
        )
    )
    
    return risk_scores


def filter_high_risk_invoices(risk_scores: pw.Table, threshold: float = 0.6) -> pw.Table:
    """
    Filter invoices that exceed a risk threshold.
    
    Args:
        risk_scores: Pathway Table with risk scores
        threshold: Risk score threshold
    
    Returns:
        Pathway Table with high-risk invoices only
    """
    
    high_risk = risk_scores.filter(pw.this.risk_score >= threshold)
    
    return high_risk


def apply_autonomous_decision(risk_scores: pw.Table) -> pw.Table:
    """
    🔥 Autonomous Decision Engine - Automatically classifies invoices based on risk.
    
    Decision Logic:
    - Risk < 30% → AUTO_APPROVE (safe, no review needed)
    - Risk 30-70% → REVIEW_REQUIRED (manual intervention)
    - Risk > 70% → AUTO_REJECT (high fraud probability, block payment)
    
    Args:
        risk_scores: Pathway Table with risk scores
    
    Returns:
        Pathway Table with autonomous decisions added
    """
    
    decisions = risk_scores.select(
        *pw.this,
        # Autonomous decision logic
        decision=pw.if_else(
            pw.this.risk_score < 0.30,
            "AUTO_APPROVE",
            pw.if_else(
                pw.this.risk_score >= 0.70,
                "AUTO_REJECT",
                "REVIEW_REQUIRED"
            )
        ),
        # Decision confidence
        decision_confidence=pw.if_else(
            pw.this.risk_score < 0.30,
            (1.0 - pw.this.risk_score) * 100,  # Lower risk = higher confidence
            pw.if_else(
                pw.this.risk_score >= 0.70,
                pw.this.risk_score * 100,  # Higher risk = higher reject confidence
                50.0  # Medium confidence for review zone
            )
        ),
        # Action required flag
        requires_action=pw.if_else(
            (pw.this.risk_score >= 0.30) & (pw.this.risk_score < 0.70),
            True,
            False
        ),
        # Auto-approval flag (for fast-track processing)
        auto_approved=pw.if_else(pw.this.risk_score < 0.30, True, False),
        # Auto-rejection flag (block payment)
        auto_rejected=pw.if_else(pw.this.risk_score >= 0.70, True, False),
        # Decision rationale
        decision_reason=pw.if_else(
            pw.this.risk_score < 0.30,
            "Low risk score - invoice meets all safety criteria",
            pw.if_else(
                pw.this.risk_score >= 0.70,
                "High fraud probability - multiple risk factors detected",
                "Moderate risk - requires manual verification"
            )
        )
    )
    
    return decisions


def create_risk_alerts(risk_scores: pw.Table) -> pw.Table:
    """
    Create actionable alerts for high-risk invoices.
    
    Args:
        risk_scores: Pathway Table with risk scores
    
    Returns:
        Pathway Table with alert information
    """
    
    alerts = risk_scores.filter(pw.this.risk_level == "HIGH").select(
        invoice_id=pw.this.invoice_id,
        vendor_id=pw.this.vendor_id,
        amount=pw.this.amount,
        risk_score=pw.this.risk_score,
        risk_level=pw.this.risk_level,
        alert_message=pw.cast(str, "HIGH RISK: Invoice " + pw.this.invoice_id +
                              " from vendor " + pw.this.vendor_id +
                              " flagged with risk score " +
                              pw.cast(str, pw.this.risk_score)),
        requires_review=True,
        priority=pw.if_else(
            pw.this.risk_score >= 0.8,
            "CRITICAL",
            "HIGH"
        )
    )
    
    return alerts


if __name__ == "__main__":
    # Example usage
    from invoice_stream import generate_invoice_stream
    from vendor_state import track_vendor_state, enrich_with_vendor_context
    
    print("Testing risk engine...")
    
    # Generate sample invoices
    invoices = generate_invoice_stream(num_invoices=100)
    
    # Track vendor statistics
    vendor_stats = track_vendor_state(invoices)
    
    # Enrich invoices with vendor context
    enriched_invoices = enrich_with_vendor_context(invoices, vendor_stats)
    
    # Compute risk scores
    risk_scores = compute_composite_risk_score(enriched_invoices)
    
    # Get high-risk invoices
    high_risk = filter_high_risk_invoices(risk_scores, threshold=0.6)
    
    # Display results
    print("\nHigh Risk Invoices:")
    pw.debug.compute_and_print(high_risk)
