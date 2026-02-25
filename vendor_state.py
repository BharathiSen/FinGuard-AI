"""
Stateful vendor behavior tracking using Pathway incremental aggregations.
Maintains running statistics for each vendor to detect anomalies.
"""

import pathway as pw
from typing import Any


class VendorStatsSchema(pw.Schema):
    """Schema for vendor statistics."""
    vendor_id: str
    total_invoices: int
    total_amount: float
    avg_amount: float
    max_amount: float
    min_amount: float
    stddev_amount: float
    last_invoice_time: str
    last_bank_account: str  # Track most recent bank account


def track_vendor_state(invoices: pw.Table) -> pw.Table:
    """
    Track stateful vendor statistics using Pathway's incremental aggregations.
    
    Maintains rolling statistics that auto-update when new invoices arrive:
    - Rolling average invoice amount
    - Total invoices and amounts
    - Min/max/stddev for anomaly detection
    - Most recent bank account (for change detection)
    
    All computations are incremental - NO full dataset recomputation.
    
    Args:
        invoices: Pathway Table with invoice data
    
    Returns:
        Pathway Table with vendor statistics (auto-updating)
    """
    
    # Group by vendor and compute incremental aggregations
    # pw.reducers maintain internal state and update only on new/changed data
    vendor_stats = invoices.groupby(invoices.vendor_id).reduce(
        vendor_id=invoices.vendor_id,
        # Count and totals
        total_invoices=pw.reducers.count(),
        total_amount=pw.reducers.sum(invoices.amount),
        # Rolling average - updates incrementally
        avg_amount=pw.reducers.avg(invoices.amount),
        # E[X²] for stddev computation (pw 0.8.0 has no pw.reducers.stddev)
        _avg_amount_sq=pw.reducers.avg(invoices.amount * invoices.amount),
        # Range statistics for anomaly detection
        max_amount=pw.reducers.max(invoices.amount),
        min_amount=pw.reducers.min(invoices.amount),
        # Temporal tracking — max() on ISO strings = most recent timestamp
        last_invoice_time=pw.reducers.max(invoices.timestamp),
        # Bank account tracking - stores most recent bank account
        last_bank_account=pw.reducers.max(invoices.bank_account)
    )

    # Compute stddev = sqrt(E[X²] - (E[X])²) via pw.apply_with_type
    vendor_stats = vendor_stats.select(
        *pw.this,
        stddev_amount=pw.apply_with_type(
            lambda sq, mean: float(max(0.0, sq - mean * mean) ** 0.5),
            float,
            pw.this._avg_amount_sq,
            pw.this.avg_amount
        )
    )

    return vendor_stats


def compute_vendor_velocity(invoices: pw.Table, window_minutes: int = 60) -> pw.Table:
    """
    Compute invoice velocity (invoices per time window) for each vendor.
    High velocity can indicate automated fraud attempts.
    
    Args:
        invoices: Pathway Table with invoice data
        window_minutes: Time window for velocity calculation
    
    Returns:
        Pathway Table with vendor velocity metrics
    """
    
    # Add a time column for windowing
    invoices_with_time = invoices.select(
        *pw.this,
        time=pw.this.timestamp.dt.utc_from_timestamp(unit="s")
    )
    
    # Use sliding window to count invoices
    velocity = invoices_with_time.windowby(
        invoices_with_time.time,
        window=pw.temporal.sliding(
            hop=pw.Duration(minutes=5),
            duration=pw.Duration(minutes=window_minutes)
        ),
        instance=invoices_with_time.vendor_id
    ).reduce(
        vendor_id=invoices_with_time.vendor_id,
        invoice_count=pw.reducers.count(),
        total_amount=pw.reducers.sum(invoices_with_time.amount),
        window_start=pw.this._pw_window_start,
        window_end=pw.this._pw_window_end
    )
    
    return velocity


def detect_vendor_anomalies(
    invoices: pw.Table,
    vendor_stats: pw.Table,
    z_threshold: float = 3.0
) -> pw.Table:
    """
    Detect anomalous invoices by comparing against vendor historical behavior.
    Uses statistical z-score to identify outliers.
    
    Args:
        invoices: Pathway Table with invoice data
        vendor_stats: Pathway Table with vendor statistics
        z_threshold: Z-score threshold for anomaly detection
    
    Returns:
        Pathway Table with anomaly flags
    """
    
    # Join invoices with their vendor statistics
    enriched = invoices.join(
        vendor_stats,
        invoices.vendor_id == vendor_stats.vendor_id,
        id=invoices.id
    ).select(
        *invoices,
        vendor_avg=vendor_stats.avg_amount,
        vendor_stddev=vendor_stats.stddev_amount,
        vendor_max=vendor_stats.max_amount,
        vendor_total_invoices=vendor_stats.total_invoices
    )
    
    # Calculate z-score for amount
    enriched = enriched.select(
        *pw.this,
        amount_z_score=pw.if_else(
            pw.this.vendor_stddev > 0,
            (pw.this.amount - pw.this.vendor_avg) / pw.this.vendor_stddev,
            0.0
        )
    )
    
    # Flag anomalies
    enriched = enriched.select(
        *pw.this,
        is_amount_anomaly=(
            (pw.this.amount_z_score > z_threshold) |
            (pw.this.amount_z_score < -z_threshold)
        ),
        is_new_vendor=(pw.this.vendor_total_invoices <= 3),
        exceeds_historical_max=(pw.this.amount > pw.this.vendor_max * 1.5)
    )
    
    return enriched


def compute_category_patterns(invoices: pw.Table) -> pw.Table:
    """
    Track category usage patterns per vendor to detect suspicious changes.
    
    Args:
        invoices: Pathway Table with invoice data
    
    Returns:
        Pathway Table with vendor-category patterns
    """
    
    # Count invoices by vendor and category
    category_stats = invoices.groupby(
        invoices.vendor_id,
        invoices.category
    ).reduce(
        vendor_id=invoices.vendor_id,
        category=invoices.category,
        count=pw.reducers.count(),
        avg_amount=pw.reducers.avg(invoices.amount)
    )
    
    # Get total invoices per vendor
    vendor_totals = invoices.groupby(invoices.vendor_id).reduce(
        vendor_id=invoices.vendor_id,
        total_invoices=pw.reducers.count()
    )
    
    # Calculate category frequency
    patterns = category_stats.join(
        vendor_totals,
        category_stats.vendor_id == vendor_totals.vendor_id
    ).select(
        vendor_id=category_stats.vendor_id,
        category=category_stats.category,
        count=category_stats.count,
        avg_amount=category_stats.avg_amount,
        frequency=category_stats.count / vendor_totals.total_invoices
    )
    
    return patterns


def detect_bank_account_changes(
    invoices: pw.Table,
    vendor_stats: pw.Table
) -> pw.Table:
    """
    Detect bank account changes from previous invoices using incremental joins.
    
    Compares current invoice bank account with the vendor's last known bank account.
    Bank account changes are HIGH RISK indicators for payment fraud.
    
    Uses streaming joins - updates automatically when new invoices arrive.
    
    Args:
        invoices: Pathway Table with invoice data
        vendor_stats: Pathway Table with vendor statistics (includes last_bank_account)
    
    Returns:
        Pathway Table with bank account change flags
    """
    
    # Join invoices with vendor stats to compare bank accounts
    bank_check = invoices.join(
        vendor_stats,
        invoices.vendor_id == vendor_stats.vendor_id,
        id=invoices.id
    ).select(
        invoice_id=invoices.invoice_id,
        vendor_id=invoices.vendor_id,
        current_bank_account=invoices.bank_account,
        previous_bank_account=vendor_stats.last_bank_account,
        vendor_total_invoices=vendor_stats.total_invoices,
        # Detect if bank account changed
        bank_account_changed=pw.if_else(
            # Skip check for first invoice (no previous bank account)
            vendor_stats.total_invoices <= 1,
            False,
            # Compare current vs previous
            invoices.bank_account != vendor_stats.last_bank_account
        )
    )
    
    return bank_check


def calculate_amount_deviation(
    invoices: pw.Table,
    vendor_stats: pw.Table
) -> pw.Table:
    """
    Calculate deviation percentage from vendor's rolling average.
    
    Deviation % = ((Current Amount - Avg Amount) / Avg Amount) * 100
    
    Uses incremental joins - auto-updates when new invoices arrive.
    
    Args:
        invoices: Pathway Table with invoice data
        vendor_stats: Pathway Table with vendor statistics (includes avg_amount)
    
    Returns:
        Pathway Table with deviation metrics
    """
    
    # Join invoices with vendor stats for rolling average comparison
    deviation_check = invoices.join(
        vendor_stats,
        invoices.vendor_id == vendor_stats.vendor_id,
        id=invoices.id
    ).select(
        invoice_id=invoices.invoice_id,
        vendor_id=invoices.vendor_id,
        amount=invoices.amount,
        vendor_avg_amount=vendor_stats.avg_amount,
        vendor_total_invoices=vendor_stats.total_invoices,
        # Calculate deviation percentage
        deviation_percentage=pw.if_else(
            vendor_stats.avg_amount > 0,
            ((invoices.amount - vendor_stats.avg_amount) / vendor_stats.avg_amount) * 100,
            0.0
        ),
        # Absolute deviation percentage (for thresholding)
        abs_deviation_percentage=pw.if_else(
            vendor_stats.avg_amount > 0,
            pw.cast(float, pw.apply(
                lambda x: abs(x),
                ((invoices.amount - vendor_stats.avg_amount) / vendor_stats.avg_amount) * 100
            )),
            0.0
        ),
        # High deviation flag (>50% deviation)
        is_high_deviation=pw.if_else(
            vendor_stats.avg_amount > 0,
            pw.apply(
                lambda x: abs(x) > 50.0,
                ((invoices.amount - vendor_stats.avg_amount) / vendor_stats.avg_amount) * 100
            ),
            False
        )
    )
    
    return deviation_check


def enrich_with_vendor_context(
    invoices: pw.Table,
    vendor_stats: pw.Table
) -> pw.Table:
    """
    Enrich invoices with comprehensive vendor context for downstream processing.
    
    Includes:
    - Rolling average and deviation percentage
    - Bank account change detection
    - Historical statistics (min/max/stddev)
    - All derived risk features
    
    Uses incremental streaming joins - updates automatically on new invoices.
    
    Args:
        invoices: Pathway Table with invoice data
        vendor_stats: Pathway Table with vendor statistics
    
    Returns:
        Pathway Table with enriched invoice data (auto-updating)
    """
    
    enriched = invoices.join(
        vendor_stats,
        invoices.vendor_id == vendor_stats.vendor_id,
        id=invoices.id
    ).select(
        # Original invoice fields
        invoice_id=invoices.invoice_id,
        vendor_id=invoices.vendor_id,
        amount=invoices.amount,
        tax=invoices.tax,
        bank_account=invoices.bank_account,
        timestamp=invoices.timestamp,
        description=invoices.description,
        category=invoices.category,
        payment_method=invoices.payment_method,
        # Vendor rolling statistics
        vendor_avg_amount=vendor_stats.avg_amount,
        vendor_total_invoices=vendor_stats.total_invoices,
        vendor_total_amount=vendor_stats.total_amount,
        vendor_max_amount=vendor_stats.max_amount,
        vendor_min_amount=vendor_stats.min_amount,
        vendor_stddev_amount=vendor_stats.stddev_amount,
        vendor_last_bank_account=vendor_stats.last_bank_account,
        # Derived features - Deviation metrics
        deviation_percentage=pw.if_else(
            vendor_stats.avg_amount > 0,
            ((invoices.amount - vendor_stats.avg_amount) / vendor_stats.avg_amount) * 100,
            0.0
        ),
        amount_vs_avg_ratio=invoices.amount / vendor_stats.avg_amount,
        amount_vs_max_ratio=invoices.amount / vendor_stats.max_amount,
        # Bank account change detection
        bank_account_changed=pw.if_else(
            vendor_stats.total_invoices <= 1,
            False,
            invoices.bank_account != vendor_stats.last_bank_account
        )
    )
    
    return enriched


if __name__ == "__main__":
    # Example usage - demonstrate stateful vendor tracking
    from invoice_stream import generate_invoice_stream
    
    print("=" * 70)
    print("Testing Stateful Vendor Tracking with Pathway Streaming")
    print("=" * 70)
    print("\n🔄 All computations are incremental - auto-update on new invoices\n")
    
    # Generate sample invoices
    print("📊 Generating invoice stream...")
    invoices = generate_invoice_stream(num_invoices=50, interval_ms=100)
    
    # Track vendor statistics (rolling average, bank account, etc.)
    print("📈 Tracking vendor statistics (rolling average, bank account)...")
    vendor_stats = track_vendor_state(invoices)
    
    # Enrich with full vendor context
    print("🔍 Enriching invoices with vendor context...")
    enriched = enrich_with_vendor_context(invoices, vendor_stats)
    
    # Detect bank account changes
    print("🏦 Detecting bank account changes...")
    bank_changes = detect_bank_account_changes(invoices, vendor_stats)
    
    # Calculate amount deviations
    print("📉 Calculating deviation percentages...")
    deviations = calculate_amount_deviation(invoices, vendor_stats)
    
    # Display results
    print("\n" + "=" * 70)
    print("VENDOR STATISTICS (Auto-updating)")
    print("=" * 70)
    pw.debug.compute_and_print(vendor_stats)
    
    print("\n" + "=" * 70)
    print("BANK ACCOUNT CHANGES (Streaming detection)")
    print("=" * 70)
    bank_changes_only = bank_changes.filter(bank_changes.bank_account_changed == True)
    pw.debug.compute_and_print(bank_changes_only)
    
    print("\n" + "=" * 70)
    print("HIGH DEVIATION INVOICES (>50% from rolling average)")
    print("=" * 70)
    high_deviations = deviations.filter(deviations.is_high_deviation == True)
    pw.debug.compute_and_print(high_deviations)
    
    print("\n✅ All tracking is stateful and updates automatically on new invoices!")
