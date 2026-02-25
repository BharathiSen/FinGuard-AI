"""
FinGuard AI - Configuration
Business-critical parameters for risk scoring and decision making.
"""

# =============================================================================
# STREAMING CONFIGURATION
# =============================================================================

# Invoice generation interval (milliseconds)
SIMULATION_INTERVAL_MS = 5000


# =============================================================================
# RISK SCORING WEIGHTS
# =============================================================================

# Contribution of amount deviation to overall risk score
RISK_WEIGHT_AMOUNT = 0.35

# Contribution of vendor history to overall risk score
RISK_WEIGHT_VENDOR = 0.30

# Contribution of pattern analysis to overall risk score
RISK_WEIGHT_PATTERN = 0.35


# =============================================================================
# RISK SCORING THRESHOLDS
# =============================================================================

# Amount deviation percentage that triggers risk flag
DEVIATION_THRESHOLD_PERCENT = 30

# Minimum cosine similarity score to flag duplicate invoices
DUPLICATE_SIMILARITY_THRESHOLD = 0.85

# Minimum acceptable tax percentage
TAX_MIN_PERCENT = 5

# Maximum acceptable tax percentage
TAX_MAX_PERCENT = 10


# =============================================================================
# RISK LEVEL BOUNDARIES
# =============================================================================

# Maximum score for LOW risk classification (0-30)
RISK_LOW_MAX = 30

# Maximum score for MEDIUM risk classification (31-70)
RISK_MEDIUM_MAX = 70

# HIGH risk is anything above RISK_MEDIUM_MAX (71-100)


# =============================================================================
# AUTONOMOUS DECISION THRESHOLDS
# =============================================================================

# Maximum risk score for automatic approval
AUTO_APPROVE_MAX = 30

# Minimum risk score for automatic rejection
AUTO_REJECT_MIN = 70

# Invoices between AUTO_APPROVE_MAX and AUTO_REJECT_MIN require manual review


# =============================================================================
# VENDOR ANOMALY DETECTION
# =============================================================================

# Z-score threshold for detecting vendor behavioral anomalies
ANOMALY_Z_THRESHOLD = 3.0
