"""
Risk tier assignment logic for multi-horizon churn scoring.
Shared between batch scoring (notebook 05) and any downstream consumers.
"""

from dataclasses import dataclass


@dataclass
class RiskThresholds:
    """Configurable thresholds for risk tier assignment."""
    red: float = 0.5     # 30d prob above this → RED
    amber: float = 0.5   # 60d prob above this → AMBER
    yellow: float = 0.5  # 90d prob above this → YELLOW


def assign_risk_tier(
    prob_30d: float,
    prob_60d: float,
    prob_90d: float,
    thresholds: RiskThresholds = RiskThresholds()
) -> str:
    """
    Assign a composite risk tier based on multi-horizon churn probabilities.

    Cascading priority:
    - RED:    30d prob > threshold → Immediate loyalty call
    - AMBER:  60d prob > threshold → Digital retention campaign
    - YELLOW: 90d prob > threshold → Customer success check-in
    - GREEN:  All below threshold  → Monitor only

    Args:
        prob_30d: 30-day churn probability
        prob_60d: 60-day churn probability
        prob_90d: 90-day churn probability
        thresholds: Configurable threshold values

    Returns:
        Risk tier string: "RED", "AMBER", "YELLOW", or "GREEN"
    """
    if prob_30d > thresholds.red:
        return "RED"
    elif prob_60d > thresholds.amber:
        return "AMBER"
    elif prob_90d > thresholds.yellow:
        return "YELLOW"
    else:
        return "GREEN"


# Business action mapping
TIER_ACTIONS = {
    "RED": {
        "action": "Emergency outbound call — best-offer retention deal",
        "owner": "Call center (Loyalty)",
        "sla": "Within 24 hours",
    },
    "AMBER": {
        "action": "Targeted digital campaign — proactive contract renewal",
        "owner": "CRM / Marketing",
        "sla": "Within 7 days",
    },
    "YELLOW": {
        "action": "Customer success check-in — experience improvement",
        "owner": "Customer Success",
        "sla": "Within 30 days",
    },
    "GREEN": {
        "action": "No action — continue monitoring",
        "owner": "Automated",
        "sla": "N/A",
    },
}
