"""
Tests for src/risk_tiers.py — canonical risk tier assignment logic.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from risk_tiers import TIER_ACTIONS, RiskThresholds, assign_risk_tier


class TestRiskThresholds:
    """Tests for the RiskThresholds dataclass."""

    def test_default_thresholds(self):
        t = RiskThresholds()
        assert t.red == 0.5
        assert t.amber == 0.5
        assert t.yellow == 0.5

    def test_custom_thresholds(self):
        t = RiskThresholds(red=0.3, amber=0.4, yellow=0.6)
        assert t.red == 0.3
        assert t.amber == 0.4
        assert t.yellow == 0.6


class TestAssignRiskTier:
    """Tests for the assign_risk_tier() cascading logic."""

    def test_red_tier_30d_above_threshold(self):
        assert assign_risk_tier(0.8, 0.2, 0.2) == "RED"

    def test_red_overrides_all_high(self):
        """RED takes priority even when all probs are high."""
        assert assign_risk_tier(0.9, 0.9, 0.9) == "RED"

    def test_amber_tier_60d_above_threshold(self):
        assert assign_risk_tier(0.3, 0.7, 0.2) == "AMBER"

    def test_yellow_tier_90d_above_threshold(self):
        assert assign_risk_tier(0.3, 0.3, 0.7) == "YELLOW"

    def test_green_tier_all_below(self):
        assert assign_risk_tier(0.1, 0.2, 0.3) == "GREEN"

    def test_boundary_exactly_at_threshold_is_green(self):
        """Condition is '>' not '>=', so exactly 0.5 should be GREEN."""
        assert assign_risk_tier(0.5, 0.5, 0.5) == "GREEN"

    def test_boundary_just_above_threshold(self):
        assert assign_risk_tier(0.500001, 0.0, 0.0) == "RED"

    def test_zero_probabilities(self):
        assert assign_risk_tier(0.0, 0.0, 0.0) == "GREEN"

    def test_custom_thresholds_change_behavior(self):
        t = RiskThresholds(red=0.9, amber=0.9, yellow=0.9)
        assert assign_risk_tier(0.8, 0.8, 0.8, t) == "GREEN"

    def test_custom_thresholds_lower_triggers_earlier(self):
        t = RiskThresholds(red=0.2, amber=0.2, yellow=0.2)
        assert assign_risk_tier(0.3, 0.1, 0.1, t) == "RED"


class TestTierActions:
    """Tests for the TIER_ACTIONS dictionary."""

    def test_all_four_tiers_present(self):
        assert set(TIER_ACTIONS.keys()) == {"RED", "AMBER", "YELLOW", "GREEN"}

    def test_each_tier_has_required_keys(self):
        for tier, action in TIER_ACTIONS.items():
            assert "action" in action, f"{tier} missing 'action'"
            assert "owner" in action, f"{tier} missing 'owner'"
            assert "sla" in action, f"{tier} missing 'sla'"

    def test_red_tier_has_loyalty_owner(self):
        assert "Loyalty" in TIER_ACTIONS["RED"]["owner"]
