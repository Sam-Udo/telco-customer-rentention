"""
Tests for serving/api/risk_engine.py — API-side risk tier logic.
Verifies parity with src/risk_tiers.py.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "serving", "api"))

from risk_engine import TIER_ACTIONS, RiskThresholds, assign_risk_tier


class TestRiskEngineApiParity:
    """Verify the API copy matches the src/ canonical copy."""

    def test_red_tier(self):
        assert assign_risk_tier(0.8, 0.2, 0.2) == "RED"

    def test_amber_tier(self):
        assert assign_risk_tier(0.3, 0.7, 0.2) == "AMBER"

    def test_yellow_tier(self):
        assert assign_risk_tier(0.3, 0.3, 0.7) == "YELLOW"

    def test_green_tier(self):
        assert assign_risk_tier(0.1, 0.2, 0.3) == "GREEN"

    def test_boundary_not_triggered(self):
        assert assign_risk_tier(0.5, 0.5, 0.5) == "GREEN"

    def test_default_thresholds(self):
        t = RiskThresholds()
        assert t.red == 0.5 and t.amber == 0.5 and t.yellow == 0.5


class TestTierActionsConsistency:
    """Verify API TIER_ACTIONS matches src/ version."""

    def test_all_four_tiers_present(self):
        assert set(TIER_ACTIONS.keys()) == {"RED", "AMBER", "YELLOW", "GREEN"}

    def test_each_tier_has_required_keys(self):
        for tier, action in TIER_ACTIONS.items():
            assert {"action", "owner", "sla"} <= set(action.keys()), f"{tier} missing keys"


class TestCrossModuleConsistency:
    """Ensure both risk tier modules produce identical results."""

    def test_identical_outputs_across_probability_vectors(self):
        """Both modules return same tier for a range of inputs."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
        from risk_tiers import assign_risk_tier as src_assign
        from risk_tiers import TIER_ACTIONS as src_actions

        test_vectors = [
            (0.8, 0.2, 0.2),
            (0.3, 0.7, 0.2),
            (0.3, 0.3, 0.7),
            (0.1, 0.2, 0.3),
            (0.5, 0.5, 0.5),
            (0.0, 0.0, 0.0),
            (1.0, 1.0, 1.0),
        ]
        for p30, p60, p90 in test_vectors:
            assert assign_risk_tier(p30, p60, p90) == src_assign(p30, p60, p90), \
                f"Mismatch for ({p30}, {p60}, {p90})"

    def test_identical_tier_actions(self):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
        from risk_tiers import TIER_ACTIONS as src_actions

        for tier in ["RED", "AMBER", "YELLOW", "GREEN"]:
            assert TIER_ACTIONS[tier] == src_actions[tier], f"TIER_ACTIONS[{tier}] mismatch"
