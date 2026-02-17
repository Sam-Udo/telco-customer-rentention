"""
Tests for pure business logic extracted from serving/dashboard/app.py.
No Streamlit dependency needed — tests the inline calculations directly.
"""


class TestTenureBucketAssignment:
    """
    Tenure bucket logic from dashboard app.py:
        "0-90d"  if tenure_days < 90
        "90d-1y" if tenure_days < 365
        "1y-2y"  if tenure_days < 730
        "2y-3y"  if tenure_days < 1095
        "3y+"    otherwise
    """

    @staticmethod
    def _tenure_bucket(tenure_days: int) -> str:
        if tenure_days < 90:
            return "0-90d"
        elif tenure_days < 365:
            return "90d-1y"
        elif tenure_days < 730:
            return "1y-2y"
        elif tenure_days < 1095:
            return "2y-3y"
        else:
            return "3y+"

    def test_under_90_days(self):
        assert self._tenure_bucket(30) == "0-90d"

    def test_boundary_at_90_is_next_bucket(self):
        """90 is NOT < 90, so it falls into 90d-1y."""
        assert self._tenure_bucket(90) == "90d-1y"

    def test_90d_to_1y(self):
        assert self._tenure_bucket(200) == "90d-1y"

    def test_1y_to_2y(self):
        assert self._tenure_bucket(500) == "1y-2y"

    def test_2y_to_3y(self):
        assert self._tenure_bucket(800) == "2y-3y"

    def test_3y_plus(self):
        assert self._tenure_bucket(1200) == "3y+"


class TestSpeedGapCalculation:
    """
    Speed gap logic from dashboard app.py:
        speed_gap_pct = round((speed - line_speed) / speed * 100, 2) if speed > 0 else 0
    """

    @staticmethod
    def _speed_gap(speed: float, line_speed: float) -> float:
        return round((speed - line_speed) / speed * 100, 2) if speed > 0 else 0

    def test_normal_gap(self):
        assert self._speed_gap(100, 80) == 20.0

    def test_zero_speed_avoids_division_by_zero(self):
        assert self._speed_gap(0, 65) == 0

    def test_no_gap(self):
        assert self._speed_gap(80, 80) == 0.0

    def test_negative_gap_faster_than_advertised(self):
        """If actual > advertised, gap is negative (over-delivery)."""
        result = self._speed_gap(80, 90)
        assert result == -12.5
