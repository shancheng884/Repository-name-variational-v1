from decimal import Decimal
from types import SimpleNamespace

from paper_engine import paper_entry_candidate, percent_to_bps


def test_percent_to_bps_uses_percent_units() -> None:
    assert percent_to_bps(Decimal("0.03")) == Decimal("3.00")


def test_entry_candidate_threshold_uses_real_bps() -> None:
    snapshot = SimpleNamespace(
        long_var_short_lighter_pct=Decimal("0.03"),
        long_median_5m_pct=Decimal("0.00"),
        long_sample_count_5m=30,
        short_var_long_lighter_pct=Decimal("0.02"),
        short_median_5m_pct=Decimal("0.00"),
        short_sample_count_5m=30,
    )

    assert paper_entry_candidate(snapshot, Decimal("3.1"), 30) is None

    candidate = paper_entry_candidate(snapshot, Decimal("3"), 30)
    assert candidate is not None
    assert candidate.direction == "long_var_short_lighter"
    assert candidate.deviation_bps == Decimal("3.00")
