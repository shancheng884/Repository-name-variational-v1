from datetime import datetime, timedelta, timezone

from tools.adaptive_threshold_report import percentile, rolling_mad


def _points(values, step_seconds=60):
    start = datetime(2026, 11, 2, tzinfo=timezone.utc)
    return [(start + timedelta(seconds=step_seconds * i), v) for i, v in enumerate(values)]


def test_rolling_mad_is_zero_before_enough_samples():
    result = rolling_mad(_points([10, 12, 11, 13]), window_minutes=30)
    assert result[0] == 0.0 and result[3] == 0.0


def test_rolling_mad_reacts_to_dispersion():
    steady = rolling_mad(_points([10.0] * 20), window_minutes=30)
    noisy = rolling_mad(_points([10.0, 20.0] * 10), window_minutes=30)
    assert steady[-1] == 0.0
    assert noisy[-1] > 4.0


def test_percentile_handles_empty_and_bounds():
    assert percentile([], 0.5) == 0.0
    assert percentile([1, 2, 3, 4], 0.0) == 1
    assert percentile([1, 2, 3, 4], 1.0) == 4