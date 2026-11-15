from __future__ import annotations
import json
from datetime import datetime, timezone

from tools.market_replay_stats import analyse_pair, load_quotes


def series(values, start=1_700_000_000, step=1):
    return [(start + i * step, v, 2.0) for i, v in enumerate(values)]


def test_flat_spread_produces_no_events():
    stats = analyse_pair(series([5.0] * 3600), window_minutes=30, slippage_bps=1.3, target_bps=1.0)
    assert stats["events_per_day"] == 0
    assert stats["hit_share"] == 0
    assert stats["median_peak_bps"] is None


def test_cost_includes_the_slippage_allowance():
    stats = analyse_pair(series([5.0] * 600), slippage_bps=1.3)
    assert abs(stats["cost_bps"] - 3.3) < 1e-9


def test_a_one_second_blip_is_ignored():
    """Quotes flicker; an execution takes about a second, so a 1s spike is noise."""
    values = [0.0] * 600 + [9.0] + [0.0] * 600
    stats = analyse_pair(series(values), window_minutes=10, slippage_bps=1.0, target_bps=1.0,
                         min_event_seconds=2)
    assert stats["events_per_day"] == 0


def test_a_persistent_spike_is_one_event():
    values = [0.0] * 1800 + [8.0] * 30 + [0.0] * 1800
    stats = analyse_pair(series(values), window_minutes=30, slippage_bps=1.0, target_bps=1.0)
    assert stats["events_per_day"] >= 1
    assert stats["median_peak_bps"] is not None and stats["median_peak_bps"] > 7.0
    assert 0 < stats["hit_share"] < 0.05


def test_the_rolling_median_absorbs_a_permanent_level_shift():
    values = [0.0] * (2 * 3600) + [10.0] * (6 * 3600)
    stats = analyse_pair(series(values), window_minutes=30, slippage_bps=1.0, target_bps=1.0)
    assert stats["hit_share"] < 0.05
    assert stats["dev_p50_bps"] < 0.5


def test_reversion_time_is_measured_and_priced():
    values = [0.0] * 600 + [8.0] * 120 + [0.0] * 600
    stats = analyse_pair(series(values), window_minutes=10, slippage_bps=1.0, target_bps=1.0,
                         max_hold_seconds=600)
    assert stats["reversion_median_s"] is not None
    assert 100 <= stats["reversion_median_s"] <= 140
    assert stats["timeout_share"] == 0
    assert stats["net_at_entry_bps"] is not None and 4.5 < stats["net_at_entry_bps"] < 5.5
    assert stats["bps_per_hour_held"] and stats["bps_per_hour_held"] > 100


def test_an_unreverted_dislocation_is_counted_as_timeout():
    values = [0.0] * 600 + [8.0] * 3000
    stats = analyse_pair(series(values), window_minutes=10, slippage_bps=1.0, target_bps=1.0,
                         max_hold_seconds=600, revert_bps=0.0)
    assert stats["timeout_share"] == 1.0
    assert stats["reversion_median_s"] is None
    assert stats["bps_per_day_serial"] is None


def test_bps_per_day_scales_with_events():
    values = [0.0] * 600 + [10.0] * 10
    stats = analyse_pair(series(values), window_minutes=10, slippage_bps=1.0, target_bps=1.0)
    assert stats["bps_per_day"] is not None and stats["bps_per_day"] > 0


def test_loader_filters_by_time_and_skips_bad_rows(tmp_path):
    venue_dir = tmp_path / "quotes" / "lighter_mainnet" / "NVDA"
    venue_dir.mkdir(parents=True)
    rows = [
        {"recv_at": "2026-11-14T16:00:00Z", "bid": "100", "ask": "100.1", "mid": "100.05"},
        {"recv_at": "2026-11-14T18:00:00Z", "bid": "101", "ask": "101.1", "mid": "101.05"},
        {"recv_at": "2026-11-14T18:00:01Z", "bid": "0", "ask": "101.1", "mid": "101.05"},
        {"recv_at": "2026-11-14T18:00:02Z", "bid": "102", "ask": "101.9", "mid": "101.95"},
    ]
    (venue_dir / "20261114-18.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    since = datetime(2026, 11, 14, 18, 0, tzinfo=timezone.utc).timestamp()
    grid = load_quotes(tmp_path, "NVDA", "lighter_mainnet", since)
    assert len(grid) == 1
    assert next(iter(grid.values()))[0] == 101.1
