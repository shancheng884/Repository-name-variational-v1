from __future__ import annotations

from tools.nvda_tier_replay import fit_tiers, percentile, replay, summarise


def series(values, start=1_700_000_000, step=1, spread=2.0):
    return [(start + i * step, v, spread) for i, v in enumerate(values)]


def test_percentile_matches_linear_interpolation():
    assert percentile([0, 10], 50) == 5
    assert percentile([1, 2, 3, 4], 100) == 4


def test_tiers_start_at_cost_and_keep_spacing():
    values = [0.0] * 600 + [8.0] * 60 + [0.0] * 600
    data = series(values)
    base, tiers, dev = fit_tiers(data, cost_bps=4.0, window_minutes=10)
    assert tiers[0] >= 4.0
    assert all(b - a >= 0.249 for a, b in zip(tiers, tiers[1:]))
    assert len(tiers) == 5


def test_replay_opens_and_reverts_a_tier():
    values = [0.0] * 600 + [8.0] * 30 + [0.0] * 600
    data = series(values)
    base, tiers, _ = fit_tiers(data, cost_bps=2.0, window_minutes=10)
    trades = replay(data, base, tiers, slippage_bps=1.0, timeout_s=900)
    assert trades
    assert all(t["reason"] in ("reverted", "timeout", "open_at_end") for t in trades)
    assert max(t["net_bps"] for t in trades) > 0


def test_summary_groups_by_tier():
    trades = [{"tier": 1, "net_bps": 5.0, "hold_s": 10, "reason": "reverted", "entry_dev": 6, "entry_edge": 6},
              {"tier": 1, "net_bps": -1.0, "hold_s": 900, "reason": "timeout", "entry_dev": 6, "entry_edge": 6},
              {"tier": 2, "net_bps": 8.0, "hold_s": 20, "reason": "reverted", "entry_dev": 9, "entry_edge": 9}]
    rows = summarise(trades, notional=20.0)
    assert [r["tier"] for r in rows] == [1, 2]
    assert rows[0]["fills"] == 2 and abs(rows[0]["win_rate"] - 0.5) < 1e-9
    assert abs(rows[1]["total_usd"] - 8.0 / 10000 * 20) < 1e-9
