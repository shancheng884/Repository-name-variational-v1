import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone

import pytest

import main as engine
from tools.mainnet_anchor_collect import MainnetAnchorCollector, anchor_status, runtime_args
from tools.lib.basis_store import BasisSampleStore, read_basis_samples
from tools.lib.mainnet_anchor import load_mainnet_anchor_rows


def sample(at, **changes):
    return {
        "logged_at": at.isoformat(), "asset": "ETH", "venue": "mainnet_lighter",
        "sample_kind": "baseline", "sample_quality": "valid",
        "sample_pair_valid": True, "sample_quality_version": 2,
        "quote_size_mode": engine.LIVE_INVENTORY_BASIS_QUOTE_SIZE_MODE,
        "quote_source": "direct_rfq", "var_quote_age_seconds": "0.1",
        "lighter_book_age_seconds": "0.1", "long_edge_bps": "8", "short_edge_bps": "-9",
        **changes,
    }


def test_collector_uses_observe_and_blocks_real_orders(tmp_path):
    args = runtime_args(tmp_path / "mainnet_anchor")
    assert args.mode == "observe"
    assert args.live_inventory_dry_decisions
    assert args.live_inventory_collect_only
    assert not args.confirm_live
    runtime = MainnetAnchorCollector(args)
    assert runtime.is_live_inventory_enabled()
    assert runtime.live_inventory_basis_quote_priority(None) == "background"
    async def run():
        for confirm in (True, None):
            with pytest.raises(RuntimeError, match="blocks_orders"):
                await runtime.send_variational_place_order(confirm=confirm)
        with pytest.raises(RuntimeError, match="blocks_orders"):
            await runtime.place_lighter_order(None)
        with pytest.raises(RuntimeError, match="blocks_orders"):
            await runtime.place_lighter_order_from_plan()
        with pytest.raises(RuntimeError, match="blocks_orders"):
            await runtime.submit_live_inventory_exit_pair()
    asyncio.run(run())


def test_collector_writes_compatible_samples_once_per_bucket(tmp_path, monkeypatch):
    runtime = MainnetAnchorCollector.__new__(MainnetAnchorCollector)
    runtime.anchor_store = BasisSampleStore(tmp_path, config_hash="test", commit="test")
    runtime.anchor_last_bucket = None
    runtime.anchor_samples = 0
    monkeypatch.setattr("tools.mainnet_anchor_collect.time.time", lambda: 1800000000)
    row = sample(datetime.now(timezone.utc))
    async def run():
        await runtime.append_live_inventory_log("live_inventory_basis_state", row)
        await runtime.append_live_inventory_log("live_inventory_basis_state", row)
        await runtime.append_live_inventory_log("live_inventory_basis_state", {**row, "sample_pair_valid": False})
        await runtime.append_live_inventory_log("live_inventory_basis_state", {**row, "quote_size_mode": "legacy"})
    asyncio.run(run())
    rows = read_basis_samples(tmp_path, limit=100)
    assert len(rows) == 1
    assert rows[0]["venue"] == "mainnet_lighter"
    assert rows[0]["execution_mode"] == "collect_only"
    assert runtime.anchor_samples == 1


def test_loader_merges_isolated_collect_data_and_drops_expired_fields(tmp_path):
    now = datetime(2026, 10, 14, tzinfo=timezone.utc)
    for root, rows in (
        (tmp_path / "basis_samples", [sample(now - timedelta(days=31)), sample(now - timedelta(days=2))]),
        (tmp_path / "mainnet_anchor" / "basis_samples", [sample(now - timedelta(days=1), large_unused_blob="x" * 10000)]),
    ):
        store = BasisSampleStore(root, config_hash="test", commit="test")
        for row in rows:
            store.append(row)
    rows = load_mainnet_anchor_rows(tmp_path, asset="ETH", now=now)
    assert len(rows) == 2
    assert all("large_unused_blob" not in row for row in rows)


@pytest.mark.parametrize("hours,expected", [(35, False), (36, True)])
def test_status_uses_real_engine_anchor_and_recent_health(tmp_path, hours, expected):
    now = datetime(2026, 10, 14, tzinfo=timezone.utc)
    store = BasisSampleStore(tmp_path / "mainnet_anchor" / "basis_samples", config_hash="test", commit="test")
    for i in range(hours * 120):
        store.append(sample(now - timedelta(seconds=i * 30 + 1)))
    store.append(sample(now - timedelta(seconds=1), venue="robinhood_chain_lighter"))
    report = anchor_status(tmp_path, now=now)
    assert report["quality_counts"]["wrong_lighter_venue"] == 1
    for values in report["directions"].values():
        assert values["v4_anchor_ready"] is expected
        assert values["v4_health_ready_observed"]
        assert values["ready_now"] is expected
    if expected:
        stale_report = anchor_status(tmp_path, now=now + timedelta(minutes=10))
        assert all(not values["ready_now"] for values in stale_report["directions"].values())


def test_status_does_not_treat_legacy_format_as_current_anchor(tmp_path):
    now = datetime(2026, 10, 14, tzinfo=timezone.utc)
    store = BasisSampleStore(tmp_path / "basis_samples", config_hash="test", commit="test")
    store.append(sample(now, quote_size_mode="legacy"))
    report = anchor_status(tmp_path, now=now)
    assert all(not values["ready_now"] for values in report["directions"].values())


def test_live_loader_accepts_isolated_collector_history(tmp_path):
    now = datetime.now(timezone.utc)
    store = BasisSampleStore(tmp_path / "mainnet_anchor" / "basis_samples", config_hash="test", commit="test")
    for i in range(4400):
        store.append(sample(now - timedelta(seconds=i * 31 + 1)))
    runtime = engine.VariationalToLighterRuntime.__new__(engine.VariationalToLighterRuntime)
    runtime.output_dir = tmp_path
    runtime.live_inventory_basis_v4_profile = engine.LIVE_INVENTORY_BASIS_V4_PROFILE_ETH_SHORT_20260724
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    report = runtime.load_live_inventory_basis_v4_history(asset="ETH")
    assert report["ready"]
    assert report["v4_anchor_ready"]
    assert report["sample_quality_counts"]["accepted"] == 4400
