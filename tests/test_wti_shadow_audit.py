import asyncio
import base64
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from tools.wti_live import WTILiveRuntime, WTIPassiveQuoteMonitor
from tools.lib.wti_shadow_audit import (
    BOOK_AGE_LIMIT_SECONDS,
    PAIR_DRIFT_LIMIT_BPS,
    QUOTE_AGE_LIMIT_SECONDS,
    ShadowWindow,
    observe_shadow_book,
    passive_wti_quote_sample,
)


def fresh_inputs():
    sample = {
        "sample_id": "baseline-1",
        "quote_id": "quote-1",
        "quote_timestamp": "2026-10-30T00:00:00+00:00",
        "var_bid": "101.00",
        "var_ask": "101.02",
        "lighter_bid": "101.30",
        "lighter_ask": "101.32",
        "lighter_qty": "0.198",
    }
    book = {
        "bid": Decimal("101.30"),
        "ask": Decimal("101.32"),
        "sell_price": Decimal("101.29"),
        "buy_price": Decimal("101.33"),
        "book_age_seconds": 0.1,
        "continuity_ok": True,
        "cold": False,
        "nonce": 19,
    }
    freshness = {
        "checks": {
            "sample_quality": True,
            "both_feeds_live": True,
            "lighter_continuity": True,
            "session_evidence": True,
            "quote_age_effective": True,
            "book_age_effective": False,
        },
    }
    thresholds = {
        "sample_id": "baseline-1",
        "by_direction": {
            "long_var_short_lighter": ["20", "30", "40", "50", "60"],
            "short_var_long_lighter": ["20", "30", "40", "50", "60"],
        },
    }
    return sample, book, thresholds, freshness


def test_shadow_scores_fresh_book_without_weakening_existing_pair_drift_limit():
    sample, book, thresholds, freshness = fresh_inputs()
    result = observe_shadow_book(
        sample, book, thresholds, freshness, now=datetime.now(timezone.utc),
    )
    assert result["status"] == "scored"
    assert result["shadow_only"] is True
    assert result["orders_attempted"] is False
    assert result["affects_strategy"] is False
    assert result["crossed_tiers"]["long_var_short_lighter"] == [1]
    assert result["lighter_pair_drift_bps"] == "0"
    assert PAIR_DRIFT_LIMIT_BPS == Decimal("1.5")
    assert BOOK_AGE_LIMIT_SECONDS == QUOTE_AGE_LIMIT_SECONDS == Decimal("2")

    moved = {**book, "bid": Decimal("101.33"), "ask": Decimal("101.35")}
    rejected = observe_shadow_book(
        sample, moved, thresholds, freshness, now=datetime.now(timezone.utc),
    )
    assert rejected["status"] == "unavailable"
    assert rejected["reason"] == "lighter_pair_drift_exceeds_1_5bps"


def test_shadow_records_stale_quote_as_unscored_and_never_as_a_trade_signal():
    sample, book, thresholds, freshness = fresh_inputs()
    freshness["checks"]["quote_age_effective"] = False
    result = observe_shadow_book(
        sample, book, thresholds, freshness, now=datetime.now(timezone.utc),
    )
    assert result["status"] == "unavailable"
    assert result["reason"] == "paired_quote_not_eligible"
    assert "crossed_tiers" not in result
    assert result["orders_attempted"] is False


def test_shadow_window_aggregates_high_frequency_extremes_and_missing_reasons():
    sample, book, thresholds, freshness = fresh_inputs()
    at = datetime.now(timezone.utc)
    first = observe_shadow_book(sample, book, thresholds, freshness, now=at)
    second_book = {**book, "nonce": 20, "sell_price": Decimal("101.31")}
    second = observe_shadow_book(sample, second_book, thresholds, freshness, now=at)
    unavailable = observe_shadow_book(
        sample, None, thresholds, freshness, now=at,
    )
    window = ShadowWindow()
    for observation in (first, second, unavailable):
        window.add(observation)
    result = window.close()
    assert result["observation_count"] == 3
    assert result["scored_count"] == 2
    assert result["source_sample_ids"] == ["baseline-1"]
    assert result["lighter_nonce_first"] == 19
    assert result["lighter_nonce_last"] == 20
    assert result["edge_extremes_bps"]["long_var_short_lighter"]["max"] > \
        result["edge_extremes_bps"]["long_var_short_lighter"]["min"]
    assert result["unscored_reasons"] == {"lighter_book_unavailable": 1}
    assert result["orders_attempted"] is False
    assert result["affects_strategy"] is False


def passive_inputs(now=None):
    now = now or datetime.now(timezone.utc)
    baseline = {
        "sample_id": "baseline-passive",
        "quote_timestamp": (now - timedelta(seconds=3)).isoformat(),
        "logged_at": (now - timedelta(seconds=3)).isoformat(),
        "sample_quality": "invalid",
        "sample_quality_reasons": ["variational_quote_age_invalid", "lighter_book_stale"],
        "market_availability": "both_feeds_live",
        "lighter_continuity_ok": True,
        "session_status": "open",
        "market_status": "open",
        "session_status_source": "variational_market_status_websocket",
        "session_status_observed_at": now.isoformat(),
        "session_status_feed_connected": True,
        "session_status_feed_heartbeat_at": now.isoformat(),
        "session_schedule_source": "variational_supported_assets_api",
        "session_id": "session-passive",
        "session_closing_at": (now + timedelta(hours=1)).isoformat(),
        "lighter_bid": "101.30",
        "lighter_ask": "101.32",
        "lighter_qty": "0.198",
        "variational_qty": "0.198",
    }
    quote_at = now - timedelta(milliseconds=200)
    quote = {
        "asset": "WTI",
        "bid": "101.00",
        "ask": "101.02",
        "timestamp": quote_at.isoformat(),
        "received_at": (now - timedelta(milliseconds=100)).isoformat(),
        "raw": {
            "__source_url": "https://omni.variational.io/api/quotes/indicative?market=USOILP&instrumentType=swap&kind=commodity&amount=0.198",
            "instrument": {"market": "USOILP", "instrument_type": "swap", "kind": "commodity"},
            "amount": "0.198",
        },
    }
    return baseline, quote, now


def test_passive_quote_reuses_only_fresh_exact_market_instrument_and_quantity():
    baseline, quote, now = passive_inputs()
    row, reason = passive_wti_quote_sample(quote, baseline, now=now)
    assert reason is None
    assert row["sample_kind"] == "passive_shadow"
    assert row["quote_source"] == "passive_browser_rest"
    assert row["sample_quality"] == "valid"
    assert row["var_bid"] == "101.00"
    assert row["var_ask"] == "101.02"
    assert row["lighter_qty"] == "0.198"
    assert row["sample_id"].startswith("passive-")


def test_wti_passive_monitor_only_caches_forwarded_indicative_rest_quotes():
    monitor = WTIPassiveQuoteMonitor()
    now = datetime.now(timezone.utc)
    body = base64.b64encode(json.dumps({
        "instrument": {
            "underlying": "WTI",
            "instrument_type": "swap",
            "kind": "commodity",
        },
        "amount": "0.198",
        "bid": "101.00",
        "ask": "101.02",
        "timestamp": now.isoformat(),
    }).encode()).decode()
    event = {
        "kind": "rest_response",
        "url": "https://omni.variational.io/api/quotes/indicative?market=USOILP&instrumentType=swap&kind=commodity&amount=0.198",
        "body": body,
        "base64Encoded": True,
    }

    asyncio.run(monitor.process_ws_event({"bid": "1", "ask": "2", "asset": "WTI"}))
    asyncio.run(monitor.process_rest_event({**event, "url": "https://omni.variational.io/api/portfolio"}))
    assert asyncio.run(monitor.get_quote_snapshot("WTI")) is None
    asyncio.run(monitor.process_rest_event(event))
    cached = asyncio.run(monitor.get_quote_snapshot("WTI"))
    assert cached["bid"] == "101.00"
    assert cached["ask"] == "101.02"
    assert cached["raw"]["__source_url"] == event["url"]


def test_passive_quote_rejects_unknown_mismatched_stale_or_old_evidence():
    baseline, quote, now = passive_inputs()
    cases = (
        ({**quote, "raw": {"__source_url": "https://omni.variational.io/api/quotes/indicative"}}, "passive_quote_market_unverified"),
        ({**quote, "raw": {**quote["raw"], "amount": "0.197"}}, "passive_quote_quantity_mismatch_or_unknown"),
        ({**quote, "timestamp": (now - timedelta(seconds=2.5)).isoformat()}, "passive_quote_stale"),
    )
    for candidate, expected in cases:
        row, reason = passive_wti_quote_sample(candidate, baseline, now=now)
        assert row is None
        assert reason == expected

    not_newer = {**baseline, "quote_timestamp": quote["timestamp"]}
    row, reason = passive_wti_quote_sample(quote, not_newer, now=now)
    assert row is None
    assert reason == "passive_quote_not_newer_than_baseline"


def test_runtime_shadow_uses_passive_quote_and_existing_book_without_execution():
    baseline, quote, now = passive_inputs()
    book = {
        "bid": Decimal("101.30"),
        "ask": Decimal("101.32"),
        "sell_price": Decimal("101.29"),
        "buy_price": Decimal("101.33"),
        "book_age_seconds": 0.1,
        "continuity_ok": True,
        "cold": False,
        "nonce": 88,
    }
    calls = []

    async def snapshot(asset, notional, *, base_qty):
        calls.append((asset, notional, base_qty))
        return book

    class CachedMonitor:
        async def get_quote_snapshot(self, asset):
            assert asset == "USOILP"
            return quote

    runtime = object.__new__(WTILiveRuntime)
    runtime.collector = SimpleNamespace(
        latest_sample=baseline,
        latest_baseline_sample=baseline,
        books=SimpleNamespace(snapshot=snapshot),
    )
    runtime.passive_quote_monitor = CachedMonitor()
    runtime.decision_shadow_thresholds = {
        "sample_id": baseline["sample_id"],
        "by_direction": {"long_var_short_lighter": ["20"], "short_var_long_lighter": ["20"]},
    }
    observation = asyncio.run(runtime._capture_decision_shadow_tick())
    assert calls == [("WTI", Decimal("0"), Decimal("0.198"))]
    assert observation["status"] == "scored"
    assert observation["quote_source"] == "passive_browser_rest"
    assert observation["lighter_nonce"] == 88
    assert observation["orders_attempted"] is False
    assert not hasattr(runtime, "executor")


def test_runtime_shadow_tick_reads_existing_book_only_and_never_calls_execution():
    now = datetime.now(timezone.utc)
    row = {
        "sample_id": "runtime-sample",
        "sample_quality": "valid",
        "market_availability": "both_feeds_live",
        "lighter_continuity_ok": True,
        "logged_at": now.isoformat(),
        "var_quote_age_seconds": "0.2",
        "lighter_book_age_seconds": "0.1",
        "session_status": "open",
        "market_status": "open",
        "session_status_source": "variational_market_status_websocket",
        "session_status_observed_at": now.isoformat(),
        "session_status_feed_connected": True,
        "session_status_feed_heartbeat_at": now.isoformat(),
        "session_schedule_source": "variational_supported_assets_api",
        "session_id": "session-1",
        "session_closing_at": (now + timedelta(hours=1)).isoformat(),
        "var_bid": "101.00",
        "var_ask": "101.02",
        "lighter_bid": "101.30",
        "lighter_ask": "101.32",
        "lighter_qty": "0.198",
    }
    book = {
        "bid": Decimal("101.30"),
        "ask": Decimal("101.32"),
        "sell_price": Decimal("101.29"),
        "buy_price": Decimal("101.33"),
        "book_age_seconds": 0.1,
        "continuity_ok": True,
        "cold": False,
        "nonce": 42,
    }
    calls = []

    async def snapshot(asset, notional, *, base_qty):
        calls.append((asset, notional, base_qty))
        return book

    runtime = object.__new__(WTILiveRuntime)
    runtime.collector = SimpleNamespace(
        latest_baseline_sample=row,
        books=SimpleNamespace(snapshot=snapshot),
    )
    runtime.decision_shadow_thresholds = {
        "sample_id": row["sample_id"],
        "by_direction": {"long_var_short_lighter": ["20"], "short_var_long_lighter": ["20"]},
    }
    observation = asyncio.run(runtime._capture_decision_shadow_tick())
    assert calls == [("WTI", Decimal("0"), Decimal("0.198"))]
    assert observation["status"] == "scored"
    assert observation["lighter_nonce"] == 42
    assert observation["orders_attempted"] is False
    assert not hasattr(runtime, "executor")


def test_shadow_task_emits_one_read_only_window_and_stops_cleanly():
    sample, book, thresholds, freshness = fresh_inputs()
    runtime = object.__new__(WTILiveRuntime)
    runtime.stop = False
    runtime.run_id = "shadow-run"
    runtime.decision_shadow_window_sequence = 0
    emitted = []
    runtime.decision_audit = SimpleNamespace(
        emit=lambda *args, **kwargs: emitted.append((args, kwargs)) or True,
    )
    ticks = 0

    async def capture():
        nonlocal ticks
        ticks += 1
        if ticks == 3:
            runtime.stop = True
        return observe_shadow_book(
            sample, book, thresholds, freshness, now=datetime.now(timezone.utc),
        )

    runtime._capture_decision_shadow_tick = capture
    asyncio.run(runtime._run_decision_shadow())
    assert ticks == 3
    assert runtime.decision_shadow_window_sequence == 1
    assert len(emitted) == 1
    args, kwargs = emitted[0]
    assert args[0] == "high_frequency_shadow_window"
    assert kwargs["evidence"] == "runtime_exact"
    assert kwargs["trace"].startswith("shadow-run:shadow:")
    assert args[1]["orders_attempted"] is False
    assert args[1]["affects_strategy"] is False
