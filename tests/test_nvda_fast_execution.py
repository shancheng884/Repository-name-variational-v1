from __future__ import annotations

import asyncio
from decimal import Decimal as D

import pytest

from tools.lib.nvda_execution_stats import (closed_child_result, failed_entry_result,
                                           hedged_entry_result, new_execution_stats, record_attempt)
from tools.lib.venue_probe import EntropyProbeExecutor
from tools.nvda_tier_live import (check_entry_depth, close_repair_id, common_qty, pair_order_id, place_pair,
                                 rollback_entry, settle_close, settle_positions)
from tests.test_nvda_tier_live import _EntropyAsset, _EntropyInfo, _entropy_executor


def fill(qty, price, fee="0"):
    return {"status": "filled", "filled_qty": str(qty), "avg_price": str(price), "fee_usd": fee}


def test_fast_entropy_order_has_no_pre_submit_network_reads_and_uses_the_ask():
    info = _EntropyInfo("0")
    executor = _entropy_executor(info)
    calls = []

    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected pre-submit query")

    info.all_mids = info.user_state = info.open_orders = info.l2_snapshot = forbidden

    def order(*args, **kwargs):
        calls.append((args, kwargs))
        return {"response": {"data": {"statuses": [{"error": "no match"}]}}}

    executor.exchange.order = order
    receipt = asyncio.run(executor.market_order(asset=_EntropyAsset(), side="buy", qty=D("0.2"),
                                                quote={"bid": "99.9", "ask": "100"},
                                                client_order_id="fast", max_slippage_bps=D("12"),
                                                known_position_before=D("0")))
    assert len(calls) == 1
    assert calls[0][0][3] == 100.12
    assert receipt["status"] == "rejected"


def test_fast_timeout_does_not_infer_a_fill_from_account_position():
    executor = _entropy_executor(_EntropyInfo(".5"), after_qty=".7")
    receipt = asyncio.run(executor.market_order(asset=_EntropyAsset(), side="buy", qty=D("0.2"),
                                                quote={"ask": "100", "bid": "99.9"},
                                                client_order_id="fast-timeout", known_position_before=D(".5")))
    assert receipt["status"] == "unknown"
    assert receipt.get("filled_qty") is None


def test_both_legs_are_dispatched_concurrently_with_stable_order_ids():
    async def run():
        entered = set()
        ready = asyncio.Event()

        class Executor:
            def __init__(self, venue):
                self.venue = venue
                self.kwargs = None

            async def market_order(self, **kwargs):
                self.kwargs = kwargs
                entered.add(self.venue)
                if len(entered) == 2:
                    ready.set()
                await asyncio.wait_for(ready.wait(), .5)
                return fill(".2", "100")

        ex = {v: Executor(v) for v in ("entropy", "lighter_rh")}
        await place_pair(ex, None, "entropy", "lighter_rh", D(".2"), opening=True,
                         quotes={}, before_positions={v: D("0") for v in ex}, token="same-token")
        assert ex["entropy"].kwargs["known_position_before"] == 0
        assert ex["entropy"].kwargs["client_order_id"] == "nvda-tier-same-token-buy"
        assert ex["lighter_rh"].kwargs["client_order_id"] == "nvda-tier-same-token-sell"

    asyncio.run(run())


def test_quantity_is_rounded_to_both_venues_before_dispatch():
    class Executor:
        def __init__(self, step):
            self.step = D(step)

        def normalize_qty(self, asset, qty):
            return qty.quantize(self.step, rounding="ROUND_DOWN")

    ex = {"a": Executor(".001"), "b": Executor(".0001")}
    assert common_qty(ex, None, ("a", "b"), D(".13287")) == D(".132")


def test_rejected_leg_and_successful_unwind_have_real_cash_pnl_and_stats():
    receipts = {"entropy": {"status": "rejected"}, "lighter_rh": fill(".2", "100", None)}
    recovery = {"ok": True, "corrections": [{"venue": "lighter_rh", "side": "buy",
                                              "receipt": fill(".2", "100.1", None)}]}
    pnl = failed_entry_result(receipts, recovery, "entropy", "lighter_rh")
    assert pnl["complete"]
    assert D(pnl["net_usd"]) == D("-.02")
    stats = new_execution_stats()
    record_attempt(stats, receipts, opening=True, hedged=False, recovery=recovery, pnl=pnl)
    assert stats["failed_entries"] == 1
    assert stats["venues"]["entropy"]["rejected"] == 1
    assert stats["rollback_orders"] == 1
    assert D(stats["rollback_net_usd"]) == D("-.02")


def test_old_rollback_without_fill_receipts_is_missing_not_zero_or_profit():
    pnl = failed_entry_result({"entropy": {"status": "rejected"},
                               "lighter_rh": fill(".2", "100")}, {"ok": True},
                              "entropy", "lighter_rh")
    assert not pnl["complete"]
    assert any("unbalanced" in row for row in pnl["missing"])


def test_partial_fills_split_retained_fees_and_excess_unwind_without_double_counting():
    receipts = {"entropy": fill(".2", "100", ".02"), "lighter_rh": fill(".1", "101", None)}
    recovery = {"ok": True, "corrections": [{"venue": "entropy", "side": "sell",
                                              "receipt": fill(".1", "99", ".01")}]}
    retained, pnl = hedged_entry_result(receipts, recovery, D(".1"), "entropy", "lighter_rh")
    assert D(retained["entropy"]["fee_usd"]) == D(".01")
    assert pnl["complete"]
    assert D(pnl["net_usd"]) == D("-.12")
    assert D(pnl["rebate_usd"]) == D(".022")


def test_close_recovery_is_included_in_child_pnl_and_rebate_is_separate():
    action = {"qty": .2, "notional_usd": 20, "entry_buy_price": 100,
              "entry_sell_price": 101, "entry_fee_usd": .01}
    receipts = {"entropy": {"status": "rejected"}, "lighter_rh": fill(".2", "100", None)}
    recovery = {"ok": True, "corrections": [{"venue": "entropy", "side": "sell",
                                              "receipt": fill(".2", "100", ".01")}]}
    pnl = closed_child_result(action, receipts, recovery, "entropy", "lighter_rh")
    assert pnl["complete"]
    assert D(pnl["gross_usd"]) == D(".2")
    assert D(pnl["net_usd"]) == D(".18")


def test_close_rejection_repairs_entropy_before_any_rate_limited_rh_read():
    events = []
    token = "single-leg"
    pending = {}
    class Entropy:
        def normalize_qty(self, asset, qty):
            return qty

        async def market_order(self, **kw):
            events.append("entropy-order")
            assert kw["reduce_only"] and kw["qty"] == D(".126")
            assert kw["known_position_before"] == D("-.126")
            assert kw["retry_rate_limit"] is False
            cid = close_repair_id(token, "buy", 1)
            assert kw["client_order_id"] == cid
            assert pending["close_corrections"][0]["receipt"]["status"] == "unknown"
            return {**fill(".126", "154.5", ".001752"), "order_id": "ent-fix",
                    "cloid": str(EntropyProbeExecutor._cloid(cid))}

        async def positions(self, asset, fresh=False):
            events.append("entropy-read")
            return {"verified": True, "qty": "0", "active_orders": 0}

    class RH:
        async def positions(self, asset, fresh=False):
            events.append("rh-read")
            if events.count("rh-read") == 1:
                raise RuntimeError("429")
            return {"verified": True, "qty": "0", "active_orders": 0}

    rh_fill = {**fill(".126", "158.32"), "order_id": "rh-fill", "is_ask": True,
               "reduce_only": True, "client_order_id": pair_order_id(token, "sell")}
    result = asyncio.run(settle_close(
        {"entropy": Entropy(), "lighter_rh": RH()}, None, "lighter_rh", "entropy", D(".126"),
        {"lighter_rh": D(".126"), "entropy": D("-.126")},
        {"entropy": {"status": "rejected", "filled_qty": "0"}, "lighter_rh": rh_fill},
        token=token, pending=pending, persist=lambda: events.append("persist")))
    assert result["ok"]
    assert events[:3] == ["persist", "entropy-order", "persist"]
    assert events.count("rh-read") == 2
    assert events.count("entropy-read") == 1


def test_unknown_rh_receipt_only_queries_existing_order():
    token = "rh-unclear"
    class RH:
        async def lookup_ioc(self, asset, cid, **kw):
            assert cid == pair_order_id(token, "buy") and kw["retry_read"]
            return {**fill(".0244", "808.07"), "order_id": "rh-close", "is_ask": False,
                    "reduce_only": True, "client_order_id": cid}

        async def positions(self, asset, fresh=False):
            return {"verified": True, "qty": "-.0491", "active_orders": 0}

    class Entropy:
        async def positions(self, asset, fresh=False):
            return {"verified": True, "qty": ".0491", "active_orders": 0}

    ent = {**fill(".0244", "807.22"), "order_id": "ent-close",
           "cloid": str(EntropyProbeExecutor._cloid(pair_order_id(token, "sell")))}
    receipts = {"lighter_rh": {"status": "unknown"}, "entropy": ent}
    result = asyncio.run(settle_close(
        {"lighter_rh": RH(), "entropy": Entropy()}, None, "entropy", "lighter_rh", D(".0244"),
        {"lighter_rh": D("-.0735"), "entropy": D(".0735")}, receipts,
        token=token, pending={}, persist=lambda: None))
    assert result["ok"] and not result["corrections"]
    assert receipts["lighter_rh"]["recovered_by"] == "read_only_order_lookup"


def test_unknown_repair_never_resubmits_or_reconciles_by_position_alone():
    token = "pending-repair"
    class Entropy:
        def normalize_qty(self, asset, qty):
            return qty

        async def market_order(self, **kw):
            assert kw["client_order_id"] == close_repair_id(token, "buy", 1)
            return {"status": "unknown"}

    rh = {**fill(".126", "158.32"), "order_id": "rh-fill", "is_ask": True,
          "reduce_only": True, "client_order_id": pair_order_id(token, "sell")}
    result = asyncio.run(settle_close(
        {"lighter_rh": object(), "entropy": Entropy()}, None, "lighter_rh", "entropy", D(".126"),
        {"lighter_rh": D(".126"), "entropy": D("-.126")},
        {"lighter_rh": rh, "entropy": {"status": "rejected", "filled_qty": "0"}},
        token=token, pending={}, persist=lambda: None))
    assert result["error"] == "unresolved_correction"
    assert len(result["corrections"]) == 1


def test_unknown_fill_and_unknown_fee_are_not_reported_as_zero_cost():
    for receipts in ({"entropy": {"status": "unknown"}},
                     {"entropy": fill(".2", "100", None)}):
        pnl = failed_entry_result(receipts, {"ok": True}, "entropy", "lighter_rh")
        assert not pnl["complete"]


@pytest.mark.parametrize("thin", [False, True])
def test_live_loop_entry_depth_is_local_and_does_not_send_either_leg_when_thin(tmp_path, monkeypatch, thin):
    from types import SimpleNamespace
    import json
    import tools.nvda_tier_live as live

    submitted = []
    order_log = tmp_path / "orders.jsonl"

    class Executor:
        def __init__(self, name):
            self.name = name
            self.qty = D("0")
            self.reads = 0

        async def start(self):
            pass

        async def close(self):
            pass

        async def ensure_leverage(self, asset, leverage):
            return {"ok": True, "leverage": leverage}

        async def account_state(self, asset):
            return {"ok": True, "available_margin": "100"}

        async def positions(self, asset, fresh=False):
            self.reads += 1
            return {"verified": True, "qty": str(self.qty), "active_orders": 0}

        def normalize_qty(self, asset, qty):
            return qty.quantize(D(".001"), rounding="ROUND_DOWN")

        def ioc_limit_price(self, asset, side, bbo, bps):
            return bbo * (1 + (1 if side == "buy" else -1) * bps / D(10000))

        async def market_order(self, **kwargs):
            assert self.reads == 1, "only startup may query before the first submit"
            qty = kwargs["qty"]
            px = D("100") if self.name == "entropy" else D("100.2")
            assert qty * px <= 20
            submitted.append((self.name, kwargs))
            self.qty += qty if kwargs["side"] == "buy" else -qty
            return fill(qty, px)

    class Hub:
        def __init__(self):
            from tools.lib.nvda_quote_alignment import QuoteClock
            self.quote_event = asyncio.Event()
            self.version = 0
            self.quote_clock = QuoteClock()

        def latest(self, venue, asset, max_age):
            from datetime import datetime, timezone
            bid = 99.99 if venue == "entropy" else 100.2
            at = datetime.now(timezone.utc).isoformat()
            row = {"venue": venue, "symbol": asset, "bid": str(bid), "ask": str(bid + .01),
                    "executable_buy": str(bid + .01), "executable_sell": str(bid),
                    "depth_bids": ((str(bid), "1"),),
                    "depth_asks": ((str(bid + .01), ".019" if thin and venue == "entropy" else "1"),),
                    "sequence": self.version, "recv_at": at, "source_at": at}
            self.quote_clock.record(row)
            return row

        def depth_audit(self, *args):
            return {"audit_only": True}

    class Capture:
        def __init__(self, config, *, entropy_race_bbo=False):
            assert entropy_race_bbo
            self.hub = Hub()
            self.stop_event = asyncio.Event()
            self.writer = SimpleNamespace(append=lambda *args, **kwargs: None)
            append = live.append_jsonl

            def stop_after_first_open(path, row):
                append(path, row)
                if ((path == order_log and row.get("event") == "opened")
                        or row.get("reason") == "entry_depth_insufficient"):
                    self.stop_event.set()
                    self.hub.quote_event.set()

            monkeypatch.setattr(live, "append_jsonl", stop_after_first_open)

        async def run(self):
            for _ in range(100):
                if self.stop_event.is_set():
                    return
                self.hub.version += 1
                self.hub.quote_event.set()
                await asyncio.sleep(.005)
            raise AssertionError("strategy failed to execute")

        def request_stop(self):
            self.stop_event.set()

    ex = {v: Executor(v) for v in ("entropy", "lighter_rh")}
    monkeypatch.setattr(live, "ROOT", tmp_path)
    monkeypatch.setattr(live, "STATE", tmp_path / "state.json")
    monkeypatch.setattr(live, "ENGINE_STATE", tmp_path / "engine.json")
    monkeypatch.setattr(live, "ORDER_LOG", order_log)
    monkeypatch.setattr(live, "BLOCK_LOG", tmp_path / "blockers.jsonl")
    monkeypatch.setattr(live, "LAST_BLOCK", {})
    monkeypatch.setattr(live, "_BLOCK_WRITTEN_AT", {})
    monkeypatch.setattr(live, "load_environment", lambda: {})
    monkeypatch.setattr(live, "build_executors", lambda env: ex)
    monkeypatch.setattr(live, "CaptureRunner", Capture)
    monkeypatch.setattr(live, "window_ready", lambda *a, **kw: True)
    monkeypatch.setattr(live, "median_of", lambda samples: 0)
    args = SimpleNamespace(asset="NVDA", pair="entropy>lighter_rh", tiers="7.77,8.36,9.6,11.21,13.84",
                           tiers_reverse="", mode="live", rh_leverage=20, child_notional=20,
                           notional=20, slippage_bps=1.3, baseline_minutes=30, poll_seconds=1,
                           max_loss_usd=3)
    asyncio.run(live.main_async(args))
    state = json.loads((tmp_path / "engine.json").read_text())
    assert state["engines"]["forward"]["pending_action"] is None
    if thin:
        assert not submitted and not order_log.exists()
        assert all(e.reads == 1 for e in ex.values())
        assert '"entry_depth_insufficient"' in (tmp_path / "blockers.jsonl").read_text()
        return
    assert len(submitted) == 2
    assert state["execution_stats"]["hedged_entries"] == 1
    assert state["engines"]["forward"]["open_tiers"][0]["notional_usd"] == 20
    assert json.loads(order_log.read_text())["entry_depth"]["ok"]


@pytest.mark.parametrize("buy,sell", [("entropy", "lighter_rh"), ("lighter_rh", "entropy")])
def test_exact_size_prices_must_clear_existing_tier_and_ioc_limit(buy, sell):
    from datetime import datetime, timezone
    from types import SimpleNamespace

    def calc(asset, side, touch, bps):
        return touch * (1 + (1 if side == "buy" else -1) * bps / D(10000))

    ex = {v: SimpleNamespace(ioc_limit_price=calc) for v in (buy, sell)}
    now = datetime(2026, 11, 18, 15, 46, 34, tzinfo=timezone.utc)
    q = {v: {"venue": v, "source_at": now, "recv_at": now} for v in (buy, sell)}
    q[buy].update(ask="100", depth_asks=(("100", ".05"), ("100.1", ".15")))
    q[sell].update(bid="100.10", depth_bids=(("100.10", ".3"),))
    args = dict(baseline=0, threshold=6.14, stamp=now.timestamp())
    checked = check_entry_depth(ex, None, buy, sell, D(".2"), q, **args)
    assert checked["reason"] == "entry_depth_below_threshold"
    assert checked["legs"][buy]["vwap"] == "100.075"
    assert check_entry_depth(ex, None, buy, sell, D(".05"), q, **args)["ok"]
    q[buy]["depth_asks"] = (("100", ".05"), ("100.13", "20"))
    checked = check_entry_depth(ex, None, buy, sell, D(".2"), q, **args)
    assert checked["reason"] == "entry_depth_insufficient"
    assert checked["legs"][buy]["covered_qty"] == "0.05"
    q[buy]["provably_stale"] = True
    assert check_entry_depth(ex, None, buy, sell, D(".05"), q, **args)["reason"] == "entry_depth_unavailable"


def test_sndk_2346_thin_bbo_rejects_before_rh_can_open():
    from datetime import datetime
    from types import SimpleNamespace

    now = datetime.fromisoformat("2026-11-18T15:46:34.014+00:00")
    quotes = {
        "entropy": {"venue": "entropy", "ask": "768.50", "depth_asks": (("768.50", ".019"),),
                    "source_at": "2026-11-18T15:46:33.612+00:00",
                    "recv_at": "2026-11-18T15:46:33.980+00:00"},
        "lighter_rh": {"venue": "lighter_rh", "bid": "770.16",
                       "depth_bids": (("770.16", "1"),), "source_at": now, "recv_at": now},
    }
    ex = {"entropy": SimpleNamespace(ioc_limit_price=lambda *args: D("769.42"))}
    checked = check_entry_depth(ex, None, "entropy", "lighter_rh", D(".0259"), quotes,
                                baseline=0, threshold=6.14, stamp=now.timestamp())
    assert checked["reason"] == "entry_depth_insufficient"
    assert checked["venue"] == "entropy"
    assert D(checked["legs"]["entropy"]["covered_qty"]) == D(".019")


def test_read_only_report_counts_legacy_rejections_but_does_not_invent_unwind_pnl(tmp_path):
    import json
    from tools.nvda_execution_report import report

    path = tmp_path / "orders.jsonl"
    path.write_text(json.dumps({"ts": "old", "event": "open_failed", "pair": "entropy>lighter_rh",
                                "receipts": {"entropy": {"status": "rejected"},
                                             "lighter_rh": fill(".2", "100")},
                                "recovery": {"ok": True}}) + "\n")
    result = report(path)
    assert result["execution_stats"]["failed_entries"] == 1
    assert result["verified_net_usd"] == "0"
    assert len(result["missing_cost_events"]) == 1


def test_post_fill_read_lag_is_waited_out_without_a_second_order():
    class Lagging:
        def __init__(self):
            self.reads = 0

        async def positions(self, asset, fresh=False):
            self.reads += 1
            return {"verified": True, "qty": "0" if self.reads == 1 else ".2", "active_orders": 0}

        async def market_order(self, **kwargs):
            raise AssertionError("a stale account read must not send a correction")

    venue = Lagging()
    result = asyncio.run(settle_positions({"rh": venue}, None, ("rh",), {"rh": D(".2")},
                                          settlement_grace=.01, allow_corrections=False))
    assert result["ok"]
    assert result["corrections"] == []


def test_a_correction_never_tries_to_increase_a_position():
    class Lagging:
        async def positions(self, asset, fresh=False):
            return {"verified": True, "qty": ".13", "active_orders": 0}

        async def market_order(self, **kwargs):
            raise AssertionError("buying more of a long is not a reduce-only correction")

    result = asyncio.run(settle_positions({"rh": Lagging()}, None, ("rh",), {"rh": D(".259")}))
    assert result["error"] == "correction_would_increase_position"


def test_fully_filled_close_with_lag_does_not_send_a_duplicate_close():
    class Lagging:
        async def positions(self, asset, fresh=False):
            return {"verified": True, "qty": ".13", "active_orders": 0}

        async def market_order(self, **kwargs):
            raise AssertionError("a confirmed filled close must not be sent twice")

    result = asyncio.run(settle_positions({"rh": Lagging()}, None, ("rh",), {"rh": D("0")},
                                          settlement_grace=.01, allow_corrections=False))
    assert result["error"] == "settlement_not_reflected"


def test_rollback_unwinds_the_confirmed_fill_even_if_account_reads_would_still_show_flat():
    class Venue:
        def __init__(self, qty):
            self.qty = D(qty)
            self.reads = 0
            self.orders = []

        async def positions(self, asset, fresh=False):
            self.reads += 1
            return {"verified": True, "qty": str(self.qty), "active_orders": 0}

        async def market_order(self, **kwargs):
            assert self.reads == 0
            assert kwargs["reduce_only"]
            self.orders.append(kwargs)
            self.qty -= kwargs["qty"]
            return fill(kwargs["qty"], 100)

    ex = {"entropy": Venue("0"), "lighter_rh": Venue(".2")}
    receipts = {"entropy": {"status": "rejected"}, "lighter_rh": fill(".2", 100)}
    result = asyncio.run(rollback_entry(ex, None, tuple(ex), {v: D("0") for v in ex}, receipts,
                                       "lighter_rh", "entropy", token="failure"))
    assert result["ok"]
    assert len(ex["lighter_rh"].orders) == 1
    assert len(result["corrections"]) == 1


def test_unresolved_correction_is_not_retried_or_blindly_unwound():
    result = asyncio.run(rollback_entry({}, None, (), {}, {}, "a", "b", token="unknown",
                                       previous={"corrections": [{"receipt": {"status": "unknown"}}]}))
    assert not result["ok"]
    assert result["error"] == "unresolved_correction"


def test_flat_ledger_recovery_requires_exact_paired_closing_fills_and_is_idempotent():
    from tools.nvda_reconcile_flat import recovery_plan

    saved = {"asset": "NVDA", "pair": "entropy>lighter_rh", "realized_usd": "0",
             "rebate_usd": "0", "engines": {"reverse": {
                 "pending_action": {"action": "close"}, "open_tiers": [{
                     "index": 1, "qty": .2, "notional_usd": 20, "opened_at": 1,
                     "entry_buy_price": 100, "entry_sell_price": 101, "entry_fee_usd": .01}]}}}
    rows = [{"event": "close_failed", "direction": "reverse", "tier": 1,
             "ts": "2026-11-18T00:00:00+00:00", "receipts": {
                 "lighter_rh": fill(".2", 100), "entropy": fill(".2", 100, ".01")}}]
    positions = {v: {"verified": True, "qty": "0", "active_orders": 0}
                 for v in ("entropy", "lighter_rh")}
    state, recovered = recovery_plan(saved, rows, positions)
    assert len(recovered) == 1
    assert D(state["realized_usd"]) == D(".18")
    assert D(recovered[0]["pnl"]["rebate_usd"]) == D(".022")
    assert not state["engines"]["reverse"]["open_tiers"]
    assert state["engines"]["reverse"]["pending_action"] is None
    again, actions = recovery_plan(state, rows, positions)
    assert not actions
    assert again["realized_usd"] == state["realized_usd"]


def test_flat_ledger_recovery_refuses_to_discard_an_unclosed_position():
    import pytest
    from tools.nvda_reconcile_flat import recovery_plan

    saved = {"asset": "NVDA", "pair": "entropy>lighter_rh", "engines": {}}
    positions = {v: {"verified": True, "qty": ".1", "active_orders": 0}
                 for v in ("entropy", "lighter_rh")}
    with pytest.raises(ValueError, match="verified_flat"):
        recovery_plan(saved, [], positions)
