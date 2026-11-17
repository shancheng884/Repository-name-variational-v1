"""The paired order must hit each venue in the direction the ladder decided."""
from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec
from tools.lib.venue_probe import EntropyProbeExecutor, best_bid_ask, leverage_from_imf_percent
from tools.nvda_tier_live import (place_pair, position_qty, quote_version, receipt_price,
                                  receipts_fee, seed_history_from_edges, settle_positions,
                                  trade_result_bps, trim_history, window_ready)


class FakeExecutor:
    def __init__(self, name):
        self.name = name
        self.calls = []

    async def market_order(self, **kwargs):
        self.calls.append(kwargs)
        return {"ok": True, "status": "filled", "client_order_id": kwargs["client_order_id"]}


def _pair():
    buy = FakeExecutor("buy")
    sell = FakeExecutor("sell")
    return buy, sell, {"buy": buy, "sell": sell}


def test_opening_buys_the_buy_leg_and_sells_the_sell_leg():
    buy, sell, ex = _pair()
    asyncio.run(place_pair(ex, None, "buy", "sell", Decimal("1"), opening=True, quotes={}))
    assert buy.calls[0]["side"] == "buy"
    assert sell.calls[0]["side"] == "sell"
    assert buy.calls[0]["reduce_only"] is False
    assert sell.calls[0]["reduce_only"] is False


def test_closing_sells_the_buy_leg_and_buys_the_sell_leg():
    buy, sell, ex = _pair()
    asyncio.run(place_pair(ex, None, "buy", "sell", Decimal("1"), opening=False, quotes={}))
    assert buy.calls[0]["side"] == "sell"
    assert sell.calls[0]["side"] == "buy"
    assert buy.calls[0]["reduce_only"] is True
    assert sell.calls[0]["reduce_only"] is True

def test_a_percent_margin_fraction_reads_back_as_leverage():
    assert leverage_from_imf_percent("5.00") == 20.0
    assert leverage_from_imf_percent("50.00") == 2.0
    assert leverage_from_imf_percent(None) is None


def test_dropping_a_refused_tier_reverses_its_ledger_legs():
    ledger = PositionLedger()
    cfg = TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_rh",
                     tiers=(TierSpec(1, 7.0),), notional_usd=20.0, timeout_seconds=None)
    engine = TierEngine(config=cfg, ledger=ledger)
    action = engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                              window=[8.0, 8.0, 8.0], exchange={"entropy": 0, "lighter_rh": 0})[0]
    engine.confirm_open(action, qty=Decimal("0.125"))
    engine.drop_tier(1)
    assert engine.open_tiers == {}
    assert ledger.net("NVDA", "entropy") == 0
    assert ledger.net("NVDA", "lighter_rh") == 0


class BookExecutor:
    """A venue whose position only moves when a reduce-only order is filled."""

    def __init__(self, qty, fill=True):
        self.qty = Decimal(str(qty))
        self.fill = fill
        self.orders = []

    async def positions(self, asset, fresh=False):
        return {"ok": True, "verified": True, "qty": str(self.qty), "active_orders": 0}

    async def market_order(self, **kwargs):
        self.orders.append(kwargs)
        if not self.fill:
            return {"ok": False, "status": "rejected"}
        delta = Decimal(str(kwargs["qty"]))
        self.qty += -delta if kwargs["side"] == "sell" else delta
        return {"ok": True, "status": "filled"}


def test_settle_closes_a_residual_with_a_reduce_only_order():
    ex = {"rh": BookExecutor("0"), "ent": BookExecutor("0.897")}
    result = asyncio.run(settle_positions(ex, None, ("rh", "ent"),
                                          {"rh": Decimal("0"), "ent": Decimal("0")}))
    assert result["ok"] is True
    assert ex["ent"].orders[0]["side"] == "sell"
    assert ex["ent"].orders[0]["reduce_only"] is True
    assert ex["ent"].qty == 0
    assert ex["rh"].orders == []


def test_settle_reads_fresh_positions_instead_of_a_cached_snapshot():
    """A 2 s position cache must not be reused to prove a fill."""
    seen = []

    class Fresh(BookExecutor):
        async def positions(self, asset, fresh=False):
            seen.append(fresh)
            return {"ok": True, "verified": True, "qty": str(self.qty), "active_orders": 0}

    ex = {"rh": Fresh("0")}
    asyncio.run(settle_positions(ex, None, ("rh",), {"rh": Decimal("0")}))
    assert seen and all(flag is True for flag in seen)


def test_settle_reports_a_leg_it_cannot_correct():
    ex = {"rh": BookExecutor("0"), "ent": BookExecutor("0.897", fill=False)}
    result = asyncio.run(settle_positions(ex, None, ("rh", "ent"),
                                          {"rh": Decimal("0"), "ent": Decimal("0")}, attempts=1))
    assert result["ok"] is False
    assert result["error"] == "position_mismatch"
    assert result["legs"]["ent"]["residual"] != "0"


def test_settle_refuses_to_treat_an_unreadable_venue_as_flat():
    class Blind:
        async def positions(self, asset, fresh=False):
            return {"ok": False, "verified": False, "error": "unreachable"}

    result = asyncio.run(settle_positions({"rh": Blind()}, None, ("rh",),
                                          {"rh": Decimal("0")}, attempts=1))
    assert result["ok"] is False
    assert result["error"] == "position_unreadable"


def test_position_qty_needs_a_verified_read():
    class Blind:
        async def positions(self, asset, fresh=False):
            return {"ok": False, "verified": False}

    class Plain:
        async def positions(self, asset):
            return {"ok": True, "verified": True, "qty": "1.5"}

    assert asyncio.run(position_qty({"rh": Blind()}, None, "rh")) is None
    # A legacy executor without the fresh flag still works.
    assert asyncio.run(position_qty({"rh": Plain()}, None, "rh")) == Decimal("1.5")


def test_best_bid_ask_uses_the_outermost_prices():
    book = {"bids": [{"price": "154.90"}, {"price": "154.95"}],
            "asks": [{"price": "155.00"}, {"price": "154.98"}]}
    assert best_bid_ask(book) == (Decimal("154.95"), Decimal("154.98"))
    assert best_bid_ask({}) == (None, None)
    assert best_bid_ask({"bids": [{"price": "x"}], "asks": []}) == (None, None)


def test_trade_result_uses_the_four_real_prices_and_charges_fees_once():
    # Entry spread 0.10, exit spread 0.00 -> ~9.995 bps captured before fees.
    result = trade_result_bps(entry_buy=100.0, entry_sell=100.10,
                              exit_buy=100.05, exit_sell=100.05,
                              entry_fee_usd=0.01, exit_fee_usd=0.01,
                              notional_usd=100.0)
    assert round(result["gross_bps"], 3) == round(0.10 / 100.05 * 10000, 3)
    assert round(result["fee_bps"], 6) == 2.0
    assert round(result["net_bps"], 6) == round(result["gross_bps"] - 2.0, 6)
    assert round(result["fees_usd"], 6) == 0.02
    assert round(result["net_usd"], 6) == round(result["net_bps"] / 10000 * 100.0, 6)


def test_trade_result_losses_when_the_exit_spread_widens():
    result = trade_result_bps(entry_buy=100.0, entry_sell=100.10,
                              exit_buy=100.00, exit_sell=100.20,
                              notional_usd=100.0)
    assert result["net_bps"] < 0


def test_window_ready_requires_real_time_coverage_not_just_sample_count():
    samples = deque((float(i), 1.0) for i in range(120))
    assert window_ready(samples, 600.0) is False       # 119 s of data
    wide = deque((float(i * 5), 1.0) for i in range(120))
    assert window_ready(wide, 600.0) is True           # 595 s of data
    assert window_ready(deque((float(i), 1.0) for i in range(10)), 60.0) is False


def test_trim_history_drops_samples_outside_the_window():
    samples = deque([(0.0, 1.0), (100.0, 2.0), (300.0, 3.0)])
    trim_history(samples, 50.0)
    assert [ts for ts, _ in samples] == [100.0, 300.0]


def test_quote_version_is_identical_only_for_the_same_snapshot():
    first = {"sequence": 1, "recv_at": "2026-11-17T12:00:00Z", "executable_buy": "100.1", "executable_sell": "100.0"}
    second = {"sequence": 2, "recv_at": "2026-11-17T12:00:01Z", "executable_buy": "100.2", "executable_sell": "100.1"}
    assert quote_version(first, second) == quote_version(first, second)
    assert quote_version(first, second) != quote_version(first, first)


def test_receipts_parse_prices_and_fees():
    assert receipt_price({"status": "filled", "avg_price": "100.5"}) == 100.5
    assert receipt_price({"status": "rejected", "avg_price": "100.5"}) is None
    assert receipt_price({"status": "filled", "avg_price": None}) is None
    assert receipts_fee({"a": {"fee_usd": "0.01"}, "b": {"fee_usd": None}, "c": {}}) == 0.01


class _EntropyAsset:
    def symbol(self, venue):
        return "NVDA"


class _EntropyInfo:
    def __init__(self, qty):
        self.qty = qty
        self.fills = []

    def all_mids(self, dex):
        return {"NVDA": "100"}

    def l2_snapshot(self, coin):
        return {"levels": [[{"px": "100", "sz": "100"}], [{"px": "100", "sz": "100"}]]}

    def user_fills_by_time(self, address, start_ms):
        return list(self.fills)

    def user_state(self, address, dex):
        return {"assetPositions": [{"position": {"coin": "NVDA", "szi": str(self.qty), "entryPx": "100"}}]}

    def open_orders(self, address, dex):
        return []


class _EntropyExchange:
    def __init__(self, info, after_qty=None):
        self.info = info
        self.after_qty = after_qty

    def order(self, coin, is_buy, qty, limit_px, order_type, reduce_only=False, cloid=None):
        if self.after_qty is not None:
            self.info.qty = self.after_qty
        return {"response": {"data": {"statuses": [{"resting": {}}]}}}


def _entropy_executor(info, after_qty=None):
    executor = object.__new__(EntropyProbeExecutor)
    executor.info = info
    executor.exchange = _EntropyExchange(info, after_qty)
    executor.available = True
    executor.reason = None
    executor.account_address = "0xabc"
    executor.dex = "io"
    executor.timeout_seconds = 0.05
    executor.universe = {"NVDA": {"szDecimals": 2}}
    executor._positions_cache = {}
    executor._last_exec_quote = None
    return executor


def test_entropy_timeout_never_claims_a_preexisting_position():
    """Regression: an old 0.5 position used to be reported as a 0.5 fill."""
    info = _EntropyInfo("0.5")
    executor = _entropy_executor(info)
    receipt = asyncio.run(executor._market_order(asset=_EntropyAsset(), side="buy", qty=Decimal("0.2"),
                                                 quote={}, client_order_id="cid-1"))
    assert receipt["status"] == "unknown"
    assert receipt.get("filled_qty") is None


def test_entropy_timeout_books_only_the_delta_this_order_caused():
    info = _EntropyInfo("0.5")
    executor = _entropy_executor(info, after_qty="0.7")
    receipt = asyncio.run(executor._market_order(asset=_EntropyAsset(), side="buy", qty=Decimal("0.2"),
                                                 quote={}, client_order_id="cid-2"))
    assert receipt["status"] == "filled"
    assert Decimal(receipt["filled_qty"]) == Decimal("0.2")
    assert receipt["raw_status"] == "position_delta"


def test_seed_history_reuses_recent_capture_edges(tmp_path):
    base = tmp_path / "edges" / "cross" / "NVDA"
    base.mkdir(parents=True)
    now = datetime.now(timezone.utc)
    rows = [
        {"event": "edge", "mode": "executable", "at": (now - timedelta(seconds=3)).isoformat(),
         "buy_venue": "entropy", "sell_venue": "lighter_rh", "edge_bps": "5.0"},
        {"event": "edge", "mode": "executable", "at": (now - timedelta(seconds=2)).isoformat(),
         "buy_venue": "lighter_rh", "sell_venue": "entropy", "edge_bps": "-3.0"},
        {"event": "edge", "mode": "indicative", "at": (now - timedelta(seconds=1)).isoformat(),
         "buy_venue": "entropy", "sell_venue": "lighter_rh", "edge_bps": "99.0"},
    ]
    (base / "20261117-15.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    history = {"forward": deque(), "reverse": deque()}
    seed_history_from_edges(history, tmp_path, "entropy>lighter_rh", "NVDA", 600)
    assert [value for _, value in history["forward"]] == [5.0]
    assert [value for _, value in history["reverse"]] == [-3.0]
