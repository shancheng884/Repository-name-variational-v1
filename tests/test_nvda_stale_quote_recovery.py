import asyncio
from datetime import datetime, timezone
from decimal import Decimal as D

import pytest

from tools.lib.nvda_quote_alignment import QuoteClock, quote_clock_issue
from tools.nvda_record_manual_rollback import reconcile
from tools.nvda_tier_live import rollback_entry, settle_positions


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).isoformat()


def quote(venue, source, received):
    return {"venue": venue, "symbol": "NVDA", "source_at": iso(source),
            "recv_at": iso(received), "executable_buy": "157.57", "executable_sell": "157.56"}


def test_recent_receive_does_not_rescue_a_four_second_old_rh_quote():
    right = quote("lighter_rh", 8.421989, 12.470417)
    left = quote("entropy", 11.878, 12.246635)
    clock = QuoteClock()
    clock.record(quote("entropy", 8.34, 8.728))
    clock.record(right)
    clock.record(left)
    issue = quote_clock_issue(right, 12.520591)
    assert issue["reason"] == "source_time_expired" and issue["recv_age_ms"] < 51
    assert clock.align(left, right, now=12.520591) is None


def test_alignment_cannot_use_an_expired_historical_leg():
    clock = QuoteClock()
    left = quote("entropy", 8, 8.3)
    right = quote("lighter_rh", 10.1, 10.2)
    for q in (quote("lighter_rh", 7, 7.01), left, right):
        clock.record(q)
    assert quote_clock_issue(left, 10.25) is None
    assert quote_clock_issue(right, 10.25) is None
    assert clock.align(left, right, now=10.25) is None


def test_missing_or_future_timestamps_are_not_tradeable():
    assert quote_clock_issue({**quote("entropy", 1, 1.1), "source_at": None}, 2)["reason"] == "timestamp_missing"
    assert quote_clock_issue(quote("entropy", 3, 2.1), 2.2)["reason"] == "timestamp_in_future"


class Venue:
    def __init__(self, pos, outcomes=(), active=0):
        self.pos = D(pos)
        self.outcomes = iter(outcomes)
        self.active = active
        self.orders = []

    async def positions(self, asset, fresh=False):
        assert fresh
        return {"verified": True, "qty": str(self.pos), "active_orders": self.active}

    async def market_order(self, **order):
        assert order["reduce_only"] and order["quote"] == {}
        assert order["max_slippage_bps"] == D("30")
        self.orders.append(order)
        result = next(self.outcomes)
        if result["status"] == "filled":
            self.pos += D(result["filled_qty"]) * (1 if order["side"] == "buy" else -1)
        return result


def rollback(ent, *, before="0"):
    venues = {"entropy": ent, "lighter_rh": Venue("0")}
    receipts = {"lighter_rh": {"status": "rejected"},
                "entropy": {"status": "filled", "filled_qty": ".126"}}
    result = asyncio.run(rollback_entry(venues, None, tuple(venues),
                                       {"entropy": D(before), "lighter_rh": D("0")},
                                       receipts, "lighter_rh", "entropy", token="failed"))
    return result


def test_rejected_rollback_then_partial_fill_only_retries_real_residual():
    ent = Venue("-.226", [{"status": "rejected"}, {"status": "filled", "filled_qty": ".026"},
                          {"status": "filled", "filled_qty": ".100"}])
    result = rollback(ent, before="-.100")
    assert result["ok"] and ent.pos == D("-.100")
    assert [o["qty"] for o in ent.orders] == [D(".126"), D(".126"), D(".100")]
    assert len({o["client_order_id"] for o in ent.orders}) == 3
    assert all(not o["retry_rate_limit"] for o in ent.orders)


def test_unknown_rollback_never_retries():
    ent = Venue("-.126", [{"status": "unknown"}])
    assert rollback(ent)["error"] == "unresolved_correction"
    assert len(ent.orders) == 1


def test_active_order_or_changed_position_prevents_another_unwind():
    ent = Venue("-.126", [{"status": "rejected"}], active=1)
    # The reported residual is still known from the initial fill; it is not
    # safe to send another correction with an outstanding order.
    result = rollback(ent)
    assert not result["ok"] and len(ent.orders) == 1


def test_lagging_partial_correction_does_not_send_the_same_close_again(monkeypatch):
    async def no_sleep(*args):
        pass
    monkeypatch.setattr("tools.nvda_tier_live.asyncio.sleep", no_sleep)
    ent = Venue("-.126", [{"status": "filled", "filled_qty": ".026"}])
    async def stale(asset, fresh=False):
        return {"verified": True, "qty": "-.126", "active_orders": 0}
    ent.positions = stale
    result = asyncio.run(settle_positions({"entropy": ent}, None, ("entropy",), {"entropy": D("0")}))
    assert result["error"] == "correction_position_not_reflected" and len(ent.orders) == 1


def test_manual_round_trip_is_booked_once_and_requires_the_real_close():
    token, oid = "token", "7"
    state = {"asset": "NVDA", "pair": "entropy>lighter_rh", "realized_usd": "0", "rebate_usd": "0",
             "engines": {"forward": {}, "reverse": {"pending_action": {"action": "open", "token": token}}},
             "execution_stats": {"rollback_net_usd": "0", "unresolved_cost_events": 1, "pending_costs": [{
                 "category": "failed", "execution_token": token, "buy": "lighter_rh", "sell": "entropy",
                 "receipts": {"lighter_rh": {"status": "rejected"}, "entropy": {
                     "status": "filled", "filled_qty": ".126", "avg_price": "158.36",
                     "fee_usd": ".001795", "submitted_at": iso(1)}},
                 "recovery": {"ok": False, "corrections": []}}]}}
    positions = {v: {"verified": True, "qty": "0", "active_orders": 0} for v in ("entropy", "lighter_rh")}
    fill = {"tid": 2, "oid": 7, "coin": "io:NVDA", "time": 2000, "side": "B", "dir": "Close Short",
            "sz": ".126", "px": "159.05", "fee": ".001803"}
    with pytest.raises(ValueError, match="size"):
        reconcile(state, token, oid, [{**fill, "sz": ".125"}], positions)
    new, event = reconcile(state, token, oid, [fill, fill], positions)
    assert D(new["realized_usd"]) == D("-.090538")
    assert event["manual_close_receipt"]["order_id"] == oid
    assert reconcile(new, token, oid, [fill], positions)[1] is None
