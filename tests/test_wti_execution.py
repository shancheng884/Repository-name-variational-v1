from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tools.lib.market_sessions import classify_usoil_session
from tools.lib.wti_execution import WTIPairExecutor, WTIExecutionError, WTIExecutionStore
from tools.lib.wti_live_gateway import WTIProductionGateway
from tools.lib.lighter_order_identity import LEGACY_INDEX_REJECTION, MAX_LIGHTER_CLIENT_ORDER_INDEX


DIRECTION = "long_var_short_lighter"


def _sample(*, now: datetime | None = None, **overrides):
    now = now or datetime.now(timezone.utc)
    row = {
        "asset": "WTI",
        "sample_quality": "valid",
        "market_availability": "both_feeds_live",
        "logged_at": now.isoformat(),
        "variational_underlying": "USOILP",
        "variational_instrument_type": "swap",
        "variational_instrument_kind": "commodity",
        "variational_funding_interval_s": 0,
        "lighter_symbol": "WTI",
        "lighter_market_type": "perp",
        "lighter_market_id": 145,
        "lighter_market_status": "active",
        "lighter_multiplier": "1",
        "lighter_min_base_amount": "0.01",
        "lighter_min_quote_amount": "1",
        "lighter_supported_size_decimals": 2,
        "lighter_supported_price_decimals": 2,
        "var_quote_age_seconds": "0.1",
        "lighter_book_age_seconds": "0.1",
        "var_bid": "70",
        "var_ask": "70.1",
        "lighter_bid": "70.02",
        "lighter_ask": "70.08",
        "lighter_buy_price": "70.08",
        "lighter_sell_price": "70.02",
        "variational_qty": "0.29",
        "lighter_qty": "0.29",
        "market_status": "open",
        "session_status": "open",
        "session_status_source": "variational_market_status_websocket",
        "session_status_observed_at": now.isoformat(),
        "session_status_feed_connected": True,
        "session_status_feed_heartbeat_at": now.isoformat(),
        "session_schedule_source": "variational_supported_assets_api",
        "session_id": "wti-session-1",
        "session_closing_at": (now + timedelta(hours=4)).isoformat(),
    }
    row.update(overrides)
    return row


def _session(
    *,
    open_: bool = True,
    now: datetime | None = None,
    closes_in: timedelta = timedelta(hours=4),
    entry_cutoff_minutes: int = 30,
    status_max_age_seconds: float = 30,
):
    now = now or datetime.now(timezone.utc)
    return classify_usoil_session(
        now,
        exchange_open=open_,
        exchange_status_observed_at=now,
        exchange_closing_at=now + closes_in if open_ else None,
        entry_cutoff_minutes=entry_cutoff_minutes,
        status_max_age_seconds=status_max_age_seconds,
    )


def _session_for_sample(sample):
    return classify_usoil_session(
        datetime.fromisoformat(sample["logged_at"].replace("Z", "+00:00")),
        exchange_open=sample.get("market_status") == "open",
        exchange_status_observed_at=datetime.fromisoformat(
            sample["session_status_observed_at"].replace("Z", "+00:00")
        ),
        exchange_closing_at=datetime.fromisoformat(
            sample["session_closing_at"].replace("Z", "+00:00")
        ),
        entry_cutoff_minutes=30,
        status_max_age_seconds=30,
    )


class SimulatedWTIGateway:
    """Test-only exchange simulator. No network or credentials are used."""

    def __init__(self, *, results=None, lookup_results=None, fail_read_calls=(), active_orders=None, clock=None):
        self.var_qty = Decimal("0")
        self.lighter_qty = Decimal("0")
        self.results = results or {}
        self.lookup_results = lookup_results or {}
        self.fail_read_calls = set(fail_read_calls)
        self.active_orders = active_orders if active_orders is not None else []
        self.read_calls = 0
        self.requests = []
        self.order_responses = {}
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def read_account(self):
        self.read_calls += 1
        if self.read_calls in self.fail_read_calls:
            raise TimeoutError("simulated account timeout")
        return {
            "observed_at": self.clock().isoformat(),
            "variational_positions": {
                "ok": True,
                "positions": [{
                    "instrument": {
                        "underlying": "USOILP",
                        "instrument_type": "swap",
                        "settlement_asset": "USDC",
                        "kind": "commodity",
                        "funding_interval_s": 0,
                    },
                    "qty": str(self.var_qty),
                }],
            },
            "lighter_positions": {
                "code": 200,
                "accounts": [{"positions": [{
                    "market_id": 145,
                    "symbol": "WTI",
                    "position": str(abs(self.lighter_qty)),
                    "sign": 1 if self.lighter_qty >= 0 else -1,
                }]}],
            },
            "active_orders": {"ok": True, "orders": list(self.active_orders)},
        }

    def submit_order(self, request):
        self.requests.append(dict(request))
        venue = request["venue"]
        result_key = f"{venue}_compensation" if request.get("reduce_only") else venue
        result = deepcopy(self.results.get(result_key, self.results.get(venue)))
        qty = Decimal(request["qty"])
        if result is None:
            result = {
                "ok": True,
                "final": True,
                "order_id": f"order-{len(self.requests)}",
                "status": "filled",
                "filled_qty": str(qty),
                "average_price": "70.05",
                "trade_ids": [f"trade-{len(self.requests)}"],
                "fee_usd": "0.01",
            }
        result.setdefault("client_order_id", request["client_order_id"])
        lookup_result = deepcopy(self.lookup_results.get(venue, result))
        if isinstance(lookup_result, dict):
            lookup_result.setdefault("client_order_id", request["client_order_id"])
        self.order_responses[request["client_order_id"]] = lookup_result
        executed_status = result.get("execute_status", result.get("status"))
        if result.get("mutate_position", True) and executed_status in {"filled", "partially_filled"}:
            fill = Decimal(str(result.get("execute_filled_qty", result.get("filled_qty", qty))))
            signed = fill if request["side"] == "buy" else -fill
            if venue == "variational":
                self.var_qty += signed
            else:
                self.lighter_qty += signed
        return result

    def lookup_order(self, client_order_id, request=None):
        return deepcopy(self.order_responses.get(client_order_id, {"ok": False, "error": "not_found"}))


def _executor(
    tmp_path, gateway=None, *, clock=None, sample=None, market_reader=None,
    market_reader_for_qty=None,
):
    sample = sample or _sample()
    gateway = gateway or SimulatedWTIGateway(clock=clock)
    if clock is not None:
        gateway.clock = clock
    return WTIPairExecutor(
        state_path=tmp_path / "wti-live-state.json",
        gateway=gateway,
        max_slippage_bps=Decimal("8"),
        market_reader=market_reader or (lambda: sample),
        market_reader_for_qty=market_reader_for_qty,
        expected_lighter_market_id=145,
        expected_lighter_multiplier=Decimal("1"),
        expected_lighter_size_decimals=2,
        clock=clock,
    ), gateway


def _open(executor, *, sample=None, session=None, pilot_id=None, pilot_reservation_id=None):
    now = datetime.now(timezone.utc)
    sample = sample or _sample(now=now)
    return executor.open(
        sample=sample,
        direction=DIRECTION,
        notional_usd=Decimal("20"),
        session=session or _session_for_sample(sample),
        pilot_id=pilot_id,
        pilot_reservation_id=pilot_reservation_id,
        now=now,
    )


def test_full_open_close_uses_exact_contracts_reduce_only_and_confirmed_positions(tmp_path):
    executor, gateway = _executor(tmp_path)
    opened = _open(executor)
    assert opened["status"] == "open"
    assert opened["open_position"]["expected_var_qty"] == "0.29"
    assert opened["open_position"]["expected_lighter_qty"] == "-0.29"
    assert len(gateway.requests) == 2
    var, lighter = gateway.requests
    assert (var["venue"], var["symbol"], var["instrument_type"], var["kind"]) == (
        "variational", "USOILP", "swap", "commodity"
    )
    assert (lighter["venue"], lighter["symbol"], lighter["market_id"]) == (
        "lighter", "WTI", 145
    )
    assert var["time_in_force"] == lighter["time_in_force"] == "IOC"
    assert Decimal(var["limit_price"]) > Decimal("70.1")
    assert Decimal(lighter["limit_price"]) < Decimal("70.02")
    assert not var["reduce_only"] and not lighter["reduce_only"]

    now = datetime.now(timezone.utc)
    closed = executor.close(sample=_sample(now=now), session=_session(now=now), now=now)
    assert closed["status"] == "flat"
    assert closed["open_position"] is None
    assert closed["last_closed_position"]["var_exit_price"] == "70.05"
    assert closed["last_closed_position"]["actual_gross_pnl_status"] == "confirmed_from_final_fills"
    assert closed["last_closed_position"]["actual_net_pnl_usd"] is None
    assert closed["last_closed_position"]["actual_net_pnl_status"] == "funding_unverified"
    assert closed["last_closed_position"]["funding_cashflow_confirmed"] is False
    assert closed["last_closed_position"]["entry_execution_id"]
    assert closed["last_closed_position"]["exit_execution_id"]
    assert len(gateway.requests) == 4
    assert all(request["reduce_only"] for request in gateway.requests[-2:])
    assert gateway.var_qty == gateway.lighter_qty == 0
    assert len(closed["execution_history"]) == 2
    var_evidence = closed["execution_history"][0]["legs"]["variational"]["result"]
    assert var_evidence["order_id"] == "order-1"
    assert var_evidence["trade_ids"] == ["trade-1"]
    assert var_evidence["fee_usd"] == "0.01"
    assert var_evidence["fee_confirmed"] is True


def test_single_round_pilot_is_persisted_through_confirmed_open_close_and_restart(tmp_path):
    executor, _ = _executor(tmp_path)
    store = WTIExecutionStore(executor.store.path)
    config = {
        "entry_threshold_bps": "4", "exit_target_bps": "2",
        "roundtrip_cost_bps": "3", "max_notional_usd": "20",
    }
    pilot = store.initialize_single_round_pilot(
        market_id=145, multiplier=Decimal("1"), config=config,
        readiness={"ready": False, "session_count": 0},
    )
    reservation_id = store.reserve_single_round_pilot_entry(pilot["pilot_id"])

    opened = _open(
        executor, pilot_id=pilot["pilot_id"],
        pilot_reservation_id=reservation_id,
    )
    assert opened["single_round_pilot"]["status"] == "in_position"
    assert opened["single_round_pilot"]["entry_action_id"] == opened["open_position"]["entry_execution_id"]
    assert opened["open_position"]["single_round_pilot_id"] == pilot["pilot_id"]

    resumed = store.initialize_single_round_pilot(
        market_id=145, multiplier=Decimal("1"), config=config,
        readiness={"ready": True, "session_count": 3},
    )
    assert resumed["pilot_id"] == pilot["pilot_id"]
    assert resumed["status"] == "in_position"
    assert resumed["entry_attempts"] == 1

    now = datetime.now(timezone.utc)
    closed = executor.close(sample=_sample(now=now), session=_session(now=now), now=now)
    assert closed["status"] == "flat"
    assert closed["single_round_pilot"]["status"] == "completed"
    assert closed["single_round_pilot"]["completed_roundtrips"] == 1
    assert closed["single_round_pilot"]["exit_action_id"] == closed["last_closed_position"]["exit_execution_id"]
    with pytest.raises(WTIExecutionError, match="single_round_pilot_entry_budget_unavailable"):
        store.reserve_single_round_pilot_entry(pilot["pilot_id"])
    with pytest.raises(WTIExecutionError, match="single_round_pilot_already_completed"):
        store.initialize_single_round_pilot(
            market_id=145, multiplier=Decimal("1"), config=config,
            readiness={"ready": True},
        )


def test_single_round_pilot_entry_attempt_limit_persists(tmp_path):
    executor, _ = _executor(tmp_path)
    store = WTIExecutionStore(executor.store.path)
    config = {"entry_threshold_bps": "4", "exit_target_bps": "2"}
    pilot = store.initialize_single_round_pilot(
        market_id=145, multiplier=Decimal("1"), config=config,
        readiness={"ready": False}, max_entry_attempts=1,
    )
    reservation = store.reserve_single_round_pilot_entry(pilot["pilot_id"])
    released = store.release_unsubmitted_single_round_pilot_entry(
        pilot["pilot_id"], reservation
    )
    assert released["single_round_pilot"]["status"] == "exhausted"
    assert released["single_round_pilot"]["entry_attempts"] == 1
    with pytest.raises(WTIExecutionError, match="single_round_pilot_entry_attempts_exhausted"):
        store.initialize_single_round_pilot(
            market_id=145, multiplier=Decimal("1"), config=config,
            readiness={"ready": True}, max_entry_attempts=1,
        )


def test_reconciled_close_completes_pilot_only_after_both_venues_are_flat(tmp_path):
    executor, gateway = _executor(tmp_path)
    store = WTIExecutionStore(executor.store.path)
    config = {"entry_threshold_bps": "4", "exit_target_bps": "2"}
    pilot = store.initialize_single_round_pilot(
        market_id=145, multiplier=Decimal("1"), config=config,
        readiness={"ready": False},
    )
    reservation = store.reserve_single_round_pilot_entry(pilot["pilot_id"])
    assert _open(
        executor, pilot_id=pilot["pilot_id"], pilot_reservation_id=reservation
    )["status"] == "open"

    gateway.fail_read_calls.add(gateway.read_calls + 2)
    pending = executor.close(
        sample=_sample(), session=_session(), now=datetime.now(timezone.utc)
    )
    assert pending["status"] == "manual_review_required"
    assert pending["single_round_pilot"]["status"] == "in_position"
    assert gateway.var_qty == gateway.lighter_qty == 0

    recovered = executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "flat"
    assert recovered["single_round_pilot"]["status"] == "completed"
    assert recovered["single_round_pilot"]["completed_roundtrips"] == 1


def test_unverified_session_snapshot_blocks_before_any_order(tmp_path):
    executor, gateway = _executor(tmp_path)
    now = datetime.now(timezone.utc)
    with pytest.raises(WTIExecutionError, match="exchange_session_observation_unverified"):
        executor.open(
            sample=_sample(now=now, session_status_feed_connected=False),
            direction=DIRECTION,
            notional_usd=Decimal("20"),
            session=_session(now=now),
            now=now,
        )
    assert gateway.requests == []


def test_short_direction_uses_opposite_signed_positions_and_sides(tmp_path):
    executor, gateway = _executor(tmp_path)
    now = datetime.now(timezone.utc)
    opened = executor.open(
        sample=_sample(now=now),
        direction="short_var_long_lighter",
        notional_usd=Decimal("20"),
        session=_session(now=now),
        now=now,
    )
    assert opened["status"] == "open"
    assert opened["open_position"]["expected_var_qty"] == "-0.29"
    assert opened["open_position"]["expected_lighter_qty"] == "0.29"
    assert [request["side"] for request in gateway.requests] == ["sell", "buy"]
    closed = executor.close(sample=_sample(), session=_session(), now=datetime.now(timezone.utc))
    assert closed["status"] == "flat"
    assert [request["side"] for request in gateway.requests] == ["sell", "buy", "buy", "sell"]


@pytest.mark.parametrize(
    ("sample_overrides", "expected_reason"),
    [
        ({"market_availability": "partial_or_unavailable"}, "market_feeds_not_both_live"),
        ({"sample_quality": "degraded"}, "market_sample_not_valid_wti"),
        ({"lighter_market_id": None}, "lighter_market_identity_or_precision_invalid"),
        ({"lighter_market_type": "spot"}, "lighter_wti_market_type_mismatch"),
        ({"variational_instrument_kind": "perp"}, "wti_instrument_identity_mismatch"),
        ({"var_quote_age_seconds": "4"}, "variational_quote_stale_or_invalid"),
        ({"lighter_market_status": "closed"}, "lighter_market_not_confirmed_active"),
    ],
)
def test_untrusted_readonly_market_data_never_submits(sample_overrides, expected_reason, tmp_path):
    executor, gateway = _executor(tmp_path)
    with pytest.raises(WTIExecutionError, match=expected_reason):
        _open(executor, sample=_sample(**sample_overrides))
    assert gateway.requests == []


def test_insufficient_readonly_depth_blocks_order_before_any_submit(tmp_path):
    executor, gateway = _executor(tmp_path)
    with pytest.raises(WTIExecutionError, match="readonly_sample_depth_below_order_quantity"):
        _open(executor, sample=_sample(variational_qty="0.1", lighter_qty="0.1"))
    assert gateway.requests == []


def test_notional_cap_blocks_order_before_any_submit(tmp_path):
    executor, gateway = _executor(tmp_path)
    with pytest.raises(WTIExecutionError, match="notional_outside_executor_limit"):
        executor.open(
            sample=_sample(), direction=DIRECTION,
            notional_usd=Decimal("26"), session=_session(),
            now=datetime.now(timezone.utc),
        )
    assert gateway.requests == []


def test_target_equal_to_cap_rounds_quantity_down_without_exceeding_cap(tmp_path):
    executor, gateway = _executor(tmp_path)
    executor.max_notional_usd = Decimal("20")
    opened = _open(executor)
    assert opened["status"] == "open"
    assert opened["open_position"]["var_qty"] == "0.28"
    assert all(Decimal(r["qty"]) == Decimal("0.28") for r in gateway.requests)
    assert Decimal("0.28") * Decimal("70.05") <= executor.max_notional_usd


@pytest.mark.parametrize(
    "minimums", [{"lighter_min_base_amount": "0.29"}, {"lighter_min_quote_amount": "20"}]
)
def test_cap_cannot_be_rounded_below_market_minimums(tmp_path, minimums):
    executor, gateway = _executor(tmp_path)
    executor.max_notional_usd = Decimal("20")
    with pytest.raises(WTIExecutionError, match="rounded_order_exceeds_executor_notional_limit"):
        _open(executor, sample=_sample(**minimums))
    assert gateway.requests == []


def test_stale_sample_and_unverified_session_never_submit(tmp_path):
    executor, gateway = _executor(tmp_path)
    now = datetime.now(timezone.utc)
    stale = _sample(now=now - timedelta(seconds=21))
    with pytest.raises(WTIExecutionError, match="market_sample_stale_or_future"):
        _open(executor, sample=stale)
    with pytest.raises(WTIExecutionError, match="exchange_session_does_not_allow_entry"):
        executor.open(
            sample=_sample(), direction=DIRECTION, notional_usd=Decimal("20"),
            session=None, now=now,
        )
    with pytest.raises(WTIExecutionError, match="exchange_session_does_not_allow_entry"):
        _open(executor, session=_session(open_=False, now=now))
    assert gateway.requests == []


def test_quote_age_includes_elapsed_time_after_sample_capture(tmp_path):
    executor, gateway = _executor(tmp_path)
    old = _sample(now=datetime.now(timezone.utc) - timedelta(seconds=19))
    with pytest.raises(WTIExecutionError, match="variational_quote_stale_or_invalid"):
        _open(executor, sample=old)
    assert gateway.requests == []


def test_quote_age_is_not_counted_twice_between_validation_and_submit(tmp_path):
    now = datetime.now(timezone.utc)
    sample = _sample(
        now=now - timedelta(seconds=1.5),
        var_quote_age_seconds="0.1",
        lighter_book_age_seconds="0.1",
    )
    executor, gateway = _executor(tmp_path, sample=sample)
    assert _open(executor, sample=sample)["status"] == "open"
    assert len(gateway.requests) == 2


def test_sample_cannot_choose_an_untrusted_lighter_market_id(tmp_path):
    executor, gateway = _executor(tmp_path)
    bad = _sample(lighter_market_id=0)
    with pytest.raises(WTIExecutionError, match="lighter_market_identity_or_precision_invalid"):
        _open(executor, sample=bad)
    assert gateway.requests == []


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"lighter_multiplier": "2"}, "lighter_market_multiplier_mismatch"),
        ({"lighter_supported_size_decimals": 3}, "lighter_market_identity_or_precision_invalid"),
    ],
)
def test_sample_cannot_override_trusted_lighter_contract_metadata(tmp_path, overrides, reason):
    executor, gateway = _executor(tmp_path)
    with pytest.raises(WTIExecutionError, match=reason):
        _open(executor, sample=_sample(**overrides))
    assert gateway.requests == []


def test_session_window_is_rejected_after_its_exchange_close(tmp_path):
    executor, gateway = _executor(tmp_path)
    created = datetime.now(timezone.utc) - timedelta(hours=5)
    expired = _session(now=created, closes_in=timedelta(hours=4))
    with pytest.raises(WTIExecutionError, match="exchange_session_does_not_allow_entry"):
        executor.open(
            sample=_sample(), direction=DIRECTION, notional_usd=Decimal("20"),
            session=expired, now=datetime.now(timezone.utc),
        )
    assert gateway.requests == []


def test_session_is_rechecked_after_slow_flat_account_read(tmp_path):
    start = datetime.now(timezone.utc)
    clock_value = [start]
    gateway = SimulatedWTIGateway(clock=lambda: clock_value[0])
    original_read = gateway.read_account

    def slow_read():
        result = original_read()
        clock_value[0] = start + timedelta(seconds=1)
        return result

    gateway.read_account = slow_read
    executor, _ = _executor(
        tmp_path, gateway, clock=lambda: clock_value[0],
        sample=_sample(
            now=start, var_quote_age_seconds="0", lighter_book_age_seconds="0",
            session_closing_at=(start + timedelta(milliseconds=500)).isoformat(),
        ),
    )
    result = executor.open(
        sample=_sample(
            now=start, var_quote_age_seconds="0", lighter_book_age_seconds="0",
            session_closing_at=(start + timedelta(milliseconds=500)).isoformat(),
        ),
        direction=DIRECTION,
        notional_usd=Decimal("20"),
        session=_session(
            now=start,
            closes_in=timedelta(milliseconds=500),
            entry_cutoff_minutes=0,
        ),
        now=start,
    )
    assert result["status"] == "manual_review_required"
    assert result["pending_action"]["legs"]["variational"]["result"]["reason"] == "exchange_session_expired_before_submit"
    assert gateway.requests == []


def test_open_window_is_rechecked_against_entry_cutoff_time(tmp_path):
    current = datetime.now(timezone.utc)
    close_at = current + timedelta(minutes=29)
    current_sample = _sample(
        now=current,
        session_closing_at=close_at.isoformat(),
    )
    executor, gateway = _executor(
        tmp_path,
        clock=lambda: current,
        sample=current_sample,
    )
    window = _session_for_sample(current_sample)
    assert window.state.value == "entry_cutoff"
    with pytest.raises(WTIExecutionError, match="exchange_session_does_not_allow_entry"):
        executor.open(
            sample=current_sample, direction=DIRECTION,
            notional_usd=Decimal("20"), session=window, now=current,
        )
    assert gateway.requests == []


def test_existing_exchange_position_or_active_order_blocks_entry(tmp_path):
    gateway = SimulatedWTIGateway(active_orders=[{"market_id": 145, "id": "existing"}])
    executor, _ = _executor(tmp_path, gateway)
    with pytest.raises(WTIExecutionError, match="not_flat_or_has_active_orders"):
        _open(executor)
    assert not gateway.requests

    gateway = SimulatedWTIGateway()
    gateway.var_qty = Decimal("0.1")
    executor, _ = _executor(tmp_path / "other", gateway)
    with pytest.raises(WTIExecutionError, match="not_flat_or_has_active_orders"):
        _open(executor)
    assert not gateway.requests


def test_partial_first_leg_is_persisted_and_never_retried_or_hedged_blindly(tmp_path):
    partial = {
        "ok": True, "final": True, "order_id": "var-partial",
        "status": "partially_filled", "filled_qty": "0.1",
    }
    gateway = SimulatedWTIGateway(results={"variational": partial})
    executor, _ = _executor(tmp_path, gateway)
    state = _open(executor)
    assert state["status"] == "manual_review_required"
    assert state["pending_action"]["legs"]["variational"]["result"]["classification"] == "partial_or_inconsistent"
    assert gateway.var_qty == Decimal("0.1")
    assert len(gateway.requests) == 1
    with pytest.raises(WTIExecutionError, match="existing_wti_state_blocks_entry"):
        _open(executor)
    assert len(gateway.requests) == 1
    recovered = executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "manual_review_required"


def test_second_leg_requires_fresh_market_within_drift_guard(tmp_path):
    gateway = SimulatedWTIGateway()
    executor, _ = _executor(tmp_path, gateway)
    reads = 0

    def changing_market():
        nonlocal reads
        reads += 1
        return _sample(lighter_ask="70.2") if reads > 1 else _sample()

    executor.market_reader = changing_market
    state = _open(executor)
    assert state["status"] == "manual_review_required"
    assert state["manual_review_reason"] == "readonly_market_moved_beyond_slippage_guard"
    assert len(gateway.requests) == 1
    assert gateway.lighter_qty == 0


def test_session_change_after_first_fill_never_submits_second_leg(tmp_path):
    gateway = SimulatedWTIGateway()
    now = datetime.now(timezone.utc)
    initial = _sample(now=now)
    executor, _ = _executor(tmp_path, gateway, sample=initial)
    refreshes = 0

    def changing_session():
        nonlocal refreshes
        refreshes += 1
        if refreshes == 1:
            return initial
        next_open = datetime.now(timezone.utc)
        return _sample(
            now=next_open,
            session_id="wti-session-2",
            session_closing_at=(next_open + timedelta(hours=8)).isoformat(),
        )

    executor.market_reader = changing_session
    state = _open(executor, sample=initial)
    assert state["status"] == "manual_review_required"
    assert state["manual_review_reason"] == "wti_session_changed_during_pair_execution"
    assert [request["venue"] for request in gateway.requests] == ["variational"]
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == 0


def test_unknown_submit_reply_is_not_replayed_after_restart(tmp_path):
    unknown = {"ok": False, "error": "timeout"}
    gateway = SimulatedWTIGateway(results={"variational": unknown})
    executor, _ = _executor(tmp_path, gateway)
    state = _open(executor)
    assert state["status"] == "manual_review_required"
    assert state["pending_action"]["legs"]["variational"]["result"]["classification"] == "unknown"
    restarted, _ = _executor(tmp_path, gateway)
    with pytest.raises(WTIExecutionError, match="existing_wti_state_blocks_entry"):
        _open(restarted)
    assert len(gateway.requests) == 1


def test_order_intent_is_durable_before_gateway_submit(tmp_path):
    executor, gateway = _executor(tmp_path)
    original_submit = gateway.submit_order
    observed = []

    def inspect_then_submit(request):
        saved = json.loads((tmp_path / "wti-live-state.json").read_text(encoding="utf-8"))
        leg = saved["pending_action"]["legs"][request["venue"]]
        observed.append((leg["request"]["client_order_id"], leg["submission_started"]))
        return original_submit(request)

    gateway.submit_order = inspect_then_submit
    assert _open(executor)["status"] == "open"
    assert observed == [(gateway.requests[0]["client_order_id"], True),
                        (gateway.requests[1]["client_order_id"], True)]


def test_confirmed_second_leg_no_fill_is_compensated_once(tmp_path):
    rejected = {
        "ok": True, "final": True, "order_id": "lighter-rejected",
        "status": "rejected", "filled_qty": "0",
    }
    gateway = SimulatedWTIGateway(results={"lighter": rejected})
    executor, _ = _executor(tmp_path, gateway)
    state = _open(executor)
    assert state["status"] == "flat"
    assert state["open_position"] is None
    assert state["execution_history"][-1]["outcome"] == "compensated"
    assert gateway.var_qty == 0
    assert gateway.lighter_qty == 0
    assert len(gateway.requests) == 3
    assert gateway.requests[-1]["venue"] == "variational"
    assert gateway.requests[-1]["reduce_only"] is True


def test_unknown_var_compensation_keeps_manual_review_without_retry(tmp_path):
    rejected = {
        "ok": True, "final": True, "order_id": "lighter-rejected",
        "status": "rejected", "filled_qty": "0",
    }
    unknown = {"ok": False, "error": "timeout"}
    gateway = SimulatedWTIGateway(
        results={"lighter": rejected, "variational_compensation": unknown}
    )
    executor, _ = _executor(tmp_path, gateway)
    state = _open(executor)
    assert state["status"] == "manual_review_required"
    assert state["manual_review_reason"] == "lighter_open_no_fill"
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == 0
    assert len(gateway.requests) == 3
    assert gateway.requests[-1]["reduce_only"] is True
    restarted, _ = _executor(tmp_path, gateway)
    with pytest.raises(WTIExecutionError, match="existing_wti_state_blocks_entry"):
        _open(restarted)
    assert len(gateway.requests) == 3


def test_restart_reconciles_confirmed_open_without_resubmitting(tmp_path):
    gateway = SimulatedWTIGateway(fail_read_calls={2})
    executor, _ = _executor(tmp_path, gateway)
    pending = _open(executor)
    assert pending["status"] == "manual_review_required"
    assert pending["manual_review_reason"] == "post_open_account_snapshot_unavailable"
    assert len(gateway.requests) == 2
    restarted, _ = _executor(tmp_path, gateway)
    recovered = restarted.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "open"
    assert recovered["open_position"]["recovered_from_pending"] is True
    assert recovered["open_position"]["session_id"] == "wti-session-1"
    assert len(gateway.requests) == 2


def test_restart_continues_only_missing_lighter_leg_after_confirmed_var_fill(tmp_path):
    sample = _sample()
    gateway = SimulatedWTIGateway()
    reads = 0

    def fail_after_first_leg():
        nonlocal reads
        reads += 1
        if reads == 2:
            raise RuntimeError("simulated restart boundary after Var fill")
        return sample

    executor, _ = _executor(tmp_path, gateway, sample=sample, market_reader=fail_after_first_leg)
    interrupted = _open(executor, sample=sample)
    assert interrupted["status"] == "manual_review_required"
    assert interrupted["pending_action"]["legs"].keys() == {"variational"}
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == 0
    assert [request["venue"] for request in gateway.requests] == ["variational"]

    restarted, _ = _executor(tmp_path, gateway, sample=sample)
    recovered = restarted.reconcile(
        sample=sample,
        session=_session_for_sample(sample),
        now=datetime.now(timezone.utc),
    )

    assert recovered["status"] == "open"
    assert recovered["open_position"]["recovered_from_pending"] is True
    assert recovered["open_position"]["expected_var_qty"] == "0.29"
    assert recovered["open_position"]["expected_lighter_qty"] == "-0.29"
    assert recovered["last_action_resolution"] == "pending_open_missing_lighter_leg_completed"
    assert [request["venue"] for request in gateway.requests] == ["variational", "lighter"]

    again = restarted.reconcile(
        sample=sample,
        session=_session_for_sample(sample),
        now=datetime.now(timezone.utc),
    )
    assert again["status"] == "open"
    assert [request["venue"] for request in gateway.requests] == ["variational", "lighter"]


def _legacy_invalid_lighter_pending(tmp_path):
    sample = _sample(lighter_continuity_ok=True)
    simulation = SimulatedWTIGateway(results={"lighter": {"ok": False, "final": False}})
    executor, _ = _executor(tmp_path, simulation, sample=sample)
    state = _open(executor, sample=sample)
    assert state["status"] == "manual_review_required"
    state["pending_action"]["action_id"] = "legacy-live-action"
    leg = state["pending_action"]["legs"]["lighter"]
    leg["request"]["client_order_id"] = "legacy-live-action-lighter"
    for key in ("native_client_order_id", "lighter_client_index_scheme", "transaction_hash"):
        leg["request"].pop(key, None)
    leg["result"] = {"classification": "unknown", "terminal_confirmed": False}
    leg.pop("submission_result", None)
    executor.store.save(state)
    return executor, simulation, sample


@pytest.mark.parametrize("response_mode", ["accepted", "lost_ack", "absent", "risk_block", "unchanged_open_status"])
def test_startup_recovery_uses_production_submission_and_lookup_without_duplicate_send(
    tmp_path, monkeypatch, response_mode,
):
    import lighter
    from lighter.transactions import CreateOrder
    from tools import wti_live
    from types import SimpleNamespace

    executor, simulation, sample = _legacy_invalid_lighter_pending(tmp_path)
    if response_mode == "unchanged_open_status":
        sample["session_status_observed_at"] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    orders, sent = [], []

    class API:
        def __init__(self, client):
            pass

        async def account_inactive_orders(self, **kwargs):
            return {"code": 200, "orders": deepcopy(orders)}

        async def account_active_orders(self, **kwargs):
            return {"code": 200, "orders": []}

        async def trades(self, **kwargs):
            return {"code": 200, "trades": []}

    class Signer:
        api_client = object()
        ORDER_TYPE_MARKET = 1
        ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
        DEFAULT_IOC_EXPIRY = 0

        def create_auth_token_with_expiry(self, **kwargs):
            return "test-token", None

        async def create_order(self, **kwargs):
            sent.append(kwargs)
            saved = executor.store.load()["pending_action"]
            req = saved["legs"]["lighter"]["request"]
            assert saved["legs"]["lighter"]["submission_started"] is True
            assert req["lighter_client_index_scheme"] == "sha256_48_v1"
            assert int(req["native_client_order_id"]) == kwargs["client_order_index"]
            assert 1 <= kwargs["client_order_index"] <= MAX_LIGHTER_CLIENT_ORDER_INDEX
            assert "replacement-1" in req["client_order_id"]
            archived = saved["superseded_legs"][0]
            assert archived["reason"] == LEGACY_INDEX_REJECTION
            assert archived["leg"]["request"]["client_order_id"] == "legacy-live-action-lighter"
            if response_mode != "absent":
                simulation.lighter_qty = Decimal("-0.29")
                orders.append({
                    "market_index": 145, "client_order_index": kwargs["client_order_index"],
                    "order_index": 123456, "is_ask": True, "status": "filled",
                    "filled_base_amount": "0.29", "filled_quote_amount": "20.30",
                })
            if response_mode == "lost_ack":
                raise TimeoutError("simulated disconnect after send")
            return CreateOrder.from_json("{}"), {"code": 200, "tx_hash": "tx-accepted"}, None

    monkeypatch.setattr(lighter, "OrderApi", API)

    async def exercise():
        production = WTIProductionGateway(
            loop=asyncio.get_running_loop(), variational=object(), account_reader=object(),
            lighter_client=Signer(), account_index=10, api_key_index=1,
            market={"market_id": 145, "supported_size_decimals": 2, "supported_price_decimals": 2},
            max_order_wait_seconds=0.01,
        )

        async def read_account():
            observed = simulation.read_account()
            risk = {
                "risk_action": "block_entry" if response_mode == "risk_block" else "normal",
                "risk_reason": "test_account_risk", "risk_data_complete": True,
                "observed_at": observed["observed_at"],
            }
            production.latest_risk = risk
            return {**observed, "risk": risk}

        production._read_account = read_account
        executor.gateway = production
        runtime = object.__new__(wti_live.WTILiveRuntime)
        runtime.args = SimpleNamespace(
            entry_cutoff_seconds=1800, entry_threshold_bps=Decimal("30"),
            exit_target_bps=Decimal("1"), roundtrip_cost_bps=Decimal("10"),
            max_notional_usd=Decimal("25"), max_slippage_bps=Decimal("8"), max_hold_seconds=21600,
        )
        runtime.config = wti_live.build_replay_config(runtime.args)
        runtime.executor = executor
        runtime.gateway = production
        runtime.manage_existing_only = True
        runtime.pending_gap_exit = False
        first = await runtime._read_reconcile(sample)
        if response_mode in {"accepted", "unchanged_open_status"}:
            assert first["status"] == "open"
            evidence = first["execution_history"][-1]["legs"]["lighter"]["submission_result"]
            assert evidence["transaction_hash"] == "tx-accepted"
        else:
            assert first["status"] == "manual_review_required"
        second = await runtime._read_reconcile(sample)
        third = await runtime._read_reconcile(sample)
        if response_mode in {"accepted", "lost_ack", "unchanged_open_status"}:
            assert second["status"] == third["status"] == "open"
            assert not third["pending_action"]
            assert len(third["execution_history"][-1]["superseded_legs"]) == 1
            assert third["open_position"]["opened_at"] == third["execution_history"][-1]["intent_at"]
            for _ in range(2):
                continuing = await runtime._process_sample(sample)
                assert continuing["status"] == "open"
                assert runtime.last_action["action"] == "hold"
            assert len(sent) == 1
        elif response_mode == "absent":
            assert third["manual_review_reason"] == "pending_order_terminal_status_unconfirmed"
            result = third["pending_action"]["legs"]["lighter"]["result"]
            assert result["lookup_reason"] == "lighter_order_absent_from_active_and_inactive"
            assert third["pending_action"]["legs"]["lighter"]["submission_result"]["transaction_hash"] == "tx-accepted"
            assert len(sent) == 1
        else:
            assert third["manual_review_reason"] == "pending_open_lighter_no_fill_requires_operator", third
            assert not sent
        assert simulation.var_qty == Decimal("0.29")
        assert len(simulation.requests) == 2  # Only setup attempts; Var never resubmitted.

    asyncio.run(exercise())


@pytest.mark.parametrize("blocker", ["closed", "stale", "active_orders", "position_mismatch", "depth"])
def test_legacy_rejection_recovery_keeps_session_freshness_depth_and_position_gates(tmp_path, blocker):
    executor, gateway, sample = _legacy_invalid_lighter_pending(tmp_path)
    proof = WTIProductionGateway(
        loop=asyncio.new_event_loop(), variational=object(), account_reader=object(),
        lighter_client=object(), account_index=10, api_key_index=1, market={"market_id": 145},
    )

    def lookup(cid, request=None):
        return asyncio.run(proof._lookup_lighter(cid, request or {}))

    gateway.lookup_order = lookup
    session = _session_for_sample(sample)
    if blocker == "closed":
        session = _session(open_=False)
    elif blocker == "stale":
        sample["var_quote_age_seconds"] = "3"
    elif blocker == "active_orders":
        gateway.active_orders.append({"order_id": "still-active"})
    elif blocker == "position_mismatch":
        gateway.var_qty = Decimal("0.30")
    else:
        executor.market_reader = lambda: {**sample, "variational_qty": "0.28", "lighter_qty": "0.28"}
    try:
        try:
            recovered = executor.reconcile(sample=sample, session=session, now=datetime.now(timezone.utc))
        except WTIExecutionError:
            recovered = executor.store.load()
        assert recovered["status"] == "manual_review_required"
        assert recovered["pending_action"].get("superseded_legs") is None
        assert len(gateway.requests) == 2
    finally:
        proof.loop.close()


def test_recovery_does_not_restart_maximum_hold_clock(tmp_path):
    from tools.lib.wti_live_strategy import decide_wti_action
    from tools.lib.wti_replay import ReplayConfig

    executor, simulation, sample = _legacy_invalid_lighter_pending(tmp_path)
    state = executor.store.load()
    intended = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()
    state["pending_action"]["intent_at"] = intended
    leg = state["pending_action"]["legs"]["lighter"]
    # Exercise the ordinary never-submitted branch; both recovery branches share
    # the preserved intent clock, independently of whether a legacy index failed.
    leg["submission_started"] = False
    leg["result"] = None
    simulation.results.clear()
    executor.store.save(state)
    recovered = executor.reconcile(
        sample=sample, session=_session_for_sample(sample), now=datetime.now(timezone.utc),
    )
    assert recovered["status"] == "open"
    assert recovered["open_position"]["opened_at"] == intended
    decision = decide_wti_action(
        sample, recovered["open_position"],
        config=ReplayConfig(entry_threshold_bps=Decimal("30"), exit_target_bps=Decimal("1"),
                            roundtrip_cost_bps=Decimal("10"), max_hold_seconds=21600,
                            max_sample_gap_seconds=120),
        now=datetime.now(timezone.utc),
    )
    assert decision["action"] == "close"
    assert decision["reason"] == "max_hold"


def test_pending_open_refresh_uses_original_pair_quantities_and_submits_hedge_once(tmp_path):
    sample = _sample()
    gateway = SimulatedWTIGateway()
    reads = 0

    def fail_after_var_fill():
        nonlocal reads
        reads += 1
        if reads == 2:
            raise RuntimeError("simulated restart boundary after Var fill")
        return sample

    executor, _ = _executor(
        tmp_path, gateway, sample=sample, market_reader=fail_after_var_fill
    )
    interrupted = _open(executor, sample=sample)
    assert interrupted["status"] == "manual_review_required"
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == 0

    requested = []

    def exact_market_reader(var_qty, lighter_qty):
        requested.append((var_qty, lighter_qty))
        return _sample(
            variational_qty=str(var_qty), lighter_qty=str(lighter_qty)
        )

    restarted, _ = _executor(
        tmp_path, gateway, sample=sample,
        market_reader_for_qty=exact_market_reader,
    )
    recovered = restarted.reconcile(
        sample=sample, session=_session_for_sample(sample), now=datetime.now(timezone.utc),
    )
    again = restarted.reconcile(
        sample=sample, session=_session_for_sample(sample), now=datetime.now(timezone.utc),
    )

    assert requested == [(Decimal("0.29"), Decimal("0.29"))]
    assert recovered["status"] == again["status"] == "open"
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == Decimal("-0.29")
    assert [request["venue"] for request in gateway.requests] == ["variational", "lighter"]


def test_pending_open_records_specific_refresh_depth_failure_without_resubmitting(tmp_path):
    sample = _sample()
    gateway = SimulatedWTIGateway()
    reads = 0

    def fail_after_var_fill():
        nonlocal reads
        reads += 1
        if reads == 2:
            raise RuntimeError("simulated restart boundary after Var fill")
        return sample

    executor, _ = _executor(
        tmp_path, gateway, sample=sample, market_reader=fail_after_var_fill
    )
    assert _open(executor, sample=sample)["status"] == "manual_review_required"

    restarted, _ = _executor(
        tmp_path, gateway, sample=sample,
        market_reader_for_qty=lambda _var_qty, _lighter_qty: _sample(
            variational_qty="0.196", lighter_qty="0.196"
        ),
    )
    recovered = restarted.reconcile(
        sample=sample, session=_session_for_sample(sample), now=datetime.now(timezone.utc),
    )
    again = restarted.reconcile(
        sample=sample, session=_session_for_sample(sample), now=datetime.now(timezone.utc),
    )

    expected_reason = (
        "pending_open_missing_lighter_readonly_sample_depth_below_order_quantity"
    )
    assert recovered["manual_review_reason"] == expected_reason
    assert again["manual_review_reason"] == expected_reason
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == 0
    assert [request["venue"] for request in gateway.requests] == ["variational"]


def test_restart_uses_production_booked_order_lookup_and_hedges_only_once(tmp_path):
    sample = _sample()
    gateway = SimulatedWTIGateway(results={"variational": {
        "ok": False, "status": "unknown", "execute_status": "filled",
    }})
    executor, _ = _executor(tmp_path, gateway, sample=sample)
    interrupted = _open(executor, sample=sample)
    assert interrupted["manual_review_reason"] == "variational_open_unknown"
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == 0
    original_request = gateway.requests[0]

    class OrdersAPI:
        calls = 0

        async def request(self, payload):
            assert payload["type"] == "VAR_API_ORDERS"
            self.calls += 1
            row = {
                "instrument": {"underlying": "USOILP"},
                "side": original_request["side"],
                "qty": original_request["qty"],
                "price": "100.426",
                "status": "cleared",
                "clearing_status": "success_trades_booked_into_pool",
                "order_id": "booked-var-order",
                "rfq_id": "booked-var-rfq",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "execution_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            return {
                "type": "VAR_API_ORDERS_RESULT", "requestId": payload["requestId"],
                "ok": True, "result": {"ok": True, "httpStatus": 200, "orders": [row]},
            }

    api = OrdersAPI()
    production = WTIProductionGateway(
        loop=asyncio.new_event_loop(), variational=api, account_reader=object(),
        lighter_client=None, account_index=10, api_key_index=1, market={"market_id": 145},
    )
    original_lookup = gateway.lookup_order

    def lookup(client_order_id, request=None):
        if request["venue"] == "variational":
            return asyncio.run(production._lookup_variational(client_order_id, request))
        return original_lookup(client_order_id, request=request)

    gateway.lookup_order = lookup
    restarted, _ = _executor(tmp_path, gateway, sample=sample)
    try:
        recovered = restarted.reconcile(
            sample=sample, session=_session_for_sample(sample), now=datetime.now(timezone.utc),
        )
        again = restarted.reconcile(
            sample=sample, session=_session_for_sample(sample), now=datetime.now(timezone.utc),
        )
    finally:
        production.loop.close()

    assert recovered["status"] == again["status"] == "open"
    assert recovered["pending_action"] is None
    assert recovered["open_position"]["var_entry_price"] == "100.426"
    assert recovered["open_position"]["recovered_from_pending"] is True
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == Decimal("-0.29")
    assert [r["venue"] for r in gateway.requests] == ["variational", "lighter"]
    assert api.calls == 1
    var_evidence = recovered["execution_history"][-1]["legs"]["variational"]["result"]
    assert var_evidence["native_order_id"] == "booked-var-order"
    assert var_evidence["native_client_order_id"] == "booked-var-rfq"
    assert var_evidence["fee_confirmed"] is False


def test_restart_does_not_submit_missing_hedge_without_original_live_session(tmp_path):
    sample = _sample()
    gateway = SimulatedWTIGateway()
    reads = 0

    def fail_after_first_leg():
        nonlocal reads
        reads += 1
        if reads == 2:
            raise RuntimeError("simulated restart boundary after Var fill")
        return sample

    executor, _ = _executor(tmp_path, gateway, sample=sample, market_reader=fail_after_first_leg)
    assert _open(executor, sample=sample)["status"] == "manual_review_required"
    restarted, _ = _executor(tmp_path, gateway, sample=sample)

    recovered = restarted.reconcile(
        sample=sample,
        session=None,
        now=datetime.now(timezone.utc),
    )

    assert recovered["status"] == "manual_review_required"
    assert recovered["manual_review_reason"] == "pending_open_var_only_requires_position_management"
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == 0
    assert [request["venue"] for request in gateway.requests] == ["variational"]


def test_restart_does_not_retry_lighter_after_confirmed_no_fill(tmp_path):
    sample = _sample()
    gateway = SimulatedWTIGateway()
    reads = 0

    def fail_after_first_leg():
        nonlocal reads
        reads += 1
        if reads == 2:
            raise RuntimeError("simulated restart boundary after Var fill")
        return sample

    executor, _ = _executor(tmp_path, gateway, sample=sample, market_reader=fail_after_first_leg)
    assert _open(executor, sample=sample)["status"] == "manual_review_required"
    gateway.results = {
        "lighter": {
            "ok": True,
            "final": True,
            "order_id": "lighter-rejected",
            "status": "rejected",
            "filled_qty": "0",
            "average_price": "0",
        }
    }
    restarted, _ = _executor(tmp_path, gateway, sample=sample)

    first_reconcile = restarted.reconcile(
        sample=sample,
        session=_session_for_sample(sample),
        now=datetime.now(timezone.utc),
    )
    second_reconcile = restarted.reconcile(
        sample=sample,
        session=_session_for_sample(sample),
        now=datetime.now(timezone.utc),
    )

    assert first_reconcile["status"] == second_reconcile["status"] == "manual_review_required"
    assert second_reconcile["manual_review_reason"] == "pending_open_lighter_no_fill_requires_operator"
    assert gateway.var_qty == Decimal("0.29")
    assert gateway.lighter_qty == 0
    assert [request["venue"] for request in gateway.requests] == ["variational", "lighter"]


def test_unknown_second_leg_can_recover_only_after_terminal_order_lookup(tmp_path):
    unknown = {
        "ok": False,
        "error": "submit_timeout",
        "execute_status": "filled",
        "execute_filled_qty": "0.29",
    }
    confirmed = {
        "ok": True,
        "final": True,
        "order_id": "lighter-recovered",
        "status": "filled",
        "filled_qty": "0.29",
        "average_price": "70.04",
        "trade_ids": ["lighter-trade-recovered"],
        "fee_usd": "0.02",
    }
    gateway = SimulatedWTIGateway(
        results={"lighter": unknown}, lookup_results={"lighter": confirmed}
    )
    executor, _ = _executor(tmp_path, gateway)
    pending = _open(executor)
    assert pending["status"] == "manual_review_required"
    assert gateway.lighter_qty == Decimal("-0.29")
    recovered = executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "open"
    assert recovered["open_position"]["lighter_entry_price"] == "70.04"
    lighter_evidence = recovered["execution_history"][-1]["legs"]["lighter"]["result"]
    assert lighter_evidence["order_id"] == "lighter-recovered"
    assert lighter_evidence["trade_ids"] == ["lighter-trade-recovered"]
    assert lighter_evidence["fee_usd"] == "0.02"
    assert len(gateway.requests) == 2
    assert all(not request.get("reduce_only") for request in gateway.requests)


def test_order_lookup_must_match_the_persisted_client_order_id(tmp_path):
    unknown = {
        "ok": False,
        "error": "submit_timeout",
        "execute_status": "filled",
        "execute_filled_qty": "0.29",
    }
    mismatched = {
        "ok": True,
        "final": True,
        "client_order_id": "some-other-order",
        "order_id": "order-foreign",
        "status": "filled",
        "filled_qty": "0.29",
        "average_price": "70.05",
    }
    gateway = SimulatedWTIGateway(
        results={"variational": unknown}, lookup_results={"variational": mismatched}
    )
    executor, _ = _executor(tmp_path, gateway)
    assert _open(executor)["status"] == "manual_review_required"
    recovered = executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "manual_review_required"
    assert recovered["manual_review_reason"] == "pending_order_terminal_status_unconfirmed"
    assert len(gateway.requests) == 1


def test_missing_fee_is_preserved_as_unknown_not_zero(tmp_path):
    no_fee = {
        "ok": True,
        "final": True,
        "order_id": "var-no-fee",
        "status": "filled",
        "filled_qty": "0.29",
        "average_price": "70.05",
        "trade_ids": ["var-no-fee-trade"],
    }
    gateway = SimulatedWTIGateway(results={"variational": no_fee})
    executor, _ = _executor(tmp_path, gateway)
    opened = _open(executor)
    evidence = opened["execution_history"][0]["legs"]["variational"]["result"]
    assert evidence["fee_usd"] is None
    assert evidence["fee_confirmed"] is False


def test_stale_pre_entry_flat_snapshot_cannot_confirm_post_close_flat(tmp_path):
    executor, gateway = _executor(tmp_path)
    stale_flat = gateway.read_account()
    assert _open(executor)["status"] == "open"
    original_read = gateway.read_account
    read_count = 0

    def stale_after_close():
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            return original_read()
        return stale_flat

    gateway.read_account = stale_after_close
    close_state = executor.close(
        sample=_sample(), session=_session(), now=datetime.now(timezone.utc)
    )
    assert close_state["status"] == "manual_review_required"
    assert gateway.var_qty == gateway.lighter_qty == 0
    recovered = executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "manual_review_required"
    assert recovered["pending_action"] is not None
    assert recovered["manual_review_reason"] == "post_order_account_snapshot_unavailable"
    assert len(gateway.requests) == 4


def test_confirmed_zero_fill_can_recover_flat_but_unknown_cannot(tmp_path):
    no_fill = {
        "ok": True, "final": True, "order_id": "var-reject",
        "status": "rejected", "filled_qty": "0",
    }
    gateway = SimulatedWTIGateway(results={"variational": no_fill})
    executor, _ = _executor(tmp_path, gateway)
    assert _open(executor)["status"] == "manual_review_required"
    assert executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))["status"] == "flat"
    assert len(gateway.requests) == 1

    gateway = SimulatedWTIGateway(results={"variational": {"ok": False}})
    executor, _ = _executor(tmp_path / "unknown", gateway)
    assert _open(executor)["status"] == "manual_review_required"
    assert executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))["status"] == "manual_review_required"


def test_terminal_lookup_can_confirm_no_order_and_restore_flat(tmp_path):
    gateway = SimulatedWTIGateway(
        results={"variational": {"ok": False, "error": "submit_timeout"}},
        lookup_results={
            "variational": {
                "ok": True,
                "final": True,
                "status": "not_found",
                "filled_qty": "0",
                "no_order_confirmed": True,
            }
        },
    )
    executor, _ = _executor(tmp_path, gateway)
    assert _open(executor)["status"] == "manual_review_required"
    recovered = executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "flat"
    assert recovered["pending_action"] is None
    assert recovered["execution_history"][-1]["outcome"] == "no_fill"
    assert len(gateway.requests) == 1


def test_close_partial_fill_blocks_rotation_and_restart_does_not_resubmit(tmp_path):
    gateway = SimulatedWTIGateway()
    executor, _ = _executor(tmp_path, gateway)
    assert _open(executor)["status"] == "open"
    partial = {
        "ok": True, "final": True, "order_id": "var-close-partial",
        "status": "partially_filled", "filled_qty": "0.1",
    }
    gateway.results = {"variational": partial}
    state = executor.close(sample=_sample(), session=_session(), now=datetime.now(timezone.utc))
    assert state["status"] == "manual_review_required"
    assert state["pending_action"]["kind"] == "close"
    assert gateway.var_qty == Decimal("0.19")
    assert gateway.lighter_qty == Decimal("-0.29")
    assert len(gateway.requests) == 3
    recovered = executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "manual_review_required"
    assert len(gateway.requests) == 3
    with pytest.raises(WTIExecutionError, match="exchange_session_does_not_allow_position_management"):
        executor.close(sample=_sample(), session=_session(open_=False), now=datetime.now(timezone.utc))
    assert len(gateway.requests) == 3


def test_close_no_fill_reconciles_back_to_open_only_when_both_positions_match(tmp_path):
    gateway = SimulatedWTIGateway()
    executor, _ = _executor(tmp_path, gateway)
    assert _open(executor)["status"] == "open"
    no_fill = {
        "ok": True, "final": True, "order_id": "var-close-reject",
        "status": "canceled", "filled_qty": "0",
    }
    gateway.results = {"variational": no_fill}
    close_state = executor.close(
        sample=_sample(), session=_session(), now=datetime.now(timezone.utc)
    )
    assert close_state["status"] == "manual_review_required"
    recovered = executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))
    assert recovered["status"] == "open"
    assert recovered["last_action_resolution"] == "close_confirmed_no_fill"
    assert len(gateway.requests) == 3


def test_close_depth_below_position_is_rejected_before_submission(tmp_path):
    executor, gateway = _executor(tmp_path)
    assert _open(executor)["status"] == "open"
    with pytest.raises(WTIExecutionError, match="readonly_sample_depth_below_close_quantity"):
        executor.close(
            sample=_sample(variational_qty="0.1", lighter_qty="0.1"),
            session=_session(),
            now=datetime.now(timezone.utc),
        )
    assert len(gateway.requests) == 2


def test_corrupt_state_is_not_overwritten(tmp_path):
    state_path = tmp_path / "wti-live-state.json"
    state_path.write_text("{broken", encoding="utf-8")
    executor, gateway = _executor(tmp_path)
    with pytest.raises(Exception, match="unreadable"):
        _open(executor)
    assert state_path.read_text(encoding="utf-8") == "{broken"
    assert gateway.requests == []


def test_open_state_reconciliation_requires_market_identity_and_no_open_orders(tmp_path):
    executor, gateway = _executor(tmp_path)
    assert _open(executor)["status"] == "open"
    with pytest.raises(WTIExecutionError, match="lighter_market_identity_or_precision_invalid"):
        executor.reconcile(sample=_sample(lighter_market_id=146), now=datetime.now(timezone.utc))
    assert len(gateway.requests) == 2
    assert executor.reconcile(sample=_sample(), now=datetime.now(timezone.utc))["status"] == "open"


def test_lighter_position_mismatch_after_order_responses_requires_manual_review(tmp_path):
    gateway = SimulatedWTIGateway()
    executor, _ = _executor(tmp_path, gateway)
    # Simulate a venue reporting a different final quantity than the order ack.
    original_read = gateway.read_account
    reads = 0

    def read_with_mismatch():
        nonlocal reads
        reads += 1
        result = original_read()
        if reads == 2:
            result["lighter_positions"]["accounts"][0]["positions"][0]["position"] = "0.2"
        return result

    gateway.read_account = read_with_mismatch
    state = _open(executor)
    assert state["status"] == "manual_review_required"
    assert state["manual_review_reason"] == "post_open_exchange_position_mismatch"
    assert state["pending_action"] is not None


def test_terminal_fill_response_must_include_quantity_order_id_and_final_flag(tmp_path):
    malformed = {"ok": True, "status": "filled", "filled_qty": "0.29", "average_price": "70"}
    gateway = SimulatedWTIGateway(results={"variational": malformed})
    executor, _ = _executor(tmp_path, gateway)
    state = _open(executor)
    assert state["status"] == "manual_review_required"
    assert state["pending_action"]["legs"]["variational"]["result"]["classification"] == "unknown"
    assert len(gateway.requests) == 1
