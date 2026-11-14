from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import asyncio, json, os

from tools.market_capture.config import DEFAULT_ASSETS
from tools.lib.probe_metrics import calibrate, decompose_round, leg_timing
from tools.lib.venue_probe import ProbeLock, ProbeRound, ProbeRunner, ProbeState, _parse_epoch_ms


class Hub:
    def __init__(self):
        now = datetime.now(timezone.utc).isoformat()
        self.quotes = {
            "variational": {"mid": "100.1", "bid": "100.0", "ask": "100.2", "recv_at": now, "quote_id": "qa"},
            "lighter_mainnet": {"mid": "100.3", "bid": "100.2", "ask": "100.4", "recv_at": now, "quote_id": "qb"},
        }

    def latest(self, venue, symbol, max_age=10):
        return dict(self.quotes[venue])


class Exec:
    """Fake venue that can be configured to leave a residual position."""

    available = True

    def __init__(self, *, residual="0", fail_positions=False):
        self.residual = residual
        self.qty = "0"
        self.fail_positions = fail_positions
        self.calls = []
        self.position_checks = 0

    async def positions(self, asset):
        self.position_checks += 1
        if self.fail_positions:
            return {"ok": False, "verified": False, "error": "venue_unreachable"}
        return {"ok": True, "verified": True, "qty": self.qty, "active_orders": 0}

    async def preflight(self, asset):
        return {"ok": True, "qty": "0", "active_orders": 0}

    def normalize_qty(self, asset, qty):
        return qty.quantize(D("0.01"))

    async def market_order(self, *, asset, side, qty, quote, client_order_id, reduce_only=False, max_slippage_bps=D("8")):
        self.calls.append((client_order_id, side, reduce_only))
        # The exit legs are the reduce-only ones; flipping the residual there
        # simulates a venue that reports a leftover position after the round.
        if reduce_only:
            # Closing leg leaves whatever the venue reports afterwards; a
            # non-zero residual models an exit that did not actually flatten.
            self.qty = self.residual
            return {"ok": True, "status": "filled", "client_order_id": client_order_id,
                    "filled_qty": str(qty), "avg_price": "100.1",
                    "source_at_epoch": datetime.now(timezone.utc).timestamp()}
        return {"ok": True, "status": "filled", "client_order_id": client_order_id,
                "filled_qty": str(qty), "avg_price": "100.3" if side == "buy" else "100.1",
                "source_at_epoch": datetime.now(timezone.utc).timestamp()}


def runner(tmp_path, *, execs=None, run_id=None, rooms=("variational", "lighter_mainnet")):
    execs = execs or {v: Exec() for v in rooms}
    state = ProbeState(tmp_path / "state.json")
    return ProbeRunner(hub=Hub(), executors=execs, state=state, writer_path=tmp_path / "rounds.jsonl",
                       assets=DEFAULT_ASSETS[:1], venues=rooms, horizons=(0.0,), run_id=run_id), execs, state


def test_round_checks_positions_before_and_after_every_round(tmp_path):
    r, execs, _ = runner(tmp_path, run_id="run-one")
    item = ProbeRound("r1", "NVDA", "variational", "lighter_mainnet", 1)
    row = asyncio.run(r._run_one(item))
    assert row["status"] == "filled"
    assert row["reconcile"]["pre"]["flat"] is True
    assert row["reconcile"]["post"]["flat"] is True
    assert row["reconcile"]["post"]["venues"]["variational"]["qty"] == "0"
    assert all(ex.position_checks == 2 for ex in execs.values())


def test_residual_position_after_round_blocks_the_rest_of_the_plan(tmp_path):
    execs = {"variational": Exec(), "lighter_mainnet": Exec(residual="0.01")}
    r, execs, state = runner(tmp_path, execs=execs, run_id="run-one")
    result = asyncio.run(r.run(allow_live=True))
    assert result["ok"] is False and result["state"] == "manual_review_required"
    assert result["rounds"] == 0, "a round that ends unresolved must not be counted as done"
    current = state.data["current"]
    assert current["round_id"] == "nvda-variational-lighter_mainnet-001", "the run must stop on the first round"
    assert current["reason"] == "post_round_position_not_flat"
    assert current["reconcile"]["post"]["venues"]["lighter_mainnet"]["flat"] is False
    assert len(execs["variational"].calls) + len(execs["lighter_mainnet"].calls) == 4


def test_unverified_position_snapshot_blocks_the_round_before_ordering(tmp_path):
    execs = {"variational": Exec(), "lighter_mainnet": Exec(fail_positions=True)}
    r, execs, _ = runner(tmp_path, execs=execs, run_id="run-one")
    row = asyncio.run(r._run_one(ProbeRound("r1", "NVDA", "variational", "lighter_mainnet", 1)))
    assert row["status"] == "manual_review_required"
    assert row["reason"] == "pre_round_position_not_flat"
    assert row["reconcile"]["pre"]["venues"]["lighter_mainnet"]["verified"] is False
    assert execs["variational"].calls == [] and execs["lighter_mainnet"].calls == [], "no order before a verified flat snapshot"


def test_second_process_cannot_start_while_the_lock_is_held(tmp_path):
    r, execs, state = runner(tmp_path, run_id="run-two")
    lock = ProbeLock(tmp_path / "run.lock")
    acquired, owner = lock.acquire("run-one")
    assert acquired is True
    try:
        result = asyncio.run(r.run(allow_live=True))
    finally:
        lock.release()
    assert result["ok"] is False
    assert result["error"] == "probe_already_running"
    assert result["owner"]["pid"] == os.getpid()
    assert execs["variational"].calls == [] and execs["lighter_mainnet"].calls == []
    assert state.data["blocked_reason"] == "probe_already_running"


def test_stale_lock_from_a_dead_process_is_replaced(tmp_path):
    (tmp_path / "state.json").write_text("{}")
    stale = {"pid": 999999999, "run_id": "dead-run", "acquired_at": "2026-11-01T00:00:00+00:00"}
    (tmp_path / "run.lock").write_text(json.dumps(stale))
    r, execs, _ = runner(tmp_path, run_id="run-two")
    result = asyncio.run(r.run(allow_live=True))
    assert result["state"] == "completed"
    assert len(execs["variational"].calls) == 4
    assert not (tmp_path / "run.lock").exists(), "the lock must be released after the run"


def test_epoch_parser_handles_seconds_milliseconds_and_microseconds():
    seconds = _parse_epoch_ms(1794641563)
    millis = _parse_epoch_ms(1794641563000)
    micros = _parse_epoch_ms(1794641563000000)
    micros_tx = _parse_epoch_ms(1794641559469291)
    assert seconds.isoformat() == "2026-11-14T07:32:43+00:00"
    assert millis == seconds == micros
    assert micros_tx == datetime(2026, 11, 14, 7, 32, 39, 469291, tzinfo=timezone.utc)
    assert _parse_epoch_ms("not-a-number") is None


def _leg(side, fill, *, exec_quote=None, submitted="2026-11-14T07:31:44.544000+00:00",
         completed="2026-11-14T07:31:45.000000+00:00", fill_at="2026-11-14T07:31:44.515000+00:00",
         fee=None, qty="0.123"):
    return {"status": "filled", "side": side, "filled_qty": qty, "avg_price": fill,
            "submitted_at": submitted, "completed_at": completed, "source_at": fill_at,
            "execution_quote": exec_quote, "fee_usd": fee}


def _round(*, lighter_exec_quote=None, lighter_fee=None):
    signal_a = {"bid": "100.0", "ask": "100.2", "mid": "100.1"}
    signal_b = {"bid": "100.2", "ask": "100.4", "mid": "100.3"}
    return {
        "event": "probe_round",
        "round_id": "r1", "run_id": "probe-1", "asset": "NVDA", "buy_venue": "variational",
        "sell_venue": "lighter_mainnet", "status": "filled",
        "decision": {"quotes": {"variational": signal_a, "lighter_mainnet": signal_b}, "signal_edge_bps": "5"},
        "entry": {"legs": {
            "variational": _leg("buy", "100.17",
                                exec_quote={"bid": "100.19", "ask": "100.21", "mid": "100.20",
                                            "quote_timestamp": "2026-11-14T07:31:44.500000+00:00",
                                            "received_at": "2026-11-14T07:31:44.560000+00:00"}),
            "lighter_mainnet": _leg("sell", "100.31", exec_quote=lighter_exec_quote, fee=lighter_fee,
                                    fill_at="2026-11-14T07:31:44.700000+00:00"),
        }, "quotes": {"variational": signal_a, "lighter_mainnet": signal_b}},
        "exit": {"legs": {
            "variational": _leg("sell", "100.22"),
            "lighter_mainnet": _leg("buy", "100.38"),
        }, "quotes_at_close": {"variational": signal_a, "lighter_mainnet": signal_b}},
    }


def test_lighter_execution_quote_splits_signal_move_from_fill_slippage():
    exec_quote = {"bid": "100.28", "ask": "100.30", "mid": "100.29", "limit_price": "100.38",
                  "quote_source_at": "2026-11-14T07:31:44.520000+00:00",
                  "quote_recv_at": "2026-11-14T07:31:44.640000+00:00"}
    summary = decompose_round(_round(lighter_exec_quote=exec_quote))
    leg = summary["components"]["entry:lighter_mainnet"]
    assert leg["stage_split"] == "three_stage"
    total = D(leg["half_spread_bps"]) + D(leg["signal_to_exec_bps"]) + D(leg["exec_to_fill_bps"])
    assert D(leg["price_cost_bps"]) == total, "stages must sum to the leg cost without double counting"
    assert D(leg["signal_to_exec_bps"]) != 0, "the signal quote drift must be visible as its own stage"


def test_legacy_round_without_execution_quote_stays_two_stage():
    summary = decompose_round(_round())
    leg = summary["components"]["entry:lighter_mainnet"]
    assert leg["stage_split"] == "two_stage"
    assert leg["signal_to_exec_bps"] is None
    assert D(leg["price_cost_bps"]) == D(leg["half_spread_bps"]) + D(leg["slippage_vs_quote_bps"])


def test_zero_fee_venues_are_marked_by_policy_and_feed_net_cost():
    summary = decompose_round(_round())
    assert summary["fees_status"] == "zero_by_policy"
    assert summary["fee_bps"] == "0"
    assert D(summary["net_cost_bps"]) == D(summary["total_price_cost_bps"])


def test_entropy_rebate_fee_reduces_net_cost():
    summary = decompose_round(_round(lighter_fee="-0.05"))
    assert summary["fees_status"] == "verified"
    buy_leg = summary["components"]["entry:lighter_mainnet"]
    assert summary["fee_bps"] != "0"
    assert D(summary["net_cost_bps"]) < D(summary["total_price_cost_bps"])
    assert buy_leg["stage_split"] == "two_stage"


def test_hedge_delay_uses_clock_aligned_exchange_fills():
    exec_quote = {"bid": "100.28", "ask": "100.30", "mid": "100.29",
                  "quote_source_at": "2026-11-14T07:31:44.520000+00:00",
                  "quote_recv_at": "2026-11-14T07:31:44.640000+00:00"}
    summary = decompose_round(_round(lighter_exec_quote=exec_quote))
    assert summary["hedge_delay_source"] == "exchange_aligned"
    entry = summary["timing"]["entry"]
    assert all((entry[v] or {}).get("clock_offset_ms") is not None for v in ("variational", "lighter_mainnet"))
    assert entry["lighter_mainnet"]["exchange_fill_to_local_ms"] is not None
    assert entry["variational"]["timestamp_anomaly"] is False


def test_fill_before_submit_is_flagged_when_no_clock_offset_is_available():
    leg = _leg("buy", "100.1", fill_at="2026-11-14T07:31:44.400000+00:00",
               submitted="2026-11-14T07:31:44.544000+00:00")
    timing = leg_timing(leg)
    assert timing["timestamp_anomaly"] is True
    assert timing["clock_offset_ms"] is None


def test_calibrate_reports_net_cost_and_delay_sources():
    out = calibrate([_round()])
    pair = out["pairs"]["NVDA:variational->lighter_mainnet"]
    assert pair["p50_net_cost_bps"] == pair["p50_total_cost_bps"]
    assert out["hedge_delay_ms"]["sources"] == {"exchange_raw": 1}
    assert out["fee_status_counts"] == {"zero_by_policy": 1}


def test_split_runs_keeps_legacy_rounds_separate():
    from tools.lib.probe_metrics import split_runs
    rows = [{"run_id": "run-a"}, {"run_id": None}, {"run_id": "run-b"}, {"run_id": "run-a"}]
    grouped = split_runs(rows)
    assert list(grouped) == ["run-a", "legacy", "run-b"]
    assert len(grouped["run-a"]) == 2 and len(grouped["legacy"]) == 1


def test_probe_report_by_run_writes_per_run_summaries(tmp_path, monkeypatch):
    import sys
    import tools.probe_report as report

    rows = [dict(_round(), run_id="probe-A"), dict(_round(), run_id="probe-B")]
    source = tmp_path / "rounds.jsonl"
    source.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    output = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", ["probe_report", "--input", str(source), "--output", str(output), "--by-run"])
    assert report.main() == 0
    payload = json.loads(output.read_text())
    assert set(payload["runs"]) == {"probe-A", "probe-B"}
    assert payload["runs"]["probe-A"]["rounds"] == 1
    assert payload["runs"]["probe-A"]["pairs"]["NVDA:variational->lighter_mainnet"]["samples"] == 1
