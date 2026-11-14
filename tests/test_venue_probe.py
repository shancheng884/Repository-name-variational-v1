from datetime import datetime, timezone
from decimal import Decimal as D
from pathlib import Path
import asyncio, json

from tools.market_capture.config import DEFAULT_ASSETS
from tools.lib.probe_metrics import calibrate, decompose_round
from tools.lib.venue_probe import ProbeRound, ProbeRunner, ProbeState, enumerate_probe_plan


class FakeHub:
    def __init__(self):
        qa={"mid": "100.1", "bid": "100.0", "ask": "100.2", "recv_at": datetime.now(timezone.utc).isoformat(), "quote_id": "qa", "bbo_source_at": "2026-11-13T10:00:00Z"}
        qb={"mid": "100.3", "bid": "100.2", "ask": "100.4", "recv_at": datetime.now(timezone.utc).isoformat(), "quote_id": "qb", "bbo_source_at": "2026-11-13T10:00:00Z"}
        self.quotes = {"a": qa, "b": qb, "variational": qa, "lighter_mainnet": qb}
    def latest(self, venue, symbol, max_age=10):
        return dict(self.quotes[venue])


class FakeExec:
    available = True
    def __init__(self, *, status="filled"):
        self.status = status
        self.calls = []
    async def preflight(self, asset):
        return {"ok": True, "qty": "0"}
    def normalize_qty(self, asset, qty):
        return qty.quantize(D("0.01"))
    async def market_order(self, *, asset, side, qty, quote, client_order_id, reduce_only=False, max_slippage_bps=D("8")):
        self.calls.append((client_order_id, side, reduce_only, qty))
        if self.status == "unknown":
            return {"ok": False, "status": "unknown", "client_order_id": client_order_id}
        return {"ok": True, "status": "filled", "client_order_id": client_order_id,
                "filled_qty": str(qty), "avg_price": "100.3" if side == "buy" else "100.1",
                "source_at_epoch": datetime.now(timezone.utc).timestamp()}


def test_plan_covers_five_assets_and_all_available_venue_pairs(tmp_path):
    plan = enumerate_probe_plan(DEFAULT_ASSETS, ("variational", "lighter_mainnet", "lighter_rh", "entropy"))
    assert len(plan) == 54
    assert len({r.round_id for r in plan}) == 54
    assert any(r.asset == "QQQ" for r in plan)
    assert all(r.buy_venue != r.sell_venue for r in plan)


def test_dry_run_writes_plan_without_orders(tmp_path):
    state = ProbeState(tmp_path / "state.json")
    runner = ProbeRunner(hub=FakeHub(), executors={"variational": FakeExec(), "lighter_mainnet": FakeExec()},
                         state=state, writer_path=tmp_path / "rounds.jsonl",
                         assets=DEFAULT_ASSETS[:1], venues=("variational", "lighter_mainnet"))
    result = asyncio.run(runner.run(allow_live=False))
    assert result["mode"] == "dry_run" and result["rounds"] == 2
    assert state.data["orders_attempted"] is False
    assert (tmp_path / "rounds.jsonl").exists()


def test_probe_round_uses_one_entry_and_one_exit_per_leg(tmp_path):
    a, b = FakeExec(), FakeExec()
    runner = ProbeRunner(hub=FakeHub(), executors={"a": a, "b": b}, state=ProbeState(tmp_path / "s.json"),
                         writer_path=tmp_path / "r.jsonl", assets=DEFAULT_ASSETS[:1],
                         venues=("a", "b"), horizons=(0.0,))
    item = ProbeRound("r1", "NVDA", "a", "b", 1)
    row = asyncio.run(runner._run_one(item))
    assert row["status"] == "filled"
    assert len(a.calls) + len(b.calls) == 4
    assert len({call[0] for call in a.calls + b.calls}) == 4
    assert sorted(call[2] for call in a.calls + b.calls) == [False, False, True, True]


def test_unknown_entry_blocks_exit_and_requires_review(tmp_path):
    a, b = FakeExec(), FakeExec(status="unknown")
    runner = ProbeRunner(hub=FakeHub(), executors={"a": a, "b": b}, state=ProbeState(tmp_path / "s.json"),
                         writer_path=tmp_path / "r.jsonl", assets=DEFAULT_ASSETS[:1],
                         venues=("a", "b"), horizons=(0.0,))
    item = ProbeRound("r1", "NVDA", "a", "b", 1)
    row = asyncio.run(runner._run_one(item))
    assert row["status"] == "manual_review_required"
    assert len(a.calls) + len(b.calls) == 2


def test_preflight_rejects_insufficient_margin(tmp_path):
    class MarginExec(FakeExec):
        def __init__(self, margin):
            super().__init__(); self.margin=margin
        async def preflight(self, asset):
            return {"ok": True, "qty": "0", "active_orders": 0, "available_margin": self.margin}
    runner = ProbeRunner(hub=FakeHub(), executors={"variational": MarginExec("0.0"), "lighter_mainnet": MarginExec("100")},
                         state=ProbeState(tmp_path / "s.json"), writer_path=tmp_path / "r.jsonl",
                         assets=DEFAULT_ASSETS[:1], venues=("variational", "lighter_mainnet"))
    out = asyncio.run(runner.preflight_all())
    assert out["variational:NVDA"]["ok"] is False
    assert out["variational:NVDA"]["error"] == "variational_insufficient_margin"
    assert out["lighter_mainnet:NVDA"]["ok"] is True
