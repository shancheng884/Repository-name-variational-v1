from __future__ import annotations
import asyncio
from tools.market_capture.config import AVAILABLE_ASSETS
from tools.lib.venue_probe import ProbeRunner, ProbeState


class OkExec:
    venue = "entropy"
    available = True

    async def preflight(self, asset):
        return {"ok": True, "qty": "0", "active_orders": 0}


class SlowHub:
    """A venue that only publishes its first quote a moment after startup."""

    def __init__(self, quiet_calls=2):
        self.calls = 0
        self.quiet_calls = quiet_calls

    def latest(self, venue, symbol, *, max_age=10.0):
        self.calls += 1
        if self.calls <= self.quiet_calls:
            return None
        return {"bid": "100", "ask": "100.5", "mid": "100.25", "quote_kind": "bbo",
                "age_s": 0.2, "quote_id": "q1"}


def build(tmp_path, quiet_calls=2):
    asset = next(a for a in AVAILABLE_ASSETS if a.name == "NVDA")
    hub = SlowHub(quiet_calls)
    runner = ProbeRunner(
        hub=hub, executors={"entropy": OkExec()}, state=ProbeState(tmp_path / "state.json"),
        writer_path=tmp_path / "rounds.jsonl", assets=(asset,), venues=("entropy",),
        market_quote_wait_seconds=1.0,
    )
    return runner, hub


def test_preflight_waits_for_a_slow_venue_quote(tmp_path):
    runner, hub = build(tmp_path)
    result = asyncio.run(runner.preflight_all())
    key = "entropy:NVDA:market"
    assert result[key]["ok"] is True
    assert hub.calls >= 3


def test_preflight_still_fails_when_the_venue_never_quotes(tmp_path):
    runner, hub = build(tmp_path, quiet_calls=10_000)
    result = asyncio.run(runner.preflight_all())
    key = "entropy:NVDA:market"
    assert result[key]["ok"] is False
    assert result[key]["error"] == "quote_not_ready"
    assert hub.calls >= 2
