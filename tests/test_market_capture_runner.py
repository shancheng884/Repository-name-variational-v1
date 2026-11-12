import asyncio
import json

from tools.market_capture.config import CaptureConfig
from tools.market_capture.runner import run_capture


def test_runner_writes_stopped_health_without_orders(tmp_path):
    config = CaptureConfig(
        output_dir=tmp_path,
        venue_enabled={"variational": False, "lighter_mainnet": False, "lighter_rh": False, "entropy": False},
        duration_seconds=0.6,
        health_seconds=1.0,
        system_seconds=10.0,
    )
    assert asyncio.run(run_capture(config)) == 0
    health = json.loads((tmp_path / "health.json").read_text())
    assert health["status"] == "stopped"
    assert health["orders_attempted"] is False
    assert health["execution_enabled"] is False
    assert health["order_submission_enabled"] is False
