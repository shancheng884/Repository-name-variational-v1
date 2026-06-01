import argparse
import json
from decimal import Decimal

from main import VariationalToLighterRuntime


def _runtime(tmp_path) -> VariationalToLighterRuntime:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.mode = "live"
    runtime.auto_live_entry = True
    runtime.auto_live_i_confirm_flat_start = True
    runtime.auto_live_reset_state_after_manual_flat = False
    runtime.auto_live_entry_max_precheck_edge_bps = Decimal("0")
    runtime.auto_live_state_file = tmp_path / "auto_live_state.json"
    runtime.args = argparse.Namespace(lang="en")
    runtime.risk_guard_max_base_amount = 1000
    runtime.risk_guard_max_price_deviation_bps = 100
    runtime.live_max_notional_usd = 25
    runtime.live_max_qty = 0
    runtime.live_require_min_edge_bps = 0
    runtime.live_cooldown_seconds = 0
    runtime.live_submit_timeout_seconds = 30
    runtime.live_allowed_assets = {"BTC"}
    runtime.live_allowed_sides = {"buy", "sell"}
    runtime.paper_notional_usd = Decimal("15")
    runtime.paper_entry_deviation_bps = Decimal("3")
    runtime.paper_exit_deviation_bps = Decimal("0.5")
    runtime.paper_max_var_half_spread_bps = Decimal("2")
    runtime.paper_max_holding_seconds = 1800.0
    runtime.paper_cooldown_seconds = 10.0
    runtime.paper_min_samples = 10
    runtime.paper_interval_seconds = 1.0
    runtime.paper_fee_bps_per_leg = Decimal("0.5")
    runtime.paper_latency_drift_bps = Decimal("0.5")
    return runtime


def test_auto_live_state_open_blocks_startup(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "1")
    monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "1")
    monkeypatch.setenv("LIGHTER_PRIVATE_KEY", "secret")
    runtime = _runtime(tmp_path)
    runtime.write_auto_live_state(
        {
            "status": "open",
            "asset": "BTC",
            "cycle_id": 1,
            "direction": "long_var_short_lighter",
            "qty": "0.00022",
        }
    )

    diagnostics = runtime.run_startup_diagnostics()

    assert any(error.startswith("auto_live_state_not_flat:") for error in diagnostics.blocking_errors)


def test_auto_live_state_reset_allows_startup_after_manual_flat(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "1")
    monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "1")
    monkeypatch.setenv("LIGHTER_PRIVATE_KEY", "secret")
    runtime = _runtime(tmp_path)
    runtime.auto_live_reset_state_after_manual_flat = True
    runtime.write_auto_live_state({"status": "manual_review_required", "asset": "BTC", "qty": "0.00022"})

    diagnostics = runtime.run_startup_diagnostics()
    state = json.loads(runtime.auto_live_state_file.read_text(encoding="utf-8"))

    assert diagnostics.blocking_errors == []
    assert "auto_live_state_reset_after_manual_flat" in diagnostics.passed
    assert state["status"] == "flat"
