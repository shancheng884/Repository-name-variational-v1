import json

from main import parse_args
from tools import live
from tools.live import LiveConfig, build_main_command, validate_state


def test_live_tool_adds_basis_addon_flags_for_three_lots() -> None:
    command = build_main_command(
        "SOL",
        LiveConfig(max_total_lots=3, addon_min_basis_improvement_bps="2.0"),
    )

    assert "--live-inventory-i-accept-basis-addon-diagnostic" in command
    assert "--live-inventory-basis-dynamic-entry-threshold" in command
    assert "--live-inventory-basis-addon-min-basis-improvement-bps" in command
    assert command[command.index("--live-inventory-max-total-lots") + 1] == "3"
    assert command[command.index("--live-inventory-max-total-notional-usd") + 1] == "60"
    assert command[command.index("--live-inventory-basis-min-entry-edge-bps") + 1] == "7"
    assert command[command.index("--live-inventory-basis-min-abs-entry-bps") + 1] == "7"
    assert command[command.index("--live-inventory-basis-dynamic-entry-noise-buffer-bps") + 1] == "2.0"
    assert command[command.index("--live-inventory-snapshot-timeout-seconds") + 1] == "10"
    assert command[command.index("--live-inventory-basis-addon-min-basis-improvement-bps") + 1] == "2.0"
    assert "--live-inventory-basis-reversion-signal-exit-min-pnl-bps" not in command


def test_live_tool_omits_basis_addon_flags_for_single_lot() -> None:
    command = build_main_command("SOL", LiveConfig(max_total_lots=1))

    assert "--live-inventory-i-accept-basis-addon-diagnostic" not in command
    assert "--live-inventory-basis-addon-min-basis-improvement-bps" not in command


def test_live_tool_can_disable_dynamic_entry_threshold() -> None:
    command = build_main_command("SOL", LiveConfig(max_total_lots=1, dynamic_entry_threshold=False))

    assert "--live-inventory-basis-dynamic-entry-threshold" not in command


def test_live_tool_reversion_forces_one_small_lot_and_omits_normalized_primary() -> None:
    command = build_main_command(
        "SOL",
        LiveConfig(
            reversion_mode=True,
            max_cycles=3,
            max_lots=3,
            max_total_lots=3,
            max_total_inventory_notional_usd="60",
            reversion_min_deviation_bps="1.2",
            reversion_long_execution_reserve_bps="4.5",
            reversion_short_execution_reserve_bps="3.5",
            reversion_min_net_expected_pnl_bps="1.5",
            reversion_signal_exit_min_pnl_bps="-0.8",
            reversion_min_normalized_edge_bps="2.5",
            reversion_max_stablecoin_edge_share="0.6",
        ),
    )

    assert "--live-inventory-basis-reversion" in command
    assert command[command.index("--live-inventory-max-total-notional-usd") + 1] == "25"
    assert command[command.index("--live-inventory-max-cycles") + 1] == "1"
    assert command[command.index("--live-inventory-max-lots") + 1] == "1"
    assert command[command.index("--live-inventory-max-total-lots") + 1] == "1"
    assert "--live-inventory-basis-use-normalized-edge-for-entry" not in command
    assert "--live-inventory-i-accept-basis-addon-diagnostic" not in command
    assert "--live-inventory-basis-refresh-exit-quote-before-submit" in command
    assert command[command.index("--live-inventory-basis-max-var-quote-age-ms") + 1] == "1500"
    assert command[command.index("--live-inventory-max-lighter-book-age-seconds") + 1] == "2"
    assert command[command.index("--live-inventory-basis-reversion-min-deviation-bps") + 1] == "1.2"
    assert command[command.index("--live-inventory-basis-reversion-long-execution-reserve-bps") + 1] == "4.5"
    assert command[command.index("--live-inventory-basis-reversion-short-execution-reserve-bps") + 1] == "3.5"
    assert command[command.index("--live-inventory-basis-reversion-min-net-expected-pnl-bps") + 1] == "1.5"
    assert command[command.index("--live-inventory-basis-reversion-signal-exit-min-pnl-bps") + 1] == "-0.8"
    assert command[command.index("--live-inventory-basis-min-normalized-filter-edge-bps") + 1] == "2.5"
    assert command[command.index("--live-inventory-basis-max-stablecoin-edge-share") + 1] == "0.6"


def test_live_tool_cost_calibrated_reversion_command_passes_main_cli_guards(monkeypatch) -> None:
    command = build_main_command("SOL", LiveConfig(reversion_mode=True))
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_reversion is True
    assert args.live_inventory_basis_reversion_long_execution_reserve_bps == 4.5
    assert args.live_inventory_basis_reversion_short_execution_reserve_bps == 3.5
    assert args.live_inventory_basis_reversion_min_net_expected_pnl_bps == 1.5


def test_live_tool_calibration_is_bounded_and_bypasses_strategy_filters() -> None:
    command = build_main_command(
        "SOL",
        LiveConfig(
            calibration_mode=True,
            calibration_max_cycles=5,
            calibration_hold_samples=5,
            calibration_warmup_samples=30,
            calibration_entry_cooldown_samples=180,
            calibration_max_run_loss_usd="0.25",
            calibration_max_cycle_loss_usd="0.10",
            calibration_max_roundtrip_cost_bps="6.0",
            max_total_lots=3,
        ),
    )

    assert "--live-inventory-execution-calibration" in command
    assert "--live-inventory-i-accept-execution-calibration-loss" in command
    assert "--live-inventory-basis-reversion" not in command
    assert "--live-inventory-basis-min-normalized-filter-edge-bps" not in command
    assert "--live-inventory-basis-max-stablecoin-edge-share" not in command
    assert command[command.index("--live-inventory-max-cycles") + 1] == "5"
    assert command[command.index("--live-inventory-max-lots") + 1] == "1"
    assert command[command.index("--live-inventory-max-total-lots") + 1] == "1"
    assert command[command.index("--live-inventory-max-total-notional-usd") + 1] == "25"
    assert command[command.index("--live-inventory-basis-max-entry-roundtrip-cost-bps") + 1] == "6.0"
    assert command[command.index("--live-inventory-min-hold-samples") + 1] == "5"
    assert command[command.index("--live-inventory-max-hold-samples") + 1] == "5"
    assert command[command.index("--live-inventory-basis-max-hold-action") + 1] == "exit"
    assert command[command.index("--live-inventory-calibration-max-run-loss-usd") + 1] == "0.25"


def test_live_tool_calibration_command_passes_main_cli_guards(monkeypatch) -> None:
    command = build_main_command("SOL", LiveConfig(calibration_mode=True))
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_execution_calibration is True
    assert args.live_inventory_i_accept_execution_calibration_loss is True
    assert args.live_inventory_max_cycles == 5
    assert args.live_inventory_min_hold_samples == args.live_inventory_max_hold_samples == 5


def test_live_tool_calibration_state_uses_calibration_cycle_cap(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 4,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(LiveConfig(calibration_mode=True, calibration_max_cycles=5))

    assert ok is True
    assert "max_cycles=5" in message


def test_live_tool_reversion_state_uses_one_cycle_cap(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(LiveConfig(reversion_mode=True, max_cycles=3))

    assert ok is False
    assert "state_cycle_cap_reached" in message
    assert "max_cycles=1" in message


def test_live_tool_reset_state_allows_next_reversion_cycle(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(
        LiveConfig(reversion_mode=True),
        reset_state_after_manual_flat=True,
    )
    command = build_main_command(
        "SOL",
        LiveConfig(reversion_mode=True),
        reset_state_after_manual_flat=True,
    )

    assert ok is True
    assert "reset_after_manual_flat_requested" in message
    assert "--live-inventory-reset-state-after-manual-flat" in command


def test_live_tool_refuses_flat_state_when_cycle_cap_reached(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(LiveConfig(max_cycles=1))

    assert ok is False
    assert "state_cycle_cap_reached" in message
    assert "completed_cycles=1" in message
