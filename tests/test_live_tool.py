import json

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
    assert command[command.index("--live-inventory-basis-min-entry-edge-bps") + 1] == "7"
    assert command[command.index("--live-inventory-basis-min-abs-entry-bps") + 1] == "7"
    assert command[command.index("--live-inventory-basis-dynamic-entry-noise-buffer-bps") + 1] == "2.0"
    assert command[command.index("--live-inventory-snapshot-timeout-seconds") + 1] == "10"
    assert command[command.index("--live-inventory-basis-addon-min-basis-improvement-bps") + 1] == "2.0"


def test_live_tool_omits_basis_addon_flags_for_single_lot() -> None:
    command = build_main_command("SOL", LiveConfig(max_total_lots=1))

    assert "--live-inventory-i-accept-basis-addon-diagnostic" not in command
    assert "--live-inventory-basis-addon-min-basis-improvement-bps" not in command


def test_live_tool_can_disable_dynamic_entry_threshold() -> None:
    command = build_main_command("SOL", LiveConfig(max_total_lots=1, dynamic_entry_threshold=False))

    assert "--live-inventory-basis-dynamic-entry-threshold" not in command


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
