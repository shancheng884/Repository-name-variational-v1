#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import LIVE_STATE, LOG_DIR, human_bytes, read_json  # noqa: E402


ALLOWED_ASSETS = {"BNB", "BTC", "ETH", "HYPE", "SOL", "XRP"}
CALIBRATION_DIRECTIONS = {
    "alternate",
    "long_var_short_lighter",
    "short_var_long_lighter",
}
LIVE_CONFIG = ROOT / "live_config.json"
V4_PROFILE_ETH_SHORT_20260724 = "eth_short_execution_calibrated_20260724_n10"


@dataclass(frozen=True)
class LiveConfig:
    live_max_notional_usd: str = "25"
    lot_notional_usd: str = "20"
    max_total_inventory_notional_usd: str = "60"
    max_cycles: int = 1
    max_lots: int = 1
    max_total_lots: int = 3
    max_lighter_slippage_bps: str = "6"
    lighter_submit_slippage_bps: str = "15"
    lighter_exit_submit_slippage_bps: str = "30"
    min_entry_edge_bps: str = "7"
    min_abs_entry_bps: str = "7"
    max_entry_roundtrip_cost_bps: str = "0"
    min_entry_quality_score_bps: str = "0"
    dynamic_entry_threshold: bool = True
    dynamic_entry_noise_buffer_bps: str = "2.0"
    spread_regime_penalty_multiplier: str = "1.0"
    min_exit_pnl_bps: str = "3.0"
    min_signal_reverted_exit_pnl_bps: str = "3.0"
    reversion_signal_exit_min_pnl_bps: str = "-1.0"
    profit_take_pnl_bps: str = "5.0"
    entry_confirm_samples: int = 1
    max_sample_move_bps: str = "3"
    min_normalized_entry_edge_bps: str = "1.0"
    min_normalized_filter_edge_bps: str = "0.5"
    reversion_mode: bool = False
    reversion_min_deviation_bps: str = "1.0"
    reversion_exit_deviation_bps: str = "0.0"
    reversion_max_entry_roundtrip_cost_bps: str = "3.0"
    reversion_context_gap_bps: str = "1.0"
    reversion_long_execution_reserve_bps: str = "4.5"
    reversion_short_execution_reserve_bps: str = "3.5"
    reversion_min_net_expected_pnl_bps: str = "1.5"
    reversion_min_normalized_edge_bps: str = "2.5"
    reversion_max_stablecoin_edge_share: str = "0.6"
    calibration_mode: bool = False
    calibration_direction: str = "alternate"
    calibration_weekdays_only: bool = False
    calibration_max_cycles: int = 5
    calibration_hold_samples: int = 5
    calibration_warmup_samples: int = 30
    calibration_entry_cooldown_samples: int = 180
    calibration_max_run_loss_usd: str = "0.25"
    calibration_max_cycle_loss_usd: str = "0.10"
    calibration_max_roundtrip_cost_bps: str = "6.0"
    v4_live_mode: bool = False
    entry_lighter_fill_timeout_seconds: str = "3"
    snapshot_timeout_seconds: str = "10"
    addon_min_basis_improvement_bps: str = "2.0"


DEFAULT_CONFIG = LiveConfig()


def default_config_dict() -> dict[str, Any]:
    return DEFAULT_CONFIG.__dict__.copy()


def running_main_processes() -> list[str]:
    try:
        result = subprocess.run(
            ["pgrep", "-af", "python.*(main.py|tools/basis_collector.py)"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return []
    return [line for line in result.stdout.splitlines() if line.strip() and "tools/live.py" not in line]


def build_multi_asset_collector_command(assets: tuple[str, ...]) -> list[str]:
    return [
        sys.executable,
        "tools/basis_collector.py",
        "--assets",
        ",".join(assets),
    ]


def validate_state(
    config: LiveConfig,
    *,
    reset_state_after_manual_flat: bool = False,
    collect_only: bool = False,
) -> tuple[bool, str]:
    state = read_json(LIVE_STATE)
    if not state:
        return True, "state=missing allowed=start_after_manual_exchange_flat_confirmation"

    status = str(state.get("status") or "unknown")
    open_lots = state.get("open_lots") or []
    pending_actions = state.get("pending_actions") or []
    asset = str(state.get("asset") or "-").upper()
    try:
        completed_cycles = int(state.get("completed_cycles") or 0)
    except (TypeError, ValueError):
        completed_cycles = 0

    if status != "flat":
        return False, f"state_not_flat status={status} asset={asset}"
    if open_lots:
        return False, f"open_lots_present count={len(open_lots)} asset={asset}"
    if pending_actions:
        return False, f"pending_actions_present count={len(pending_actions)} asset={asset}"
    effective_max_cycles = (
        config.calibration_max_cycles
        if config.calibration_mode
        else 1
        if config.reversion_mode or config.v4_live_mode
        else config.max_cycles
    )
    if completed_cycles >= effective_max_cycles and not collect_only:
        if reset_state_after_manual_flat:
            return (
                True,
                f"state=flat asset={asset} open_lots=0 pending_actions=0 completed_cycles={completed_cycles} "
                f"max_cycles={effective_max_cycles} reset_after_manual_flat_requested",
            )
        return (
            False,
            f"state_cycle_cap_reached asset={asset} completed_cycles={completed_cycles} max_cycles={effective_max_cycles} "
            "open_lots=0 pending_actions=0",
        )
    mode = " collect_only=true" if collect_only else ""
    return True, f"state=flat asset={asset} open_lots=0 pending_actions=0 completed_cycles={completed_cycles} max_cycles={effective_max_cycles}{mode}"


def disk_warning() -> str:
    usage = shutil.disk_usage(ROOT)
    used_pct = usage.used / usage.total * 100
    return f"disk_used={used_pct:.0f}% disk_free={human_bytes(usage.free)} log_dir={human_bytes(dir_size_safe(LOG_DIR))}"


def dir_size_safe(path: Path) -> int:
    total = 0
    if not path.exists():
        return total
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                continue
    return total


def _positive_decimal(data: dict[str, Any], key: str) -> str:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        number = float(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a positive number") from exc
    if number <= 0:
        raise ValueError(f"{key} must be a positive number")
    return str(value)


def _non_negative_decimal(data: dict[str, Any], key: str) -> str:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        number = float(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a non-negative number") from exc
    if number < 0:
        raise ValueError(f"{key} must be a non-negative number")
    return str(value)


def _decimal(data: dict[str, Any], key: str) -> str:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        float(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a number") from exc
    return str(value)


def _positive_int(data: dict[str, Any], key: str, *, max_value: int | None = None) -> int:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a positive integer") from exc
    if number <= 0:
        raise ValueError(f"{key} must be a positive integer")
    if max_value is not None and number > max_value:
        raise ValueError(f"{key} must be <= {max_value}")
    return number


def _calibration_direction(data: dict[str, Any]) -> str:
    value = str(data.get("calibration_direction", DEFAULT_CONFIG.calibration_direction))
    if value not in CALIBRATION_DIRECTIONS:
        raise ValueError(
            "calibration_direction must be one of "
            + ", ".join(sorted(CALIBRATION_DIRECTIONS))
        )
    return value


def load_config(path: Path) -> LiveConfig:
    if not path.exists():
        path.write_text(json.dumps(default_config_dict(), indent=2) + "\n", encoding="utf-8")

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to read {path.name}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{path.name} must contain a JSON object")

    return LiveConfig(
        live_max_notional_usd=_positive_decimal(raw, "live_max_notional_usd"),
        lot_notional_usd=_positive_decimal(raw, "lot_notional_usd"),
        max_total_inventory_notional_usd=_positive_decimal(raw, "max_total_inventory_notional_usd"),
        max_cycles=_positive_int(raw, "max_cycles", max_value=10),
        max_lots=_positive_int(raw, "max_lots", max_value=10),
        max_total_lots=_positive_int(raw, "max_total_lots", max_value=10),
        max_lighter_slippage_bps=_positive_decimal(raw, "max_lighter_slippage_bps"),
        lighter_submit_slippage_bps=_positive_decimal(raw, "lighter_submit_slippage_bps"),
        lighter_exit_submit_slippage_bps=_positive_decimal(raw, "lighter_exit_submit_slippage_bps"),
        min_entry_edge_bps=_positive_decimal(raw, "min_entry_edge_bps"),
        min_abs_entry_bps=_positive_decimal(raw, "min_abs_entry_bps"),
        max_entry_roundtrip_cost_bps=_non_negative_decimal(raw, "max_entry_roundtrip_cost_bps"),
        min_entry_quality_score_bps=_decimal(raw, "min_entry_quality_score_bps"),
        dynamic_entry_threshold=bool(raw.get("dynamic_entry_threshold", DEFAULT_CONFIG.dynamic_entry_threshold)),
        dynamic_entry_noise_buffer_bps=_positive_decimal(raw, "dynamic_entry_noise_buffer_bps"),
        spread_regime_penalty_multiplier=_positive_decimal(raw, "spread_regime_penalty_multiplier"),
        min_exit_pnl_bps=_positive_decimal(raw, "min_exit_pnl_bps"),
        min_signal_reverted_exit_pnl_bps=_positive_decimal(raw, "min_signal_reverted_exit_pnl_bps"),
        reversion_signal_exit_min_pnl_bps=_decimal(raw, "reversion_signal_exit_min_pnl_bps"),
        profit_take_pnl_bps=_positive_decimal(raw, "profit_take_pnl_bps"),
        entry_confirm_samples=_positive_int(raw, "entry_confirm_samples", max_value=20),
        max_sample_move_bps=_positive_decimal(raw, "max_sample_move_bps"),
        min_normalized_entry_edge_bps=_positive_decimal(raw, "min_normalized_entry_edge_bps"),
        min_normalized_filter_edge_bps=_positive_decimal(raw, "min_normalized_filter_edge_bps"),
        reversion_mode=bool(raw.get("reversion_mode", DEFAULT_CONFIG.reversion_mode)),
        reversion_min_deviation_bps=_positive_decimal(raw, "reversion_min_deviation_bps"),
        reversion_exit_deviation_bps=_non_negative_decimal(raw, "reversion_exit_deviation_bps"),
        reversion_max_entry_roundtrip_cost_bps=_non_negative_decimal(raw, "reversion_max_entry_roundtrip_cost_bps"),
        reversion_context_gap_bps=_positive_decimal(raw, "reversion_context_gap_bps"),
        reversion_long_execution_reserve_bps=_non_negative_decimal(raw, "reversion_long_execution_reserve_bps"),
        reversion_short_execution_reserve_bps=_non_negative_decimal(raw, "reversion_short_execution_reserve_bps"),
        reversion_min_net_expected_pnl_bps=_positive_decimal(raw, "reversion_min_net_expected_pnl_bps"),
        reversion_min_normalized_edge_bps=_non_negative_decimal(raw, "reversion_min_normalized_edge_bps"),
        reversion_max_stablecoin_edge_share=_non_negative_decimal(raw, "reversion_max_stablecoin_edge_share"),
        calibration_mode=bool(raw.get("calibration_mode", DEFAULT_CONFIG.calibration_mode)),
        calibration_direction=_calibration_direction(raw),
        calibration_weekdays_only=bool(
            raw.get("calibration_weekdays_only", DEFAULT_CONFIG.calibration_weekdays_only)
        ),
        calibration_max_cycles=_positive_int(raw, "calibration_max_cycles", max_value=5),
        calibration_hold_samples=_positive_int(raw, "calibration_hold_samples", max_value=60),
        calibration_warmup_samples=_positive_int(raw, "calibration_warmup_samples", max_value=3600),
        calibration_entry_cooldown_samples=_positive_int(raw, "calibration_entry_cooldown_samples", max_value=3600),
        calibration_max_run_loss_usd=_positive_decimal(raw, "calibration_max_run_loss_usd"),
        calibration_max_cycle_loss_usd=_positive_decimal(raw, "calibration_max_cycle_loss_usd"),
        calibration_max_roundtrip_cost_bps=_positive_decimal(raw, "calibration_max_roundtrip_cost_bps"),
        v4_live_mode=bool(raw.get("v4_live_mode", DEFAULT_CONFIG.v4_live_mode)),
        entry_lighter_fill_timeout_seconds=_positive_decimal(raw, "entry_lighter_fill_timeout_seconds"),
        snapshot_timeout_seconds=_positive_decimal(raw, "snapshot_timeout_seconds"),
        addon_min_basis_improvement_bps=_positive_decimal(raw, "addon_min_basis_improvement_bps"),
    )


def build_main_command(
    asset: str,
    config: LiveConfig,
    *,
    reset_state_after_manual_flat: bool = False,
    collect_only: bool = False,
) -> list[str]:
    reversion_mode = config.reversion_mode
    calibration_mode = config.calibration_mode
    v4_live_mode = config.v4_live_mode
    diagnostic_single_lot = reversion_mode or calibration_mode or v4_live_mode or collect_only
    effective_max_total_notional_usd = "25" if diagnostic_single_lot else config.max_total_inventory_notional_usd
    effective_max_cycles = (
        config.calibration_max_cycles
        if calibration_mode
        else 1
        if reversion_mode or v4_live_mode
        else config.max_cycles
    )
    effective_max_lots = 1 if diagnostic_single_lot else config.max_lots
    effective_max_total_lots = 1 if diagnostic_single_lot else config.max_total_lots
    effective_min_entry_edge_bps = "0" if diagnostic_single_lot else config.min_entry_edge_bps
    effective_min_abs_entry_bps = "0" if diagnostic_single_lot else config.min_abs_entry_bps
    effective_max_entry_roundtrip_cost_bps = (
        config.calibration_max_roundtrip_cost_bps
        if calibration_mode
        else config.reversion_max_entry_roundtrip_cost_bps
        if reversion_mode
        else config.max_entry_roundtrip_cost_bps
    )
    command = [
        sys.executable,
        "main.py",
        "--mode",
        "live",
        "--confirm-live",
        "--live-allowed-assets",
        asset,
        "--variational-submit-transport",
        "api",
        "--lighter-submit-transport",
        "ws",
        "--lighter-order-mode",
        "market-ioc",
        "--live-max-notional-usd",
        config.live_max_notional_usd,
        "--live-inventory",
        "--live-inventory-signal-mode",
        "basis",
        "--live-inventory-basis-entry-mode",
        "concurrent",
        "--live-inventory-lot-notional-usd",
        config.lot_notional_usd,
        "--live-inventory-max-total-notional-usd",
        effective_max_total_notional_usd,
        "--live-inventory-max-cycles",
        str(effective_max_cycles),
        "--live-inventory-max-lots",
        str(effective_max_lots),
        "--live-inventory-max-total-lots",
        str(effective_max_total_lots),
        "--live-inventory-max-lighter-slippage-bps",
        config.max_lighter_slippage_bps,
        "--live-inventory-lighter-submit-slippage-bps",
        config.lighter_submit_slippage_bps,
        "--live-inventory-lighter-exit-submit-slippage-bps",
        config.lighter_exit_submit_slippage_bps,
        "--live-inventory-basis-min-entry-edge-bps",
        effective_min_entry_edge_bps,
        "--live-inventory-basis-min-abs-entry-bps",
        effective_min_abs_entry_bps,
        "--live-inventory-basis-max-entry-roundtrip-cost-bps",
        effective_max_entry_roundtrip_cost_bps,
        "--live-inventory-basis-min-entry-quality-score-bps",
        config.min_entry_quality_score_bps,
        "--live-inventory-basis-dynamic-entry-noise-buffer-bps",
        config.dynamic_entry_noise_buffer_bps,
        "--live-inventory-basis-spread-regime-penalty-multiplier",
        config.spread_regime_penalty_multiplier,
        "--live-inventory-basis-min-exit-pnl-bps",
        config.min_exit_pnl_bps,
        "--live-inventory-basis-min-signal-reverted-exit-pnl-bps",
        config.min_signal_reverted_exit_pnl_bps,
        "--live-inventory-basis-profit-take-pnl-bps",
        "0" if v4_live_mode else config.profit_take_pnl_bps,
        "--live-inventory-basis-entry-confirm-samples",
        str(config.entry_confirm_samples),
        "--live-inventory-basis-max-sample-move-bps",
        config.max_sample_move_bps,
        "--live-inventory-basis-stablecoin-normalization",
        "--live-inventory-entry-lighter-fill-timeout-seconds",
        config.entry_lighter_fill_timeout_seconds,
        "--live-inventory-snapshot-timeout-seconds",
        config.snapshot_timeout_seconds,
        "--live-inventory-i-confirm-flat-start",
    ]
    if collect_only:
        command.extend(["--live-inventory-dry-decisions", "--live-inventory-collect-only"])
    else:
        command.extend(["--lighter-prewarm-submit-ws", "--live-inventory-i-accept-basis-real-diagnostic"])
    if config.dynamic_entry_threshold and not v4_live_mode:
        command.append("--live-inventory-basis-dynamic-entry-threshold")
    if not reversion_mode and not calibration_mode and not v4_live_mode:
        command.extend(
            [
                "--live-inventory-basis-use-normalized-edge-for-entry",
                "--live-inventory-basis-stablecoin-regime-entry",
                "--live-inventory-basis-min-normalized-entry-edge-bps",
                config.min_normalized_entry_edge_bps,
                "--live-inventory-basis-min-normalized-filter-edge-bps",
                config.min_normalized_filter_edge_bps,
            ]
        )
    elif v4_live_mode:
        command.extend(
            [
                "--live-inventory-basis-v4-profile",
                V4_PROFILE_ETH_SHORT_20260724,
                "--live-inventory-i-accept-basis-v4-live",
                "--live-inventory-basis-max-hold-action",
                "exit",
                "--live-inventory-basis-refresh-exit-quote-before-submit",
                "--live-inventory-min-hold-samples",
                "0",
                "--live-inventory-max-hold-samples",
                "2147483647",
                "--live-inventory-basis-max-var-quote-age-ms",
                "1500",
                "--live-inventory-max-lighter-book-age-seconds",
                "2",
                "--live-inventory-basis-size-ladder-notionals-usd",
                "20,40,60",
            ]
        )
    elif reversion_mode:
        command.extend(
            [
                "--live-inventory-basis-max-hold-action",
                "exit",
                "--live-inventory-basis-refresh-exit-quote-before-submit",
                "--live-inventory-basis-max-var-quote-age-ms",
                "1500",
                "--live-inventory-max-lighter-book-age-seconds",
                "2",
                "--live-inventory-basis-reversion",
                "--live-inventory-basis-reversion-min-deviation-bps",
                config.reversion_min_deviation_bps,
                "--live-inventory-basis-reversion-exit-deviation-bps",
                config.reversion_exit_deviation_bps,
                "--live-inventory-basis-reversion-max-entry-roundtrip-cost-bps",
                config.reversion_max_entry_roundtrip_cost_bps,
                "--live-inventory-basis-reversion-context-gap-bps",
                config.reversion_context_gap_bps,
                "--live-inventory-basis-reversion-long-execution-reserve-bps",
                config.reversion_long_execution_reserve_bps,
                "--live-inventory-basis-reversion-short-execution-reserve-bps",
                config.reversion_short_execution_reserve_bps,
                "--live-inventory-basis-reversion-min-net-expected-pnl-bps",
                config.reversion_min_net_expected_pnl_bps,
                "--live-inventory-basis-reversion-signal-exit-min-pnl-bps",
                config.reversion_signal_exit_min_pnl_bps,
                "--live-inventory-basis-min-normalized-filter-edge-bps",
                config.reversion_min_normalized_edge_bps,
                "--live-inventory-basis-max-stablecoin-edge-share",
                config.reversion_max_stablecoin_edge_share,
            ]
        )
    else:
        command.extend(
            [
                "--live-inventory-execution-calibration",
                "--live-inventory-i-accept-execution-calibration-loss",
                "--live-inventory-calibration-direction",
                config.calibration_direction,
                "--live-inventory-calibration-warmup-samples",
                str(config.calibration_warmup_samples),
                "--live-inventory-calibration-entry-cooldown-samples",
                str(config.calibration_entry_cooldown_samples),
                "--live-inventory-calibration-max-run-loss-usd",
                config.calibration_max_run_loss_usd,
                "--live-inventory-calibration-max-cycle-loss-usd",
                config.calibration_max_cycle_loss_usd,
                "--live-inventory-min-hold-samples",
                str(config.calibration_hold_samples),
                "--live-inventory-max-hold-samples",
                str(config.calibration_hold_samples),
                "--live-inventory-basis-max-hold-action",
                "exit",
                "--live-inventory-basis-profit-take-pnl-bps",
                "0",
                "--live-inventory-basis-max-var-quote-age-ms",
                "750",
                "--live-inventory-max-lighter-book-age-seconds",
                "1",
            ]
        )
        if config.calibration_weekdays_only:
            command.append("--live-inventory-calibration-weekdays-only")
    if effective_max_total_lots > 1:
        command.extend(
            [
                "--live-inventory-i-accept-basis-addon-diagnostic",
                "--live-inventory-basis-addon-min-basis-improvement-bps",
                config.addon_min_basis_improvement_bps,
            ]
        )
    if reset_state_after_manual_flat:
        command.append("--live-inventory-reset-state-after-manual-flat")
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description="Start the real live arbitrage runner.")
    asset_group = parser.add_mutually_exclusive_group(required=True)
    asset_group.add_argument("--asset", help="Single live or collect-only asset: BTC, ETH, or SOL.")
    asset_group.add_argument("--assets", help="Comma-separated assets for the hard-isolated multi-asset collect-only process.")
    parser.add_argument("--config", default=str(LIVE_CONFIG), help="Startup config JSON. Default: live_config.json.")
    parser.add_argument("--reversion", action="store_true", help="Explicitly enable the one-lot basis reversion live test.")
    parser.add_argument(
        "--v4-live",
        action="store_true",
        help="Enable the immutable, one-cycle ETH V4 live profile.",
    )
    parser.add_argument(
        "--calibration",
        action="store_true",
        help="Explicitly enable bounded real execution-cost calibration. This intentionally submits and closes small real positions.",
    )
    parser.add_argument(
        "--calibration-direction",
        choices=sorted(CALIBRATION_DIRECTIONS),
        help="Override the execution-calibration direction for this run.",
    )
    parser.add_argument(
        "--calibration-weekdays-only",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Allow new execution-calibration entries only on UTC weekdays.",
    )
    parser.add_argument(
        "--collect-only",
        action="store_true",
        help="Continuously collect executable basis states without real or dry inventory entries.",
    )
    parser.add_argument(
        "--reset-state-after-manual-flat",
        action="store_true",
        help="After manually confirming both venues are flat, reset the completed-cycle state during startup.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print checks without starting live.")
    parser.add_argument("--verbose", action="store_true", help="Print the full main.py command.")
    args = parser.parse_args()

    asset = args.asset.upper() if args.asset else None
    assets = tuple(dict.fromkeys(token.strip().upper() for token in str(args.assets or "").split(",") if token.strip()))
    if asset is not None and asset not in ALLOWED_ASSETS:
        parser.error(f"--asset must be one of {sorted(ALLOWED_ASSETS)}")
    if assets and (len(assets) < 2 or any(item not in ALLOWED_ASSETS for item in assets)):
        parser.error(f"--assets requires at least two unique values from {sorted(ALLOWED_ASSETS)}")
    if assets and not args.collect_only:
        parser.error("--assets is hard-isolated to --collect-only and cannot be used by live/reversion/calibration")
    try:
        config = load_config(Path(args.config))
    except ValueError as exc:
        print(f"REFUSE_START reason=config_invalid detail={exc}")
        return 2
    if sum(bool(value) for value in (args.reversion, args.calibration, args.v4_live, args.collect_only)) > 1:
        parser.error("use only one of --reversion, --calibration, --v4-live, or --collect-only")
    if args.collect_only and args.reset_state_after_manual_flat:
        parser.error("--collect-only does not allow --reset-state-after-manual-flat")
    if args.reversion:
        config = replace(
            config,
            reversion_mode=True,
            calibration_mode=False,
            v4_live_mode=False,
        )
    if args.calibration:
        config = replace(config, calibration_mode=True, reversion_mode=False, v4_live_mode=False)
    if args.v4_live:
        config = replace(
            config,
            calibration_mode=False,
            reversion_mode=False,
            v4_live_mode=True,
        )
    if args.calibration_direction is not None:
        config = replace(config, calibration_direction=args.calibration_direction)
    if args.calibration_weekdays_only is not None:
        config = replace(config, calibration_weekdays_only=args.calibration_weekdays_only)
    if args.collect_only:
        config = replace(config, calibration_mode=False, reversion_mode=False, v4_live_mode=False)
    if sum(bool(value) for value in (config.reversion_mode, config.calibration_mode, config.v4_live_mode)) > 1:
        print("REFUSE_START reason=config_invalid detail=live_strategy_modes_are_mutually_exclusive")
        return 2
    if config.v4_live_mode and asset != "ETH":
        parser.error("--v4-live requires --asset ETH")
    if (
        args.calibration_direction is not None
        or args.calibration_weekdays_only is not None
    ) and not config.calibration_mode:
        parser.error("--calibration-direction/--calibration-weekdays-only require calibration mode")

    processes = running_main_processes()
    if processes:
        print("REFUSE_START reason=python_main_already_running")
        for process in processes:
            print(process)
        return 2

    state_ok, state_message = validate_state(
        config,
        reset_state_after_manual_flat=args.reset_state_after_manual_flat,
        collect_only=args.collect_only,
    )
    print(state_message)
    print(disk_warning())
    if not state_ok:
        print("REFUSE_START reason=local_live_state_not_flat")
        return 2

    command = (
        build_multi_asset_collector_command(assets)
        if assets
        else build_main_command(
            str(asset),
            config,
            reset_state_after_manual_flat=args.reset_state_after_manual_flat,
            collect_only=args.collect_only,
        )
    )
    effective_max_cycles = (
        config.calibration_max_cycles
        if config.calibration_mode
        else 1
        if config.reversion_mode or config.v4_live_mode
        else config.max_cycles
    )
    strategy_mode = (
        "basis_v3_collect_only"
        if args.collect_only
        else "execution_calibration"
        if config.calibration_mode
        else "basis_v4_live"
        if config.v4_live_mode
        else "reversion"
        if config.reversion_mode
        else "basis"
    )
    asset_text = ",".join(assets) if assets else str(asset)
    if assets:
        strategy_mode = "basis_multi_asset_collect_only"
    print(
        f"starting asset={asset_text} strategy_mode={strategy_mode} "
        f"max_cycles={effective_max_cycles} lot_notional_usd={config.lot_notional_usd}"
    )
    if args.verbose:
        print("main_command=" + " ".join(command))
    if args.dry_run:
        print("DRY_RUN no live process started")
        return 0

    if args.collect_only:
        print("Starting collect-only basis logging. Real and dry inventory entries are disabled.")
    else:
        print("Starting live. You are responsible for confirming both exchanges are flat before running this command.")
    return subprocess.call(command, cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
