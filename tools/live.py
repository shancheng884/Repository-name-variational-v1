#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import LIVE_STATE, LOG_DIR, human_bytes, read_json  # noqa: E402


ALLOWED_ASSETS = {"BTC", "ETH", "SOL"}
LIVE_CONFIG = ROOT / "live_config.json"


@dataclass(frozen=True)
class LiveConfig:
    live_max_notional_usd: str = "25"
    lot_notional_usd: str = "20"
    max_cycles: int = 1
    max_lots: int = 1
    max_total_lots: int = 1
    max_lighter_slippage_bps: str = "6"
    lighter_submit_slippage_bps: str = "15"
    lighter_exit_submit_slippage_bps: str = "30"
    min_entry_edge_bps: str = "13"
    min_abs_entry_bps: str = "13"
    min_exit_pnl_bps: str = "8.0"
    min_signal_reverted_exit_pnl_bps: str = "8.0"
    profit_take_pnl_bps: str = "10.0"
    entry_confirm_samples: int = 2
    max_sample_move_bps: str = "5"
    min_normalized_entry_edge_bps: str = "1.0"
    min_normalized_filter_edge_bps: str = "0.5"
    entry_lighter_fill_timeout_seconds: str = "3"


DEFAULT_CONFIG = LiveConfig()


def default_config_dict() -> dict[str, Any]:
    return DEFAULT_CONFIG.__dict__.copy()


def running_main_processes() -> list[str]:
    try:
        result = subprocess.run(
            ["pgrep", "-af", "python.*main.py"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return []
    return [line for line in result.stdout.splitlines() if line.strip() and "tools/live.py" not in line]


def validate_state() -> tuple[bool, str]:
    state = read_json(LIVE_STATE)
    if not state:
        return True, "state=missing allowed=start_after_manual_exchange_flat_confirmation"

    status = str(state.get("status") or "unknown")
    open_lots = state.get("open_lots") or []
    pending_actions = state.get("pending_actions") or []
    asset = str(state.get("asset") or "-").upper()

    if status != "flat":
        return False, f"state_not_flat status={status} asset={asset}"
    if open_lots:
        return False, f"open_lots_present count={len(open_lots)} asset={asset}"
    if pending_actions:
        return False, f"pending_actions_present count={len(pending_actions)} asset={asset}"
    return True, f"state=flat asset={asset} open_lots=0 pending_actions=0"


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
        max_cycles=_positive_int(raw, "max_cycles", max_value=10),
        max_lots=_positive_int(raw, "max_lots", max_value=10),
        max_total_lots=_positive_int(raw, "max_total_lots", max_value=10),
        max_lighter_slippage_bps=_positive_decimal(raw, "max_lighter_slippage_bps"),
        lighter_submit_slippage_bps=_positive_decimal(raw, "lighter_submit_slippage_bps"),
        lighter_exit_submit_slippage_bps=_positive_decimal(raw, "lighter_exit_submit_slippage_bps"),
        min_entry_edge_bps=_positive_decimal(raw, "min_entry_edge_bps"),
        min_abs_entry_bps=_positive_decimal(raw, "min_abs_entry_bps"),
        min_exit_pnl_bps=_positive_decimal(raw, "min_exit_pnl_bps"),
        min_signal_reverted_exit_pnl_bps=_positive_decimal(raw, "min_signal_reverted_exit_pnl_bps"),
        profit_take_pnl_bps=_positive_decimal(raw, "profit_take_pnl_bps"),
        entry_confirm_samples=_positive_int(raw, "entry_confirm_samples", max_value=20),
        max_sample_move_bps=_positive_decimal(raw, "max_sample_move_bps"),
        min_normalized_entry_edge_bps=_positive_decimal(raw, "min_normalized_entry_edge_bps"),
        min_normalized_filter_edge_bps=_positive_decimal(raw, "min_normalized_filter_edge_bps"),
        entry_lighter_fill_timeout_seconds=_positive_decimal(raw, "entry_lighter_fill_timeout_seconds"),
    )


def build_main_command(asset: str, config: LiveConfig) -> list[str]:
    return [
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
        "--lighter-prewarm-submit-ws",
        "--live-max-notional-usd",
        config.live_max_notional_usd,
        "--live-inventory",
        "--live-inventory-signal-mode",
        "basis",
        "--live-inventory-basis-entry-mode",
        "concurrent",
        "--live-inventory-lot-notional-usd",
        config.lot_notional_usd,
        "--live-inventory-max-cycles",
        str(config.max_cycles),
        "--live-inventory-max-lots",
        str(config.max_lots),
        "--live-inventory-max-total-lots",
        str(config.max_total_lots),
        "--live-inventory-max-lighter-slippage-bps",
        config.max_lighter_slippage_bps,
        "--live-inventory-lighter-submit-slippage-bps",
        config.lighter_submit_slippage_bps,
        "--live-inventory-lighter-exit-submit-slippage-bps",
        config.lighter_exit_submit_slippage_bps,
        "--live-inventory-basis-min-entry-edge-bps",
        config.min_entry_edge_bps,
        "--live-inventory-basis-min-abs-entry-bps",
        config.min_abs_entry_bps,
        "--live-inventory-basis-min-exit-pnl-bps",
        config.min_exit_pnl_bps,
        "--live-inventory-basis-min-signal-reverted-exit-pnl-bps",
        config.min_signal_reverted_exit_pnl_bps,
        "--live-inventory-basis-profit-take-pnl-bps",
        config.profit_take_pnl_bps,
        "--live-inventory-basis-entry-confirm-samples",
        str(config.entry_confirm_samples),
        "--live-inventory-basis-max-sample-move-bps",
        config.max_sample_move_bps,
        "--live-inventory-basis-stablecoin-normalization",
        "--live-inventory-basis-use-normalized-edge-for-entry",
        "--live-inventory-basis-stablecoin-regime-entry",
        "--live-inventory-basis-min-normalized-entry-edge-bps",
        config.min_normalized_entry_edge_bps,
        "--live-inventory-basis-min-normalized-filter-edge-bps",
        config.min_normalized_filter_edge_bps,
        "--live-inventory-entry-lighter-fill-timeout-seconds",
        config.entry_lighter_fill_timeout_seconds,
        "--live-inventory-i-confirm-flat-start",
        "--live-inventory-i-accept-basis-real-diagnostic",
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Start the real live arbitrage runner.")
    parser.add_argument("--asset", required=True, help="Live asset: BTC, ETH, or SOL.")
    parser.add_argument("--config", default=str(LIVE_CONFIG), help="Startup config JSON. Default: live_config.json.")
    parser.add_argument("--dry-run", action="store_true", help="Print checks without starting live.")
    parser.add_argument("--verbose", action="store_true", help="Print the full main.py command.")
    args = parser.parse_args()

    asset = args.asset.upper()
    if asset not in ALLOWED_ASSETS:
        parser.error(f"--asset must be one of {sorted(ALLOWED_ASSETS)}")
    try:
        config = load_config(Path(args.config))
    except ValueError as exc:
        print(f"REFUSE_START reason=config_invalid detail={exc}")
        return 2

    processes = running_main_processes()
    if processes:
        print("REFUSE_START reason=python_main_already_running")
        for process in processes:
            print(process)
        return 2

    state_ok, state_message = validate_state()
    print(state_message)
    print(disk_warning())
    if not state_ok:
        print("REFUSE_START reason=local_live_state_not_flat")
        return 2

    command = build_main_command(asset, config)
    print(f"starting asset={asset} max_cycles={config.max_cycles} lot_notional_usd={config.lot_notional_usd}")
    if args.verbose:
        print("main_command=" + " ".join(command))
    if args.dry_run:
        print("DRY_RUN no live process started")
        return 0

    print("Starting live. You are responsible for confirming both exchanges are flat before running this command.")
    return subprocess.call(command, cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
