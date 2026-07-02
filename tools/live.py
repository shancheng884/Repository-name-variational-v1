#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import LIVE_STATE, LOG_DIR, human_bytes, read_json  # noqa: E402


ALLOWED_ASSETS = {"BTC", "ETH", "SOL"}


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


def build_main_command(asset: str, cycles: int) -> list[str]:
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
        "25",
        "--live-inventory",
        "--live-inventory-signal-mode",
        "basis",
        "--live-inventory-basis-entry-mode",
        "concurrent",
        "--live-inventory-lot-notional-usd",
        "20",
        "--live-inventory-max-cycles",
        str(cycles),
        "--live-inventory-max-lots",
        "1",
        "--live-inventory-max-total-lots",
        "1",
        "--live-inventory-max-lighter-slippage-bps",
        "6",
        "--live-inventory-lighter-submit-slippage-bps",
        "15",
        "--live-inventory-lighter-exit-submit-slippage-bps",
        "30",
        "--live-inventory-basis-min-entry-edge-bps",
        "13",
        "--live-inventory-basis-min-abs-entry-bps",
        "13",
        "--live-inventory-basis-min-exit-pnl-bps",
        "8.0",
        "--live-inventory-basis-min-signal-reverted-exit-pnl-bps",
        "8.0",
        "--live-inventory-basis-profit-take-pnl-bps",
        "10.0",
        "--live-inventory-basis-entry-confirm-samples",
        "2",
        "--live-inventory-basis-max-sample-move-bps",
        "5",
        "--live-inventory-basis-stablecoin-normalization",
        "--live-inventory-basis-use-normalized-edge-for-entry",
        "--live-inventory-basis-stablecoin-regime-entry",
        "--live-inventory-basis-min-normalized-entry-edge-bps",
        "1.0",
        "--live-inventory-basis-min-normalized-filter-edge-bps",
        "0.5",
        "--live-inventory-entry-lighter-fill-timeout-seconds",
        "3",
        "--live-inventory-i-confirm-flat-start",
        "--live-inventory-i-accept-basis-real-diagnostic",
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Start the real live arbitrage runner.")
    parser.add_argument("--asset", required=True, help="Live asset: BTC, ETH, or SOL.")
    parser.add_argument("--cycles", type=int, default=1, help="Maximum completed cycles for this foreground run. Default: 1.")
    parser.add_argument("--dry-run", action="store_true", help="Print checks and the main.py command without starting live.")
    parser.add_argument("--yes", action="store_true", help="Skip the interactive manual flat confirmation prompt.")
    args = parser.parse_args()

    asset = args.asset.upper()
    if asset not in ALLOWED_ASSETS:
        parser.error(f"--asset must be one of {sorted(ALLOWED_ASSETS)}")
    if args.cycles <= 0 or args.cycles > 10:
        parser.error("--cycles must be between 1 and 10")

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

    command = build_main_command(asset, args.cycles)
    print("main_command=" + " ".join(command))
    if args.dry_run:
        print("DRY_RUN no live process started")
        return 0

    if not args.yes:
        print("Before starting, manually confirm Variational and Lighter have no positions and no open orders.")
        confirmation = input("Type YES to start live: ").strip()
        if confirmation != "YES":
            print("CANCELLED no live process started")
            return 1

    return subprocess.call(command, cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
