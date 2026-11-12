#!/usr/bin/env python3
"""Run the shared frozen-median engine for USOILP swap / WTI, with isolated state."""

import argparse
from dataclasses import replace
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.live import build_main_command, load_config, running_main_processes


def build_command(root=ROOT):
    output = root/"log/wti_median"
    path = output/"live_inventory_state.json"
    state = json.loads(path.read_text()) if path.exists() else {}
    if state and state.get("asset") != "WTI":
        raise ValueError("wti_median_state_asset_mismatch")
    config = replace(load_config(root/"live_config.json"), calibration_mode=False,
                     reversion_mode=False, v4_live_mode=True, max_cycles=0)
    resume = bool(state.get("open_lots") or state.get("pending_actions")
                  or state.get("status") in ("open", "pending", "manual_review_required"))
    command = build_main_command("WTI", config, v4_real_gradient=True,
        v4_bidirectional=True, v4_continuous=True, resume_open_position=resume)
    command += ["--live-inventory-frozen-median", "--output-dir", str(output),
                "--instance-lock-file", str(root/"log/main.instance.lock")]
    for flag in ("--live-inventory-basis-min-exit-pnl-bps",
                 "--live-inventory-basis-min-signal-reverted-exit-pnl-bps"):
        command[command.index(flag)+1] = "1"
    return command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-eth-flat", action="store_true")
    parser.add_argument("--i-understand-live-orders", action="store_true")
    args = parser.parse_args()
    command = build_command()
    if not args.execute:
        print(json.dumps({"orders_attempted":False,"command":command}))
        return 0
    if not (args.confirm_eth_flat and args.i_understand_live_orders):
        parser.error("execution requires ETH flat and live-order confirmations")
    if running_main_processes():
        raise SystemExit("strategy_already_running_no_second_instance")
    for name in ("live_inventory_state.json", "wti_live_state.json"):
        path = ROOT/"log"/name
        if not path.exists():
            continue
        state = json.loads(path.read_text())
        if (state.get("status") != "flat" or state.get("open_lots")
                or state.get("pending_actions") or state.get("open_position") or state.get("pending_action")):
            raise SystemExit("legacy_state_not_flat_preserve_and_reconcile:"+name)
    os.chdir(ROOT)
    os.execv(command[0], command)


if __name__ == "__main__":
    raise SystemExit(main())
