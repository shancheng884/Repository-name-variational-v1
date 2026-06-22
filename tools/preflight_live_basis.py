from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_STATE = Path("log/live_inventory_state.json")
DEFAULT_METRICS = Path("log/order_metrics.jsonl")


def load_state(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"status": "missing"}


def recent_manual_review(path: Path, *, asset: str, scan: int) -> bool:
    if not path.exists():
        return False
    rows = path.read_text(encoding="utf-8").splitlines()[-scan:]
    for line in rows:
        if "manual_review_required" not in line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if str(row.get("asset") or "").upper() == asset.upper():
            return True
    return False


def check(state_path: Path, metrics_path: Path, *, asset: str, scan: int) -> dict[str, Any]:
    state = load_state(state_path)
    open_lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
    manual = state.get("status") == "manual_review_required" or recent_manual_review(metrics_path, asset=asset, scan=scan)
    if manual:
        command_type = "stop_manual_review"
    elif state.get("status") == "open" or open_lots:
        command_type = "open_state_resume"
    elif state.get("status") in {"flat", "missing"}:
        command_type = "flat_start_after_manual_venue_check"
    else:
        command_type = "inspect_manually"
    return {
        "state_status": state.get("status"),
        "open_lots": len(open_lots),
        "manual_review_detected": manual,
        "recommended_command_type": command_type,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only live basis preflight state checker.")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--metrics", type=Path, default=DEFAULT_METRICS)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--scan", type=int, default=500)
    args = parser.parse_args()
    for key, value in check(args.state, args.metrics, asset=args.asset, scan=args.scan).items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
