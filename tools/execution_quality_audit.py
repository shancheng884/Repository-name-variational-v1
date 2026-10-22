"""Read-only execution-cost audit and strictly prior exit-gate replay."""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.execution_quality import advance_shortfall_reserve, shortfall_tail


def number(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except InvalidOperation:
        return None


def summarize(rows: list[dict[str, Any]], *, since: str | None = None, net_target: Decimal = Decimal("1")) -> dict[str, Any]:
    histories: dict[str, deque[Decimal]] = {}
    floors: dict[str, Decimal] = {}
    seen: set[tuple[Any, Any]] = set()
    result: dict[str, Any] = {
        "groups": 0, "wins": 0, "losses": 0, "actual_price_pnl_usd": Decimal("0"),
        "estimate_gap_usd": Decimal("0"), "prior_gate_pass": 0, "prior_gate_reject": 0,
        "wire_to_report_samples": [], "shortfalls": [],
    }
    for row in sorted(rows, key=lambda item: str(item.get("logged_at") or "")):
        direction = row.get("direction")
        if row.get("actual_pnl_status") != "lighter_final_fill_confirmed" or direction not in {
            "long_var_short_lighter", "short_var_long_lighter",
        }:
            continue
        estimated, actual = number(row.get("estimated_pnl_bps")), number(row.get("actual_pnl_bps"))
        actual_usd, estimated_usd = number(row.get("actual_pnl_usd")), number(row.get("estimated_pnl_usd"))
        if None in (estimated, actual, actual_usd, estimated_usd):
            continue
        identity = row.get("run_id"), row.get("lot_id")
        if all(value is not None for value in identity):
            if identity in seen:
                continue
            seen.add(identity)
        history = histories.setdefault(direction, deque(maxlen=20))
        _, _, tail = shortfall_tail(history)
        stage_floor = Decimal("3.5") if len(history) < 10 else Decimal("1.5") if len(history) < 20 else Decimal("0.5")
        # Evaluate with prior groups only. The current realized loss is not
        # available when its exit decision is made.
        required = net_target + max(stage_floor, tail, floors.get(direction, Decimal("0")))
        in_window = since is None or str(row.get("logged_at") or "") >= since
        shortfall = max(estimated - actual, Decimal("0"))
        if in_window:
            result["groups"] += 1
            result["wins"] += actual_usd > 0
            result["losses"] += actual_usd < 0
            result["actual_price_pnl_usd"] += actual_usd
            result["estimate_gap_usd"] += estimated_usd - actual_usd
            result["prior_gate_pass" if estimated >= required else "prior_gate_reject"] += 1
            result["shortfalls"].append(shortfall)
            latency = number(row.get("exit_lighter_wire_to_fill_report_ms"))
            if latency is not None and latency >= 0:
                result["wire_to_report_samples"].append(latency)
        history.append(shortfall)
        _, _, tail = shortfall_tail(history)
        floors[direction] = advance_shortfall_reserve(floors.get(direction, Decimal("0")), tail)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log-dir", type=Path, default=ROOT / "log")
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--since-beijing")
    parser.add_argument("--net-target-bps", type=Decimal, default=Decimal("1"))
    args = parser.parse_args()
    if not args.net_target_bps.is_finite() or args.net_target_bps < 0:
        parser.error("--net-target-bps must be finite and nonnegative")
    since = None
    if args.since_beijing:
        try:
            at = datetime.fromisoformat(args.since_beijing)
        except ValueError:
            parser.error("--since-beijing must be an ISO date or datetime")
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone(timedelta(hours=8)))
        since = at.astimezone(timezone.utc).isoformat()
    fields = {
        "logged_at", "run_id", "lot_id", "direction", "actual_pnl_status",
        "estimated_pnl_bps", "actual_pnl_bps", "estimated_pnl_usd", "actual_pnl_usd",
        "exit_lighter_wire_to_fill_report_ms",
    }
    rows = []
    files = scanned = invalid = 0
    for path in sorted(args.log_dir.glob("order_metrics.jsonl*")):
        if not re.fullmatch(r"order_metrics\.jsonl(?:\.\d+)?(?:\.gz)?", path.name):
            continue
        files += 1
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rb") as handle:
            while True:
                line = handle.readline(8 * 1024 * 1024 + 1)
                if not line:
                    break
                scanned += 1
                if len(line) > 8 * 1024 * 1024:
                    invalid += 1
                    while line and not line.endswith(b"\n"):
                        line = handle.readline(8 * 1024 * 1024)
                    continue
                if b"live_inventory_actual_pnl" not in line:
                    continue
                try:
                    row = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    invalid += 1
                    continue
                if not isinstance(row, dict) or row.get("event") != "live_inventory_actual_pnl" or str(row.get("asset") or "").upper() != args.asset.upper():
                    continue
                rows.append({key: row.get(key) for key in fields})
    report = summarize(rows, since=since, net_target=args.net_target_bps)
    _, p95, _ = shortfall_tail(report["shortfalls"])
    if not report["shortfalls"]:
        p95 = None
    print(f"files={files} scanned_lines={scanned} invalid_or_oversized={invalid}")
    print(f"groups={report['groups']} wins={report['wins']} losses={report['losses']}")
    print(f"actual_price_pnl_usd={report['actual_price_pnl_usd']} estimate_gap_usd={report['estimate_gap_usd']}")
    print(f"observed_shortfall_p95_bps={p95} wire_to_fill_report_samples={len(report['wire_to_report_samples'])}")
    print(f"strictly_prior_gate_pass={report['prior_gate_pass']} reject={report['prior_gate_reject']} net_target_bps={args.net_target_bps}")
    print("gate_replay_is_not_a_fill_backtest; fees_funding_and_alternative_exit_pnl_not_inferred; missing_latency_is_unknown")


if __name__ == "__main__":
    main()
