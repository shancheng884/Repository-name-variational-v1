import argparse
import math
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


LOG_TS_FORMAT = "%Y-%m-%d %H:%M:%S,%f"


ENTRY_RE = re.compile(
    r"auto_live_entry_submitted cycle_id=(?P<cycle_id>\S+) asset=(?P<asset>\S+) "
    r"direction=(?P<direction>\S+) qty=(?P<qty>\S+) var_side=(?P<side>\S+)"
)
EXIT_RE = re.compile(
    r"auto_live_exit_submitted cycle_id=(?P<cycle_id>\S+) asset=(?P<asset>\S+) "
    r"side=(?P<side>\S+) qty=(?P<qty>\S+) reason=(?P<reason>\S+)"
)
ENTRY_PRECHECK_RE = re.compile(
    r"auto_live_entry_precheck_(?P<status>passed|failed) cycle_id=(?P<cycle_id>\S+) asset=(?P<asset>\S+) "
    r"side=(?P<side>\S+) qty=(?P<qty>\S+) (?:reason=(?P<reason>\S+) )?edge_bps=(?P<edge>\S+)"
)
EXIT_PRECHECK_RE = re.compile(
    r"auto_live_exit_precheck_(?P<status>passed|failed) cycle_id=(?P<cycle_id>\S+) asset=(?P<asset>\S+) "
    r"side=(?P<side>\S+) qty=(?P<qty>\S+) (?:reason=(?P<reason>\S+) )?edge_bps=(?P<edge>\S+)"
)
MANUAL_REVIEW_RE = re.compile(
    r"auto_live_manual_review_required cycle_id=(?P<cycle_id>\S+) asset=(?P<asset>\S+) "
    r"qty=(?P<qty>\S+) reason=(?P<reason>\S+) action=(?P<action>\S+)"
)
GUARD_RE = re.compile(r"auto_live_guard_blocked reason=(?P<reason>\S+).*")
KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\S+)")


@dataclass
class Cycle:
    key: tuple[str, str, int]
    asset: str
    cycle_id: str
    occurrence: int
    entry_at: datetime | None = None
    entry_side: str = ""
    direction: str = ""
    qty: str = ""
    exit_at: datetime | None = None
    exit_side: str = ""
    exit_reason: str = ""
    manual_review_at: datetime | None = None
    manual_review_reason: str = ""
    last_exit_precheck_at: datetime | None = None
    exit_precheck_status: str = ""
    last_exit_precheck_edge_bps: Decimal | None = None
    last_exit_precheck_reason: str = ""
    entry_precheck_status: str = ""
    entry_precheck_failures: int = 0
    last_entry_precheck_edge_bps: Decimal | None = None
    last_entry_precheck_reason: str = ""
    entry_precheck_ms: Decimal | None = None
    entry_var_preview_ms: Decimal | None = None
    entry_var_submit_ms: Decimal | None = None
    entry_lighter_submit_ms: Decimal | None = None
    entry_total_ms: Decimal | None = None
    exit_precheck_ms: Decimal | None = None
    exit_var_submit_ms: Decimal | None = None
    exit_lighter_submit_ms: Decimal | None = None
    exit_total_ms: Decimal | None = None

    @property
    def status(self) -> str:
        if self.manual_review_at is not None:
            return "manual_review_required"
        if self.exit_at is not None:
            return "flat"
        if self.entry_at is not None:
            return "open"
        return "pre_entry_blocked"

    @property
    def holding_seconds(self) -> Decimal | None:
        if self.entry_at is None:
            return None
        end = self.exit_at or self.manual_review_at
        if end is None:
            return None
        return Decimal(str((end - self.entry_at).total_seconds()))


def parse_decimal(value: str) -> Decimal | None:
    if not value or value == "-":
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def parse_log_ts(line: str) -> datetime | None:
    try:
        return datetime.strptime(line[:23], LOG_TS_FORMAT)
    except ValueError:
        return None


def parse_key_values(line: str) -> dict[str, str]:
    return {match.group("key"): match.group("value") for match in KEY_VALUE_RE.finditer(line)}


def fmt(value: Any, places: int = 3) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, Decimal):
        return f"{value:.{places}f}"
    return str(value)


def percentile_nearest_rank(values: list[Decimal], percentile: int) -> Decimal | None:
    if not values:
        return None
    if percentile <= 0:
        return min(values)
    if percentile >= 100:
        return max(values)
    ordered = sorted(values)
    rank = math.ceil((Decimal(percentile) / Decimal("100")) * Decimal(len(ordered)))
    index = max(0, min(len(ordered) - 1, int(rank) - 1))
    return ordered[index]


def cycle_sort_key(cycle: Cycle) -> tuple[str, datetime, int]:
    return (cycle.asset, cycle.entry_at or cycle.last_exit_precheck_at or cycle.manual_review_at or datetime.min, cycle.occurrence)


def parse_runtime_log(path: Path, asset_filter: set[str]) -> list[Cycle]:
    cycles: list[Cycle] = []
    active_by_asset_cycle: dict[tuple[str, str], Cycle] = {}
    occurrence_by_asset_cycle: dict[tuple[str, str], int] = {}

    def include(asset: str) -> bool:
        return not asset_filter or asset.upper() in asset_filter

    def new_cycle(ts: datetime, asset: str, cycle_id: str) -> Cycle:
        base_key = (asset.upper(), cycle_id)
        previous = active_by_asset_cycle.get(base_key)
        if previous is not None and previous.entry_at is None:
            occurrence = previous.occurrence
        else:
            occurrence_by_asset_cycle[base_key] = occurrence_by_asset_cycle.get(base_key, 0) + 1
            occurrence = occurrence_by_asset_cycle[base_key]
        cycle = Cycle(key=(asset.upper(), cycle_id, occurrence), asset=asset.upper(), cycle_id=cycle_id, occurrence=occurrence)
        if previous is not None and previous.entry_at is None:
            cycle.entry_precheck_status = previous.entry_precheck_status
            cycle.entry_precheck_failures = previous.entry_precheck_failures
            cycle.last_entry_precheck_edge_bps = previous.last_entry_precheck_edge_bps
            cycle.last_entry_precheck_reason = previous.last_entry_precheck_reason
            cycles.remove(previous)
        cycle.entry_at = ts
        cycles.append(cycle)
        active_by_asset_cycle[base_key] = cycle
        return cycle

    def current_cycle(asset: str, cycle_id: str) -> Cycle | None:
        return active_by_asset_cycle.get((asset.upper(), cycle_id))

    def new_pre_entry_cycle(asset: str, cycle_id: str) -> Cycle:
        base_key = (asset.upper(), cycle_id)
        occurrence_by_asset_cycle[base_key] = occurrence_by_asset_cycle.get(base_key, 0) + 1
        occurrence = occurrence_by_asset_cycle[base_key]
        cycle = Cycle(key=(asset.upper(), cycle_id, occurrence), asset=asset.upper(), cycle_id=cycle_id, occurrence=occurrence)
        cycles.append(cycle)
        active_by_asset_cycle[base_key] = cycle
        return cycle

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            ts = parse_log_ts(line)
            if ts is None:
                continue

            if match := ENTRY_RE.search(line):
                asset = match.group("asset").upper()
                if not include(asset):
                    continue
                cycle = new_cycle(ts, asset, match.group("cycle_id"))
                fields = parse_key_values(line)
                cycle.direction = match.group("direction")
                cycle.qty = match.group("qty")
                cycle.entry_side = match.group("side")
                cycle.entry_total_ms = parse_decimal(fields.get("entry_total_ms", ""))
                cycle.entry_precheck_ms = parse_decimal(fields.get("entry_precheck_ms", ""))
                cycle.entry_var_preview_ms = parse_decimal(fields.get("var_preview_ms", ""))
                cycle.entry_var_submit_ms = parse_decimal(fields.get("var_submit_ms", ""))
                cycle.entry_lighter_submit_ms = parse_decimal(fields.get("lighter_submit_ms", ""))
                continue

            if match := ENTRY_PRECHECK_RE.search(line):
                asset = match.group("asset").upper()
                if not include(asset):
                    continue
                cycle_id = match.group("cycle_id")
                cycle = current_cycle(asset, cycle_id)
                if cycle is None or cycle.entry_at is not None:
                    cycle = new_pre_entry_cycle(asset, cycle_id)
                status = match.group("status")
                fields = parse_key_values(line)
                cycle.entry_precheck_status = status
                if status == "failed":
                    cycle.entry_precheck_failures += 1
                cycle.last_entry_precheck_edge_bps = parse_decimal(match.group("edge"))
                cycle.last_entry_precheck_reason = match.group("reason") or ""
                cycle.entry_precheck_ms = parse_decimal(fields.get("duration_ms", ""))
                continue

            if match := EXIT_PRECHECK_RE.search(line):
                asset = match.group("asset").upper()
                if not include(asset):
                    continue
                cycle = current_cycle(asset, match.group("cycle_id"))
                if cycle is None:
                    continue
                fields = parse_key_values(line)
                cycle.last_exit_precheck_at = ts
                cycle.exit_precheck_status = match.group("status")
                cycle.last_exit_precheck_edge_bps = parse_decimal(match.group("edge"))
                cycle.last_exit_precheck_reason = match.group("reason") or ""
                cycle.exit_precheck_ms = parse_decimal(fields.get("duration_ms", ""))
                continue

            if match := MANUAL_REVIEW_RE.search(line):
                asset = match.group("asset").upper()
                if not include(asset):
                    continue
                cycle = current_cycle(asset, match.group("cycle_id"))
                if cycle is None:
                    continue
                cycle.manual_review_at = ts
                cycle.manual_review_reason = match.group("reason")
                if not cycle.qty:
                    cycle.qty = match.group("qty")
                continue

            if match := EXIT_RE.search(line):
                asset = match.group("asset").upper()
                if not include(asset):
                    continue
                cycle = current_cycle(asset, match.group("cycle_id"))
                if cycle is None:
                    continue
                fields = parse_key_values(line)
                cycle.exit_at = ts
                cycle.exit_side = match.group("side")
                cycle.exit_reason = match.group("reason")
                cycle.exit_total_ms = parse_decimal(fields.get("exit_total_ms", ""))
                cycle.exit_precheck_ms = parse_decimal(fields.get("exit_precheck_ms", ""))
                cycle.exit_var_submit_ms = parse_decimal(fields.get("var_submit_ms", ""))
                cycle.exit_lighter_submit_ms = parse_decimal(fields.get("lighter_submit_ms", ""))
                if not cycle.qty:
                    cycle.qty = match.group("qty")
                continue

    return sorted(cycles, key=cycle_sort_key)


def print_summary(cycles: list[Cycle], source: Path, limit: int) -> None:
    print(f"Source: {source}")
    print()
    if not cycles:
        print("No matching auto-live cycles found.")
        return

    status_counts: dict[str, int] = {}
    manual_reasons: dict[str, int] = {}
    for cycle in cycles:
        status_counts[cycle.status] = status_counts.get(cycle.status, 0) + 1
        if cycle.manual_review_reason:
            manual_reasons[cycle.manual_review_reason] = manual_reasons.get(cycle.manual_review_reason, 0) + 1

    print("status breakdown")
    for status, count in sorted(status_counts.items()):
        print(f"{status}: {count}")

    print()
    print("manual review reasons")
    if not manual_reasons:
        print("none")
    else:
        for reason, count in sorted(manual_reasons.items(), key=lambda item: (-item[1], item[0])):
            print(f"{reason}: {count}")

    metric_values: dict[str, list[Decimal]] = {
        "entry_total_ms": [],
        "entry_var_preview_ms": [],
        "entry_var_submit_ms": [],
        "entry_lighter_submit_ms": [],
        "exit_total_ms": [],
        "exit_var_submit_ms": [],
        "exit_lighter_submit_ms": [],
    }
    for cycle in cycles:
        for key in metric_values:
            value = getattr(cycle, key)
            if isinstance(value, Decimal):
                metric_values[key].append(value)

    print()
    print("latency percentiles")
    print("metric count p50_ms p90_ms min_ms max_ms")
    for key, values in metric_values.items():
        print(
            "{metric} {count} {p50} {p90} {min_v} {max_v}".format(
                metric=key,
                count=len(values),
                p50=fmt(percentile_nearest_rank(values, 50), 3),
                p90=fmt(percentile_nearest_rank(values, 90), 3),
                min_v=fmt(min(values) if values else None, 3),
                max_v=fmt(max(values) if values else None, 3),
            )
        )

    selected = cycles[-limit:] if limit > 0 else cycles
    print()
    print("cycle details")
    print(
        "asset cycle_id occurrence status entry_at entry_side direction qty holding_seconds "
        "entry_precheck_status entry_precheck_edge_bps entry_precheck_reason "
        "entry_precheck_ms entry_var_preview_ms entry_var_submit_ms entry_lighter_submit_ms entry_total_ms "
        "exit_at exit_side exit_reason exit_precheck_status exit_precheck_edge_bps exit_precheck_reason "
        "exit_precheck_ms exit_var_submit_ms exit_lighter_submit_ms exit_total_ms "
        "manual_review_at manual_review_reason entry_precheck_failures"
    )
    for cycle in selected:
        print(
            "{asset} {cycle_id} {occurrence} {status} {entry_at} {entry_side} {direction} {qty} {holding} "
            "{entry_precheck_status} {entry_edge} {entry_precheck_reason} "
            "{entry_precheck_ms} {entry_var_preview_ms} {entry_var_submit_ms} {entry_lighter_submit_ms} {entry_total_ms} "
            "{exit_at} {exit_side} {exit_reason} {exit_precheck_status} {exit_edge} {exit_precheck_reason} "
            "{exit_precheck_ms} {exit_var_submit_ms} {exit_lighter_submit_ms} {exit_total_ms} "
            "{manual_at} {manual_reason} {entry_precheck_failures}".format(
                asset=cycle.asset,
                cycle_id=cycle.cycle_id,
                occurrence=cycle.occurrence,
                status=cycle.status,
                entry_at=cycle.entry_at.isoformat(sep=" ") if cycle.entry_at else "-",
                entry_side=cycle.entry_side or "-",
                direction=cycle.direction or "-",
                qty=cycle.qty or "-",
                holding=fmt(cycle.holding_seconds, 3),
                entry_precheck_status=cycle.entry_precheck_status or "-",
                entry_edge=fmt(cycle.last_entry_precheck_edge_bps, 3),
                entry_precheck_reason=cycle.last_entry_precheck_reason or "-",
                entry_precheck_ms=fmt(cycle.entry_precheck_ms, 3),
                entry_var_preview_ms=fmt(cycle.entry_var_preview_ms, 3),
                entry_var_submit_ms=fmt(cycle.entry_var_submit_ms, 3),
                entry_lighter_submit_ms=fmt(cycle.entry_lighter_submit_ms, 3),
                entry_total_ms=fmt(cycle.entry_total_ms, 3),
                exit_at=cycle.exit_at.isoformat(sep=" ") if cycle.exit_at else "-",
                exit_side=cycle.exit_side or "-",
                exit_reason=cycle.exit_reason or "-",
                exit_precheck_status=cycle.exit_precheck_status or "-",
                exit_edge=fmt(cycle.last_exit_precheck_edge_bps, 3),
                exit_precheck_reason=cycle.last_exit_precheck_reason or "-",
                exit_precheck_ms=fmt(cycle.exit_precheck_ms, 3),
                exit_var_submit_ms=fmt(cycle.exit_var_submit_ms, 3),
                exit_lighter_submit_ms=fmt(cycle.exit_lighter_submit_ms, 3),
                exit_total_ms=fmt(cycle.exit_total_ms, 3),
                manual_at=cycle.manual_review_at.isoformat(sep=" ") if cycle.manual_review_at else "-",
                manual_reason=cycle.manual_review_reason or "-",
                entry_precheck_failures=cycle.entry_precheck_failures,
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze auto-live cycles from runtime.log.")
    parser.add_argument("--runtime-log", default="log/runtime.log", help="Path to runtime.log. Default: log/runtime.log")
    parser.add_argument("--assets", default="", help="Optional comma-separated asset filter, e.g. BTC,SOL")
    parser.add_argument("--limit", type=int, default=30, help="Number of latest cycle detail rows to print. Use 0 for all.")
    args = parser.parse_args()

    runtime_log = Path(args.runtime_log)
    if not runtime_log.exists():
        raise SystemExit(f"runtime log not found: {runtime_log}")
    asset_filter = {asset.strip().upper() for asset in args.assets.split(",") if asset.strip()}
    cycles = parse_runtime_log(runtime_log, asset_filter)
    print_summary(cycles, runtime_log, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
