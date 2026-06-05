import argparse
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    entry_lighter_fill_ms: Decimal | None = None
    entry_signal_to_both_filled_ms: Decimal | None = None
    entry_total_ms: Decimal | None = None
    exit_precheck_ms: Decimal | None = None
    exit_var_submit_ms: Decimal | None = None
    exit_lighter_submit_ms: Decimal | None = None
    exit_lighter_fill_ms: Decimal | None = None
    exit_signal_to_both_filled_ms: Decimal | None = None
    exit_total_ms: Decimal | None = None
    entry_var_fill_price: Decimal | None = None
    entry_lighter_fill_price: Decimal | None = None
    exit_var_fill_price: Decimal | None = None
    exit_lighter_fill_price: Decimal | None = None
    entry_spread_usd: Decimal | None = None
    exit_spread_usd: Decimal | None = None
    spread_capture_usd: Decimal | None = None
    spread_capture_bps: Decimal | None = None
    gross_pnl_usd: Decimal | None = None
    gross_pnl_bps: Decimal | None = None

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


def parse_iso_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


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


def compute_spread_capture(cycle: Cycle) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    qty = parse_decimal(cycle.qty)
    if qty is None:
        return None, None, None, None
    if (
        cycle.entry_var_fill_price is None
        or cycle.entry_lighter_fill_price is None
        or cycle.exit_var_fill_price is None
        or cycle.exit_lighter_fill_price is None
    ):
        return None, None, None, None
    if cycle.entry_var_fill_price == 0:
        return None, None, None, None

    entry_side = cycle.entry_side.strip().upper()
    if entry_side == "BUY":
        entry_spread = cycle.entry_lighter_fill_price - cycle.entry_var_fill_price
        exit_spread = cycle.exit_lighter_fill_price - cycle.exit_var_fill_price
    elif entry_side == "SELL":
        entry_spread = cycle.entry_var_fill_price - cycle.entry_lighter_fill_price
        exit_spread = cycle.exit_var_fill_price - cycle.exit_lighter_fill_price
    else:
        return None, None, None, None

    spread_capture = entry_spread - exit_spread
    notional = qty * cycle.entry_var_fill_price
    if notional == 0:
        return entry_spread, exit_spread, spread_capture, None
    return entry_spread, exit_spread, spread_capture, (spread_capture / cycle.entry_var_fill_price) * Decimal("10000")


def compute_gross_pnl(cycle: Cycle) -> tuple[Decimal | None, Decimal | None]:
    qty = parse_decimal(cycle.qty)
    if qty is None or cycle.spread_capture_usd is None:
        return None, None
    pnl = qty * cycle.spread_capture_usd
    if cycle.entry_var_fill_price is None or cycle.entry_var_fill_price == 0:
        return pnl, None
    return pnl, (pnl / (qty * cycle.entry_var_fill_price)) * Decimal("10000")


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


def prefer_metric_record(existing: dict[str, Any] | None, incoming: dict[str, Any]) -> dict[str, Any]:
    if existing is None:
        return incoming
    existing_real = not bool(existing.get("synthetic_eager_fill"))
    incoming_real = not bool(incoming.get("synthetic_eager_fill"))
    if incoming_real and not existing_real:
        return incoming
    existing_logged = parse_iso_ts(existing.get("logged_at")) or datetime.min
    incoming_logged = parse_iso_ts(incoming.get("logged_at")) or datetime.min
    return incoming if incoming_logged >= existing_logged else existing


def enrich_cycles_with_order_metrics(cycles: list[Cycle], order_metrics_path: Path, asset_filter: set[str]) -> None:
    candidates: list[dict[str, Any]] = []

    with order_metrics_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            asset = str(payload.get("asset") or "").upper()
            if not asset or (asset_filter and asset not in asset_filter):
                continue
            cycle_id = payload.get("auto_live_cycle_id")
            role = str(payload.get("auto_live_role") or "").lower()
            if cycle_id is None or role not in {"entry", "exit"}:
                continue
            if not payload.get("lighter_filled_at") or not payload.get("variational_filled_at"):
                continue
            candidates.append(payload)

    grouped: dict[tuple[str, str, str, int], dict[str, Any]] = {}
    for payload in candidates:
        asset = str(payload.get("asset") or "").upper()
        cycle_id = str(payload.get("auto_live_cycle_id"))
        role = str(payload.get("auto_live_role") or "").lower()
        logged_at = parse_iso_ts(payload.get("logged_at"))
        matching_cycles = [cycle for cycle in cycles if cycle.asset == asset and str(cycle.cycle_id) == cycle_id]
        if not matching_cycles:
            continue
        if logged_at is None:
            cycle = matching_cycles[-1]
        else:
            cycle = min(
                matching_cycles,
                key=lambda item: abs((logged_at - ((item.entry_at if role == "entry" else item.exit_at) or datetime.min)).total_seconds()),
            )
        grouped_key = (asset, cycle_id, role, cycle.occurrence)
        grouped[grouped_key] = prefer_metric_record(grouped.get(grouped_key), payload)

    for (_asset, _cycle_id, role, occurrence), payload in grouped.items():
        asset = str(payload.get("asset") or "").upper()
        cycle_id = str(payload.get("auto_live_cycle_id"))
        cycle = next(
            (item for item in cycles if item.asset == asset and str(item.cycle_id) == cycle_id and item.occurrence == occurrence),
            None,
        )
        if cycle is None:
            continue
        signal_at = cycle.entry_at if role == "entry" else cycle.exit_at
        total_ms = cycle.entry_total_ms if role == "entry" else cycle.exit_total_ms
        if signal_at is not None and total_ms is not None:
            signal_at = signal_at - timedelta_ms(total_ms)
        var_filled_at = parse_iso_ts(payload.get("variational_filled_at"))
        lighter_filled_at = parse_iso_ts(payload.get("lighter_filled_at"))
        both_filled_at = max([ts for ts in (var_filled_at, lighter_filled_at) if ts is not None], default=None)
        lighter_fill_ms = parse_decimal(str(payload.get("live_submit_sent_to_fill_ms") or ""))
        signal_to_both_ms = None
        if signal_at is not None and both_filled_at is not None:
            signal_to_both_ms = Decimal(str((both_filled_at - signal_at).total_seconds() * 1000))
        if role == "entry":
            cycle.entry_lighter_fill_ms = lighter_fill_ms
            cycle.entry_signal_to_both_filled_ms = signal_to_both_ms
            cycle.entry_var_fill_price = parse_decimal(str(payload.get("variational_filled_price") or ""))
            cycle.entry_lighter_fill_price = parse_decimal(str(payload.get("lighter_filled_price") or ""))
        else:
            cycle.exit_lighter_fill_ms = lighter_fill_ms
            cycle.exit_signal_to_both_filled_ms = signal_to_both_ms
            cycle.exit_var_fill_price = parse_decimal(str(payload.get("variational_filled_price") or ""))
            cycle.exit_lighter_fill_price = parse_decimal(str(payload.get("lighter_filled_price") or ""))

    for cycle in cycles:
        (
            cycle.entry_spread_usd,
            cycle.exit_spread_usd,
            cycle.spread_capture_usd,
            cycle.spread_capture_bps,
        ) = compute_spread_capture(cycle)
        cycle.gross_pnl_usd, cycle.gross_pnl_bps = compute_gross_pnl(cycle)


def timedelta_ms(value: Decimal):
    return timedelta(milliseconds=float(value))


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
        "entry_lighter_fill_ms": [],
        "entry_signal_to_both_filled_ms": [],
        "exit_total_ms": [],
        "exit_var_submit_ms": [],
        "exit_lighter_submit_ms": [],
        "exit_lighter_fill_ms": [],
        "exit_signal_to_both_filled_ms": [],
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

    pnl_cycles = [cycle for cycle in cycles if cycle.gross_pnl_usd is not None]
    print()
    print("gross pnl summary (fees assumed zero)")
    if not pnl_cycles:
        print("none")
    else:
        gross_values = [cycle.gross_pnl_usd for cycle in pnl_cycles if cycle.gross_pnl_usd is not None]
        bps_values = [cycle.gross_pnl_bps for cycle in pnl_cycles if cycle.gross_pnl_bps is not None]
        winners = [value for value in gross_values if value > 0]
        losers = [value for value in gross_values if value < 0]
        print(
            "cycles={count} total_usd={total} avg_usd={avg} winners={winners} losers={losers} avg_bps={avg_bps}".format(
                count=len(gross_values),
                total=fmt(sum(gross_values), 6),
                avg=fmt(sum(gross_values) / Decimal(len(gross_values)), 6),
                winners=len(winners),
                losers=len(losers),
                avg_bps=fmt(sum(bps_values) / Decimal(len(bps_values)) if bps_values else None, 3),
            )
        )

    selected = cycles[-limit:] if limit > 0 else cycles
    print()
    print("cycle details")
    print(
        "asset cycle_id occurrence status entry_at entry_side direction qty holding_seconds "
        "entry_precheck_status entry_precheck_edge_bps entry_precheck_reason "
        "entry_precheck_ms entry_var_preview_ms entry_var_submit_ms entry_lighter_submit_ms entry_total_ms "
        "entry_lighter_fill_ms entry_signal_to_both_filled_ms "
        "exit_at exit_side exit_reason exit_precheck_status exit_precheck_edge_bps exit_precheck_reason "
        "exit_precheck_ms exit_var_submit_ms exit_lighter_submit_ms exit_total_ms "
        "exit_lighter_fill_ms exit_signal_to_both_filled_ms "
        "entry_var_fill_price entry_lighter_fill_price exit_var_fill_price exit_lighter_fill_price "
        "entry_spread_usd exit_spread_usd spread_capture_usd spread_capture_bps gross_pnl_usd gross_pnl_bps "
        "manual_review_at manual_review_reason entry_precheck_failures"
    )
    for cycle in selected:
        print(
            "{asset} {cycle_id} {occurrence} {status} {entry_at} {entry_side} {direction} {qty} {holding} "
            "{entry_precheck_status} {entry_edge} {entry_precheck_reason} "
            "{entry_precheck_ms} {entry_var_preview_ms} {entry_var_submit_ms} {entry_lighter_submit_ms} {entry_total_ms} "
            "{entry_lighter_fill_ms} {entry_signal_to_both_filled_ms} "
            "{exit_at} {exit_side} {exit_reason} {exit_precheck_status} {exit_edge} {exit_precheck_reason} "
            "{exit_precheck_ms} {exit_var_submit_ms} {exit_lighter_submit_ms} {exit_total_ms} "
            "{exit_lighter_fill_ms} {exit_signal_to_both_filled_ms} "
            "{entry_var_fill_price} {entry_lighter_fill_price} {exit_var_fill_price} {exit_lighter_fill_price} "
            "{entry_spread_usd} {exit_spread_usd} {spread_capture_usd} {spread_capture_bps} "
            "{gross_pnl_usd} {gross_pnl_bps} "
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
                entry_lighter_fill_ms=fmt(cycle.entry_lighter_fill_ms, 3),
                entry_signal_to_both_filled_ms=fmt(cycle.entry_signal_to_both_filled_ms, 3),
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
                exit_lighter_fill_ms=fmt(cycle.exit_lighter_fill_ms, 3),
                exit_signal_to_both_filled_ms=fmt(cycle.exit_signal_to_both_filled_ms, 3),
                entry_var_fill_price=fmt(cycle.entry_var_fill_price, 2),
                entry_lighter_fill_price=fmt(cycle.entry_lighter_fill_price, 2),
                exit_var_fill_price=fmt(cycle.exit_var_fill_price, 2),
                exit_lighter_fill_price=fmt(cycle.exit_lighter_fill_price, 2),
                entry_spread_usd=fmt(cycle.entry_spread_usd, 2),
                exit_spread_usd=fmt(cycle.exit_spread_usd, 2),
                spread_capture_usd=fmt(cycle.spread_capture_usd, 2),
                spread_capture_bps=fmt(cycle.spread_capture_bps, 3),
                gross_pnl_usd=fmt(cycle.gross_pnl_usd, 6),
                gross_pnl_bps=fmt(cycle.gross_pnl_bps, 3),
                manual_at=cycle.manual_review_at.isoformat(sep=" ") if cycle.manual_review_at else "-",
                manual_reason=cycle.manual_review_reason or "-",
                entry_precheck_failures=cycle.entry_precheck_failures,
            )
        )


def filter_cycles(cycles: list[Cycle], min_occurrence: int, completed_only: bool, pnl_only: bool) -> list[Cycle]:
    filtered = cycles
    if min_occurrence > 0:
        filtered = [cycle for cycle in filtered if cycle.occurrence >= min_occurrence]
    if completed_only:
        filtered = [cycle for cycle in filtered if cycle.status == "flat"]
    if pnl_only:
        filtered = [cycle for cycle in filtered if cycle.gross_pnl_usd is not None]
    return filtered


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze auto-live cycles from runtime.log.")
    parser.add_argument("--runtime-log", default="log/runtime.log", help="Path to runtime.log. Default: log/runtime.log")
    parser.add_argument("--order-metrics", default="", help="Optional path to order_metrics.jsonl for fill-result latency metrics.")
    parser.add_argument("--assets", default="", help="Optional comma-separated asset filter, e.g. BTC,SOL")
    parser.add_argument("--limit", type=int, default=30, help="Number of latest cycle detail rows to print. Use 0 for all.")
    parser.add_argument("--min-occurrence", type=int, default=0, help="Only include cycles with occurrence >= this value.")
    parser.add_argument("--completed-only", action="store_true", help="Only include flat cycles.")
    parser.add_argument("--pnl-only", action="store_true", help="Only include cycles with computed PnL.")
    args = parser.parse_args()

    runtime_log = Path(args.runtime_log)
    if not runtime_log.exists():
        raise SystemExit(f"runtime log not found: {runtime_log}")
    asset_filter = {asset.strip().upper() for asset in args.assets.split(",") if asset.strip()}
    cycles = parse_runtime_log(runtime_log, asset_filter)
    if args.order_metrics:
        order_metrics = Path(args.order_metrics)
        if not order_metrics.exists():
            raise SystemExit(f"order metrics log not found: {order_metrics}")
        enrich_cycles_with_order_metrics(cycles, order_metrics, asset_filter)
    cycles = filter_cycles(cycles, args.min_occurrence, args.completed_only, args.pnl_only)
    print_summary(cycles, runtime_log, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
