"""Per-signal, de-duplicated audit of entry and exit decisions.

The live loop re-evaluates the same market signal many times per second, so raw
event counts overstate opportunity: one signal that was blocked five hundred
times is still one opportunity. This module folds rows into one record per
(run, signal) and counts distinct signals, while keeping the duplicate rows so
log pressure stays visible.
"""

from __future__ import annotations

from collections import Counter, OrderedDict
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

SIGNAL_EVENTS = {
    "live_inventory_exit_blocked",
    "live_inventory_exit_refresh",
    "live_inventory_v4_exit_fast_refresh_confirmed",
    "live_inventory_v4_exit_current_quote_confirmed",
    "live_inventory_v4_tier_exit_locked",
    "live_inventory_basis_quote_skipped",
    "live_inventory_basis_quote_failed",
    "live_inventory_entered",
    "live_inventory_exited",
    "live_inventory_final_pnl",
    "live_inventory_actual_pnl",
}


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def signal_key(row: dict[str, Any]) -> tuple[str, ...]:
    run_id = str(row.get("run_id") or "")
    sample_id = row.get("sample_id") or row.get("signal_sample_id")
    if sample_id:
        return (run_id, "sample", str(sample_id))
    sample_index = row.get("sample_index")
    if sample_index is not None:
        return (run_id, "index", str(sample_index))
    cycle_id = row.get("cycle_id")
    if cycle_id is not None:
        return (run_id, "cycle", str(cycle_id), str(row.get("lot_id") or ""))
    return (run_id, "lot", str(row.get("lot_id") or ""), str(row.get("logged_at") or ""))


def signal_id(key: tuple[str, ...]) -> str:
    return ":".join(part for part in key if part != "")


def row_signature(row: dict[str, Any]) -> tuple[str, ...]:
    """Rows that repeat the same decision inside one signal fold together."""

    return (
        str(row.get("event") or ""),
        str(row.get("lot_id") or ""),
        str(row.get("reason") or ""),
        str(row.get("refreshed_pnl_bps") or ""),
        str(row.get("executable_pnl_bps") or ""),
        str(row.get("effective_min_exit_pnl_bps") or ""),
        str(row.get("threshold_bps") or ""),
    )


def _edge_bps(row: dict[str, Any]) -> Decimal | None:
    for field in ("refreshed_pnl_bps", "executable_pnl_bps", "pnl_bps"):
        value = _decimal(row.get(field))
        if value is not None:
            return value
    return None


def _threshold_bps(row: dict[str, Any]) -> Decimal | None:
    for field in ("effective_min_exit_pnl_bps", "threshold_bps"):
        value = _decimal(row.get(field))
        if value is not None:
            return value
    return None


def audit_rows(
    rows: Iterable[dict[str, Any]],
    *,
    asset: str | None = None,
    limit: int | None = 20,
) -> dict[str, Any]:
    wanted_asset = str(asset).upper() if asset else ""
    signals: "OrderedDict[tuple[str, ...], dict[str, Any]]" = OrderedDict()
    total_rows = 0
    duplicate_rows = 0
    for row in rows:
        event = str(row.get("event") or "")
        if event not in SIGNAL_EVENTS:
            continue
        row_asset = str(row.get("asset") or "").upper()
        if wanted_asset and row_asset and row_asset != wanted_asset:
            continue
        total_rows += 1
        key = signal_key(row)
        entry = signals.get(key)
        if entry is None:
            entry = {
                "signal": signal_id(key),
                "first_seen": row.get("logged_at"),
                "last_seen": row.get("logged_at"),
                "rows": 0,
                "duplicate_rows": 0,
                "reasons": Counter(),
                "signatures": set(),
                "best_pnl_bps": None,
                "threshold_bps": None,
                "crossed": False,
                "refreshed": False,
                "exited": False,
            }
            signals[key] = entry
        entry["rows"] += 1
        entry["last_seen"] = row.get("logged_at") or entry["last_seen"]
        signature = row_signature(row)
        if signature in entry["signatures"]:
            entry["duplicate_rows"] += 1
            duplicate_rows += 1
        else:
            entry["signatures"].add(signature)
        reason = str(row.get("reason") or "")
        if reason:
            entry["reasons"][reason] += 1
        if event in {
            "live_inventory_exit_refresh",
            "live_inventory_v4_exit_fast_refresh_confirmed",
            "live_inventory_v4_exit_current_quote_confirmed",
        }:
            entry["refreshed"] = True
        if event == "live_inventory_exited":
            entry["exited"] = True
        edge = _edge_bps(row)
        threshold = _threshold_bps(row)
        if edge is not None:
            best = entry["best_pnl_bps"]
            if best is None or edge > best:
                entry["best_pnl_bps"] = edge
        if threshold is not None:
            entry["threshold_bps"] = threshold
        if (
            edge is not None
            and threshold is not None
            and edge >= threshold
        ):
            entry["crossed"] = True

    records = []
    for entry in signals.values():
        records.append(
            {
                "signal": entry["signal"],
                "first_seen": entry["first_seen"],
                "last_seen": entry["last_seen"],
                "rows": entry["rows"],
                "duplicate_rows": entry["duplicate_rows"],
                "reasons": dict(entry["reasons"]),
                "best_pnl_bps": str(entry["best_pnl_bps"])
                if entry["best_pnl_bps"] is not None
                else None,
                "threshold_bps": str(entry["threshold_bps"])
                if entry["threshold_bps"] is not None
                else None,
                "crossed": entry["crossed"],
                "refreshed": entry["refreshed"],
                "exited": entry["exited"],
            }
        )

    reason_counts: Counter[str] = Counter()
    crossed_by_reason: Counter[str] = Counter()
    for record in records:
        for reason in record["reasons"]:
            reason_counts[reason] += 1
            if record["crossed"]:
                crossed_by_reason[reason] += 1

    summary = {
        "asset": wanted_asset or "ALL",
        "rows": total_rows,
        "signals": len(records),
        "duplicate_rows": duplicate_rows,
        "signals_crossed": sum(1 for record in records if record["crossed"]),
        "signals_refreshed": sum(1 for record in records if record["refreshed"]),
        "signals_exited": sum(1 for record in records if record["exited"]),
        "reasons": dict(reason_counts),
        "crossed_reasons": dict(crossed_by_reason),
    }
    if limit is not None and limit >= 0:
        summary["signal_detail"] = records[-limit:] if limit else []
    else:
        summary["signal_detail"] = records
    return summary


def summary_lines(summary: dict[str, Any]) -> list[str]:
    rows = summary.get("rows") or 0
    signals = summary.get("signals") or 0
    duplicates = summary.get("duplicate_rows") or 0
    ratio = f"{(duplicates / rows * 100):.1f}%" if rows else "-"
    lines = [
        f"信号={signals} 行数={rows} 重复行={duplicates} 重复占比={ratio}",
        "越线信号="
        f"{summary.get('signals_crossed') or 0} 已刷新={summary.get('signals_refreshed') or 0} "
        f"已平仓={summary.get('signals_exited') or 0}",
    ]
    for reason, count in sorted(
        (summary.get("reasons") or {}).items(), key=lambda item: -item[1]
    )[:6]:
        lines.append(f"  原因 {reason}={count} 个信号")
    return lines