"""Exit latency acceptance: how long after a lot qualifies does it close?

A profit exit only counts as working if the lot leaves within minutes of the
executable PnL reaching its threshold. This module measures that gap from the
order-metrics stream (archives included) instead of from an ad-hoc watcher, so
the number can be replayed and regression tested.
"""

from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

QUALIFIED_EVENTS = {
    "live_inventory_exit_refresh",
    "live_inventory_v4_exit_fast_refresh_confirmed",
    "live_inventory_v4_exit_current_quote_confirmed",
    "live_inventory_exit_blocked",
}

DEFAULT_MAX_LATENCY_SECONDS = 180.0


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def _time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _pnl_bps(row: dict[str, Any]) -> Decimal | None:
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


def _qualified(row: dict[str, Any]) -> bool:
    event = str(row.get("event") or "")
    if event not in QUALIFIED_EVENTS:
        return False
    reason = str(row.get("reason") or "")
    if event == "live_inventory_exit_blocked" and "below_threshold" in reason:
        return False
    edge = _pnl_bps(row)
    threshold = _threshold_bps(row)
    if edge is None or threshold is None:
        return event != "live_inventory_exit_blocked"
    return edge >= threshold


def measure(
    rows: Iterable[dict[str, Any]],
    *,
    asset: str | None = None,
    max_latency_seconds: float = DEFAULT_MAX_LATENCY_SECONDS,
    now: datetime | None = None,
) -> dict[str, Any]:
    wanted_asset = str(asset).upper() if asset else ""
    observed = now or datetime.now(timezone.utc)
    lots: "OrderedDict[tuple[str, str], dict[str, Any]]" = OrderedDict()
    for row in rows:
        row_asset = str(row.get("asset") or "").upper()
        if wanted_asset and row_asset and row_asset != wanted_asset:
            continue
        event = str(row.get("event") or "")
        lot_id = row.get("lot_id")
        if lot_id is None:
            lot_id = row.get("cycle_id")
        if lot_id is None:
            continue
        key = (str(row.get("run_id") or ""), str(lot_id))
        entry = lots.get(key)
        if entry is None:
            entry = {
                "run_id": key[0],
                "lot_id": key[1],
                "asset": row_asset or wanted_asset or "",
                "qualified_at": None,
                "qualified_edge_bps": None,
                "threshold_bps": None,
                "closed_at": None,
                "exit_reason": None,
            }
            lots[key] = entry
        stamp = _time(row.get("logged_at"))
        if event == "live_inventory_exited":
            if entry["closed_at"] is None and stamp is not None:
                entry["closed_at"] = stamp
                entry["exit_reason"] = row.get("exit_reason") or row.get("reason")
            continue
        if _qualified(row) and stamp is not None:
            if entry["qualified_at"] is None or stamp < entry["qualified_at"]:
                entry["qualified_at"] = stamp
                entry["qualified_edge_bps"] = _pnl_bps(row)
                entry["threshold_bps"] = _threshold_bps(row)

    records = []
    latencies: list[float] = []
    for entry in lots.values():
        qualified_at = entry["qualified_at"]
        closed_at = entry["closed_at"]
        latency = None
        if qualified_at is not None and closed_at is not None and closed_at >= qualified_at:
            latency = (closed_at - qualified_at).total_seconds()
        if latency is not None:
            latencies.append(latency)
            status = "PASS" if latency <= max_latency_seconds else "SLOW"
        elif qualified_at is not None:
            status = "OPEN"
            latency = (observed - qualified_at).total_seconds()
        else:
            status = "NOT_QUALIFIED"
        records.append(
            {
                "lot_id": entry["lot_id"],
                "run_id": entry["run_id"],
                "asset": entry["asset"],
                "status": status,
                "qualified_at": qualified_at.isoformat() if qualified_at else None,
                "closed_at": closed_at.isoformat() if closed_at else None,
                "latency_seconds": round(latency, 1) if latency is not None else None,
                "qualified_edge_bps": str(entry["qualified_edge_bps"])
                if entry["qualified_edge_bps"] is not None
                else None,
                "threshold_bps": str(entry["threshold_bps"])
                if entry["threshold_bps"] is not None
                else None,
                "exit_reason": entry["exit_reason"],
            }
        )

    latencies.sort()
    summary = {
        "asset": wanted_asset or "ALL",
        "max_latency_seconds": float(max_latency_seconds),
        "lots": len(records),
        "pass": sum(1 for item in records if item["status"] == "PASS"),
        "slow": sum(1 for item in records if item["status"] == "SLOW"),
        "open": sum(1 for item in records if item["status"] == "OPEN"),
        "not_qualified": sum(1 for item in records if item["status"] == "NOT_QUALIFIED"),
        "p50_latency_seconds": round(_percentile(latencies, 50), 1) if latencies else None,
        "p95_latency_seconds": round(_percentile(latencies, 95), 1) if latencies else None,
        "worst_latency_seconds": round(latencies[-1], 1) if latencies else None,
        "detail": records,
    }
    return summary


def _percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * (percent / 100.0)
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    fraction = rank - low
    return values[low] + (values[high] - values[low]) * fraction


def summary_lines(summary: dict[str, Any]) -> list[str]:
    lines = [
        f"批次={summary.get('lots') or 0} 达标={summary.get('pass') or 0} "
        f"超时={summary.get('slow') or 0} 等待中={summary.get('open') or 0} "
        f"未达标={summary.get('not_qualified') or 0}",
        f"平仓延迟中位={summary.get('p50_latency_seconds')} 秒 "
        f"P95={summary.get('p95_latency_seconds')} 秒 "
        f"最差={summary.get('worst_latency_seconds')} 秒 "
        f"门槛={summary.get('max_latency_seconds')} 秒",
    ]
    for item in summary.get("detail") or []:
        if item["status"] in {"SLOW", "OPEN"}:
            lines.append(
                f"  {item['status']} lot={item['lot_id']} 延迟={item['latency_seconds']} 秒 "
                f"达标价差={item['qualified_edge_bps']} 门槛={item['threshold_bps']}"
            )
    return lines