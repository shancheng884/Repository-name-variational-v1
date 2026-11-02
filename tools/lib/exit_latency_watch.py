"""Incremental, read-only exit-latency watch over the order-metrics stream.

The ad-hoc watcher that used to live in /tmp only knew about the live file and
lost its state on every restart. This module keeps the qualification state in a
small JSON file, survives rotation, and never writes to the trading path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools.lib.exit_latency_acceptance import (
    is_qualified,
    row_pnl_bps,
    row_threshold_bps,
)


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


def lot_key(row: dict[str, Any]) -> tuple[str, str] | None:
    lot_id = row.get("lot_id")
    if lot_id is None:
        lot_id = row.get("cycle_id")
    if lot_id is None:
        return None
    return (str(row.get("run_id") or ""), str(lot_id))


@dataclass
class WatchState:
    """One open record per qualified lot, keyed by (run_id, lot_id)."""

    qualified: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    closed: set[tuple[str, str]] = field(default_factory=set)
    qualified_total: int = 0
    closed_total: int = 0

    def observe(
        self, row: dict[str, Any], *, now: datetime | None = None
    ) -> dict[str, Any] | None:
        key = lot_key(row)
        if key is None:
            return None
        observed = now or datetime.now(timezone.utc)
        event = str(row.get("event") or "")
        if event == "live_inventory_exited":
            if key in self.closed:
                return None
            self.closed.add(key)
            self.closed_total += 1
            entry = self.qualified.get(key)
            latency = None
            if entry is not None:
                latency = (observed - entry["at"]).total_seconds()
            self.qualified.pop(key, None)
            return {
                "kind": "CLOSED",
                "lot": key[1],
                "run_id": key[0],
                "latency_s": None if latency is None else round(latency, 1),
                "qualified_at": entry["at"].isoformat() if entry else None,
                "pnl_bps": entry.get("pnl_bps") if entry else None,
                "exit_reason": row.get("exit_reason") or row.get("reason"),
            }
        if key in self.qualified or not is_qualified(row):
            return None
        stamp = _time(row.get("logged_at")) or observed
        pnl = row_pnl_bps(row)
        threshold = row_threshold_bps(row)
        self.qualified[key] = {"at": stamp, "pnl_bps": pnl, "threshold_bps": threshold}
        self.qualified_total += 1
        return {
            "kind": "QUALIFIED",
            "lot": key[1],
            "run_id": key[0],
            "pnl_bps": None if pnl is None else str(pnl),
            "threshold_bps": None if threshold is None else str(threshold),
            "at": stamp.isoformat(),
        }

    def to_json(self) -> dict[str, Any]:
        return {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "qualified_total": self.qualified_total,
            "closed_total": self.closed_total,
            "qualified": [
                {
                    "run_id": run_id,
                    "lot": lot,
                    "at": entry["at"].isoformat(),
                    "pnl_bps": None if entry.get("pnl_bps") is None else str(entry["pnl_bps"]),
                    "threshold_bps": (
                        None
                        if entry.get("threshold_bps") is None
                        else str(entry["threshold_bps"])
                    ),
                }
                for (run_id, lot), entry in self.qualified.items()
            ],
            "closed": [{"run_id": run_id, "lot": lot} for run_id, lot in sorted(self.closed)],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "WatchState":
        state = cls()
        if not isinstance(payload, dict):
            return state
        for item in payload.get("qualified") or []:
            stamp = _time(item.get("at"))
            if stamp is None:
                continue
            state.qualified[(str(item.get("run_id") or ""), str(item.get("lot")))] = {
                "at": stamp,
                "pnl_bps": item.get("pnl_bps"),
                "threshold_bps": item.get("threshold_bps"),
            }
        for item in payload.get("closed") or []:
            state.closed.add((str(item.get("run_id") or ""), str(item.get("lot"))))
        try:
            state.qualified_total = int(payload.get("qualified_total") or 0)
            state.closed_total = int(payload.get("closed_total") or 0)
        except (TypeError, ValueError):
            pass
        return state


def format_event(event: dict[str, Any]) -> str:
    if event["kind"] == "QUALIFIED":
        return (
            f"QUALIFIED lot={event['lot']} pnl={event['pnl_bps']} "
            f"thr={event['threshold_bps']}"
        )
    if event.get("latency_s") is None:
        return (
            f"CLOSED lot={event['lot']} latency_s=unknown "
            f"reason={event.get('exit_reason')}"
        )
    return (
        f"CLOSED lot={event['lot']} latency_s={event['latency_s']} "
        f"qualified_at={event['qualified_at']} pnl={event.get('pnl_bps')}"
    )


def load_state(path: Path) -> WatchState:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return WatchState()
    return WatchState.from_json(payload)


def save_state(path: Path, state: WatchState) -> None:
    target = Path(path)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(
        json.dumps(state.to_json(), sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(target)