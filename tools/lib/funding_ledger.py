"""Funding ledger: what the venues actually paid or charged us.

Lighter exposes per-position funding payments, so those rows are authoritative.
Variational has no funding endpoint through the browser bridge, so its side is
recorded as unknown rather than guessed.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

BEIJING = timezone(timedelta(hours=8), name="Asia/Shanghai")


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def read_ledger(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except OSError:
        return []
    return rows


def append_rows(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    """Append only funding payments we have not stored yet (by venue + id)."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    seen = {
        (str(row.get("venue")), str(row.get("funding_id")))
        for row in read_ledger(target)
    }
    written = 0
    with target.open("a", encoding="utf-8") as handle:
        for row in rows:
            key = (str(row.get("venue")), str(row.get("funding_id")))
            if key in seen:
                continue
            seen.add(key)
            handle.write(json.dumps(row, sort_keys=True) + "\n")
            written += 1
    return written


def beijing_day_bounds(day: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=BEIJING)
    return start, start + timedelta(days=1)


def sum_funding_between(
    rows: Iterable[dict[str, Any]],
    start: datetime,
    end: datetime,
    *,
    venue: str | None = None,
    asset: str | None = None,
) -> Decimal:
    total = Decimal("0")
    wanted_venue = str(venue or "").lower()
    wanted_asset = str(asset or "").upper()
    for row in rows:
        if wanted_venue and str(row.get("venue") or "").lower() != wanted_venue:
            continue
        if wanted_asset and str(row.get("asset") or "").upper() != wanted_asset:
            continue
        stamp = row.get("funding_at")
        try:
            at = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        except ValueError:
            continue
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        if not (start <= at < end):
            continue
        value = _decimal(row.get("change_usd"))
        if value is not None:
            total += value
    return total


def daily_funding_usd(
    path: Path, day: str, *, venue: str = "lighter", asset: str = "ETH"
) -> Decimal:
    start, end = beijing_day_bounds(day)
    return sum_funding_between(
        read_ledger(path), start, end, venue=venue, asset=asset
    )