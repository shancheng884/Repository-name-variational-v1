"""Causal source-time alignment. Never extrapolates an executable quote."""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path

BASIS = "source_time_aligned_v1"


def epoch(value):
    if isinstance(value, datetime):
        return value.timestamp()
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError):
        return None


def edges(left, right):
    la, lb = Decimal(str(left["executable_buy"])), Decimal(str(left["executable_sell"]))
    ra, rb = Decimal(str(right["executable_buy"])), Decimal(str(right["executable_sell"]))
    return {"forward": float((rb - la) / ((rb + la) / 2) * 10000),
            "reverse": float((lb - ra) / ((lb + ra) / 2) * 10000)}


class QuoteClock:
    def __init__(self, capacity=4096):
        self.capacity = capacity
        self.tapes = {}

    def record(self, quote):
        source, received = epoch(quote.get("source_at")), epoch(quote.get("recv_at"))
        if source is None or received is None:
            return False
        if not all(quote.get(k) is not None for k in ("executable_buy", "executable_sell")):
            return False
        try:
            prices = [Decimal(str(quote[k])) for k in ("executable_buy", "executable_sell")]
            if any(not p.is_finite() or p <= 0 for p in prices) or prices[0] < prices[1]:
                return False
        except (ArithmeticError, ValueError):
            return False
        key = (quote["venue"], quote["symbol"])
        stamps, rows = self.tapes.setdefault(key, ([], []))
        i = bisect_left(stamps, source)
        row = {k: quote.get(k) for k in ("venue", "symbol", "bid", "ask", "executable_buy",
                                        "executable_sell", "recv_at", "source_at", "sequence", "quote_kind")}
        if i < len(stamps) and stamps[i] == source:
            if received >= epoch(rows[i]["recv_at"]):
                rows[i] = row
        else:
            stamps.insert(i, source)
            rows.insert(i, row)
        if len(stamps) > self.capacity:
            del stamps[:len(stamps) - self.capacity]
            del rows[:len(rows) - self.capacity]
        return True

    def as_of(self, venue, symbol, source, *, received_by):
        stamps, rows = self.tapes.get((venue, symbol), ([], []))
        i = bisect_right(stamps, source) - 1
        while i >= 0:
            if epoch(rows[i]["recv_at"]) <= received_by:
                return dict(rows[i])
            i -= 1
        return None

    def align(self, left, right, *, now):
        sources = [epoch(q.get("source_at")) for q in (left, right)]
        if any(t is None for t in sources):
            return None
        target = min(sources)
        pair = [self.as_of(q["venue"], q["symbol"], target, received_by=now)
                for q in (left, right)]
        if any(q is None for q in pair):
            return None
        version = tuple((epoch(q.get("source_at")), q.get("sequence"),
                         Decimal(str(q["executable_buy"])), Decimal(str(q["executable_sell"]))) for q in pair)
        return {"basis": BASIS, "target_source_at": datetime.fromtimestamp(target, timezone.utc).isoformat(),
                "left": pair[0], "right": pair[1], "edges": edges(*pair), "version": version,
                "source_gap_ms": round(abs(sources[0] - sources[1]) * 1000, 3)}


def seed_aligned_history(history, out_dir, venues, asset, window_seconds, *, now):
    """Replay received quote events, not a future/source-time sorted join."""
    cutoff = now - window_seconds
    events = []
    for venue in venues:
        for path in sorted((Path(out_dir) / "quotes" / venue / asset).glob("*.jsonl"))[-3:]:
            with path.open(encoding="utf-8", errors="ignore") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                        received = epoch(row.get("recv_at"))
                        if received is not None and cutoff - 120 <= received <= now:
                            row.setdefault("venue", venue)
                            row.setdefault("symbol", asset)
                            events.append((received, row))
                    except (ValueError, TypeError):
                        continue
    clock = QuoteClock()
    latest = {}
    last_second = None
    count = 0
    for received, row in sorted(events, key=lambda e: e[0]):
        if not clock.record(row):
            continue
        if row.get("executable_buy") is None or row.get("executable_sell") is None:
            continue
        previous = latest.get(row["venue"])
        if previous is None or (epoch(row.get("source_at")) or 0) >= (epoch(previous.get("source_at")) or 0):
            latest[row["venue"]] = row
        if received < cutoff or any(v not in latest for v in venues) or int(received) == last_second:
            continue
        if any(received - epoch(latest[v]["recv_at"]) > (6 if v == "entropy" else 3) for v in venues):
            continue
        aligned = clock.align(*(latest[v] for v in venues), now=received)
        if aligned is None:
            continue
        for direction in ("forward", "reverse"):
            if not history[direction] or received > history[direction][-1][0]:
                history[direction].append((received, aligned["edges"][direction]))
        last_second = int(received)
        count += 1
    return count
