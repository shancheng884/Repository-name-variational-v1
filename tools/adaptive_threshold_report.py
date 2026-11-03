#!/usr/bin/env python3
"""Suggest spread-adaptive thresholds from recorded basis samples.

Read-only analysis: it never changes a live threshold. The idea is the one the
desk quotes: a fixed bps gate is wrong when the venue spread itself moves, so
the gate should follow the recent dispersion of the basis.
"""

from __future__ import annotations

import argparse
import glob
import json
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def rolling_mad(
    points: list[tuple[datetime, float]], window_minutes: int = 30
) -> list[float]:
    """Median absolute deviation of the basis over a trailing window."""

    result: list[float] = []
    window: list[tuple[datetime, float]] = []
    for at, value in points:
        window.append((at, value))
        cutoff = at - timedelta(minutes=window_minutes)
        while window and window[0][0] < cutoff:
            window.pop(0)
        values = [item[1] for item in window]
        if len(values) < 5:
            result.append(0.0)
            continue
        median = statistics.median(values)
        deviations = [abs(item - median) for item in values]
        result.append(statistics.median(deviations))
    return result


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(len(ordered) * fraction)))
    return ordered[index]


def load_points(paths: list[str]) -> list[tuple[datetime, float]]:
    points: list[tuple[datetime, float]] = []
    for path in paths:
        try:
            handle = open(path, "r", encoding="utf-8", errors="ignore")
        except OSError:
            continue
        with handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                value = row.get("basis_bps")
                if value is None:
                    continue
                try:
                    at = datetime.fromisoformat(str(row.get("logged_at")))
                    points.append((at, abs(float(value))))
                except (TypeError, ValueError):
                    continue
    points.sort(key=lambda item: item[0])
    return points


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--files", type=int, default=3)
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paths = sorted(glob.glob(str(ROOT / "log" / "basis_samples" / args.asset / "*.jsonl")))[-args.files:]
    points = load_points(paths)
    if not points:
        print("adaptive_threshold=NO_DATA")
        return 1
    mad = [value for value in rolling_mad(points, args.window_minutes) if value > 0]
    level = [value for _, value in points]
    summary = {
        "asset": args.asset.upper(),
        "samples": len(points),
        "from": points[0][0].isoformat(),
        "to": points[-1][0].isoformat(),
        "spread_bps": {
            "p25": round(percentile(level, 0.25), 3),
            "p50": round(percentile(level, 0.50), 3),
            "p75": round(percentile(level, 0.75), 3),
            "p95": round(percentile(level, 0.95), 3),
        },
        "rolling_mad_bps": {
            "p25": round(percentile(mad, 0.25), 3),
            "p50": round(percentile(mad, 0.50), 3),
            "p75": round(percentile(mad, 0.75), 3),
        },
        "suggested": {
            "entry_min_bps": round(max(7.0, percentile(level, 0.50) + 2 * percentile(mad, 0.50)), 2),
            "entry_max_bps": round(max(10.0, percentile(level, 0.75) + 2 * percentile(mad, 0.75)), 2),
            "exit_min_bps": round(max(1.5, 0.5 * percentile(mad, 0.50)), 2),
            "exit_max_bps": round(max(3.0, 0.5 * percentile(mad, 0.75)), 2),
        },
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print("== 自适应门槛建议（只读） ==")
        print(f"样本 {summary['samples']} 条 {summary['from']} → {summary['to']}")
        print("价差分布 bps:", summary["spread_bps"])
        print("30 分钟滚动 MAD:", summary["rolling_mad_bps"])
        print("建议门槛:", summary["suggested"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())