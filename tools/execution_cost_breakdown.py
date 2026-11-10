#!/usr/bin/env python3
"""Summarise entry execution cost from log/execution_measurements.jsonl."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * q))
    return ordered[index]


def summarise(path: Path) -> int:
    if not path.exists():
        print(f"没有数据文件: {path}")
        return 2
    rows = []
    for line in path.read_text(encoding="utf-8").strip().splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    if not rows:
        print("文件为空")
        return 2

    def collect(key: str) -> list[float]:
        out = []
        for row in rows:
            value = row.get(key)
            if value is None:
                continue
            try:
                out.append(float(value))
            except (TypeError, ValueError):
                continue
        return out

    print(f"样本数: {len(rows)}")
    for label, key in (
        ("Var 滑点", "var_slippage_bps"),
        ("Lighter 滑点", "lighter_slippage_bps"),
    ):
        values = collect(key)
        if not values:
            print(f"{label}: 无数据")
            continue
        print(
            f"{label}: n={len(values)} p50={statistics.median(values):+.2f} "
            f"p80={pct(values, 0.8):+.2f} p95={pct(values, 0.95):+.2f} "
            f"worst={max(values):+.2f} mean={statistics.mean(values):+.2f}"
        )
    for label, key in (("报价年龄", "quote_age_ms"), ("Var 提交", "var_submit_ms"), ("Lighter 提交", "lighter_submit_ms")):
        values = collect(key)
        if values:
            print(
                f"{label}: n={len(values)} p50={statistics.median(values):.0f}ms "
                f"p80={pct(values, 0.8):.0f}ms p95={pct(values, 0.95):.0f}ms"
            )
    total = []
    for row in rows:
        try:
            total.append(float(row["var_slippage_bps"]) + float(row["lighter_slippage_bps"]))
        except (KeyError, TypeError, ValueError):
            continue
    if total:
        print(
            f"两腿合计: n={len(total)} p50={statistics.median(total):+.2f} "
            f"p80={pct(total, 0.8):+.2f} p95={pct(total, 0.95):+.2f} worst={max(total):+.2f}"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        default=str(ROOT / "log" / "execution_measurements.jsonl"),
    )
    return summarise(Path(parser.parse_args().path))


if __name__ == "__main__":
    raise SystemExit(main())
