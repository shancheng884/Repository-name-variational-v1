#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


DIRECTION_LONG = "long_var_short_lighter"
DIRECTION_SHORT = "short_var_long_lighter"


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def fmt(value: Decimal | None, places: str = "0.01") -> str:
    if value is None:
        return "-"
    return format(value.quantize(Decimal(places)), "f")


def tail_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=max(1, limit))
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return list(rows)


def max_decimal(values: list[Decimal | None]) -> Decimal | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def avg(values: list[Decimal]) -> Decimal | None:
    return sum(values) / Decimal(len(values)) if values else None


@dataclass
class AssetStats:
    asset: str
    samples: int = 0
    latest_sample_index: int | None = None
    latest_logged_at: str | None = None
    latest_reason: str | None = None
    latest_stablecoin_basis_bps: Decimal | None = None
    latest_required_raw_edge_bps: Decimal | None = None
    latest_long_raw_edge_bps: Decimal | None = None
    latest_short_raw_edge_bps: Decimal | None = None
    latest_long_normalized_edge_bps: Decimal | None = None
    latest_short_normalized_edge_bps: Decimal | None = None
    latest_long_lighter_slippage_bps: Decimal | None = None
    latest_short_lighter_slippage_bps: Decimal | None = None
    latest_lighter_book_age_seconds: Decimal | None = None
    regime_hits: int = 0
    positive_normalized_samples: int = 0
    raw_margins: list[Decimal] = field(default_factory=list)
    normalized_edges: list[Decimal] = field(default_factory=list)
    actual_pnls: list[Decimal] = field(default_factory=list)
    manual_reviews: int = 0
    final_fill_misses: int = 0
    reasons: Counter[str] = field(default_factory=Counter)

    def update_basis_state(self, row: dict[str, Any]) -> None:
        self.samples += 1
        self.latest_logged_at = str(row.get("logged_at") or "") or self.latest_logged_at
        sample_index = row.get("sample_index")
        if isinstance(sample_index, int):
            self.latest_sample_index = sample_index

        self.latest_reason = str(row.get("stablecoin_regime_reason") or "unknown")
        self.reasons[self.latest_reason] += 1
        self.latest_stablecoin_basis_bps = to_decimal(row.get("stablecoin_basis_bps"))
        self.latest_required_raw_edge_bps = to_decimal(row.get("stablecoin_regime_required_raw_edge_bps"))
        self.latest_long_raw_edge_bps = to_decimal(row.get("long_edge_bps"))
        self.latest_short_raw_edge_bps = to_decimal(row.get("short_edge_bps"))
        self.latest_long_normalized_edge_bps = to_decimal(row.get("normalized_long_edge_bps"))
        self.latest_short_normalized_edge_bps = to_decimal(row.get("normalized_short_edge_bps"))
        self.latest_lighter_book_age_seconds = to_decimal(row.get("lighter_book_age_seconds"))

        long_depth = row.get("long_entry_lighter_depth") if isinstance(row.get("long_entry_lighter_depth"), dict) else {}
        short_depth = row.get("short_entry_lighter_depth") if isinstance(row.get("short_entry_lighter_depth"), dict) else {}
        self.latest_long_lighter_slippage_bps = to_decimal(long_depth.get("slippage_bps"))
        self.latest_short_lighter_slippage_bps = to_decimal(short_depth.get("slippage_bps"))

        if row.get("long_stablecoin_regime_ok") or row.get("short_stablecoin_regime_ok"):
            self.regime_hits += 1

        best_normalized = max_decimal([
            self.latest_long_normalized_edge_bps,
            self.latest_short_normalized_edge_bps,
        ])
        if best_normalized is not None:
            self.normalized_edges.append(best_normalized)
            if best_normalized > 0:
                self.positive_normalized_samples += 1

        best_raw = max_decimal([
            self.latest_long_raw_edge_bps,
            self.latest_short_raw_edge_bps,
        ])
        if best_raw is not None and self.latest_required_raw_edge_bps is not None:
            self.raw_margins.append(best_raw - self.latest_required_raw_edge_bps)

    def score(self) -> Decimal:
        score = Decimal("0")
        score += Decimal(self.regime_hits) * Decimal("5")
        score += Decimal(self.positive_normalized_samples) * Decimal("0.5")

        raw_margin_avg = avg(self.raw_margins[-50:])
        if raw_margin_avg is not None:
            score += raw_margin_avg * Decimal("1.5")

        normalized_avg = avg(self.normalized_edges[-50:])
        if normalized_avg is not None:
            score += normalized_avg

        actual_avg = avg(self.actual_pnls[-20:])
        if actual_avg is not None:
            score += actual_avg * Decimal("2")

        latest_slippage = max_decimal([
            self.latest_long_lighter_slippage_bps,
            self.latest_short_lighter_slippage_bps,
        ])
        if latest_slippage is not None:
            score -= latest_slippage * Decimal("2")

        if self.latest_lighter_book_age_seconds is not None and self.latest_lighter_book_age_seconds > Decimal("1"):
            score -= Decimal("5")

        score -= Decimal(self.manual_reviews) * Decimal("8")
        score -= Decimal(self.final_fill_misses) * Decimal("12")
        return score

    def best_direction(self) -> str:
        long_edge = self.latest_long_normalized_edge_bps
        short_edge = self.latest_short_normalized_edge_bps
        if long_edge is None and short_edge is None:
            long_edge = self.latest_long_raw_edge_bps
            short_edge = self.latest_short_raw_edge_bps
        if long_edge is not None and (short_edge is None or long_edge >= short_edge):
            return DIRECTION_LONG
        if short_edge is not None:
            return DIRECTION_SHORT
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rank live basis assets from order_metrics.jsonl without touching the live trading process."
    )
    parser.add_argument("--file", default="log/order_metrics.jsonl", help="Path to order_metrics.jsonl")
    parser.add_argument("--tail", type=int, default=50000, help="Read only the last N JSONL rows")
    parser.add_argument("--min-samples", type=int, default=30, help="Minimum basis_state samples before an asset is eligible")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        raise SystemExit(f"missing log file: {path}")

    stats: dict[str, AssetStats] = {}
    rows = tail_jsonl(path, args.tail)
    for row in rows:
        asset = str(row.get("asset") or "").upper()
        if not asset:
            continue
        item = stats.setdefault(asset, AssetStats(asset=asset))
        event = row.get("event")
        if event == "live_inventory_basis_state":
            item.update_basis_state(row)
        elif event == "live_inventory_actual_pnl":
            pnl = to_decimal(row.get("actual_pnl_bps"))
            if pnl is not None:
                item.actual_pnls.append(pnl)
        elif event == "live_inventory_manual_review_required":
            item.manual_reviews += 1
            reason = str(row.get("reason") or row.get("manual_review_reason") or "manual_review")
            if "final_fill_not_confirmed" in reason:
                item.final_fill_misses += 1

    ranked = sorted(stats.values(), key=lambda item: item.score(), reverse=True)
    eligible = [item for item in ranked if item.samples >= args.min_samples]
    recommendation = eligible[0].asset if eligible else None

    print(f"rows={len(rows)} assets={len(stats)} min_samples={args.min_samples}")
    print(f"recommendation={recommendation or '-'}")
    print()
    print(
        "asset score samples regime_hits pos_norm best_dir raw_margin_avg norm_avg actual_avg "
        "stablecoin_bps required_raw latest_reason"
    )
    for item in ranked:
        raw_margin_avg = avg(item.raw_margins[-50:])
        normalized_avg = avg(item.normalized_edges[-50:])
        actual_avg = avg(item.actual_pnls[-20:])
        print(
            f"{item.asset} {fmt(item.score())} {item.samples} {item.regime_hits} "
            f"{item.positive_normalized_samples} {item.best_direction()} {fmt(raw_margin_avg)} "
            f"{fmt(normalized_avg)} {fmt(actual_avg)} {fmt(item.latest_stablecoin_basis_bps)} "
            f"{fmt(item.latest_required_raw_edge_bps)} {item.latest_reason or '-'}"
        )

    if recommendation:
        print()
        print(f"suggested_action=keep_or_switch_to_{recommendation}_only_after_flat_manual_confirmation")


if __name__ == "__main__":
    main()
