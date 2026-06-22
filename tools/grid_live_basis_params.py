#!/usr/bin/env python3
from __future__ import annotations

import argparse
import itertools
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from replay_live_basis_filters import fmt, load_rows, replay


def parse_decimal_list(text: str) -> list[Decimal]:
    return [Decimal(item.strip()) for item in text.split(",") if item.strip()]


def parse_int_list(text: str) -> list[int]:
    return [int(item.strip()) for item in text.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Grid search live basis filter parameters using order_metrics.jsonl.")
    parser.add_argument("--file", default="log/order_metrics.jsonl")
    parser.add_argument("--run-id")
    parser.add_argument("--tail", type=int, default=10000)
    parser.add_argument("--z-entry", default="1.5,2,2.5")
    parser.add_argument("--min-entry-edge-bps", default="5,6,7,8,10")
    parser.add_argument("--min-abs-entry-bps", default="6,7,8,10")
    parser.add_argument("--max-entry-roundtrip-cost-bps", default="3,4")
    parser.add_argument("--entry-confirm-samples", default="1,2,3")
    parser.add_argument("--max-sample-move-bps", default="3,5,8,10")
    parser.add_argument("--min-trades", type=int, default=1)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    rows = load_rows(Path(args.file), args.tail, args.run_id)
    results: list[dict[str, object]] = []
    for z_entry, edge, abs_entry, roundtrip, confirm, move in itertools.product(
        parse_decimal_list(args.z_entry),
        parse_decimal_list(args.min_entry_edge_bps),
        parse_decimal_list(args.min_abs_entry_bps),
        parse_decimal_list(args.max_entry_roundtrip_cost_bps),
        parse_int_list(args.entry_confirm_samples),
        parse_decimal_list(args.max_sample_move_bps),
    ):
        replay_args = SimpleNamespace(
            z_entry=z_entry,
            min_entry_edge_bps=edge,
            min_abs_entry_bps=abs_entry,
            max_entry_roundtrip_cost_bps=roundtrip,
            entry_confirm_samples=confirm,
            max_sample_move_bps=move,
            allow_overlap=False,
        )
        replay_result = replay(rows, replay_args)
        pnls = replay_result["matched_pnls"]
        if len(pnls) < args.min_trades:
            continue
        avg = sum(pnls) / Decimal(len(pnls)) if pnls else Decimal("0")
        wins = sum(1 for value in pnls if value > 0)
        worst = min(pnls) if pnls else Decimal("0")
        score = avg + worst / Decimal("4")
        results.append(
            {
                "score": score,
                "avg": avg,
                "worst": worst,
                "wins": wins,
                "trades": len(pnls),
                "selected": len(replay_result["selected"]),
                "z_entry": z_entry,
                "edge": edge,
                "abs_entry": abs_entry,
                "roundtrip": roundtrip,
                "confirm": confirm,
                "move": move,
            }
        )

    print(f"rows={len(rows)} tested={len(results)}")
    print("rank score avg_pnl worst_pnl win_rate matched/selected z edge abs roundtrip confirm move")
    for idx, item in enumerate(sorted(results, key=lambda row: row["score"], reverse=True)[: args.top], start=1):
        trades = int(item["trades"])
        selected = int(item["selected"])
        print(
            f"{idx} {fmt(item['score'])} {fmt(item['avg'])} {fmt(item['worst'])} "
            f"{item['wins']}/{trades} {trades}/{selected} "
            f"{item['z_entry']} {item['edge']} {item['abs_entry']} {item['roundtrip']} {item['confirm']} {item['move']}"
        )


if __name__ == "__main__":
    main()
