from __future__ import annotations

import argparse
from decimal import Decimal
from pathlib import Path

from tools.replay_live_basis_params import replay


DEFAULT_INPUT = Path("log/order_metrics.jsonl")


def parse_decimals(value: str) -> list[Decimal]:
    return [Decimal(item.strip()) for item in value.split(",") if item.strip()]


def parse_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def run_grid(args: argparse.Namespace) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for z_entry in args.z_entry:
        for min_abs in args.min_abs_entry_bps:
            for min_edge in args.min_entry_edge_bps:
                for max_cost in args.max_entry_roundtrip_cost_bps:
                    for addon in args.addon_min_basis_improvement_bps:
                        for min_exit in args.min_exit_pnl_bps:
                            for max_total in args.max_total_lots:
                                replay_args = argparse.Namespace(
                                    asset=args.asset,
                                    lot_notional_usd=args.lot_notional_usd,
                                    max_total_lots=max_total,
                                    max_cycles=args.max_cycles,
                                    z_entry=z_entry,
                                    z_exit=args.z_exit,
                                    min_entry_edge_bps=min_edge,
                                    min_abs_entry_bps=min_abs,
                                    max_entry_roundtrip_cost_bps=max_cost,
                                    addon_min_basis_improvement_bps=addon,
                                    min_exit_pnl_bps=min_exit,
                                    min_hold_samples=args.min_hold_samples,
                                )
                                result = replay(args.input, replay_args)
                                realized = Decimal(str(result["realized_pnl_usd"]))
                                adjusted = realized - (Decimal(int(result["exited"])) * args.lot_notional_usd * args.adjust_shortfall_bps / Decimal("10000"))
                                rows.append(
                                    {
                                        "z_entry": z_entry,
                                        "min_abs": min_abs,
                                        "min_edge": min_edge,
                                        "max_cost": max_cost,
                                        "addon": addon,
                                        "min_exit": min_exit,
                                        "max_total": max_total,
                                        "adjusted_pnl_usd": adjusted,
                                        **result,
                                    }
                                )
    rows.sort(key=lambda row: (Decimal(str(row["adjusted_pnl_usd"])), Decimal(str(row["realized_pnl_usd"])), int(row["exited"]), -int(row["open_lots"])), reverse=True)
    return rows


def print_rows(rows: list[dict[str, object]], *, limit: int) -> None:
    print("rank z_entry min_abs min_edge max_cost addon min_exit max_total entered exited open_lots realized_pnl_usd adjusted_pnl_usd")
    for idx, row in enumerate(rows[:limit], start=1):
        print(
            idx,
            row["z_entry"],
            row["min_abs"],
            row["min_edge"],
            row["max_cost"],
            row["addon"],
            row["min_exit"],
            row["max_total"],
            row["entered"],
            row["exited"],
            row["open_lots"],
            row["realized_pnl_usd"],
            row["adjusted_pnl_usd"],
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Grid replay live basis_state rows across parameter combinations.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--lot-notional-usd", type=Decimal, default=Decimal("20"))
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--z-entry", type=parse_decimals, default=parse_decimals("3,4"))
    parser.add_argument("--z-exit", type=Decimal, default=Decimal("999"))
    parser.add_argument("--min-entry-edge-bps", type=parse_decimals, default=parse_decimals("7,8"))
    parser.add_argument("--min-abs-entry-bps", type=parse_decimals, default=parse_decimals("10,12,14"))
    parser.add_argument("--max-entry-roundtrip-cost-bps", type=parse_decimals, default=parse_decimals("3,4"))
    parser.add_argument("--addon-min-basis-improvement-bps", type=parse_decimals, default=parse_decimals("3,4,5"))
    parser.add_argument("--min-exit-pnl-bps", type=parse_decimals, default=parse_decimals("0.5,1,1.5"))
    parser.add_argument("--max-total-lots", type=parse_ints, default=parse_ints("1,2"))
    parser.add_argument("--min-hold-samples", type=int, default=0)
    parser.add_argument("--adjust-shortfall-bps", type=Decimal, default=Decimal("0"), help="Subtract this bps per exited lot from replay PnL for actual-vs-estimated shortfall stress testing.")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    print_rows(run_grid(args), limit=args.limit)


if __name__ == "__main__":
    main()
