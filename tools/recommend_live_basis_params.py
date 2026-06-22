from __future__ import annotations

import argparse
from decimal import Decimal
from pathlib import Path

from tools.analyze_live_basis_slippage import summarize as summarize_slippage
from tools.grid_replay_live_basis_params import parse_decimals, parse_ints, run_grid


DEFAULT_INPUT = Path("log/order_metrics.jsonl")


def recommend(args: argparse.Namespace) -> dict[str, object]:
    slippage = summarize_slippage(args.input, asset=args.asset)
    p80 = slippage.get("p80_positive_shortfall_bps")
    if not hasattr(args, "adjust_shortfall_bps") or args.adjust_shortfall_bps is None:
        args.adjust_shortfall_bps = Decimal("0") if p80 is None else Decimal(str(p80))
    grid_rows = run_grid(args)
    candidates = [row for row in grid_rows if int(row["entered"]) > 0 and int(row["exited"]) > 0 and int(row["open_lots"]) == 0]
    best = candidates[0] if candidates else None
    suggested_exit_buffer = Decimal("0") if p80 is None else Decimal(str(p80)).quantize(Decimal("0.1"))
    return {
        "matched_actual_exits": slippage["matched_exits"],
        "p80_positive_shortfall_bps": p80,
        "suggested_exit_safety_buffer_bps": suggested_exit_buffer,
        "candidate_count": len(candidates),
        "best": best,
    }


def print_recommendation(result: dict[str, object]) -> None:
    print(f"matched_actual_exits: {result['matched_actual_exits']}")
    print(f"p80_positive_shortfall_bps: {result['p80_positive_shortfall_bps']}")
    print(f"suggested_exit_safety_buffer_bps: {result['suggested_exit_safety_buffer_bps']}")
    print(f"candidate_count: {result['candidate_count']}")
    best = result.get("best")
    if not best:
        print("best: n/a")
        print("reason: no grid candidate had entered>0, exited>0, open_lots=0")
        return
    assert isinstance(best, dict)
    print("best:")
    for key in ("z_entry", "min_abs", "min_edge", "max_cost", "addon", "min_exit", "max_total", "entered", "exited", "realized_pnl_usd"):
        print(f"  {key}: {best[key]}")
    print("suggested_flags:")
    print(f"  --live-inventory-basis-z-entry {best['z_entry']}")
    print(f"  --live-inventory-basis-min-abs-entry-bps {best['min_abs']}")
    print(f"  --live-inventory-basis-min-entry-edge-bps {best['min_edge']}")
    print(f"  --live-inventory-basis-max-entry-roundtrip-cost-bps {best['max_cost']}")
    print(f"  --live-inventory-basis-addon-min-basis-improvement-bps {best['addon']}")
    print(f"  --live-inventory-basis-min-exit-pnl-bps {best['min_exit']}")
    print(f"  --live-inventory-basis-exit-safety-buffer-bps {result['suggested_exit_safety_buffer_bps']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Recommend live basis parameters from slippage stats and grid replay.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--lot-notional-usd", type=Decimal, default=Decimal("20"))
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--z-entry", type=parse_decimals, default=parse_decimals("3,4,5"))
    parser.add_argument("--z-exit", type=Decimal, default=Decimal("999"))
    parser.add_argument("--min-entry-edge-bps", type=parse_decimals, default=parse_decimals("7,8,10"))
    parser.add_argument("--min-abs-entry-bps", type=parse_decimals, default=parse_decimals("10,12,14,16"))
    parser.add_argument("--max-entry-roundtrip-cost-bps", type=parse_decimals, default=parse_decimals("3,4"))
    parser.add_argument("--addon-min-basis-improvement-bps", type=parse_decimals, default=parse_decimals("3,4,5"))
    parser.add_argument("--min-exit-pnl-bps", type=parse_decimals, default=parse_decimals("0.5,1,1.5,2"))
    parser.add_argument("--max-total-lots", type=parse_ints, default=parse_ints("1,2"))
    parser.add_argument("--min-hold-samples", type=int, default=0)
    parser.add_argument("--adjust-shortfall-bps", type=Decimal, default=None, help="Replay stress shortfall bps per exited lot. Default: p80 positive shortfall from actual exits.")
    args = parser.parse_args()
    print_recommendation(recommend(args))


if __name__ == "__main__":
    main()
