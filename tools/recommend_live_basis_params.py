from __future__ import annotations

import argparse
from decimal import Decimal
from pathlib import Path

from tools.analyze_live_basis_slippage import summarize as summarize_slippage
from tools.grid_replay_live_basis_params import parse_decimals, parse_ints, run_grid


DEFAULT_INPUT = Path("log/order_metrics.jsonl")


def suggested_flags(best: dict[str, object] | None, result: dict[str, object]) -> list[str]:
    if not best:
        return []
    return [
        f"--live-inventory-basis-z-entry {best['z_entry']}",
        f"--live-inventory-basis-min-abs-entry-bps {best['min_abs']}",
        f"--live-inventory-basis-min-entry-edge-bps {best['min_edge']}",
        f"--live-inventory-basis-max-entry-roundtrip-cost-bps {best['max_cost']}",
        f"--live-inventory-basis-addon-min-basis-improvement-bps {best['addon']}",
        f"--live-inventory-basis-min-exit-pnl-bps {best['min_exit']}",
        f"--live-inventory-basis-exit-safety-buffer-bps {result['suggested_exit_safety_buffer_bps']}",
    ]


def recommend(args: argparse.Namespace) -> dict[str, object]:
    slippage = summarize_slippage(args.input, asset=args.asset)
    p80 = slippage.get("p80_positive_shortfall_bps")
    matched_actual_exits = int(slippage["matched_exits"])
    min_actual_exits = int(getattr(args, "min_actual_exits", 3))
    if not hasattr(args, "adjust_shortfall_bps") or args.adjust_shortfall_bps is None:
        args.adjust_shortfall_bps = Decimal("0") if p80 is None else Decimal(str(p80))
    grid_rows = run_grid(args)
    candidates = [row for row in grid_rows if int(row["entered"]) > 0 and int(row["exited"]) > 0 and int(row["open_lots"]) == 0]
    best = candidates[0] if candidates else None
    suggested_exit_buffer = Decimal("0") if p80 is None else Decimal(str(p80)).quantize(Decimal("0.1"))
    warnings = []
    if matched_actual_exits < min_actual_exits:
        warnings.append("sample_too_small")
        warnings.append("p80_shortfall_unreliable")
    if not candidates:
        warnings.append("do_not_trade")
    action = "do_not_trade" if not candidates else "review_manually"
    if candidates and matched_actual_exits < min_actual_exits:
        action = "use_conservative_defaults_or_wait_for_more_samples"
    result = {
        "matched_actual_exits": matched_actual_exits,
        "min_actual_exits": min_actual_exits,
        "p80_positive_shortfall_bps": p80,
        "suggested_exit_safety_buffer_bps": suggested_exit_buffer,
        "candidate_count": len(candidates),
        "warnings": warnings,
        "suggested_action": action,
        "best": best,
    }
    flags = suggested_flags(best, result)
    result["suggested_flags"] = flags
    result["suggested_flags_one_line"] = " ".join(flags)
    return result


def print_recommendation(result: dict[str, object]) -> None:
    print(f"matched_actual_exits: {result['matched_actual_exits']}")
    print(f"min_actual_exits: {result['min_actual_exits']}")
    print(f"p80_positive_shortfall_bps: {result['p80_positive_shortfall_bps']}")
    print(f"suggested_exit_safety_buffer_bps: {result['suggested_exit_safety_buffer_bps']}")
    print(f"candidate_count: {result['candidate_count']}")
    print(f"warnings: {','.join(result['warnings']) if result['warnings'] else 'none'}")
    print(f"suggested_action: {result['suggested_action']}")
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
    for flag in result["suggested_flags"]:
        print(f"  {flag}")
    print(f"suggested_flags_one_line: {result['suggested_flags_one_line']}")


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
    parser.add_argument("--min-actual-exits", type=int, default=3, help="Minimum matched actual exits before p80 shortfall is treated as reliable.")
    args = parser.parse_args()
    print_recommendation(recommend(args))


if __name__ == "__main__":
    main()
