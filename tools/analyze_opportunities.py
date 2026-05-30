import argparse
import csv
import json
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "none":
        return ""
    return text


def avg(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def median(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal("2")


def percentile(values: list[Decimal], pct: int) -> Decimal | None:
    if not values:
        return None
    if pct <= 0:
        return min(values)
    if pct >= 100:
        return max(values)
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (Decimal(len(ordered) - 1) * Decimal(pct)) / Decimal("100")
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def count_positive(values: list[Decimal]) -> int:
    return sum(1 for value in values if value > 0)


def count_negative(values: list[Decimal]) -> int:
    return sum(1 for value in values if value < 0)


def count_zero(values: list[Decimal]) -> int:
    return sum(1 for value in values if value == 0)


def win_rate(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(count_positive(values)) / Decimal(len(values)) * Decimal("100")


def fmt(value: Decimal | None, places: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{places}f}"


def write_summary_csv(path: Path, row: dict[str, Any]) -> None:
    fieldnames = list(row.keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def load_events(
    path: Path,
    date_filter: str,
    since_filter: str,
    asset_filter: set[str],
    direction_filter: set[str],
    min_entry_dev_bps: Decimal | None,
    max_holding_seconds: Decimal | None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not path.exists():
        return events

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            event = json.loads(line)
            logged_at = clean_text(event.get("logged_at", ""))
            if date_filter and not logged_at.startswith(date_filter):
                continue
            if since_filter and logged_at and logged_at < since_filter:
                continue

            asset = clean_text(event.get("asset", "")).upper()
            if asset_filter and asset not in asset_filter:
                continue
            direction = clean_text(event.get("direction", "")).lower()
            if direction_filter and direction not in direction_filter:
                continue
            if min_entry_dev_bps is not None:
                entry_dev = to_decimal(event.get("entry_spread_deviation_bps"))
                if entry_dev is None or entry_dev < min_entry_dev_bps:
                    continue
            if max_holding_seconds is not None and clean_text(event.get("event", "")) == "paper_closed":
                holding = to_decimal(event.get("holding_seconds"))
                if holding is None or holding > max_holding_seconds:
                    continue
            events.append(event)
    return events


def group_by_opportunity(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for event in events:
        opportunity_id = clean_text(event.get("opportunity_id", ""))
        if not opportunity_id:
            continue
        bucket = grouped.setdefault(opportunity_id, {"entered": None, "closed": None, "events": []})
        bucket["events"].append(event)
        event_name = clean_text(event.get("event", ""))
        if event_name == "paper_entered":
            bucket["entered"] = event
        elif event_name == "paper_closed":
            bucket["closed"] = event
    return grouped


def filter_by_exit_reason(
    grouped: dict[str, dict[str, Any]], exit_reason_filter: set[str]
) -> dict[str, dict[str, Any]]:
    if not exit_reason_filter:
        return grouped
    filtered: dict[str, dict[str, Any]] = {}
    for opportunity_id, bucket in grouped.items():
        closed = bucket.get("closed")
        if not closed:
            continue
        exit_reason = clean_text(closed.get("exit_reason", "")).lower()
        if exit_reason in exit_reason_filter:
            filtered[opportunity_id] = bucket
    return filtered


def summarize(grouped: dict[str, dict[str, Any]], fee_bps: Decimal) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "total_opportunities": len(grouped),
        "closed_opportunities": 0,
        "open_opportunities": 0,
        "exit_reasons": Counter(),
        "by_exit_reason": defaultdict(
            lambda: {
                "count": 0,
                "gross_pnl": [],
                "net_pnl": [],
                "fee_adjusted_pnl": [],
                "var_spread_cost": [],
                "holding": [],
            }
        ),
        "by_direction": defaultdict(
            lambda: {
                "count": 0,
                "gross_pnl": [],
                "net_pnl": [],
                "fee_adjusted_pnl": [],
                "var_spread_cost": [],
                "holding": [],
            }
        ),
        "by_asset": defaultdict(
            lambda: {
                "count": 0,
                "gross_pnl": [],
                "net_pnl": [],
                "fee_adjusted_pnl": [],
                "var_spread_cost": [],
                "holding": [],
            }
        ),
        "gross_pnl": [],
        "net_pnl": [],
        "fee_adjusted_pnl": [],
        "holding": [],
        "entry_deviation": [],
        "exit_deviation": [],
        "var_spread_cost": [],
    }

    for bucket in grouped.values():
        entered = bucket.get("entered")
        closed = bucket.get("closed")
        if not closed:
            summary["open_opportunities"] += 1
            continue

        summary["closed_opportunities"] += 1
        asset = clean_text(closed.get("asset", "UNKNOWN")).upper() or "UNKNOWN"
        direction = clean_text(closed.get("direction", "unknown")) or "unknown"
        exit_reason = clean_text(closed.get("exit_reason", "unknown")) or "unknown"
        summary["exit_reasons"][exit_reason] += 1

        gross_pnl = to_decimal(closed.get("entry_spread_pnl_usd"))
        pnl = to_decimal(closed.get("net_pnl_conservative_usd"))
        notional = to_decimal(closed.get("planned_notional_usd"))
        holding = to_decimal(closed.get("holding_seconds"))
        entry_deviation = to_decimal(closed.get("entry_spread_deviation_bps"))
        exit_deviation = to_decimal(closed.get("exit_spread_deviation_bps"))
        if entry_deviation is None and entered:
            entry_deviation = to_decimal(entered.get("entry_spread_deviation_bps"))

        var_spread_cost = to_decimal(closed.get("gross_var_spread_cost_usd"))
        if var_spread_cost is None:
            entry_var_spread_cost = to_decimal(closed.get("entry_var_spread_cost_usd"))
            exit_var_spread_cost = to_decimal(closed.get("exit_var_spread_cost_usd"))
            if entry_var_spread_cost is None and entered:
                entry_var_spread_cost = to_decimal(entered.get("entry_var_spread_cost_usd"))
            if entry_var_spread_cost is not None or exit_var_spread_cost is not None:
                var_spread_cost = (entry_var_spread_cost or Decimal("0")) + (exit_var_spread_cost or Decimal("0"))

        fee_usd = Decimal("0")
        if notional is not None and fee_bps > 0:
            # Two legs on entry and two legs on exit. This is only a rough sensitivity check.
            fee_usd = notional * fee_bps / Decimal("10000") * Decimal("4")
        adjusted = pnl - fee_usd if pnl is not None else None

        for target in (
            summary["by_direction"][direction],
            summary["by_asset"][asset],
            summary["by_exit_reason"][exit_reason],
        ):
            target["count"] += 1
            if gross_pnl is not None:
                target["gross_pnl"].append(gross_pnl)
            if pnl is not None:
                target["net_pnl"].append(pnl)
            if adjusted is not None:
                target["fee_adjusted_pnl"].append(adjusted)
            if var_spread_cost is not None:
                target["var_spread_cost"].append(var_spread_cost)
            if holding is not None:
                target["holding"].append(holding)

        if gross_pnl is not None:
            summary["gross_pnl"].append(gross_pnl)
        if pnl is not None:
            summary["net_pnl"].append(pnl)
        if adjusted is not None:
            summary["fee_adjusted_pnl"].append(adjusted)
        if holding is not None:
            summary["holding"].append(holding)
        if entry_deviation is not None:
            summary["entry_deviation"].append(entry_deviation)
        if exit_deviation is not None:
            summary["exit_deviation"].append(exit_deviation)
        if var_spread_cost is not None:
            summary["var_spread_cost"].append(var_spread_cost)

    return summary


def print_bucket_table(title: str, buckets: dict[str, dict[str, Any]]) -> None:
    print()
    print(title)
    if not buckets:
        print("none")
        return
    print(
        "key count win_rate_net_pct pos_net neg_net zero_net p25_net_pnl median_net_pnl p75_net_pnl "
        "avg_gross_pnl avg_var_spread_cost avg_net_pnl avg_fee_adjusted_pnl avg_holding_s median_holding_s"
    )
    for key in sorted(buckets):
        bucket = buckets[key]
        print(
            "{key} {count} {win_rate_net} {pos_net} {neg_net} {zero_net} {p25_net} {median_net} {p75_net} "
            "{avg_gross} {avg_var_cost} {avg_net} {avg_adj} {avg_holding} {median_holding}".format(
                key=key,
                count=bucket["count"],
                win_rate_net=fmt(win_rate(bucket["net_pnl"]), 2),
                pos_net=count_positive(bucket["net_pnl"]),
                neg_net=count_negative(bucket["net_pnl"]),
                zero_net=count_zero(bucket["net_pnl"]),
                p25_net=fmt(percentile(bucket["net_pnl"], 25), 6),
                median_net=fmt(median(bucket["net_pnl"]), 6),
                p75_net=fmt(percentile(bucket["net_pnl"], 75), 6),
                avg_gross=fmt(avg(bucket["gross_pnl"]), 6),
                avg_var_cost=fmt(avg(bucket["var_spread_cost"]), 6),
                avg_net=fmt(avg(bucket["net_pnl"]), 6),
                avg_adj=fmt(avg(bucket["fee_adjusted_pnl"]), 6),
                avg_holding=fmt(avg(bucket["holding"]), 3),
                median_holding=fmt(median(bucket["holding"]), 3),
            )
        )


def print_recent_closed(grouped: dict[str, dict[str, Any]], limit: int) -> None:
    closed_rows = []
    for opportunity_id, bucket in grouped.items():
        closed = bucket.get("closed")
        if not closed:
            continue
        closed_rows.append((clean_text(closed.get("exit_time", closed.get("logged_at", ""))), opportunity_id, closed))
    closed_rows.sort()
    if limit <= 0:
        return

    print()
    print(f"recent closed opportunities (last {limit})")
    print("exit_time asset direction reason holding_s entry_dev_bps exit_dev_bps gross_pnl_usd var_spread_cost_usd net_pnl_usd opportunity_id")
    for _, opportunity_id, row in closed_rows[-limit:]:
        gross_pnl = to_decimal(row.get("entry_spread_pnl_usd"))
        var_spread_cost = to_decimal(row.get("gross_var_spread_cost_usd"))
        if var_spread_cost is None:
            entry_var_spread_cost = to_decimal(row.get("entry_var_spread_cost_usd"))
            exit_var_spread_cost = to_decimal(row.get("exit_var_spread_cost_usd"))
            var_spread_cost = (entry_var_spread_cost or Decimal("0")) + (exit_var_spread_cost or Decimal("0"))
        print(
            "{exit_time} {asset} {direction} {reason} {holding} {entry_dev} {exit_dev} {gross_pnl} {var_cost} {net_pnl} {opportunity_id}".format(
                exit_time=clean_text(row.get("exit_time", "-")) or "-",
                asset=clean_text(row.get("asset", "-")) or "-",
                direction=clean_text(row.get("direction", "-")) or "-",
                reason=clean_text(row.get("exit_reason", "-")) or "-",
                holding=fmt(to_decimal(row.get("holding_seconds")), 3),
                entry_dev=fmt(to_decimal(row.get("entry_spread_deviation_bps")), 3),
                exit_dev=fmt(to_decimal(row.get("exit_spread_deviation_bps")), 3),
                gross_pnl=fmt(gross_pnl, 6),
                var_cost=fmt(var_spread_cost, 6),
                net_pnl=fmt(to_decimal(row.get("net_pnl_conservative_usd")), 6),
                opportunity_id=opportunity_id,
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze paper opportunities from opportunities.jsonl.")
    parser.add_argument(
        "--jsonl",
        default="log/opportunities.jsonl",
        help="Path to opportunities.jsonl. Default: log/opportunities.jsonl",
    )
    parser.add_argument(
        "--assets",
        default="",
        help="Optional comma-separated asset filter, e.g. BTC,SOL,ETH",
    )
    parser.add_argument(
        "--direction",
        default="",
        help="Optional comma-separated direction filter, e.g. long_var_short_lighter,short_var_long_lighter",
    )
    parser.add_argument(
        "--exit-reason",
        default="",
        help="Optional comma-separated exit reason filter, e.g. spread_reverted,timeout_exit",
    )
    parser.add_argument(
        "--date",
        default="",
        help="Optional UTC date filter, e.g. 2026-05-25",
    )
    parser.add_argument(
        "--since",
        default="",
        help="Optional inclusive UTC timestamp prefix/lower bound, e.g. 2026-05-25T15:14",
    )
    parser.add_argument(
        "--fee-bps",
        type=float,
        default=0.0,
        help="Optional rough per-leg fee bps for sensitivity check. Applied to 4 legs. Default: 0",
    )
    parser.add_argument(
        "--min-entry-dev-bps",
        type=float,
        default=None,
        help="Optional minimum entry spread deviation in bps, inclusive.",
    )
    parser.add_argument(
        "--max-holding-seconds",
        type=float,
        default=None,
        help="Optional maximum holding seconds for closed opportunities, inclusive.",
    )
    parser.add_argument(
        "--recent",
        type=int,
        default=10,
        help="Number of recent closed opportunities to print. Default: 10",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print only top-level summary metrics and skip grouped tables and recent closed rows.",
    )
    parser.add_argument(
        "--csv",
        default="",
        help="Optional CSV path to append one summary row per run.",
    )
    args = parser.parse_args()

    jsonl_path = Path(args.jsonl)
    assets = {item.strip().upper() for item in args.assets.split(",") if item.strip()}
    directions = {item.strip().lower() for item in args.direction.split(",") if item.strip()}
    exit_reasons = {item.strip().lower() for item in args.exit_reason.split(",") if item.strip()}
    fee_bps = Decimal(str(args.fee_bps))
    min_entry_dev_bps = Decimal(str(args.min_entry_dev_bps)) if args.min_entry_dev_bps is not None else None
    max_holding_seconds = Decimal(str(args.max_holding_seconds)) if args.max_holding_seconds is not None else None

    events = load_events(
        jsonl_path,
        args.date.strip(),
        args.since.strip(),
        assets,
        directions,
        min_entry_dev_bps,
        max_holding_seconds,
    )
    grouped = group_by_opportunity(events)
    grouped = filter_by_exit_reason(grouped, exit_reasons)
    summary = summarize(grouped, fee_bps)
    avg_gross_pnl = avg(summary["gross_pnl"])
    median_gross_pnl = median(summary["gross_pnl"])
    avg_net_pnl = avg(summary["net_pnl"])
    median_net_pnl = median(summary["net_pnl"])
    p25_net_pnl = percentile(summary["net_pnl"], 25)
    p75_net_pnl = percentile(summary["net_pnl"], 75)
    net_win_rate = win_rate(summary["net_pnl"])
    avg_fee_adjusted_pnl = avg(summary["fee_adjusted_pnl"])
    avg_var_spread_cost = avg(summary["var_spread_cost"])
    avg_holding = avg(summary["holding"])
    median_holding = median(summary["holding"])
    avg_entry_deviation = avg(summary["entry_deviation"])
    avg_exit_deviation = avg(summary["exit_deviation"])

    if args.csv:
        csv_row = {
            "jsonl": str(jsonl_path),
            "date_filter": args.date.strip(),
            "since_filter": args.since.strip(),
            "assets": ",".join(sorted(assets)),
            "directions": ",".join(sorted(directions)),
            "exit_reasons": ",".join(sorted(exit_reasons)),
            "fee_bps": str(fee_bps),
            "min_entry_dev_bps": "" if min_entry_dev_bps is None else str(min_entry_dev_bps),
            "max_holding_seconds": "" if max_holding_seconds is None else str(max_holding_seconds),
            "events": len(events),
            "opportunities": summary["total_opportunities"],
            "closed": summary["closed_opportunities"],
            "open": summary["open_opportunities"],
            "avg_gross_spread_pnl_usd": fmt(avg_gross_pnl, 6),
            "median_gross_spread_pnl_usd": fmt(median_gross_pnl, 6),
            "avg_net_pnl_usd": fmt(avg_net_pnl, 6),
            "median_net_pnl_usd": fmt(median_net_pnl, 6),
            "p25_net_pnl_usd": fmt(p25_net_pnl, 6),
            "p75_net_pnl_usd": fmt(p75_net_pnl, 6),
            "net_win_rate_pct": fmt(net_win_rate, 2),
            "net_positive_count": count_positive(summary["net_pnl"]),
            "net_negative_count": count_negative(summary["net_pnl"]),
            "net_zero_count": count_zero(summary["net_pnl"]),
            "avg_fee_adjusted_pnl_usd": fmt(avg_fee_adjusted_pnl, 6),
            "avg_var_spread_cost_usd": fmt(avg_var_spread_cost, 6),
            "avg_holding_seconds": fmt(avg_holding, 3),
            "median_holding_seconds": fmt(median_holding, 3),
            "avg_entry_deviation_bps": fmt(avg_entry_deviation, 3),
            "avg_exit_deviation_bps": fmt(avg_exit_deviation, 3),
        }
        write_summary_csv(Path(args.csv), csv_row)

    print(f"Source: {jsonl_path}")
    if args.date:
        print(f"Date filter: {args.date}")
    if args.since:
        print(f"Since filter: {args.since}")
    if assets:
        print(f"Asset filter: {','.join(sorted(assets))}")
    if directions:
        print(f"Direction filter: {','.join(sorted(directions))}")
    if exit_reasons:
        print(f"Exit reason filter: {','.join(sorted(exit_reasons))}")
    if min_entry_dev_bps is not None:
        print(f"Min entry deviation bps: {min_entry_dev_bps}")
    if max_holding_seconds is not None:
        print(f"Max holding seconds: {max_holding_seconds}")
    print(f"Fee sensitivity: {fee_bps} bps per leg, applied to 4 legs")
    print()
    print(f"events: {len(events)}")
    print(f"opportunities: {summary['total_opportunities']}")
    print(f"closed: {summary['closed_opportunities']}")
    print(f"open: {summary['open_opportunities']}")
    print(f"avg_gross_spread_pnl_usd: {fmt(avg_gross_pnl, 6)}")
    print(f"median_gross_spread_pnl_usd: {fmt(median_gross_pnl, 6)}")
    print(f"avg_net_pnl_usd: {fmt(avg_net_pnl, 6)}")
    print(f"median_net_pnl_usd: {fmt(median_net_pnl, 6)}")
    print(f"p25_net_pnl_usd: {fmt(p25_net_pnl, 6)}")
    print(f"p75_net_pnl_usd: {fmt(p75_net_pnl, 6)}")
    print(f"net_win_rate_pct: {fmt(net_win_rate, 2)}")
    print(f"net_positive_count: {count_positive(summary['net_pnl'])}")
    print(f"net_negative_count: {count_negative(summary['net_pnl'])}")
    print(f"net_zero_count: {count_zero(summary['net_pnl'])}")
    print(f"avg_fee_adjusted_pnl_usd: {fmt(avg_fee_adjusted_pnl, 6)}")
    print(f"avg_var_spread_cost_usd: {fmt(avg_var_spread_cost, 6)}")
    print(f"avg_holding_seconds: {fmt(avg_holding, 3)}")
    print(f"median_holding_seconds: {fmt(median_holding, 3)}")
    print(f"avg_entry_deviation_bps: {fmt(avg_entry_deviation, 3)}")
    print(f"avg_exit_deviation_bps: {fmt(avg_exit_deviation, 3)}")

    if not args.summary_only:
        print()
        print("exit reasons")
        if summary["exit_reasons"]:
            for reason, count in summary["exit_reasons"].most_common():
                print(f"{reason}: {count}")
        else:
            print("none")

        print_bucket_table("by asset", summary["by_asset"])
        print_bucket_table("by direction", summary["by_direction"])
        print_bucket_table("by exit reason", summary["by_exit_reason"])
        print_recent_closed(grouped, args.recent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
