import argparse
import csv
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def fmt(value: Decimal | None, places: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{places}f}"


def load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def sort_key(row: dict[str, str], metric: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    primary = to_decimal(row.get(metric)) or Decimal("-999999")
    p25 = to_decimal(row.get("p25_net_pnl_usd")) or Decimal("-999999")
    win_rate = to_decimal(row.get("net_win_rate_pct")) or Decimal("-999999")
    closed = to_decimal(row.get("closed")) or Decimal("-999999")
    return (primary, p25, win_rate, closed)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare exported opportunity analysis summaries.")
    parser.add_argument(
        "--csv",
        default="log/opportunity_analysis_summary.csv",
        help="Path to summary CSV. Default: log/opportunity_analysis_summary.csv",
    )
    parser.add_argument(
        "--assets",
        default="",
        help="Optional exact assets filter, e.g. BTC or BTC,SOL",
    )
    parser.add_argument(
        "--direction",
        default="",
        help="Optional exact directions filter, e.g. short_var_long_lighter",
    )
    parser.add_argument(
        "--since",
        default="",
        help="Optional exact since_filter match.",
    )
    parser.add_argument(
        "--fee-bps",
        type=float,
        default=None,
        help="Optional exact fee_bps match.",
    )
    parser.add_argument(
        "--metric",
        choices=["avg_fee_adjusted_pnl_usd", "avg_net_pnl_usd", "p25_net_pnl_usd", "net_win_rate_pct"],
        default="avg_fee_adjusted_pnl_usd",
        help="Primary ranking metric. Default: avg_fee_adjusted_pnl_usd",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Number of top rows to print. Default: 20",
    )
    args = parser.parse_args()

    rows = load_rows(Path(args.csv))
    assets_filter = clean_text(args.assets)
    direction_filter = clean_text(args.direction)
    since_filter = clean_text(args.since)
    fee_filter = None if args.fee_bps is None else clean_text(Decimal(str(args.fee_bps)))

    filtered: list[dict[str, str]] = []
    for row in rows:
        if assets_filter and clean_text(row.get("assets")) != assets_filter:
            continue
        if direction_filter and clean_text(row.get("directions")) != direction_filter:
            continue
        if since_filter and clean_text(row.get("since_filter")) != since_filter:
            continue
        if fee_filter is not None and clean_text(row.get("fee_bps")) != fee_filter:
            continue
        filtered.append(row)

    filtered.sort(key=lambda row: sort_key(row, args.metric), reverse=True)

    print(f"Source: {args.csv}")
    print(f"Rows: {len(filtered)}")
    print(f"Metric: {args.metric}")
    print()
    print(
        "rank assets directions since fee_bps min_entry_dev_bps max_holding_seconds closed "
        "win_rate avg_fee_adj avg_net p25_net median_net avg_hold"
    )
    for idx, row in enumerate(filtered[: max(0, args.top)], start=1):
        print(
            "{rank} {assets} {directions} {since} {fee_bps} {min_entry} {max_holding} {closed} {win_rate} {avg_fee_adj} {avg_net} {p25_net} {median_net} {avg_hold}".format(
                rank=idx,
                assets=clean_text(row.get("assets")) or "-",
                directions=clean_text(row.get("directions")) or "-",
                since=clean_text(row.get("since_filter")) or "-",
                fee_bps=clean_text(row.get("fee_bps")) or "-",
                min_entry=clean_text(row.get("min_entry_dev_bps")) or "-",
                max_holding=clean_text(row.get("max_holding_seconds")) or "-",
                closed=clean_text(row.get("closed")) or "-",
                win_rate=fmt(to_decimal(row.get("net_win_rate_pct")), 2),
                avg_fee_adj=fmt(to_decimal(row.get("avg_fee_adjusted_pnl_usd")), 6),
                avg_net=fmt(to_decimal(row.get("avg_net_pnl_usd")), 6),
                p25_net=fmt(to_decimal(row.get("p25_net_pnl_usd")), 6),
                median_net=fmt(to_decimal(row.get("median_net_pnl_usd")), 6),
                avg_hold=fmt(to_decimal(row.get("avg_holding_seconds")), 3),
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
