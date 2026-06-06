from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


DIRECTION_LONG = "long_var_short_lighter"
DIRECTION_SHORT = "short_var_long_lighter"
DEFAULT_THRESHOLDS = (Decimal("30"), Decimal("35"), Decimal("40"), Decimal("45"), Decimal("50"))
DEFAULT_SLIPPAGE_BPS = Decimal("100")


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def fmt(value: Decimal | None, places: int = 6) -> str:
    if value is None:
        return "-"
    return f"{value:.{places}f}"


@dataclass(slots=True)
class MarketSample:
    logged_at: str
    asset: str
    var_buy_price: Decimal
    var_sell_price: Decimal
    lighter_bid: Decimal
    lighter_ask: Decimal
    long_edge_bps: Decimal
    short_edge_bps: Decimal


@dataclass(slots=True)
class DirectionEdgeSummary:
    direction: str
    samples: int
    latest: Decimal | None
    max_edge: Decimal | None
    threshold_counts: dict[Decimal, int]
    executable_count: int
    executable_threshold_counts: dict[Decimal, int]


@dataclass(slots=True)
class LiveInventoryEdgeSummary:
    file: Path
    asset: str
    samples: int
    lot_notional_usd: Decimal
    lighter_min_base_amount: Decimal | None
    lighter_min_quote_amount: Decimal | None
    thresholds: tuple[Decimal, ...]
    by_direction: dict[str, DirectionEdgeSummary]


def load_samples(path: Path, *, asset: str, latest: int | None = None) -> list[MarketSample]:
    rows: list[MarketSample] = []
    asset = asset.upper()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("event") != "market_sample":
                continue
            if str(row.get("asset") or "").upper() != asset:
                continue
            var_buy = to_decimal(row.get("var_buy_price"))
            var_sell = to_decimal(row.get("var_sell_price"))
            lighter_bid = to_decimal(row.get("lighter_bid"))
            lighter_ask = to_decimal(row.get("lighter_ask"))
            long_edge = to_decimal(row.get("long_var_short_lighter_bps"))
            short_edge = to_decimal(row.get("short_var_long_lighter_bps"))
            if None in {var_buy, var_sell, lighter_bid, lighter_ask, long_edge, short_edge}:
                continue
            rows.append(
                MarketSample(
                    logged_at=str(row.get("logged_at") or ""),
                    asset=asset,
                    var_buy_price=var_buy,
                    var_sell_price=var_sell,
                    lighter_bid=lighter_bid,
                    lighter_ask=lighter_ask,
                    long_edge_bps=long_edge,
                    short_edge_bps=short_edge,
                )
            )
    if latest is not None and latest > 0:
        return rows[-latest:]
    return rows


def edge_for(sample: MarketSample, direction: str) -> Decimal:
    if direction == DIRECTION_LONG:
        return sample.long_edge_bps
    if direction == DIRECTION_SHORT:
        return sample.short_edge_bps
    raise ValueError(f"Unsupported direction: {direction}")


def var_entry_price_for(sample: MarketSample, direction: str) -> Decimal:
    if direction == DIRECTION_LONG:
        return sample.var_buy_price
    if direction == DIRECTION_SHORT:
        return sample.var_sell_price
    raise ValueError(f"Unsupported direction: {direction}")


def lighter_entry_price_for(sample: MarketSample, direction: str) -> Decimal:
    if direction == DIRECTION_LONG:
        return sample.lighter_bid
    if direction == DIRECTION_SHORT:
        return sample.lighter_ask
    raise ValueError(f"Unsupported direction: {direction}")


def executable_for_live_inventory(
    sample: MarketSample,
    *,
    direction: str,
    lot_notional_usd: Decimal,
    lighter_min_base_amount: Decimal | None,
    lighter_min_quote_amount: Decimal | None,
    slippage_bps: Decimal = DEFAULT_SLIPPAGE_BPS,
) -> bool:
    var_price = var_entry_price_for(sample, direction)
    if var_price <= 0:
        return False
    notional_price = var_price * (Decimal("1") + slippage_bps / Decimal("10000"))
    qty = lot_notional_usd / notional_price
    if lighter_min_base_amount is not None and qty < lighter_min_base_amount:
        return False
    lighter_notional = qty * lighter_entry_price_for(sample, direction)
    if lighter_min_quote_amount is not None and lighter_notional < lighter_min_quote_amount:
        return False
    return True


def summarize_direction(
    samples: list[MarketSample],
    *,
    direction: str,
    thresholds: tuple[Decimal, ...],
    lot_notional_usd: Decimal,
    lighter_min_base_amount: Decimal | None,
    lighter_min_quote_amount: Decimal | None,
) -> DirectionEdgeSummary:
    edges = [edge_for(sample, direction) for sample in samples]
    threshold_counts = {threshold: sum(1 for edge in edges if edge >= threshold) for threshold in thresholds}
    executable_flags = [
        executable_for_live_inventory(
            sample,
            direction=direction,
            lot_notional_usd=lot_notional_usd,
            lighter_min_base_amount=lighter_min_base_amount,
            lighter_min_quote_amount=lighter_min_quote_amount,
        )
        for sample in samples
    ]
    executable_threshold_counts = {
        threshold: sum(
            1
            for sample, is_executable in zip(samples, executable_flags, strict=True)
            if is_executable and edge_for(sample, direction) >= threshold
        )
        for threshold in thresholds
    }
    return DirectionEdgeSummary(
        direction=direction,
        samples=len(samples),
        latest=edges[-1] if edges else None,
        max_edge=max(edges) if edges else None,
        threshold_counts=threshold_counts,
        executable_count=sum(1 for flag in executable_flags if flag),
        executable_threshold_counts=executable_threshold_counts,
    )


def summarize_edges(
    samples: list[MarketSample],
    *,
    file: Path,
    asset: str,
    lot_notional_usd: Decimal,
    lighter_min_base_amount: Decimal | None,
    lighter_min_quote_amount: Decimal | None,
    thresholds: tuple[Decimal, ...] = DEFAULT_THRESHOLDS,
) -> LiveInventoryEdgeSummary:
    by_direction = {
        direction: summarize_direction(
            samples,
            direction=direction,
            thresholds=thresholds,
            lot_notional_usd=lot_notional_usd,
            lighter_min_base_amount=lighter_min_base_amount,
            lighter_min_quote_amount=lighter_min_quote_amount,
        )
        for direction in (DIRECTION_LONG, DIRECTION_SHORT)
    }
    return LiveInventoryEdgeSummary(
        file=file,
        asset=asset.upper(),
        samples=len(samples),
        lot_notional_usd=lot_notional_usd,
        lighter_min_base_amount=lighter_min_base_amount,
        lighter_min_quote_amount=lighter_min_quote_amount,
        thresholds=thresholds,
        by_direction=by_direction,
    )


def print_summary(summary: LiveInventoryEdgeSummary) -> None:
    print(f"file: {summary.file}")
    print(f"asset: {summary.asset}")
    print(f"samples: {summary.samples}")
    print(f"lot_notional_usd: {summary.lot_notional_usd}")
    print(f"lighter_min_base_amount: {summary.lighter_min_base_amount or '-'}")
    print(f"lighter_min_quote_amount: {summary.lighter_min_quote_amount or '-'}")
    print()
    for direction, row in summary.by_direction.items():
        print(f"direction: {direction}")
        print(f"  latest_bps: {fmt(row.latest)}")
        print(f"  max_bps: {fmt(row.max_edge)}")
        print(f"  executable_count: {row.executable_count}")
        print("  threshold_counts:")
        for threshold in summary.thresholds:
            print(f"    >= {threshold}bps: {row.threshold_counts[threshold]}")
        print("  executable_threshold_counts:")
        for threshold in summary.thresholds:
            print(f"    >= {threshold}bps: {row.executable_threshold_counts[threshold]}")
        print()


def parse_thresholds(raw: str) -> tuple[Decimal, ...]:
    values = tuple(to_decimal(item.strip()) for item in raw.split(",") if item.strip())
    if any(value is None for value in values) or not values:
        raise argparse.ArgumentTypeError("thresholds must be comma-separated decimals")
    return tuple(sorted(value for value in values if value is not None))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze live inventory entry edge distribution from market_samples.jsonl.")
    parser.add_argument("--market-samples", type=Path, default=Path("log/market_samples.jsonl"))
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--latest", type=int, default=1000)
    parser.add_argument("--lot-notional-usd", type=str, default="15")
    parser.add_argument("--lighter-min-base-amount", type=str, default="0.00020")
    parser.add_argument("--lighter-min-quote-amount", type=str, default="10")
    parser.add_argument("--thresholds", type=parse_thresholds, default=DEFAULT_THRESHOLDS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.market_samples.exists():
        raise SystemExit(f"market samples not found: {args.market_samples}")
    lot_notional = to_decimal(args.lot_notional_usd)
    if lot_notional is None or lot_notional <= 0:
        raise SystemExit("--lot-notional-usd must be > 0")
    min_base = to_decimal(args.lighter_min_base_amount)
    min_quote = to_decimal(args.lighter_min_quote_amount)
    samples = load_samples(args.market_samples, asset=args.asset, latest=args.latest)
    print_summary(
        summarize_edges(
            samples,
            file=args.market_samples,
            asset=args.asset,
            lot_notional_usd=lot_notional,
            lighter_min_base_amount=min_base,
            lighter_min_quote_amount=min_quote,
            thresholds=args.thresholds,
        )
    )


if __name__ == "__main__":
    main()
