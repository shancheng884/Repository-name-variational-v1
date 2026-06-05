from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


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
class Sample:
    logged_at: str
    asset: str
    var_buy_price: Decimal
    var_sell_price: Decimal
    lighter_bid: Decimal
    lighter_ask: Decimal
    long_edge_bps: Decimal


@dataclass(slots=True)
class Lot:
    qty: Decimal
    entry_var_price: Decimal
    entry_lighter_price: Decimal
    entry_edge_bps: Decimal
    entered_at: str


@dataclass(slots=True)
class SimulationResult:
    samples: int
    entries: int
    exits: int
    forced_exits: int
    max_open_lots: int
    realized_pnl_usd: Decimal
    avg_pnl_bps_per_closed_lot: Decimal | None
    open_lots: int


def read_samples(path: Path, asset: str) -> list[Sample]:
    samples: list[Sample] = []
    asset = asset.upper()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
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
            edge = to_decimal(row.get("long_var_short_lighter_bps"))
            if None in {var_buy, var_sell, lighter_bid, lighter_ask, edge}:
                continue
            samples.append(
                Sample(
                    logged_at=str(row.get("logged_at") or ""),
                    asset=asset,
                    var_buy_price=var_buy,
                    var_sell_price=var_sell,
                    lighter_bid=lighter_bid,
                    lighter_ask=lighter_ask,
                    long_edge_bps=edge,
                )
            )
    return samples


def close_lot(lot: Lot, sample: Sample) -> Decimal:
    var_leg = (sample.var_sell_price - lot.entry_var_price) * lot.qty
    lighter_leg = (lot.entry_lighter_price - sample.lighter_ask) * lot.qty
    return var_leg + lighter_leg


def simulate_long_inventory(
    samples: list[Sample],
    *,
    lot_notional_usd: Decimal,
    max_lots: int,
    entry_bps: Decimal,
    exit_bps: Decimal,
) -> SimulationResult:
    lots: list[Lot] = []
    realized = Decimal("0")
    entries = 0
    exits = 0
    forced_exits = 0
    max_open = 0
    pnl_bps_values: list[Decimal] = []

    for sample in samples:
        if lots and sample.long_edge_bps <= exit_bps:
            lot = lots.pop(0)
            pnl = close_lot(lot, sample)
            realized += pnl
            exits += 1
            pnl_bps_values.append(pnl / (lot.qty * lot.entry_var_price) * Decimal("10000"))
            continue

        if sample.long_edge_bps >= entry_bps and len(lots) < max_lots:
            qty = lot_notional_usd / sample.var_buy_price
            lots.append(
                Lot(
                    qty=qty,
                    entry_var_price=sample.var_buy_price,
                    entry_lighter_price=sample.lighter_bid,
                    entry_edge_bps=sample.long_edge_bps,
                    entered_at=sample.logged_at,
                )
            )
            entries += 1
            max_open = max(max_open, len(lots))

    if samples:
        final_sample = samples[-1]
        while lots:
            lot = lots.pop(0)
            pnl = close_lot(lot, final_sample)
            realized += pnl
            forced_exits += 1
            pnl_bps_values.append(pnl / (lot.qty * lot.entry_var_price) * Decimal("10000"))

    avg_bps = sum(pnl_bps_values) / Decimal(len(pnl_bps_values)) if pnl_bps_values else None
    return SimulationResult(
        samples=len(samples),
        entries=entries,
        exits=exits,
        forced_exits=forced_exits,
        max_open_lots=max_open,
        realized_pnl_usd=realized,
        avg_pnl_bps_per_closed_lot=avg_bps,
        open_lots=0,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulate a simple layered inventory strategy from market_samples.jsonl.")
    parser.add_argument("--market-samples", default="log/market_samples.jsonl")
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--lot-notional-usd", type=str, default="50")
    parser.add_argument("--max-lots", type=int, default=10)
    parser.add_argument("--entry-bps", type=str, default="8")
    parser.add_argument("--exit-bps", type=str, default="4")
    args = parser.parse_args()

    path = Path(args.market_samples)
    if not path.exists():
        raise SystemExit(f"market samples not found: {path}")
    if args.max_lots <= 0:
        raise SystemExit("--max-lots must be > 0")

    samples = read_samples(path, args.asset)
    result = simulate_long_inventory(
        samples,
        lot_notional_usd=Decimal(args.lot_notional_usd),
        max_lots=args.max_lots,
        entry_bps=Decimal(args.entry_bps),
        exit_bps=Decimal(args.exit_bps),
    )

    print("inventory simulation: long_var_short_lighter")
    print(f"samples={result.samples}")
    print(f"entries={result.entries} exits={result.exits} forced_exits={result.forced_exits}")
    print(f"max_open_lots={result.max_open_lots}")
    print(f"realized_pnl_usd={fmt(result.realized_pnl_usd, 6)}")
    print(f"avg_pnl_bps_per_closed_lot={fmt(result.avg_pnl_bps_per_closed_lot, 3)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
