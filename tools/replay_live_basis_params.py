from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


DEFAULT_INPUT = Path("log/order_metrics.jsonl")


def dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def txt(value: Decimal | None) -> str:
    return "n/a" if value is None else format(value, "f")


@dataclass
class Lot:
    lot_id: int
    direction: str
    qty: Decimal
    entry_var: Decimal
    entry_lighter: Decimal
    entry_basis: Decimal
    entered_sample: int


def pair_pnl(direction: str, qty: Decimal, entry_var: Decimal, entry_lighter: Decimal, exit_var: Decimal, exit_lighter: Decimal) -> Decimal:
    if direction == "long_var_short_lighter":
        return ((exit_var - entry_var) + (entry_lighter - exit_lighter)) * qty
    if direction == "short_var_long_lighter":
        return ((entry_var - exit_var) + (exit_lighter - entry_lighter)) * qty
    return Decimal("0")


def direction_signal(direction: str, z: Decimal) -> Decimal:
    return -z if direction == "long_var_short_lighter" else z


def basis_abs_ok(direction: str, basis: Decimal, threshold: Decimal) -> bool:
    if threshold <= 0:
        return True
    return basis <= -threshold if direction == "long_var_short_lighter" else basis >= threshold


def replay(path: Path, args: argparse.Namespace) -> dict[str, Any]:
    lots: list[Lot] = []
    realized = Decimal("0")
    entered = 0
    exited = 0
    next_lot_id = 1
    rows_seen = 0
    for raw in path.read_text(encoding="utf-8").splitlines():
        if '"event": "live_inventory_basis_state"' not in raw:
            continue
        row = json.loads(raw)
        if str(row.get("asset") or "").upper() != args.asset.upper():
            continue
        sample = int(row.get("sample_index") or rows_seen)
        rows_seen += 1
        basis = dec(row.get("basis_bps"))
        z = dec(row.get("z"))
        if basis is None or z is None:
            continue
        var_bid = dec(row.get("var_bid"))
        var_ask = dec(row.get("var_ask"))
        lighter_buy = dec(row.get("lighter_buy_price"))
        lighter_sell = dec(row.get("lighter_sell_price"))
        if None in {var_bid, var_ask, lighter_buy, lighter_sell}:
            continue

        eligible: tuple[int, Lot, Decimal] | None = None
        for idx, lot in enumerate(lots):
            exit_var = var_bid if lot.direction == "long_var_short_lighter" else var_ask
            exit_lighter = lighter_buy if lot.direction == "long_var_short_lighter" else lighter_sell
            pnl = pair_pnl(lot.direction, lot.qty, lot.entry_var, lot.entry_lighter, exit_var, exit_lighter)  # type: ignore[arg-type]
            notional = lot.qty * lot.entry_var
            pnl_bps = pnl / notional * Decimal("10000") if notional else Decimal("0")
            if sample - lot.entered_sample >= args.min_hold_samples and direction_signal(lot.direction, z) <= args.z_exit and pnl_bps >= args.min_exit_pnl_bps:
                if eligible is None or pnl_bps > eligible[2]:
                    eligible = (idx, lot, pnl_bps)
        if eligible is not None:
            idx, lot, _ = eligible
            exit_var = var_bid if lot.direction == "long_var_short_lighter" else var_ask
            exit_lighter = lighter_buy if lot.direction == "long_var_short_lighter" else lighter_sell
            realized += pair_pnl(lot.direction, lot.qty, lot.entry_var, lot.entry_lighter, exit_var, exit_lighter)  # type: ignore[arg-type]
            lots.pop(idx)
            exited += 1
            continue

        if entered >= args.max_cycles and not lots:
            continue
        if len(lots) >= args.max_total_lots:
            continue
        direction: str | None = None
        entry_var: Decimal | None = None
        entry_lighter: Decimal | None = None
        roundtrip: Decimal | None = None
        edge: Decimal | None = None
        long_signal = direction_signal("long_var_short_lighter", z)
        short_signal = direction_signal("short_var_long_lighter", z)
        if long_signal >= args.z_entry:
            direction = "long_var_short_lighter"
            entry_var = var_ask
            entry_lighter = lighter_sell
            roundtrip = dec(row.get("long_roundtrip_pnl_bps"))
            edge = dec(row.get("long_edge_bps"))
        elif short_signal >= args.z_entry:
            direction = "short_var_long_lighter"
            entry_var = var_bid
            entry_lighter = lighter_buy
            roundtrip = dec(row.get("short_roundtrip_pnl_bps"))
            edge = dec(row.get("short_edge_bps"))
        if direction is None or entry_var is None or entry_lighter is None or roundtrip is None or edge is None:
            continue
        if lots:
            if any(lot.direction != direction for lot in lots):
                continue
            entry_bases = [lot.entry_basis for lot in lots]
            if direction == "long_var_short_lighter" and basis > min(entry_bases) - args.addon_min_basis_improvement_bps:
                continue
            if direction == "short_var_long_lighter" and basis < max(entry_bases) + args.addon_min_basis_improvement_bps:
                continue
        if not basis_abs_ok(direction, basis, args.min_abs_entry_bps):
            continue
        if edge < args.min_entry_edge_bps:
            continue
        if roundtrip < -args.max_entry_roundtrip_cost_bps:
            continue
        qty = args.lot_notional_usd / entry_var
        lots.append(Lot(next_lot_id, direction, qty, entry_var, entry_lighter, basis, sample))
        next_lot_id += 1
        entered += 1

    return {
        "rows_seen": rows_seen,
        "entered": entered,
        "exited": exited,
        "open_lots": len(lots),
        "realized_pnl_usd": txt(realized),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay live basis_state rows with alternate rough parameters.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--lot-notional-usd", type=Decimal, default=Decimal("20"))
    parser.add_argument("--max-total-lots", type=int, default=2)
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--z-entry", type=Decimal, default=Decimal("3"))
    parser.add_argument("--z-exit", type=Decimal, default=Decimal("999"))
    parser.add_argument("--min-entry-edge-bps", type=Decimal, default=Decimal("7"))
    parser.add_argument("--min-abs-entry-bps", type=Decimal, default=Decimal("12"))
    parser.add_argument("--max-entry-roundtrip-cost-bps", type=Decimal, default=Decimal("3"))
    parser.add_argument("--addon-min-basis-improvement-bps", type=Decimal, default=Decimal("4"))
    parser.add_argument("--min-exit-pnl-bps", type=Decimal, default=Decimal("1.5"))
    parser.add_argument("--min-hold-samples", type=int, default=0)
    args = parser.parse_args()
    for key, value in replay(args.input, args).items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
