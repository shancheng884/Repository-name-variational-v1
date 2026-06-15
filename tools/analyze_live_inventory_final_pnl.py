from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


DIRECTION_LONG = "long_var_short_lighter"
DIRECTION_SHORT = "short_var_long_lighter"


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def fmt(value: Decimal | None, places: int = 8) -> str:
    if value is None:
        return "-"
    return f"{value:.{places}f}"


def var_sides_for(direction: str) -> tuple[str, str]:
    if direction == DIRECTION_LONG:
        return "BUY", "SELL"
    if direction == DIRECTION_SHORT:
        return "SELL", "BUY"
    raise ValueError(f"Unsupported direction: {direction}")


def pair_pnl(
    *,
    direction: str,
    qty: Decimal,
    entry_var_price: Decimal,
    entry_lighter_price: Decimal,
    exit_var_price: Decimal,
    exit_lighter_price: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    if direction == DIRECTION_LONG:
        var_leg = (exit_var_price - entry_var_price) * qty
        lighter_leg = (entry_lighter_price - exit_lighter_price) * qty
    elif direction == DIRECTION_SHORT:
        var_leg = (entry_var_price - exit_var_price) * qty
        lighter_leg = (exit_lighter_price - entry_lighter_price) * qty
    else:
        raise ValueError(f"Unsupported direction: {direction}")
    return var_leg, lighter_leg, var_leg + lighter_leg


@dataclass(slots=True)
class Fill:
    logged_at: datetime
    asset: str
    side: str
    qty: Decimal | None
    price: Decimal
    role: str | None = None


@dataclass(slots=True)
class Lot:
    entry_at: datetime
    asset: str
    lot_id: int
    direction: str
    qty: Decimal
    entry_estimated_var_price: Decimal | None
    entry_estimated_lighter_price: Decimal | None
    exit_at: datetime | None = None
    exit_estimated_var_price: Decimal | None = None
    exit_estimated_lighter_price: Decimal | None = None
    estimated_pnl_usd: Decimal | None = None
    reported_actual_pnl_usd: Decimal | None = None
    entry_var_final_price: Decimal | None = None
    entry_lighter_final_price: Decimal | None = None
    exit_var_final_price: Decimal | None = None
    exit_lighter_final_price: Decimal | None = None
    final_var_leg_pnl_usd: Decimal | None = None
    final_lighter_leg_pnl_usd: Decimal | None = None
    final_pnl_usd: Decimal | None = None

    @property
    def key(self) -> str:
        return f"{self.entry_at.isoformat()}#{self.lot_id}"


def qty_matches(expected: Decimal, actual: Decimal | None, tolerance: Decimal) -> bool:
    if actual is None:
        return True
    return abs(expected - actual) <= tolerance


def choose_fill(
    fills: list[Fill],
    *,
    after: datetime,
    before: datetime | None,
    asset: str,
    side: str | None,
    qty: Decimal,
    role: str | None = None,
    tolerance: Decimal = Decimal("0.000002"),
    used: set[int],
) -> Fill | None:
    candidates: list[tuple[float, int, Fill]] = []
    for index, fill in enumerate(fills):
        if index in used:
            continue
        if fill.logged_at < after:
            continue
        if before is not None and fill.logged_at > before:
            continue
        if fill.asset.upper() != asset.upper():
            continue
        if side is not None and fill.side.upper() != side.upper():
            continue
        if role is not None and fill.role != role:
            continue
        if not qty_matches(qty, fill.qty, tolerance):
            continue
        candidates.append(((fill.logged_at - after).total_seconds(), index, fill))
    if not candidates:
        return None
    _, index, fill = min(candidates, key=lambda item: item[0])
    used.add(index)
    return fill


def load_lots(path: Path) -> tuple[list[Lot], list[Fill], list[Fill]]:
    lots: list[Lot] = []
    open_lots: list[Lot] = []
    var_fills: list[Fill] = []
    lighter_fills: list[Fill] = []

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = row.get("event")
            logged_at = parse_ts(row.get("logged_at"))
            if logged_at is None:
                continue

            if event == "live_inventory_entered":
                qty = to_decimal(row.get("qty"))
                if qty is None:
                    continue
                lot = Lot(
                    entry_at=logged_at,
                    asset=str(row.get("asset") or ""),
                    lot_id=int(row.get("lot_id") or 0),
                    direction=str(row.get("direction") or ""),
                    qty=qty,
                    entry_estimated_var_price=to_decimal(row.get("var_price")),
                    entry_estimated_lighter_price=to_decimal(row.get("lighter_price")),
                )
                lots.append(lot)
                open_lots.append(lot)
                continue

            if event == "live_inventory_exited":
                lot_id = int(row.get("lot_id") or 0)
                lot = next((item for item in reversed(open_lots) if item.lot_id == lot_id), None)
                if lot is None:
                    continue
                lot.exit_at = logged_at
                lot.exit_estimated_var_price = to_decimal(row.get("var_price"))
                lot.exit_estimated_lighter_price = to_decimal(row.get("lighter_price"))
                lot.estimated_pnl_usd = to_decimal(row.get("pnl_usd"))
                open_lots.remove(lot)
                continue

            if event == "live_inventory_actual_pnl":
                lot_id = int(row.get("lot_id") or 0)
                lot = next((item for item in reversed(lots) if item.lot_id == lot_id), None)
                if lot is not None:
                    lot.reported_actual_pnl_usd = to_decimal(row.get("actual_pnl_usd"))
                continue

            if event == "variational_fill" and row.get("synthetic_eager_fill") is False:
                price = to_decimal(row.get("variational_filled_price"))
                if price is None:
                    continue
                var_fills.append(
                    Fill(
                        logged_at=logged_at,
                        asset=str(row.get("asset") or ""),
                        side=str(row.get("side") or "").upper(),
                        qty=to_decimal(row.get("qty")),
                        price=price,
                    )
                )
                continue

            if event == "lighter_fill":
                price = to_decimal(row.get("lighter_filled_price"))
                if price is None:
                    continue
                lighter_fills.append(
                    Fill(
                        logged_at=logged_at,
                        asset=str(row.get("asset") or ""),
                        side=str(row.get("lighter_order_side") or "").upper(),
                        qty=to_decimal(row.get("qty")),
                        price=price,
                        role=row.get("auto_live_role"),
                    )
                )

    return lots, var_fills, lighter_fills


def attach_final_fills(lots: list[Lot], var_fills: list[Fill], lighter_fills: list[Fill]) -> None:
    used_var: set[int] = set()
    used_lighter: set[int] = set()
    for lot in lots:
        if lot.exit_at is None:
            continue
        entry_var_side, exit_var_side = var_sides_for(lot.direction)
        next_entry = next((other.entry_at for other in lots if other.entry_at > lot.entry_at), None)
        entry_var = choose_fill(
            var_fills,
            after=lot.entry_at,
            before=lot.exit_at,
            asset=lot.asset,
            side=entry_var_side,
            qty=lot.qty,
            used=used_var,
        )
        exit_var = choose_fill(
            var_fills,
            after=lot.exit_at,
            before=next_entry,
            asset=lot.asset,
            side=exit_var_side,
            qty=lot.qty,
            used=used_var,
        )
        entry_lighter = choose_fill(
            lighter_fills,
            after=lot.entry_at,
            before=lot.exit_at,
            asset=lot.asset,
            side=None,
            qty=lot.qty,
            role="live_inventory_entry",
            used=used_lighter,
        )
        exit_lighter = choose_fill(
            lighter_fills,
            after=lot.exit_at,
            before=next_entry,
            asset=lot.asset,
            side=None,
            qty=lot.qty,
            role="live_inventory_exit",
            used=used_lighter,
        )
        if entry_var is not None:
            lot.entry_var_final_price = entry_var.price
        if exit_var is not None:
            lot.exit_var_final_price = exit_var.price
        if entry_lighter is not None:
            lot.entry_lighter_final_price = entry_lighter.price
        if exit_lighter is not None:
            lot.exit_lighter_final_price = exit_lighter.price
        if None not in {
            lot.entry_var_final_price,
            lot.entry_lighter_final_price,
            lot.exit_var_final_price,
            lot.exit_lighter_final_price,
        }:
            var_leg, lighter_leg, pnl = pair_pnl(
                direction=lot.direction,
                qty=lot.qty,
                entry_var_price=lot.entry_var_final_price,  # type: ignore[arg-type]
                entry_lighter_price=lot.entry_lighter_final_price,  # type: ignore[arg-type]
                exit_var_price=lot.exit_var_final_price,  # type: ignore[arg-type]
                exit_lighter_price=lot.exit_lighter_final_price,  # type: ignore[arg-type]
            )
            lot.final_var_leg_pnl_usd = var_leg
            lot.final_lighter_leg_pnl_usd = lighter_leg
            lot.final_pnl_usd = pnl


def analyze(path: Path, *, latest: int | None = None) -> list[Lot]:
    lots, var_fills, lighter_fills = load_lots(path)
    attach_final_fills(lots, var_fills, lighter_fills)
    closed = [lot for lot in lots if lot.exit_at is not None]
    if latest is not None and latest > 0:
        return closed[-latest:]
    return closed


def print_summary(lots: list[Lot]) -> None:
    final_total = sum((lot.final_pnl_usd for lot in lots if lot.final_pnl_usd is not None), Decimal("0"))
    reported_total = sum(
        (lot.reported_actual_pnl_usd for lot in lots if lot.reported_actual_pnl_usd is not None), Decimal("0")
    )
    print(f"lots={len(lots)} reported_actual_total={fmt(reported_total)} final_total={fmt(final_total)}")
    for lot in lots:
        missing = [
            name
            for name, value in (
                ("entry_var", lot.entry_var_final_price),
                ("entry_lighter", lot.entry_lighter_final_price),
                ("exit_var", lot.exit_var_final_price),
                ("exit_lighter", lot.exit_lighter_final_price),
            )
            if value is None
        ]
        print(
            "lot="
            f"{lot.lot_id} at={lot.entry_at.isoformat()} direction={lot.direction} "
            f"reported={fmt(lot.reported_actual_pnl_usd)} final={fmt(lot.final_pnl_usd)} "
            f"var_leg={fmt(lot.final_var_leg_pnl_usd)} lighter_leg={fmt(lot.final_lighter_leg_pnl_usd)} "
            f"entry_var={fmt(lot.entry_var_final_price, 2)} exit_var={fmt(lot.exit_var_final_price, 2)} "
            f"entry_lighter={fmt(lot.entry_lighter_final_price, 2)} exit_lighter={fmt(lot.exit_lighter_final_price, 2)} "
            f"missing={','.join(missing) if missing else '-'}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze live inventory PnL using final Var and Lighter fills.")
    parser.add_argument("--file", type=Path, default=Path("log/order_metrics.jsonl"))
    parser.add_argument("--latest", type=int, default=10)
    args = parser.parse_args()

    print_summary(analyze(args.file, latest=args.latest))


if __name__ == "__main__":
    main()
