from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


DIRECTION_LONG_VAR_SHORT_LIGHTER = "long_var_short_lighter"
DIRECTION_SHORT_VAR_LONG_LIGHTER = "short_var_long_lighter"
INVENTORY_DIRECTIONS = (DIRECTION_LONG_VAR_SHORT_LIGHTER, DIRECTION_SHORT_VAR_LONG_LIGHTER)


@dataclass(slots=True)
class InventoryLot:
    lot_id: int
    direction: str
    qty: Decimal
    entry_var_price: Decimal
    entry_lighter_price: Decimal
    entry_edge_bps: Decimal
    entered_at: str
    entered_sample_index: int


@dataclass(slots=True)
class InventoryEvent:
    event: str
    direction: str
    lot_id: int
    qty: Decimal
    edge_bps: Decimal
    var_price: Decimal
    lighter_price: Decimal
    pnl_usd: Decimal | None = None
    pnl_bps: Decimal | None = None
    holding_samples: int | None = None


class PaperInventoryEngine:
    def __init__(
        self,
        *,
        lot_notional_usd: Decimal,
        max_lots: int,
        entry_bps: Decimal,
        exit_bps: Decimal,
        min_hold_samples: int,
    ) -> None:
        if lot_notional_usd <= 0:
            raise ValueError("lot_notional_usd must be > 0")
        if max_lots <= 0:
            raise ValueError("max_lots must be > 0")
        if min_hold_samples < 0:
            raise ValueError("min_hold_samples must be >= 0")
        self.lot_notional_usd = lot_notional_usd
        self.max_lots = max_lots
        self.entry_bps = entry_bps
        self.exit_bps = exit_bps
        self.min_hold_samples = min_hold_samples
        self.lots: dict[str, list[InventoryLot]] = {direction: [] for direction in INVENTORY_DIRECTIONS}
        self.next_lot_id = 1
        self.realized_pnl_usd = Decimal("0")

    @staticmethod
    def close_lot(lot: InventoryLot, exit_var_price: Decimal, exit_lighter_price: Decimal) -> Decimal:
        if lot.direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
            return (exit_var_price - lot.entry_var_price) * lot.qty + (lot.entry_lighter_price - exit_lighter_price) * lot.qty
        if lot.direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
            return (lot.entry_var_price - exit_var_price) * lot.qty + (exit_lighter_price - lot.entry_lighter_price) * lot.qty
        raise ValueError(f"Unsupported direction: {lot.direction}")

    def on_sample(
        self,
        *,
        direction: str,
        edge_bps: Decimal,
        var_entry_price: Decimal,
        lighter_entry_price: Decimal,
        var_exit_price: Decimal,
        lighter_exit_price: Decimal,
        logged_at: str,
        sample_index: int,
    ) -> list[InventoryEvent]:
        if direction not in INVENTORY_DIRECTIONS:
            raise ValueError(f"Unsupported direction: {direction}")

        events: list[InventoryEvent] = []
        lots = self.lots[direction]
        if lots and edge_bps <= self.exit_bps and sample_index - lots[0].entered_sample_index >= self.min_hold_samples:
            lot = lots.pop(0)
            pnl = self.close_lot(lot, var_exit_price, lighter_exit_price)
            notional = lot.qty * lot.entry_var_price
            pnl_bps = pnl / notional * Decimal("10000") if notional else None
            self.realized_pnl_usd += pnl
            events.append(
                InventoryEvent(
                    event="inventory_paper_exited",
                    direction=direction,
                    lot_id=lot.lot_id,
                    qty=lot.qty,
                    edge_bps=edge_bps,
                    var_price=var_exit_price,
                    lighter_price=lighter_exit_price,
                    pnl_usd=pnl,
                    pnl_bps=pnl_bps,
                    holding_samples=sample_index - lot.entered_sample_index,
                )
            )
            return events

        if edge_bps >= self.entry_bps and len(lots) < self.max_lots:
            qty = self.lot_notional_usd / var_entry_price
            lot = InventoryLot(
                lot_id=self.next_lot_id,
                direction=direction,
                qty=qty,
                entry_var_price=var_entry_price,
                entry_lighter_price=lighter_entry_price,
                entry_edge_bps=edge_bps,
                entered_at=logged_at,
                entered_sample_index=sample_index,
            )
            self.next_lot_id += 1
            lots.append(lot)
            events.append(
                InventoryEvent(
                    event="inventory_paper_entered",
                    direction=direction,
                    lot_id=lot.lot_id,
                    qty=qty,
                    edge_bps=edge_bps,
                    var_price=var_entry_price,
                    lighter_price=lighter_entry_price,
                )
            )
        return events

    def open_lots(self, direction: str | None = None) -> int:
        if direction is not None:
            return len(self.lots[direction])
        return sum(len(lots) for lots in self.lots.values())
