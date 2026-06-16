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
class PendingInventoryAction:
    action: str
    direction: str
    edge_bps: Decimal
    logged_at: str
    execute_sample_index: int
    lot: InventoryLot | None = None


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
    exit_reason: str | None = None


class PaperInventoryEngine:
    def __init__(
        self,
        *,
        lot_notional_usd: Decimal,
        max_lots: int,
        entry_bps: Decimal,
        exit_bps: Decimal,
        min_hold_samples: int,
        max_total_lots: int | None = None,
        latency_samples: int = 0,
        min_exit_pnl_bps: Decimal | None = None,
        max_hold_samples: int | None = None,
        max_unrealized_loss_bps: Decimal | None = None,
    ) -> None:
        if lot_notional_usd <= 0:
            raise ValueError("lot_notional_usd must be > 0")
        if max_lots <= 0:
            raise ValueError("max_lots must be > 0")
        if max_total_lots is not None and max_total_lots <= 0:
            raise ValueError("max_total_lots must be > 0")
        if min_hold_samples < 0:
            raise ValueError("min_hold_samples must be >= 0")
        if latency_samples < 0:
            raise ValueError("latency_samples must be >= 0")
        if max_hold_samples is not None and max_hold_samples < 0:
            raise ValueError("max_hold_samples must be >= 0")
        if max_unrealized_loss_bps is not None and max_unrealized_loss_bps < 0:
            raise ValueError("max_unrealized_loss_bps must be >= 0")
        self.lot_notional_usd = lot_notional_usd
        self.max_lots = max_lots
        self.max_total_lots = max_total_lots
        self.entry_bps = entry_bps
        self.exit_bps = exit_bps
        self.min_hold_samples = min_hold_samples
        self.latency_samples = latency_samples
        self.min_exit_pnl_bps = min_exit_pnl_bps
        self.max_hold_samples = max_hold_samples
        self.max_unrealized_loss_bps = max_unrealized_loss_bps
        self.lots: dict[str, list[InventoryLot]] = {direction: [] for direction in INVENTORY_DIRECTIONS}
        self.pending_actions: dict[str, list[PendingInventoryAction]] = {direction: [] for direction in INVENTORY_DIRECTIONS}
        self.next_lot_id = 1
        self.realized_pnl_usd = Decimal("0")

    def pending_entries(self) -> int:
        return sum(1 for actions in self.pending_actions.values() for action in actions if action.action == "enter")

    def pending_exits(self, direction: str) -> int:
        return sum(1 for action in self.pending_actions[direction] if action.action == "exit")

    def can_enter(self, direction: str) -> bool:
        if len(self.lots[direction]) + sum(
            1 for action in self.pending_actions[direction] if action.action == "enter"
        ) >= self.max_lots:
            return False
        if self.max_total_lots is not None and self.open_lots() + self.pending_entries() >= self.max_total_lots:
            return False
        return True

    @staticmethod
    def close_lot(lot: InventoryLot, exit_var_price: Decimal, exit_lighter_price: Decimal) -> Decimal:
        if lot.direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
            return (exit_var_price - lot.entry_var_price) * lot.qty + (lot.entry_lighter_price - exit_lighter_price) * lot.qty
        if lot.direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
            return (lot.entry_var_price - exit_var_price) * lot.qty + (exit_lighter_price - lot.entry_lighter_price) * lot.qty
        raise ValueError(f"Unsupported direction: {lot.direction}")

    @staticmethod
    def lot_pnl_bps(lot: InventoryLot, exit_var_price: Decimal, exit_lighter_price: Decimal) -> Decimal | None:
        pnl = PaperInventoryEngine.close_lot(lot, exit_var_price, exit_lighter_price)
        notional = lot.qty * lot.entry_var_price
        return pnl / notional * Decimal("10000") if notional else None

    def exit_reason(
        self,
        *,
        lot: InventoryLot,
        edge_bps: Decimal,
        pnl_bps: Decimal | None,
        holding_samples: int,
    ) -> str | None:
        if holding_samples < self.min_hold_samples:
            return None
        if self.max_hold_samples is not None and holding_samples >= self.max_hold_samples:
            return "max_hold_samples"
        if self.max_unrealized_loss_bps is not None and pnl_bps is not None and pnl_bps <= -self.max_unrealized_loss_bps:
            return "max_unrealized_loss_bps"
        if edge_bps > self.exit_bps:
            return None
        if self.min_exit_pnl_bps is not None and (pnl_bps is None or pnl_bps < self.min_exit_pnl_bps):
            return None
        return "signal_reverted"

    def should_exit(
        self,
        *,
        lot: InventoryLot,
        edge_bps: Decimal,
        pnl_bps: Decimal | None,
        holding_samples: int,
    ) -> bool:
        return self.exit_reason(lot=lot, edge_bps=edge_bps, pnl_bps=pnl_bps, holding_samples=holding_samples) is not None

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

        pending = self.pending_actions[direction]
        ready = [action for action in pending if sample_index >= action.execute_sample_index]
        self.pending_actions[direction] = [action for action in pending if sample_index < action.execute_sample_index]
        for action in ready:
            if action.action == "exit":
                if action.lot is None:
                    raise ValueError("pending exit requires a lot")
                lot = action.lot
                if lot in lots:
                    lots.remove(lot)
                else:
                    continue
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
                        edge_bps=action.edge_bps,
                        var_price=var_exit_price,
                        lighter_price=lighter_exit_price,
                        pnl_usd=pnl,
                        pnl_bps=pnl_bps,
                        holding_samples=sample_index - lot.entered_sample_index,
                    )
                )
                continue

            qty = self.lot_notional_usd / var_entry_price
            lot = InventoryLot(
                lot_id=self.next_lot_id,
                direction=direction,
                qty=qty,
                entry_var_price=var_entry_price,
                entry_lighter_price=lighter_entry_price,
                entry_edge_bps=action.edge_bps,
                entered_at=action.logged_at,
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
                    edge_bps=action.edge_bps,
                    var_price=var_entry_price,
                    lighter_price=lighter_entry_price,
                )
            )

        if events:
            return events

        if lots and self.pending_exits(direction) == 0:
            lot = lots[0]
            holding_samples = sample_index - lot.entered_sample_index
            pnl_bps = self.lot_pnl_bps(lot, var_exit_price, lighter_exit_price)
            exit_reason = self.exit_reason(lot=lot, edge_bps=edge_bps, pnl_bps=pnl_bps, holding_samples=holding_samples)
            if exit_reason is None:
                return events
            if self.latency_samples > 0:
                self.pending_actions[direction].append(
                    PendingInventoryAction(
                        action="exit",
                        direction=direction,
                        edge_bps=edge_bps,
                        logged_at=logged_at,
                        execute_sample_index=sample_index + self.latency_samples,
                        lot=lot,
                    )
                )
                return events
            lots.pop(0)
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
                        exit_reason=exit_reason,
                    )
                )
            return events

        if edge_bps >= self.entry_bps and self.can_enter(direction):
            if self.latency_samples > 0:
                self.pending_actions[direction].append(
                    PendingInventoryAction(
                        action="enter",
                        direction=direction,
                        edge_bps=edge_bps,
                        logged_at=logged_at,
                        execute_sample_index=sample_index + self.latency_samples,
                    )
                )
                return events
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
