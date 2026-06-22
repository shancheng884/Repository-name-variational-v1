from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any


DEFAULT_STATE = Path("log/live_inventory_state.json")
DEFAULT_METRICS = Path("log/order_metrics.jsonl")


def dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def text(value: Decimal | None) -> str:
    return "n/a" if value is None else format(value, "f")


def load_latest_basis_state(path: Path, *, asset: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    latest: dict[str, Any] | None = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if '"event": "live_inventory_basis_state"' not in line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(row.get("asset") or "").upper() == asset.upper():
                latest = row
    return latest


def pair_pnl(direction: str, qty: Decimal, entry_var: Decimal, entry_lighter: Decimal, exit_var: Decimal, exit_lighter: Decimal) -> Decimal:
    if direction == "long_var_short_lighter":
        return ((exit_var - entry_var) + (entry_lighter - exit_lighter)) * qty
    if direction == "short_var_long_lighter":
        return ((entry_var - exit_var) + (exit_lighter - entry_lighter)) * qty
    return Decimal("0")


def inspect(state_path: Path, metrics_path: Path, *, asset: str) -> dict[str, Any]:
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    latest = load_latest_basis_state(metrics_path, asset=asset) or {}
    lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
    var_bid = dec(latest.get("var_bid"))
    var_ask = dec(latest.get("var_ask"))
    lighter_buy = dec(latest.get("lighter_buy_price"))
    lighter_sell = dec(latest.get("lighter_sell_price"))
    total_qty = Decimal("0")
    weighted_basis = Decimal("0")
    estimated_pnl = Decimal("0")
    estimated_notional = Decimal("0")
    lot_rows: list[dict[str, Any]] = []
    for lot in lots:
        qty = dec(lot.get("qty")) or Decimal("0")
        entry_basis = dec(lot.get("entry_basis_bps"))
        entry_var = dec(lot.get("entry_var_fill_price"))
        entry_lighter = dec(lot.get("entry_lighter_fill_price"))
        direction = str(lot.get("direction") or "")
        if entry_basis is not None:
            weighted_basis += entry_basis * qty
        total_qty += qty
        pnl: Decimal | None = None
        pnl_bps: Decimal | None = None
        if direction == "long_var_short_lighter" and None not in {entry_var, entry_lighter, var_bid, lighter_buy}:
            pnl = pair_pnl(direction, qty, entry_var, entry_lighter, var_bid, lighter_buy)  # type: ignore[arg-type]
        elif direction == "short_var_long_lighter" and None not in {entry_var, entry_lighter, var_ask, lighter_sell}:
            pnl = pair_pnl(direction, qty, entry_var, entry_lighter, var_ask, lighter_sell)  # type: ignore[arg-type]
        notional = qty * entry_var if entry_var is not None else Decimal("0")
        if pnl is not None:
            estimated_pnl += pnl
            estimated_notional += notional
            pnl_bps = pnl / notional * Decimal("10000") if notional else None
        lot_rows.append(
            {
                "lot_id": lot.get("lot_id"),
                "entry_kind": lot.get("entry_kind"),
                "direction": direction,
                "qty": text(qty),
                "entry_basis_bps": text(entry_basis),
                "estimated_pnl_usd": text(pnl),
                "estimated_pnl_bps": text(pnl_bps),
            }
        )
    return {
        "status": state.get("status"),
        "completed_cycles": state.get("completed_cycles"),
        "open_lots": len(lots),
        "realized_pnl_usd": state.get("realized_pnl_usd"),
        "latest_basis_bps": latest.get("basis_bps"),
        "latest_z": latest.get("z"),
        "total_qty": text(total_qty),
        "weighted_entry_basis_bps": text(weighted_basis / total_qty if total_qty else None),
        "estimated_open_pnl_usd": text(estimated_pnl if lots else None),
        "estimated_open_pnl_bps": text(estimated_pnl / estimated_notional * Decimal("10000") if estimated_notional else None),
        "lots": lot_rows,
    }


def print_report(report: dict[str, Any]) -> None:
    for key in (
        "status",
        "completed_cycles",
        "open_lots",
        "realized_pnl_usd",
        "latest_basis_bps",
        "latest_z",
        "total_qty",
        "weighted_entry_basis_bps",
        "estimated_open_pnl_usd",
        "estimated_open_pnl_bps",
    ):
        print(f"{key}: {report.get(key)}")
    print("lots:")
    for lot in report.get("lots", []):
        print(
            "  lot={lot_id} kind={entry_kind} dir={direction} qty={qty} entry_basis={entry_basis_bps} est_pnl_usd={estimated_pnl_usd} est_pnl_bps={estimated_pnl_bps}".format(
                **lot
            )
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect current live ETH basis inventory state.")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--metrics", type=Path, default=DEFAULT_METRICS)
    parser.add_argument("--asset", default="ETH")
    args = parser.parse_args()
    print_report(inspect(args.state, args.metrics, asset=args.asset))


if __name__ == "__main__":
    main()
