from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


DRIFT_FIELDS = {
    "bid": "entry_var_final_vs_snapshot_bid_bps",
    "ask": "entry_var_final_vs_snapshot_ask_bps",
    "mid": "entry_var_final_vs_snapshot_mid_bps",
    "buy": "entry_var_final_vs_snapshot_buy_bps",
    "sell": "entry_var_final_vs_snapshot_sell_bps",
}


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


def median(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal("2")


@dataclass(slots=True)
class VarDiagnosticRow:
    logged_at: str
    asset: str
    lot_id: str
    direction: str
    final_pnl_bps: Decimal | None
    entry_signal_edge_bps: Decimal | None
    entry_final_edge_bps: Decimal | None
    entry_edge_capture_loss_bps: Decimal | None
    entry_var_fill_drift_bps: Decimal | None
    entry_lighter_fill_drift_bps: Decimal | None
    var_full_spread_bps: Decimal | None
    drift_by_price: dict[str, Decimal]

    @property
    def closest_snapshot_price(self) -> str | None:
        if not self.drift_by_price:
            return None
        return min(self.drift_by_price, key=lambda key: abs(self.drift_by_price[key]))

    @property
    def closest_snapshot_drift_bps(self) -> Decimal | None:
        closest = self.closest_snapshot_price
        if closest is None:
            return None
        return self.drift_by_price[closest]

    @property
    def expected_entry_snapshot_price(self) -> str | None:
        if self.direction == "long_var_short_lighter":
            return "buy"
        if self.direction == "short_var_long_lighter":
            return "sell"
        return None

    @property
    def expected_entry_snapshot_drift_bps(self) -> Decimal | None:
        expected = self.expected_entry_snapshot_price
        if expected is None:
            return None
        return self.drift_by_price.get(expected)


def load_rows(path: Path, *, latest: int | None = None) -> list[VarDiagnosticRow]:
    rows: list[VarDiagnosticRow] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("event") != "live_inventory_final_pnl":
                continue
            drift_by_price = {
                label: drift
                for label, field in DRIFT_FIELDS.items()
                if (drift := to_decimal(row.get(field))) is not None
            }
            rows.append(
                VarDiagnosticRow(
                    logged_at=str(row.get("logged_at") or ""),
                    asset=str(row.get("asset") or ""),
                    lot_id=str(row.get("lot_id") or ""),
                    direction=str(row.get("direction") or ""),
                    final_pnl_bps=to_decimal(row.get("final_pnl_bps")),
                    entry_signal_edge_bps=to_decimal(row.get("entry_signal_edge_bps")),
                    entry_final_edge_bps=to_decimal(row.get("entry_final_edge_bps")),
                    entry_edge_capture_loss_bps=to_decimal(row.get("entry_edge_capture_loss_bps")),
                    entry_var_fill_drift_bps=to_decimal(row.get("entry_var_fill_drift_bps")),
                    entry_lighter_fill_drift_bps=to_decimal(row.get("entry_lighter_fill_drift_bps")),
                    var_full_spread_bps=to_decimal(row.get("var_full_spread_bps")),
                    drift_by_price=drift_by_price,
                )
            )
    if latest is not None and latest > 0:
        return rows[-latest:]
    return rows


def print_summary(rows: list[VarDiagnosticRow]) -> None:
    print(f"rows={len(rows)}")
    if not rows:
        return

    capture_losses = [row.entry_edge_capture_loss_bps for row in rows if row.entry_edge_capture_loss_bps is not None]
    var_drifts = [row.entry_var_fill_drift_bps for row in rows if row.entry_var_fill_drift_bps is not None]
    lighter_drifts = [row.entry_lighter_fill_drift_bps for row in rows if row.entry_lighter_fill_drift_bps is not None]
    print(
        "summary "
        f"capture_loss_median_bps={fmt(median(capture_losses))} "
        f"var_drift_median_bps={fmt(median(var_drifts))} "
        f"lighter_drift_median_bps={fmt(median(lighter_drifts))}"
    )

    closest_counts: dict[str, int] = {}
    for row in rows:
        closest = row.closest_snapshot_price
        if closest is not None:
            closest_counts[closest] = closest_counts.get(closest, 0) + 1
    if closest_counts:
        counts = " ".join(f"{key}={value}" for key, value in sorted(closest_counts.items()))
        print(f"closest_snapshot_counts {counts}")

    print("rows_detail:")
    for row in rows:
        drift_parts = " ".join(f"{label}={fmt(row.drift_by_price.get(label))}" for label in DRIFT_FIELDS)
        print(
            f"at={row.logged_at} lot={row.lot_id} direction={row.direction} "
            f"final_pnl_bps={fmt(row.final_pnl_bps)} "
            f"signal_edge_bps={fmt(row.entry_signal_edge_bps)} "
            f"final_edge_bps={fmt(row.entry_final_edge_bps)} "
            f"capture_loss_bps={fmt(row.entry_edge_capture_loss_bps)} "
            f"var_drift_bps={fmt(row.entry_var_fill_drift_bps)} "
            f"lighter_drift_bps={fmt(row.entry_lighter_fill_drift_bps)} "
            f"expected={row.expected_entry_snapshot_price or '-'} "
            f"expected_drift_bps={fmt(row.expected_entry_snapshot_drift_bps)} "
            f"closest={row.closest_snapshot_price or '-'} "
            f"closest_drift_bps={fmt(row.closest_snapshot_drift_bps)} "
            f"var_full_spread_bps={fmt(row.var_full_spread_bps)} "
            f"{drift_parts}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize live inventory Var final-fill price diagnostics.")
    parser.add_argument("--file", type=Path, default=Path("log/order_metrics.jsonl"))
    parser.add_argument("--latest", type=int, default=10)
    args = parser.parse_args()

    print_summary(load_rows(args.file, latest=args.latest))


if __name__ == "__main__":
    main()
