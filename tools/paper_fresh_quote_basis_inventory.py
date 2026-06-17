from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import time
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from inventory_engine import (  # noqa: E402
    DIRECTION_LONG_VAR_SHORT_LIGHTER,
    DIRECTION_SHORT_VAR_LONG_LIGHTER,
    PaperInventoryEngine,
)
from tools.analyze_fresh_quote_edges import latest_lighter_snapshot  # noqa: E402
from tools.analyze_var_quote_sources import DEFAULT_ENDPOINT, decimal_to_str, request_var_quote  # noqa: E402
from tools.paper_fresh_quote_inventory import FreshInventorySample, make_sample, sample_prices  # noqa: E402
from tools.paper_fresh_quote_median_inventory import open_lot_details, tradable_directions  # noqa: E402


class EwmaBasisState:
    def __init__(self, *, half_life_seconds: float, warmup_samples: int, gap_reset_seconds: float, sigma_floor_bps: float) -> None:
        if half_life_seconds <= 0:
            raise ValueError("half_life_seconds must be > 0")
        if warmup_samples <= 0:
            raise ValueError("warmup_samples must be > 0")
        if gap_reset_seconds <= 0:
            raise ValueError("gap_reset_seconds must be > 0")
        if sigma_floor_bps < 0:
            raise ValueError("sigma_floor_bps must be >= 0")
        self.half_life_seconds = half_life_seconds
        self.warmup_samples = warmup_samples
        self.gap_reset_seconds = gap_reset_seconds
        self.sigma_floor_bps = sigma_floor_bps
        self.mean: float | None = None
        self.var = 0.0
        self.seen = 0
        self.last_ts: float | None = None
        self.signal_mean: float | None = None
        self.signal_sigma: float | None = None

    @property
    def sigma(self) -> float | None:
        if self.mean is None or self.seen < 1:
            return None
        return math.sqrt(self.var)

    def update(self, ts: float, basis_bps: float) -> tuple[float, bool]:
        if self.last_ts is not None and ts - self.last_ts > self.gap_reset_seconds:
            self.mean = None
            self.var = 0.0
            self.seen = 0
        dt = ts - self.last_ts if self.last_ts is not None else None
        self.last_ts = ts

        if self.mean is None or self.seen < self.warmup_samples:
            z = 0.0
            warm = False
        else:
            raw_sigma = math.sqrt(self.var)
            if raw_sigma <= self.sigma_floor_bps:
                z = 0.0
                warm = False
            else:
                z = (basis_bps - self.mean) / raw_sigma
                warm = True

        self.signal_mean = self.mean
        self.signal_sigma = math.sqrt(self.var) if self.mean is not None else None

        if self.mean is None:
            self.mean = basis_bps
            self.var = 0.0
            self.seen = 1
        else:
            alpha = 1.0 - 0.5 ** (max(dt or 1.0, 1e-3) / self.half_life_seconds)
            diff = basis_bps - self.mean
            self.mean += alpha * diff
            self.var = (1.0 - alpha) * (self.var + alpha * diff * diff)
            self.seen += 1
        return z, warm


class EntryConfirmationState:
    def __init__(self, *, confirm_samples: int, min_z_improvement: float) -> None:
        if confirm_samples < 0:
            raise ValueError("confirm_samples must be >= 0")
        if min_z_improvement < 0:
            raise ValueError("min_z_improvement must be >= 0")
        self.confirm_samples = confirm_samples
        self.min_z_improvement = float(min_z_improvement)
        self.direction: str | None = None
        self.peak_signal = 0.0
        self.samples_since_peak = 0

    def allowed(self, *, direction: str | None, signal: float) -> bool:
        if self.confirm_samples == 0 or self.min_z_improvement == 0:
            return True
        if direction is None:
            self.direction = None
            self.peak_signal = 0.0
            self.samples_since_peak = 0
            return False
        if self.direction != direction:
            self.direction = direction
            self.peak_signal = signal
            self.samples_since_peak = 0
            return False
        if signal >= self.peak_signal:
            self.peak_signal = signal
            self.samples_since_peak = 0
            return False
        self.samples_since_peak += 1
        if self.samples_since_peak > self.confirm_samples:
            self.peak_signal = signal
            self.samples_since_peak = 0
            return False
        return self.peak_signal - signal >= self.min_z_improvement


def basis_bps(sample: FreshInventorySample) -> Decimal | None:
    var_mid = (sample.var_bid + sample.var_ask) / Decimal("2")
    lighter_mid = (sample.lighter_bid + sample.lighter_ask) / Decimal("2")
    if var_mid <= 0 or lighter_mid <= 0:
        return None
    return (var_mid - lighter_mid) / lighter_mid * Decimal("10000")


def entry_direction(z: float, *, warm: bool, z_entry: float) -> str | None:
    if not warm or abs(z) < z_entry:
        return None
    return DIRECTION_SHORT_VAR_LONG_LIGHTER if z > 0 else DIRECTION_LONG_VAR_SHORT_LIGHTER


def direction_signal(direction: str, z: float) -> Decimal:
    if direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
        return Decimal(str(z))
    if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
        return Decimal(str(-z))
    raise ValueError(f"Unsupported direction: {direction}")


def roundtrip_pnl_bps(
    *,
    direction: str,
    var_entry_price: Decimal,
    lighter_entry_price: Decimal,
    var_exit_price: Decimal,
    lighter_exit_price: Decimal,
) -> Decimal:
    if var_entry_price <= 0:
        raise ValueError("var_entry_price must be > 0")
    if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
        pnl_per_unit = (var_exit_price - var_entry_price) + (lighter_entry_price - lighter_exit_price)
    elif direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
        pnl_per_unit = (var_entry_price - var_exit_price) + (lighter_exit_price - lighter_entry_price)
    else:
        raise ValueError(f"Unsupported direction: {direction}")
    return pnl_per_unit / var_entry_price * Decimal("10000")


def entry_roundtrip_cost_allowed(
    *,
    direction: str,
    var_entry_price: Decimal,
    lighter_entry_price: Decimal,
    var_exit_price: Decimal,
    lighter_exit_price: Decimal,
    max_entry_roundtrip_cost_bps: Decimal,
) -> bool:
    if max_entry_roundtrip_cost_bps <= 0:
        return True
    pnl_bps = roundtrip_pnl_bps(
        direction=direction,
        var_entry_price=var_entry_price,
        lighter_entry_price=lighter_entry_price,
        var_exit_price=var_exit_price,
        lighter_exit_price=lighter_exit_price,
    )
    return pnl_bps >= -max_entry_roundtrip_cost_bps


def event_to_json(event: Any) -> dict[str, Any]:
    row = asdict(event)
    for key, value in list(row.items()):
        if isinstance(value, Decimal):
            row[key] = decimal_to_str(value)
    return row


def state_row(
    *,
    sample: FreshInventorySample,
    engine: PaperInventoryEngine,
    sample_index: int,
    events: list[Any],
    basis: Decimal | None,
    z: float,
    warm: bool,
    state: EwmaBasisState,
    signal_direction: str | None,
) -> dict[str, Any]:
    _, long_var_entry, long_lighter_entry, long_var_exit, long_lighter_exit = sample_prices(sample, DIRECTION_LONG_VAR_SHORT_LIGHTER)
    _, short_var_entry, short_lighter_entry, short_var_exit, short_lighter_exit = sample_prices(sample, DIRECTION_SHORT_VAR_LONG_LIGHTER)
    return {
        "event": "fresh_quote_basis_inventory_paper_state",
        "sample_index": sample_index,
        "asset": sample.asset,
        "logged_at": sample.logged_at,
        "quote_timestamp": sample.quote_timestamp,
        "quote_id": sample.quote_id,
        "quote_ms": decimal_to_str(sample.quote_ms),
        "var_bid": decimal_to_str(sample.var_bid),
        "var_ask": decimal_to_str(sample.var_ask),
        "lighter_bid": decimal_to_str(sample.lighter_bid),
        "lighter_ask": decimal_to_str(sample.lighter_ask),
        "long_edge_bps": decimal_to_str(sample.long_edge_bps),
        "short_edge_bps": decimal_to_str(sample.short_edge_bps),
        "long_roundtrip_pnl_bps": decimal_to_str(
            roundtrip_pnl_bps(
                direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
                var_entry_price=long_var_entry,
                lighter_entry_price=long_lighter_entry,
                var_exit_price=long_var_exit,
                lighter_exit_price=long_lighter_exit,
            )
        ),
        "short_roundtrip_pnl_bps": decimal_to_str(
            roundtrip_pnl_bps(
                direction=DIRECTION_SHORT_VAR_LONG_LIGHTER,
                var_entry_price=short_var_entry,
                lighter_entry_price=short_lighter_entry,
                var_exit_price=short_var_exit,
                lighter_exit_price=short_lighter_exit,
            )
        ),
        "basis_bps": decimal_to_str(basis),
        "basis_mean_bps": None if state.signal_mean is None else str(state.signal_mean),
        "basis_sigma_bps": None if state.signal_sigma is None else str(state.signal_sigma),
        "basis_seen": state.seen,
        "z": str(z),
        "warm": warm,
        "signal_direction": signal_direction,
        "open_lots": engine.open_lots(),
        "open_long_lots": engine.open_lots(DIRECTION_LONG_VAR_SHORT_LIGHTER),
        "open_short_lots": engine.open_lots(DIRECTION_SHORT_VAR_LONG_LIGHTER),
        "open_lot_details": open_lot_details(engine=engine, sample=sample, sample_index=sample_index),
        "realized_pnl_usd": decimal_to_str(engine.realized_pnl_usd),
        "actions": [event_to_json(event) for event in events],
    }


async def run(args: argparse.Namespace) -> None:
    state = EwmaBasisState(
        half_life_seconds=args.basis_half_life_seconds,
        warmup_samples=args.basis_warmup_samples,
        gap_reset_seconds=args.basis_gap_reset_seconds,
        sigma_floor_bps=args.basis_sigma_floor_bps,
    )
    entry_confirmation = EntryConfirmationState(
        confirm_samples=args.entry_confirm_samples,
        min_z_improvement=args.entry_confirm_min_z_improvement,
    )
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal(str(args.lot_notional_usd)),
        max_lots=args.max_lots,
        max_total_lots=args.max_total_lots,
        entry_bps=Decimal(str(args.z_entry)),
        exit_bps=Decimal(str(args.z_exit)),
        min_hold_samples=args.min_hold_samples,
        latency_samples=0,
        min_exit_pnl_bps=Decimal(str(args.min_exit_pnl_bps)),
        max_hold_samples=args.max_hold_samples,
        max_unrealized_loss_bps=Decimal(str(args.max_unrealized_loss_bps)),
    )
    sample_index = 0
    while True:
        snapshot = latest_lighter_snapshot(args.file, asset=args.asset, latest=args.latest)
        if snapshot is None:
            print("no_lighter_snapshot", flush=True)
            await asyncio.sleep(args.interval_seconds)
            continue
        started = time.perf_counter()
        quote = await request_var_quote(
            args.endpoint,
            asset=snapshot.asset,
            amount=Decimal(str(args.lot_notional_usd)),
            timeout_seconds=args.timeout_seconds,
        )
        quote_ms = Decimal(str((time.perf_counter() - started) * 1000))
        sample = make_sample(snapshot=snapshot, quote_message=quote, quote_ms=quote_ms)
        if sample is None:
            print(json.dumps({"event": "fresh_quote_basis_inventory_quote_failed", "ok": bool(quote.get("ok")), "result": quote.get("result")}, ensure_ascii=False, sort_keys=True), flush=True)
            await asyncio.sleep(args.interval_seconds)
            continue

        basis = basis_bps(sample)
        z = 0.0
        warm = False
        signal_direction = None
        events: list[Any] = []
        if basis is not None:
            z, warm = state.update(time.time(), float(basis))
            signal_direction = entry_direction(z, warm=warm, z_entry=args.z_entry)
            confirmed_entry_direction = signal_direction
            if signal_direction is not None:
                signal_value = float(direction_signal(signal_direction, z))
                if not entry_confirmation.allowed(direction=signal_direction, signal=signal_value):
                    confirmed_entry_direction = None
            else:
                entry_confirmation.allowed(direction=None, signal=0.0)
            for direction in tradable_directions(engine):
                active = engine.open_lots(direction) > 0
                if not active and direction != confirmed_entry_direction:
                    continue
                signal = direction_signal(direction, z)
                if not active and args.min_entry_edge_bps > 0:
                    edge = sample.long_edge_bps if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER else sample.short_edge_bps
                    if edge < Decimal(str(args.min_entry_edge_bps)):
                        continue
                _, var_entry, lighter_entry, var_exit, lighter_exit = sample_prices(sample, direction)
                if not active and not entry_roundtrip_cost_allowed(
                    direction=direction,
                    var_entry_price=var_entry,
                    lighter_entry_price=lighter_entry,
                    var_exit_price=var_exit,
                    lighter_exit_price=lighter_exit,
                    max_entry_roundtrip_cost_bps=Decimal(str(args.max_entry_roundtrip_cost_bps)),
                ):
                    continue
                events.extend(
                    engine.on_sample(
                        direction=direction,
                        edge_bps=signal,
                        var_entry_price=var_entry,
                        lighter_entry_price=lighter_entry,
                        var_exit_price=var_exit,
                        lighter_exit_price=lighter_exit,
                        logged_at=sample.logged_at,
                        sample_index=sample_index,
                    )
                )

        print(
            json.dumps(
                state_row(
                    sample=sample,
                    engine=engine,
                    sample_index=sample_index,
                    events=events,
                    basis=basis,
                    z=z,
                    warm=warm,
                    state=state,
                    signal_direction=signal_direction,
                ),
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )
        sample_index += 1
        if args.max_samples > 0 and sample_index >= args.max_samples:
            return
        await asyncio.sleep(args.interval_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Paper trade inventory using fresh quote EWMA basis z-score signals.")
    parser.add_argument("--file", type=Path, default=Path("log/market_samples.jsonl"))
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--lot-notional-usd", type=float, default=20.0)
    parser.add_argument("--z-entry", type=float, default=4.0)
    parser.add_argument("--z-exit", type=float, default=0.0)
    parser.add_argument("--min-entry-edge-bps", type=float, default=0.0)
    parser.add_argument("--max-entry-roundtrip-cost-bps", type=float, default=0.0, help="Skip new entries whose immediate round-trip loss is worse than this many bps; 0 disables the gate.")
    parser.add_argument("--entry-confirm-samples", type=int, default=0, help="Require z-score to begin reverting within this many samples before opening a new lot; 0 disables confirmation.")
    parser.add_argument("--entry-confirm-min-z-improvement", type=float, default=0.0, help="Minimum z-score improvement from the local extreme required by --entry-confirm-samples.")
    parser.add_argument("--max-lots", type=int, default=2)
    parser.add_argument("--max-total-lots", type=int, default=2)
    parser.add_argument("--min-hold-samples", type=int, default=20)
    parser.add_argument("--min-exit-pnl-bps", type=float, default=1.0)
    parser.add_argument("--max-hold-samples", type=int, default=300)
    parser.add_argument("--max-unrealized-loss-bps", type=float, default=5.0)
    parser.add_argument("--basis-half-life-seconds", type=float, default=300.0)
    parser.add_argument("--basis-warmup-samples", type=int, default=120)
    parser.add_argument("--basis-gap-reset-seconds", type=float, default=120.0)
    parser.add_argument("--basis-sigma-floor-bps", type=float, default=0.3)
    parser.add_argument("--latest", type=int, default=1000)
    parser.add_argument("--interval-seconds", type=float, default=0.5)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-samples", type=int, default=0, help="Stop after this many samples; 0 means run forever.")
    args = parser.parse_args()
    if args.lot_notional_usd <= 0:
        parser.error("--lot-notional-usd must be > 0")
    if args.z_entry <= 0:
        parser.error("--z-entry must be > 0")
    if args.z_exit < 0:
        parser.error("--z-exit must be >= 0")
    if args.min_entry_edge_bps < 0:
        parser.error("--min-entry-edge-bps must be >= 0")
    if args.max_entry_roundtrip_cost_bps < 0:
        parser.error("--max-entry-roundtrip-cost-bps must be >= 0")
    if args.entry_confirm_samples < 0:
        parser.error("--entry-confirm-samples must be >= 0")
    if args.entry_confirm_min_z_improvement < 0:
        parser.error("--entry-confirm-min-z-improvement must be >= 0")
    if args.max_lots <= 0:
        parser.error("--max-lots must be > 0")
    if args.max_total_lots <= 0:
        parser.error("--max-total-lots must be > 0")
    if args.min_hold_samples < 0:
        parser.error("--min-hold-samples must be >= 0")
    if args.min_exit_pnl_bps < 0:
        parser.error("--min-exit-pnl-bps must be >= 0")
    if args.max_hold_samples < 0:
        parser.error("--max-hold-samples must be >= 0")
    if args.max_hold_samples == 0:
        args.max_hold_samples = None
    if args.max_unrealized_loss_bps < 0:
        parser.error("--max-unrealized-loss-bps must be >= 0")
    if args.basis_half_life_seconds <= 0:
        parser.error("--basis-half-life-seconds must be > 0")
    if args.basis_warmup_samples <= 0:
        parser.error("--basis-warmup-samples must be > 0")
    if args.basis_gap_reset_seconds <= 0:
        parser.error("--basis-gap-reset-seconds must be > 0")
    if args.basis_sigma_floor_bps < 0:
        parser.error("--basis-sigma-floor-bps must be >= 0")
    if args.latest <= 0:
        parser.error("--latest must be > 0")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be > 0")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0")
    if args.max_samples < 0:
        parser.error("--max-samples must be >= 0")
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
