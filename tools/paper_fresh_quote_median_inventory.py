from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from collections import deque
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from inventory_engine import (  # noqa: E402
    DIRECTION_LONG_VAR_SHORT_LIGHTER,
    DIRECTION_SHORT_VAR_LONG_LIGHTER,
    INVENTORY_DIRECTIONS,
    PaperInventoryEngine,
)
from tools.analyze_fresh_quote_edges import latest_lighter_snapshot  # noqa: E402
from tools.analyze_var_quote_sources import DEFAULT_ENDPOINT, decimal_to_str, request_var_quote  # noqa: E402
from tools.paper_fresh_quote_inventory import FreshInventorySample, make_sample, sample_prices  # noqa: E402


class RollingMedian:
    def __init__(self, maxlen: int) -> None:
        if maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        self.values: deque[Decimal] = deque(maxlen=maxlen)

    def add(self, value: Decimal) -> None:
        self.values.append(value)

    def median(self) -> Decimal | None:
        if not self.values:
            return None
        return Decimal(str(statistics.median(self.values)))

    def count(self) -> int:
        return len(self.values)


class MedianState:
    def __init__(self, windows: dict[str, int]) -> None:
        self.windows = windows
        self.long_windows = {name: RollingMedian(size) for name, size in windows.items()}
        self.short_windows = {name: RollingMedian(size) for name, size in windows.items()}

    def add(self, sample: FreshInventorySample) -> None:
        for window in self.long_windows.values():
            window.add(sample.long_edge_bps)
        for window in self.short_windows.values():
            window.add(sample.short_edge_bps)

    def medians(self, direction: str) -> dict[str, Decimal | None]:
        windows = self.long_windows if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER else self.short_windows
        return {name: window.median() for name, window in windows.items()}

    def counts(self, direction: str) -> dict[str, int]:
        windows = self.long_windows if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER else self.short_windows
        return {name: window.count() for name, window in windows.items()}


def parse_windows(raw: str) -> dict[str, int]:
    windows: dict[str, int] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, sep, size = item.partition(":")
        if not sep:
            raise ValueError(f"invalid window {item!r}; expected name:size")
        value = int(size)
        if value <= 0:
            raise ValueError("window sizes must be > 0")
        windows[name.strip()] = value
    if not windows:
        raise ValueError("at least one window is required")
    return windows


def direction_edge(sample: FreshInventorySample, direction: str) -> Decimal:
    if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
        return sample.long_edge_bps
    if direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
        return sample.short_edge_bps
    raise ValueError(f"Unsupported direction: {direction}")


def median_signal(
    *,
    sample: FreshInventorySample,
    median_state: MedianState,
    direction: str,
    baseline_window: str,
    min_baseline_samples: int,
) -> tuple[Decimal | None, Decimal | None, dict[str, Decimal | None], dict[str, int]]:
    medians = median_state.medians(direction)
    counts = median_state.counts(direction)
    baseline = medians.get(baseline_window)
    if baseline is None or counts.get(baseline_window, 0) < min_baseline_samples:
        return None, baseline, medians, counts
    deviation = direction_edge(sample, direction) - baseline
    return deviation, baseline, medians, counts


def active_direction(engine: PaperInventoryEngine) -> str | None:
    long_lots = engine.open_lots(DIRECTION_LONG_VAR_SHORT_LIGHTER)
    short_lots = engine.open_lots(DIRECTION_SHORT_VAR_LONG_LIGHTER)
    if long_lots and short_lots:
        raise RuntimeError("median inventory paper has mixed long and short lots")
    if long_lots:
        return DIRECTION_LONG_VAR_SHORT_LIGHTER
    if short_lots:
        return DIRECTION_SHORT_VAR_LONG_LIGHTER
    return None


def tradable_directions(engine: PaperInventoryEngine) -> tuple[str, ...]:
    active = active_direction(engine)
    if active is not None:
        return (active,)
    return INVENTORY_DIRECTIONS


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
    median_state: MedianState,
    sample_index: int,
    events: list[Any],
    baseline_window: str,
    min_baseline_samples: int,
) -> dict[str, Any]:
    long_dev, long_baseline, long_medians, long_counts = median_signal(
        sample=sample,
        median_state=median_state,
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        baseline_window=baseline_window,
        min_baseline_samples=min_baseline_samples,
    )
    short_dev, short_baseline, short_medians, short_counts = median_signal(
        sample=sample,
        median_state=median_state,
        direction=DIRECTION_SHORT_VAR_LONG_LIGHTER,
        baseline_window=baseline_window,
        min_baseline_samples=min_baseline_samples,
    )
    long_score = long_dev if long_dev is not None else Decimal("-999999")
    short_score = short_dev if short_dev is not None else Decimal("-999999")
    best_direction = DIRECTION_LONG_VAR_SHORT_LIGHTER if long_score >= short_score else DIRECTION_SHORT_VAR_LONG_LIGHTER
    best_dev = long_dev if best_direction == DIRECTION_LONG_VAR_SHORT_LIGHTER else short_dev
    return {
        "event": "fresh_quote_median_inventory_paper_state",
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
        "long_baseline_bps": decimal_to_str(long_baseline),
        "short_baseline_bps": decimal_to_str(short_baseline),
        "long_deviation_bps": decimal_to_str(long_dev),
        "short_deviation_bps": decimal_to_str(short_dev),
        "long_medians_bps": {key: decimal_to_str(value) for key, value in long_medians.items()},
        "short_medians_bps": {key: decimal_to_str(value) for key, value in short_medians.items()},
        "long_counts": long_counts,
        "short_counts": short_counts,
        "best_direction": best_direction,
        "best_deviation_bps": decimal_to_str(best_dev),
        "open_lots": engine.open_lots(),
        "open_long_lots": engine.open_lots(DIRECTION_LONG_VAR_SHORT_LIGHTER),
        "open_short_lots": engine.open_lots(DIRECTION_SHORT_VAR_LONG_LIGHTER),
        "realized_pnl_usd": decimal_to_str(engine.realized_pnl_usd),
        "actions": [event_to_json(event) for event in events],
    }


async def run(args: argparse.Namespace) -> None:
    windows = parse_windows(args.windows)
    median_state = MedianState(windows)
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal(str(args.lot_notional_usd)),
        max_lots=args.max_lots,
        max_total_lots=args.max_total_lots,
        entry_bps=Decimal(str(args.entry_deviation_bps)),
        exit_bps=Decimal(str(args.exit_deviation_bps)),
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
            print(json.dumps({"event": "fresh_quote_median_inventory_quote_failed", "ok": bool(quote.get("ok")), "result": quote.get("result")}, ensure_ascii=False, sort_keys=True), flush=True)
            await asyncio.sleep(args.interval_seconds)
            continue

        median_state.add(sample)
        events = []
        for direction in tradable_directions(engine):
            deviation, _, _, _ = median_signal(
                sample=sample,
                median_state=median_state,
                direction=direction,
                baseline_window=args.baseline_window,
                min_baseline_samples=args.min_baseline_samples,
            )
            if deviation is None:
                continue
            _, var_entry, lighter_entry, var_exit, lighter_exit = sample_prices(sample, direction)
            events.extend(
                engine.on_sample(
                    direction=direction,
                    edge_bps=deviation,
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
                    median_state=median_state,
                    sample_index=sample_index,
                    events=events,
                    baseline_window=args.baseline_window,
                    min_baseline_samples=args.min_baseline_samples,
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
    parser = argparse.ArgumentParser(description="Paper trade inventory using fresh quote deviations from rolling median edge.")
    parser.add_argument("--file", type=Path, default=Path("log/market_samples.jsonl"))
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--lot-notional-usd", type=float, default=20.0)
    parser.add_argument("--entry-deviation-bps", type=float, default=2.0)
    parser.add_argument("--exit-deviation-bps", type=float, default=0.5)
    parser.add_argument("--max-lots", type=int, default=3)
    parser.add_argument("--max-total-lots", type=int, default=3)
    parser.add_argument("--min-hold-samples", type=int, default=10)
    parser.add_argument("--min-exit-pnl-bps", type=float, default=0.5, help="Only exit on signal reversion when lot PnL is at least this many bps.")
    parser.add_argument("--max-hold-samples", type=int, default=300, help="Force exit after this many samples; 0 disables forced time exit.")
    parser.add_argument("--max-unrealized-loss-bps", type=float, default=5.0, help="Force stop loss when lot PnL is below negative this many bps.")
    parser.add_argument("--windows", default="5m:300,30m:1800,1h:3600")
    parser.add_argument("--baseline-window", default="5m")
    parser.add_argument("--min-baseline-samples", type=int, default=30)
    parser.add_argument("--latest", type=int, default=1000)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-samples", type=int, default=0, help="Stop after this many samples; 0 means run forever.")
    args = parser.parse_args()
    try:
        windows = parse_windows(args.windows)
    except ValueError as exc:
        parser.error(str(exc))
    if args.baseline_window not in windows:
        parser.error("--baseline-window must be one of --windows names")
    if args.lot_notional_usd <= 0:
        parser.error("--lot-notional-usd must be > 0")
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
    if args.min_baseline_samples <= 0:
        parser.error("--min-baseline-samples must be > 0")
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
