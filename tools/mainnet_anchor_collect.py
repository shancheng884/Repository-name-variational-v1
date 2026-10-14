#!/usr/bin/env python3
"""Collect strategy-compatible ETH mainnet anchors without trading credentials."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import main as engine
from tools.lib.basis_store import BasisSampleStore
from tools.lib.mainnet_anchor import load_mainnet_anchor_rows


def runtime_args(output_dir: Path):
    previous = sys.argv
    try:
        sys.argv = ["main.py", "--mode", "observe", "--output-dir", str(output_dir),
                    "--live-allowed-assets", "ETH", "--variational-submit-transport", "api"]
        args = engine.parse_args()
    finally:
        sys.argv = previous
    args.live_inventory = True
    args.live_inventory_dry_decisions = True
    args.live_inventory_collect_only = True
    args.live_inventory_signal_mode = engine.LIVE_INVENTORY_SIGNAL_BASIS
    args.live_inventory_lot_notional_usd = 20
    args.lighter_prewarm_submit_ws = False
    return args


class MainnetAnchorCollector(engine.VariationalToLighterRuntime):
    def __init__(self, args):
        if engine.LIGHTER_WS_URL != "wss://mainnet.zklighter.elliot.ai/stream":
            raise RuntimeError("mainnet_collector_refuses_non_mainnet_engine")
        super().__init__(args)
        self.anchor_store = BasisSampleStore(
            self.output_dir / "basis_samples", config_hash="mainnet-eth-readonly-v1",
            commit="mainnet-anchor-collector",
        )
        self.anchor_samples = 0
        self.anchor_last_at = None
        self.anchor_last_bucket = None

    def is_live_inventory_enabled(self):
        return self.is_observe_mode() and self.live_inventory_collect_only and self.live_inventory_dry_decisions

    def live_inventory_basis_quote_priority(self, snapshot):
        return "background"

    def should_log_live_inventory_basis_state(self, *, crossing):
        return True

    def schedule_live_inventory_basis_size_ladder_shadow(self, **kwargs):
        return

    async def send_variational_place_order(self, **kwargs):
        if kwargs.get("confirm") is not False:
            raise RuntimeError("mainnet_anchor_collector_blocks_orders")
        return await super().send_variational_place_order(**kwargs)

    async def place_lighter_order(self, *args, **kwargs):
        raise RuntimeError("mainnet_anchor_collector_blocks_orders")

    async def place_lighter_order_from_plan(self, *args, **kwargs):
        raise RuntimeError("mainnet_anchor_collector_blocks_orders")

    async def submit_live_inventory_exit_pair(self, *args, **kwargs):
        raise RuntimeError("mainnet_anchor_collector_blocks_orders")

    async def append_market_sample(self, snapshot):
        return

    async def append_live_inventory_log(self, event_type, payload):
        if event_type != "live_inventory_basis_state":
            return
        if payload.get("asset") != "ETH" or payload.get("sample_pair_valid") is not True:
            return
        if payload.get("quote_size_mode") != engine.LIVE_INVENTORY_BASIS_QUOTE_SIZE_MODE:
            return
        if payload.get("passive_reference_duplicate"):
            return
        bucket = int(time.time()) // engine.LIVE_INVENTORY_BASIS_V4_HISTORY_SAMPLE_SECONDS
        if bucket == self.anchor_last_bucket:
            return
        captured_monotonic = time.monotonic()
        last_write = getattr(self, "anchor_last_write_monotonic", None)
        # The strategy reloads samples at least 30 seconds apart, rather than
        # rounding timestamps into clock buckets. Allow one second for jitter.
        if last_write is not None and captured_monotonic - last_write < 31:
            return
        at = datetime.now(timezone.utc).isoformat()
        row = {**payload, "logged_at": at, "event": event_type,
               "venue": "mainnet_lighter", "sample_kind": "baseline",
               "sample_quality": "valid", "execution_mode": "collect_only",
               "run_id": getattr(self, "live_inventory_run_id", "mainnet-anchor"),
               "sample_id": f"mainnet-anchor:{bucket}:{at}"}
        await asyncio.to_thread(self.anchor_store.append, row)
        self.anchor_last_bucket = bucket
        self.anchor_last_write_monotonic = captured_monotonic
        self.anchor_last_at = at
        self.anchor_samples += 1

    def write_anchor_health(self, status):
        p = self.output_dir / "anchor_health.json"
        temporary = p.with_suffix(".json.tmp")
        temporary.write_text(json.dumps({
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "status": status, "venue": "mainnet_lighter", "execution_mode": "collect_only",
            "samples_this_run": self.anchor_samples, "last_sample_at": self.anchor_last_at,
            "source_root": str(self.anchor_store.root),
        }, indent=2) + "\n", encoding="utf-8")
        temporary.replace(p)

    async def heartbeat(self):
        while not self.stop_flag:
            self.write_anchor_health("running")
            await asyncio.sleep(10)


def anchor_status(output_dir: Path, *, now=None):
    observed = now or datetime.now(timezone.utc)
    runtime = engine.VariationalToLighterRuntime.__new__(engine.VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_bidirectional = True
    runtime.live_inventory_basis_v4_test_skip_recent_health = False
    runtime.live_inventory_basis_v4_shadow_cached_at_by_direction = {}
    runtime.live_inventory_basis_v4_shadow_cache_by_direction = {}
    runtime.live_inventory_basis_v4_projection_cached_at_by_direction = {}
    runtime.live_inventory_basis_v4_projection_cache_by_direction = {}
    runtime.live_inventory_max_lighter_book_age_seconds = 2
    runtime.live_inventory_basis_max_var_quote_age_ms = 1500
    rows, counts = runtime.filter_live_inventory_basis_v4_history_rows(
        load_mainnet_anchor_rows(output_dir, asset="ETH", now=observed)
    )
    rows = [r for r in rows if r.get("quote_size_mode") == engine.LIVE_INVENTORY_BASIS_QUOTE_SIZE_MODE]
    report = {"venue": "mainnet_lighter", "quality_counts": counts, "directions": {}}
    for direction, key in ((engine.DIRECTION_LONG_VAR_SHORT_LIGHTER, "long_edge_bps"),
                           (engine.DIRECTION_SHORT_VAR_LONG_LIGHTER, "short_edge_bps")):
        history = []
        next_at = observed.timestamp() - 30 * 86400
        for row in rows:
            try:
                at = datetime.fromisoformat(row["logged_at"].replace("Z", "+00:00")).timestamp()
                edge = engine.to_decimal(row.get(key))
                if edge is not None and edge.is_finite() and next_at <= at < observed.timestamp():
                    history.append((at, edge))
                    next_at = at + engine.LIVE_INVENTORY_BASIS_V4_HISTORY_SAMPLE_SECONDS
            except (ValueError, KeyError):
                continue
        threshold, context = runtime.live_inventory_basis_v4_entry_threshold(
            now=observed.timestamp(), direction=direction,
            history_override=history, health_history_override=history, cache_result=False,
        )
        age = observed.timestamp() - history[-1][0] if history else None
        report["directions"][direction] = {
            **{k: context.get(k) for k in (
                "v4_anchor_ready", "v4_anchor_count", "v4_anchor_effective_seconds",
                "v4_anchor_missing_effective_seconds", "v4_health_ready_observed",
                "v4_health_max_sample_gap_seconds",
                "v4_anchor_projected_ready_seconds", "v4_anchor_projected_ready_at",
            )},
            "latest_sample_age_seconds": age,
            "ready_now": threshold is not None and age is not None
            and age <= engine.LIVE_INVENTORY_BASIS_V4_MAX_SAMPLE_GAP_SECONDS,
        }
    return report


async def collect(output_dir: Path):
    target = output_dir / "mainnet_anchor"
    target.mkdir(parents=True, exist_ok=True)
    args = runtime_args(target)
    owner = engine.acquire_instance_lock(args.instance_lock_file)
    runtime = None
    heartbeat = None
    try:
        runtime = MainnetAnchorCollector(args)
        heartbeat = asyncio.create_task(runtime.heartbeat())
        await runtime.run()
    finally:
        if heartbeat is not None:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
        if runtime is not None:
            await runtime.close()
            runtime.write_anchor_health("stopped")
        engine.release_instance_lock(args.instance_lock_file, owner)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "log")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--source-root", type=Path, help="Use fresh Var samples from another process without owning its ports.")
    args = parser.parse_args()
    if args.status:
        print(json.dumps(anchor_status(args.output_dir), ensure_ascii=False, indent=2))
    elif args.source_root is not None:
        from tools.lib.mainnet_anchor_follow import follow_mainnet
        target = args.output_dir / "mainnet_anchor"
        target.mkdir(parents=True, exist_ok=True)
        lock = target / "main.instance.lock"
        owner = engine.acquire_instance_lock(lock)
        try:
            asyncio.run(follow_mainnet(args.source_root, args.output_dir))
        finally:
            engine.release_instance_lock(lock, owner)
    else:
        asyncio.run(collect(args.output_dir))


if __name__ == "__main__":
    main()
