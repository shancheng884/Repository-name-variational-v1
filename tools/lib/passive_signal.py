"""Wake the existing serial trader from passive quotes; never request a quote."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from tools.lib.frozen_median import LONG, SHORT


def monitor_for(runtime):
    if not (getattr(runtime, "live_inventory_frozen_median", False)
            and runtime.is_live_inventory_enabled()):
        return None
    monitor = getattr(getattr(runtime, "runtime", None), "monitor", None)
    if not callable(getattr(monitor, "wait_for_indicative_quote", None)):
        return None
    return monitor


def begin_cycle(runtime):
    monitor = monitor_for(runtime)
    if monitor is not None:
        asset = str(runtime.variational_ticker or runtime.ticker or "").upper()
        runtime.median_signal_cycle_version = monitor.indicative_quote_version(asset)


async def wait_next(runtime):
    monitor = monitor_for(runtime)
    if monitor is None:
        await asyncio.sleep(runtime.paper_interval_seconds)
        return
    asset = str(runtime.variational_ticker or runtime.ticker or "").upper()
    after = max(getattr(runtime, "median_signal_cycle_version", 0),
                getattr(runtime, "median_signal_used_version", 0))
    version = await monitor.wait_for_indicative_quote(
        asset, after_version=after, timeout=runtime.paper_interval_seconds,
    )
    runtime.median_signal_trigger = "new_var_quote" if version > after else "timer"
    runtime.median_signal_coalesced_versions = max(0, version - after - 1)


async def execution_command(runtime, asset, command, *, request_id=None):
    monitor = monitor_for(runtime)
    if monitor is None:
        return await command
    monitor.begin_execution_quote(asset)
    result = None
    try:
        result = await command
        return result
    finally:
        monitor.end_execution_quote(asset, result, request_id or str(id(command)))


def pair_audit(runtime, quote, snapshot):
    now = time.monotonic()
    lighter = dict(getattr(snapshot, "lighter_quote_audit", None) or {})
    var_received = quote.get("passive_indicative_received_monotonic")
    lighter_received = lighter.get("received_monotonic")
    version = quote.get("passive_indicative_version")
    if isinstance(version, int):
        runtime.median_signal_used_version = version
    return {
        "quote_pair_id": f"{getattr(runtime, 'live_inventory_run_id', 'unknown')}:{runtime.live_inventory_sample_index}",
        "decision_at": datetime.now(timezone.utc).isoformat(),
        "signal_trigger": getattr(runtime, "median_signal_trigger", "startup"),
        "coalesced_versions": getattr(runtime, "median_signal_coalesced_versions", 0),
        "rfq_consumed": False,
        "var_quote": {
            "version": version,
            "source_timestamp": quote.get("passive_indicative_source_timestamp"),
            "received_at": quote.get("passive_indicative_received_at"),
            "received_monotonic": var_received,
            "age_seconds": now - var_received if var_received is not None else None,
            "bid": str(quote.get("bid")), "ask": str(quote.get("ask")),
            "reference_received_at": quote.get("reference_quote_received_at"),
            "reference_source_timestamp": quote.get("reference_source_timestamp"),
        },
        "lighter_book": {
            **lighter,
            "bid": str(snapshot.lighter_bid), "ask": str(snapshot.lighter_ask),
            "age_seconds": now - lighter_received if lighter_received is not None else None,
        },
        "receive_skew_ms": ((lighter_received - var_received) * 1000
                            if lighter_received is not None and var_received is not None else None),
    }


def record_decision(runtime, context, *, rejection=None):
    version = context["var_quote"]["version"]
    if version is None:
        return
    # Timer evaluations remain in the usual state log, not duplicate quote rows.
    if version == getattr(runtime, "median_signal_last_audited_version", None):
        return
    model = runtime.frozen_median_model
    now = time.time()
    anchor = model.anchor if model.anchor is not None else model.rolling_median(now=now)
    confirmations = [c for c in model.confirmations if 0 <= now - c[1] <= 3]
    payload = {
        "schema": 1, "run_id": getattr(runtime, "live_inventory_run_id", "unknown"),
        "asset": snapshot_asset(runtime), **context, "rejection": rejection,
        "median_bps": str(anchor) if anchor is not None else None,
        "basis_bps": str(model.last_basis) if rejection is None and model.last_basis is not None else None,
        "confirmations": [{"event_id": c[0], "sample_second": c[1], "basis_bps": str(c[2])}
                          for c in confirmations],
        "directions": {},
    }
    if rejection is None and anchor is not None:
        for direction in (LONG, SHORT):
            hits = sum(model.signed_distance(c[2], anchor, direction) >= model.distances[0]
                       for c in confirmations)
            payload["directions"][direction] = {
                "first_tier_confirmations": hits,
                "latest_quote_matches": bool(confirmations and confirmations[-1][2] == model.last_basis),
                "active_tier": model.candidate_tier(direction=direction, now=now),
            }
    root = Path(runtime.output_dir) / "median_quote_audit"
    root.mkdir(parents=True, exist_ok=True)
    path = root / (datetime.now(timezone.utc).strftime("%Y-%m-%d") + ".jsonl")
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, default=str) + "\n")
    runtime.median_signal_last_audited_version = version
    runtime.median_signal_last_audit_at = context["decision_at"]


def snapshot_asset(runtime):
    return str(runtime.variational_ticker or runtime.ticker or "").upper()
