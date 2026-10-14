"""Pair fresh Var observations with mainnet depth without owning forwarder ports."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import shutil
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from tools.basis_collector import MultiLighterBooks, LIGHTER_WS_URL
from tools.lib.basis_store import BasisSampleStore


class SourceFollower:
    def __init__(self, root):
        self.root = Path(root) / "ETH"
        self.path = None
        self.identity = None
        self.handle = None
        self.started = False

    def close(self):
        if self.handle is not None:
            self.handle.close()
            self.handle = None

    def read_new(self):
        paths = sorted(self.root.glob("*.jsonl"))
        if not paths:
            return []
        path = paths[-1]
        stat = path.stat()
        identity = (stat.st_dev, stat.st_ino)
        if path != self.path or identity != self.identity or self.handle is None:
            self.close()
            self.path, self.identity = path, identity
            self.handle = path.open("rb")
            if not self.started:
                self.handle.seek(0, 2)
            self.started = True
        elif stat.st_size < self.handle.tell():
            self.handle.seek(0)
        rows = []
        for _ in range(500):
            offset = self.handle.tell()
            line = self.handle.readline(1024 * 1024 + 1)
            if not line:
                break
            if len(line) > 1024 * 1024:
                while line and not line.endswith(b"\n"):
                    line = self.handle.readline(1024 * 1024 + 1)
                continue
            if not line.endswith(b"\n"):
                self.handle.seek(offset)
                break
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    rows.append(row)
            except (ValueError, UnicodeDecodeError):
                continue
        return rows


def base_fill(levels, qty, *, buy):
    remaining, notional = qty, Decimal("0")
    for price, size in sorted(levels.items(), reverse=not buy):
        if price <= 0 or size <= 0:
            continue
        take = min(remaining, size)
        remaining -= take
        notional += take * price
        if remaining <= 0:
            return notional / qty
    return None


def paired_row(source, *, bids, asks, book_age, now=None):
    now = now or datetime.now(timezone.utc)
    if (source.get("asset") != "ETH" or source.get("sample_kind") != "baseline"
        or source.get("sample_quality") != "valid" or source.get("sample_pair_valid") is not True
        or source.get("quote_size_mode") != "exact_base_qty_v1"
        or source.get("passive_reference_duplicate")):
        return None, "source_quality"
    try:
        at = datetime.fromisoformat(source["logged_at"].replace("Z", "+00:00"))
        source_age = (now - at).total_seconds()
        qty = Decimal(str(source["quote_request_qty"]))
        var_bid = Decimal(str(source["var_bid"]))
        var_ask = Decimal(str(source["var_ask"]))
        var_age = Decimal(str(source["var_quote_age_seconds"]))
        if not all(value.is_finite() for value in (qty, var_bid, var_ask, var_age)):
            return None, "source_numbers"
        if qty <= 0 or var_bid <= 0 or var_ask <= 0 or var_bid > var_ask or var_age < 0:
            return None, "source_numbers"
    except (KeyError, ValueError, TypeError, InvalidOperation):
        return None, "source_numbers"
    if source_age < 0 or source_age > 1.5:
        return None, "source_stale"
    passive = source.get("quote_source") == "passive_browser_stream"
    if var_age + Decimal(str(source_age)) > Decimal("3" if passive else "1.5"):
        return None, "var_stale"
    if book_age is None or book_age < 0 or book_age > 2:
        return None, "mainnet_book_stale"
    if abs(qty * (var_bid + var_ask) / 2 - Decimal("20")) > Decimal("1"):
        return None, "source_size_not_20usd"
    buy, sell = base_fill(asks, qty, buy=True), base_fill(bids, qty, buy=False)
    if buy is None or sell is None:
        return None, "mainnet_depth_insufficient"
    copied = {key: source.get(key) for key in (
        "quote_source", "quote_received_at", "source_quote_timestamp", "reference_price",
        "var_bid", "var_ask", "quote_request_qty", "quote_size_mode",
    )}
    return {
        **copied, "logged_at": now.isoformat(), "asset": "ETH", "venue": "mainnet_lighter",
        "event": "live_inventory_basis_state", "execution_mode": "collect_only",
        "sample_kind": "baseline", "sample_quality": "valid", "sample_pair_valid": True,
        "sample_quality_version": 2, "source_logged_at": source["logged_at"],
        "source_sample_id": source.get("sample_id"), "source_age_seconds": source_age,
        "var_quote_age_seconds": str(var_age + Decimal(str(source_age))),
        "lighter_book_age_seconds": book_age, "lighter_buy_price": str(buy),
        "lighter_sell_price": str(sell), "long_edge_bps": str((sell-var_ask)/var_ask*10000),
        "short_edge_bps": str((var_bid-buy)/buy*10000),
    }, None


async def follow_mainnet(source_root, output_dir):
    if LIGHTER_WS_URL != "wss://mainnet.zklighter.elliot.ai/stream":
        raise RuntimeError("refuse_non_mainnet_market_data")
    target = Path(output_dir) / "mainnet_anchor"
    target.mkdir(parents=True, exist_ok=True)
    follower = SourceFollower(source_root)
    store = BasisSampleStore(target / "basis_samples", config_hash="mainnet-follow-v1", commit="mainnet-follow")
    logger = logging.getLogger("mainnet_anchor_follow")
    books = MultiLighterBooks(("ETH",), logger, websocket_url=LIGHTER_WS_URL + "?readonly=true")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    task = None
    errors = Counter()
    count, last_sample, last_health = 0, None, 0

    def health(status):
        path = target / "anchor_health.json"
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps({
            "status": status, "updated_at": datetime.now(timezone.utc).isoformat(),
            "venue": "mainnet_lighter", "execution_mode": "collect_only",
            "source_mode": "fresh_var_sample_follower", "source_root": str(source_root),
            "samples_this_run": count, "last_sample_at": last_sample, "errors": dict(errors),
            "disk_free_gb": round(shutil.disk_usage(target).free / 1024**3, 3),
        }, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)

    try:
        await books.load_markets()
        task = asyncio.create_task(books.run())
        health("waiting_for_data")
        while not stop.is_set():
            if task.done():
                await task
                raise RuntimeError("mainnet_book_task_stopped")
            for source in follower.read_new():
                async with books.lock:
                    book = books.by_asset["ETH"]
                    if not book.ready or book.resyncing or time.monotonic() < book.cold_until:
                        errors["mainnet_book_not_ready"] += 1
                        continue
                    age = None if book.received_monotonic is None else time.monotonic() - book.received_monotonic
                    row, reason = paired_row(source, bids=book.bids, asks=book.asks, book_age=age)
                if row is None:
                    errors[reason] += 1
                else:
                    await asyncio.to_thread(store.append, row)
                    count += 1
                    last_sample = row["logged_at"]
            if time.monotonic() - last_health >= 10:
                if shutil.disk_usage(target).free < 3 * 1024**3:
                    raise RuntimeError("mainnet_collection_disk_free_below_3gb")
                health("running" if last_sample else "waiting_for_data")
                last_health = time.monotonic()
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.2)
            except asyncio.TimeoutError:
                pass
    finally:
        books.stop = True
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        follower.close()
        health("stopped")
