#!/usr/bin/env python3
"""Read-only paired quote collector for Variational USOIL swap / Lighter WTI."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import signal
import shutil
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.basis_collector import (  # noqa: E402
    MultiLighterBooks,
    VariationalQuoteClient,
    decimal_text,
    edge_bps,
    parse_timestamp,
    roundtrip_bps,
    spread_bps,
    utc_now,
)
from tools.lib.basis_store import BasisSampleStore  # noqa: E402
from tools.lib.market_sessions import (  # noqa: E402
    VariationalMarketStatusFeed,
    extract_exchange_session_observation,
)


ASSET = "WTI"
VAR_MARKET = "USOILP"
INSTRUMENT_TYPE = "swap"
INSTRUMENT_KIND = "commodity"
FUNDING_INTERVAL_S = 0


def pair_drift_bps(before: dict[str, Any], after: dict[str, Any]) -> Decimal:
    drift: list[Decimal] = []
    for key in ("bid", "ask"):
        left, right = before.get(key), after.get(key)
        if isinstance(left, Decimal) and isinstance(right, Decimal) and left > 0:
            drift.append(abs(right - left) / left * Decimal("10000"))
    return max(drift) if drift else Decimal("999")


def quantity_for_notional(
    *,
    notional_usd: Decimal,
    reference_price: Decimal,
    market: dict[str, Any],
) -> Decimal:
    """Round up to Lighter lot precision and enforce both market minimums."""
    if notional_usd <= 0 or reference_price <= 0:
        raise ValueError("notional and reference price must be positive")
    decimals = int(market["supported_size_decimals"])
    multiplier = Decimal(str(market.get("multiplier") or "1"))
    if multiplier <= 0:
        raise ValueError("Lighter market multiplier must be positive")
    min_base = Decimal(str(market.get("min_base_amount") or "0"))
    min_quote = Decimal(str(market.get("min_quote_amount") or "0"))
    step = Decimal(1).scaleb(-decimals)
    required = max(notional_usd / (reference_price * multiplier), min_base,
                   min_quote / (reference_price * multiplier))
    steps = (required / step).to_integral_value(rounding=ROUND_CEILING)
    return steps * step


def validate_market_metadata(market: dict[str, Any]) -> None:
    if str(market.get("status") or "").lower() != "active":
        raise RuntimeError(f"Lighter WTI market is not active: {market.get('status')}")
    if market.get("is_frozen") is True:
        raise RuntimeError("Lighter WTI market is frozen")
    if int(market.get("supported_size_decimals", -1)) < 0:
        raise RuntimeError("Lighter WTI market has invalid size precision")
    for key in ("market_id", "min_base_amount", "min_quote_amount", "multiplier"):
        if market.get(key) is None:
            raise RuntimeError(f"Lighter WTI market metadata missing {key}")


class OilSwapCollector:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.output_dir = Path(args.output_dir).resolve()
        self.sample_root = self.output_dir / "basis_samples"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = f"oilbasis-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        self.commit = self._git_commit()
        config = {key: str(value) for key, value in sorted(vars(args).items())}
        config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:16]
        self.store = BasisSampleStore(self.sample_root, config_hash=config_hash, commit=self.commit)
        self.logger = logging.getLogger("oil_swap_collector")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        handler = RotatingFileHandler(
            self.output_dir / "oil_swap_collector.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(handler)
        self.logger.addHandler(logging.StreamHandler())
        self.books = MultiLighterBooks((ASSET,), self.logger)
        self.quote_client = VariationalQuoteClient(
            f"ws://{args.forwarder_host}:{args.forwarder_command_port}",
            timeout=args.quote_timeout_seconds,
            logger=self.logger,
        )
        self.market_status_feed = VariationalMarketStatusFeed()
        self.trading_sessions: list[dict[str, Any]] = []
        self.session_schedule_updated_at: str | None = None
        self.session_schedule_error: str | None = None
        self.last_session_refresh = 0.0
        self.stop = False
        self.errors: dict[str, int] = {}
        self.samples = 0
        self.sample_quality_counts: Counter[str] = Counter()
        self.last_sample_at: str | None = None
        self.last_quote_state = "not_checked"
        self.last_book_state = "not_checked"
        self.last_session_status = "unknown"
        self.last_session_status_observed_at: str | None = None
        self.last_session_id: str | None = None
        self.last_session_closing_at: str | None = None
        self.latest_sample: dict[str, Any] | None = None
        self.sample_lock = asyncio.Lock()
        self.last_health_write = 0.0
        self.last_maintenance = 0.0

    @staticmethod
    def _git_commit() -> str:
        import subprocess

        try:
            return subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
            ).strip()
        except Exception:
            return "unknown"

    def request_stop(self, *_args: Any) -> None:
        self.stop = True

    def _write_health(self, *, force: bool = False) -> None:
        now_mono = time.monotonic()
        if not force and now_mono - self.last_health_write < 10:
            return
        self.last_health_write = now_mono
        usage = shutil.disk_usage(self.output_dir)
        metadata = self.books.market_metadata.get(ASSET, {})
        healthy = self.last_quote_state == "fresh" and self.last_book_state == "fresh"
        payload = {
            "event": "oil_swap_collector_health",
            "updated_at": utc_now(),
            "run_id": self.run_id,
            "pid": os.getpid(),
            "status": "running" if not self.stop else "stopped",
            "availability_state": "both_feeds_live" if healthy else "partial_or_unavailable",
            "venue_pair": "variational_usoil_swap_lighter_mainnet_wti",
            "execution_mode": "collect_only",
            "orders_attempted": False,
            "variational_underlying": VAR_MARKET,
            "instrument_type": INSTRUMENT_TYPE,
            "instrument_kind": INSTRUMENT_KIND,
            "funding_interval_s": FUNDING_INTERVAL_S,
            "lighter_market": {
                key: metadata.get(key)
                for key in (
                    "symbol", "market_id", "market_type", "status", "is_frozen",
                    "min_base_amount", "min_quote_amount", "supported_size_decimals",
                    "supported_price_decimals", "multiplier", "taker_fee",
                    "is_taker_fee_enabled",
                )
            },
            "var_quote_state": self.last_quote_state,
            "lighter_book_state": self.last_book_state,
            "session_status": self.last_session_status,
            "session_status_observed_at": self.last_session_status_observed_at,
            "session_id": self.last_session_id,
            "session_closing_at": self.last_session_closing_at,
            "market_status": self.market_status_feed.status or "unknown",
            "market_status_feed_connected": self.market_status_feed.connected,
            "market_status_feed_last_frame_at": (
                None if self.market_status_feed.last_frame_at is None
                else self.market_status_feed.last_frame_at.isoformat()
            ),
            "market_status_feed_heartbeat_at": (
                None if self.market_status_feed.heartbeat_at is None
                else self.market_status_feed.heartbeat_at.isoformat()
            ),
            "market_status_feed_error": self.market_status_feed.error,
            "session_schedule_source": (
                "variational_supported_assets_api" if self.trading_sessions else None
            ),
            "session_schedule_updated_at": self.session_schedule_updated_at,
            "session_schedule_error": self.session_schedule_error,
            "samples_this_run": self.samples,
            "sample_quality_counts": dict(self.sample_quality_counts),
            "last_sample_at": self.last_sample_at,
            "errors": self.errors,
            "disk_free_gb": round(usage.free / (1024**3), 3),
            "collector_commit": self.commit,
        }
        path = self.sample_root / "health.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, path)

    async def _collect_once(
        self,
        *,
        var_qty: Decimal | None = None,
        lighter_qty: Decimal | None = None,
    ) -> dict[str, Any] | None:
        async with self.sample_lock:
            return await self._collect_once_unlocked(var_qty=var_qty, lighter_qty=lighter_qty)

    async def _collect_once_unlocked(
        self,
        *,
        var_qty: Decimal | None = None,
        lighter_qty: Decimal | None = None,
    ) -> dict[str, Any] | None:
        metadata = self.books.market_metadata[ASSET]
        multiplier = Decimal(str(metadata.get("multiplier") or "1"))
        requested_pair = var_qty is not None or lighter_qty is not None
        if requested_pair and (var_qty is None or lighter_qty is None):
            raise RuntimeError("requested_pair_quantity_incomplete")

        if requested_pair:
            var_qty = Decimal(str(var_qty))
            lighter_qty = Decimal(str(lighter_qty))
            if (
                not var_qty.is_finite() or not lighter_qty.is_finite()
                or var_qty <= 0 or lighter_qty <= 0
            ):
                raise RuntimeError("requested_pair_quantity_invalid")
            decimals = int(metadata["supported_size_decimals"])
            qty_step = Decimal(1).scaleb(-decimals)
            if lighter_qty % qty_step != 0:
                raise RuntimeError("requested_lighter_quantity_precision_mismatch")
            if var_qty != lighter_qty * multiplier:
                raise RuntimeError("requested_pair_quantity_multiplier_mismatch")
            min_base = Decimal(str(metadata.get("min_base_amount") or "0"))
            if lighter_qty < min_base:
                raise RuntimeError("requested_lighter_quantity_below_market_minimum")
            before = await self.books.snapshot(
                ASSET, Decimal(str(self.args.notional_usd)), base_qty=lighter_qty
            )
        else:
            top = await self.books.snapshot(ASSET, Decimal(str(self.args.notional_usd)))
            if top is None:
                self.last_book_state = "unavailable"
                raise RuntimeError("lighter_book_unavailable")
            midpoint = (top["bid"] + top["ask"]) / Decimal("2")
            lighter_qty = quantity_for_notional(
                notional_usd=Decimal(str(self.args.notional_usd)),
                reference_price=midpoint,
                market=metadata,
            )
            var_qty = lighter_qty * multiplier
            before = await self.books.snapshot(
                ASSET, Decimal(str(self.args.notional_usd)), base_qty=lighter_qty
            )
        if before is None:
            self.last_book_state = "unavailable"
            raise RuntimeError("lighter_book_unavailable_before_quote")
        if requested_pair:
            min_quote = Decimal(str(metadata.get("min_quote_amount") or "0"))
            reference_price = (before["bid"] + before["ask"]) / Decimal("2")
            if lighter_qty * multiplier * reference_price < min_quote:
                raise RuntimeError("requested_pair_notional_below_market_minimum")
        self.last_book_state = "fresh" if before.get("book_age_seconds", 999) <= self.args.max_book_age_seconds else "stale"
        self.last_quote_state = "pending"
        message, quote_ms = await self.quote_client.quote(
            ASSET,
            var_qty,
            market=VAR_MARKET,
            instrument_type=INSTRUMENT_TYPE,
            kind=INSTRUMENT_KIND,
            funding_interval_s=FUNDING_INTERVAL_S,
        )
        if not message.get("ok"):
            self.last_quote_state = "unavailable"
            raise RuntimeError(str(message.get("error") or "variational_quote_failed"))
        quote = message.get("result") if isinstance(message.get("result"), dict) else message
        try:
            var_bid = Decimal(str(quote["bid"]))
            var_ask = Decimal(str(quote["ask"]))
        except (KeyError, TypeError, ValueError) as exc:
            self.last_quote_state = "invalid"
            raise RuntimeError("variational_quote_missing_bid_ask") from exc
        after = await self.books.snapshot(ASSET, Decimal(str(self.args.notional_usd)), base_qty=lighter_qty)
        if after is None:
            self.last_book_state = "unavailable"
            raise RuntimeError("lighter_book_unavailable_after_quote")
        now = datetime.now(timezone.utc)
        session_observation = extract_exchange_session_observation(
            self.market_status_feed.status,
            self.trading_sessions,
            observed_at=now,
            status_observed_at=self.market_status_feed.status_observed_at,
            feed_connected=self.market_status_feed.connected,
            feed_heartbeat_at=self.market_status_feed.heartbeat_at,
        )
        self.last_session_status = str(session_observation["session_status"])
        self.last_session_status_observed_at = session_observation["session_status_observed_at"]
        self.last_session_id = session_observation["session_id"]
        self.last_session_closing_at = session_observation["session_closing_at"]
        quote_text = quote.get("quoteTimestamp") or quote.get("timestamp")
        quote_dt = parse_timestamp(quote_text)
        quote_age = None if quote_dt is None else (now - quote_dt).total_seconds()
        book_age = after.get("book_age_seconds")
        server_timestamp_ms = after.get("server_timestamp_ms")
        clock_drift = (
            None if server_timestamp_ms is None
            else abs(now.timestamp() - float(server_timestamp_ms) / 1000)
        )
        lighter_sell = after["sell_price"]
        lighter_buy = after["buy_price"]
        if lighter_sell is None or lighter_buy is None:
            self.last_book_state = "insufficient_depth"
            raise RuntimeError("lighter_depth_insufficient_for_requested_qty")
        if not after.get("continuity_ok"):
            self.last_book_state = "sequence_gap"
        elif book_age is None or book_age > self.args.max_book_age_seconds:
            self.last_book_state = "stale"
        else:
            self.last_book_state = "fresh"
        self.last_quote_state = (
            "fresh" if quote_age is not None
            and -1 <= quote_age <= self.args.max_quote_age_seconds else "stale_or_untimestamped"
        )
        reasons: list[str] = []
        invalid = False
        if (
            session_observation["session_status"] != "open"
            or not session_observation["session_id"]
            or not session_observation["session_closing_at"]
        ):
            invalid = True
            reasons.append("variational_market_session_unverified")
        if self.last_quote_state != "fresh":
            invalid = True
            reasons.append("variational_quote_age_invalid")
        if self.last_book_state != "fresh":
            invalid = True
            reasons.append(f"lighter_book_{self.last_book_state}")
        if clock_drift is not None and clock_drift > self.args.max_clock_drift_seconds:
            invalid = True
            reasons.append("lighter_clock_drift")
        drift = pair_drift_bps(before, after)
        degraded = False
        if after.get("cold"):
            degraded = True
            reasons.append("lighter_resync_cooldown")
        if quote_ms > self.args.max_quote_roundtrip_ms:
            degraded = True
            reasons.append("variational_quote_slow")
        if drift > Decimal(str(self.args.max_pair_drift_bps)):
            degraded = True
            reasons.append("lighter_moved_during_var_quote")
        mid_var = (var_bid + var_ask) / Decimal("2")
        mid_lighter = (after["bid"] + after["ask"]) / Decimal("2")
        basis = (mid_var - mid_lighter) / mid_lighter * Decimal("10000")
        sample_quality = "invalid" if invalid else "degraded" if degraded else "valid"
        row = {
            "event": "live_inventory_basis_state",
            "logged_at": now.isoformat(),
            "sample_id": uuid.uuid4().hex,
            "sample_kind": "baseline",
            "sample_quality": sample_quality,
            "sample_quality_reasons": reasons,
            "record_kind": "basis_market_sample",
            "execution_mode": "collect_only",
            "run_id": self.run_id,
            "strategy_version": "oil-swap-readonly-v1",
            "strategy_variant": "variational-usoil-swap-mainnet-lighter-wti",
            "asset": ASSET,
            "variational_underlying": VAR_MARKET,
            "variational_instrument_type": INSTRUMENT_TYPE,
            "variational_instrument_kind": INSTRUMENT_KIND,
            "variational_funding_interval_s": FUNDING_INTERVAL_S,
            "variational_qty": decimal_text(var_qty.normalize()),
            "lighter_symbol": metadata.get("symbol"),
            "lighter_market_id": metadata.get("market_id"),
            "lighter_market_status": metadata.get("status"),
            "lighter_market_type": metadata.get("market_type"),
            "lighter_multiplier": decimal_text(multiplier),
            "lighter_min_base_amount": metadata.get("min_base_amount"),
            "lighter_min_quote_amount": metadata.get("min_quote_amount"),
            "lighter_supported_size_decimals": metadata.get("supported_size_decimals"),
            "lighter_supported_price_decimals": metadata.get("supported_price_decimals"),
            "lighter_taker_fee": metadata.get("taker_fee"),
            "lighter_taker_fee_enabled": metadata.get("is_taker_fee_enabled"),
            "lighter_market_cost_metadata": {
                key: value for key, value in metadata.items()
                if any(token in str(key).lower() for token in ("fee", "fund", "roll", "oracle", "session"))
            },
            "requested_notional_usd": decimal_text(Decimal(str(self.args.notional_usd))),
            "estimated_notional_usd": decimal_text(lighter_qty * multiplier * mid_lighter),
            "lighter_qty": decimal_text(lighter_qty),
            "quote_id": quote.get("quoteId") or quote.get("quote_id"),
            "variational_quote_cost_metadata": self._cost_metadata(quote),
            "quote_timestamp": quote_text,
            "quote_ms": f"{quote_ms:.3f}",
            "var_quote_age_seconds": None if quote_age is None else f"{quote_age:.6f}",
            "lighter_book_age_seconds": None if book_age is None else f"{book_age:.6f}",
            "lighter_clock_drift_seconds": None if clock_drift is None else f"{clock_drift:.6f}",
            "lighter_pair_drift_bps": decimal_text(drift),
            "lighter_nonce": after.get("nonce"),
            "lighter_sequence_gaps": after.get("sequence_gaps"),
            "lighter_continuity_ok": after.get("continuity_ok"),
            "var_bid": decimal_text(var_bid),
            "var_ask": decimal_text(var_ask),
            "lighter_bid": decimal_text(after["bid"]),
            "lighter_ask": decimal_text(after["ask"]),
            "lighter_buy_price": decimal_text(lighter_buy),
            "lighter_sell_price": decimal_text(lighter_sell),
            "var_spread_bps": decimal_text(spread_bps(var_bid, var_ask)),
            "lighter_spread_bps": decimal_text(spread_bps(after["bid"], after["ask"])),
            "basis_bps": decimal_text(basis),
            "long_edge_bps": decimal_text(edge_bps(lighter_sell, var_ask)),
            "short_edge_bps": decimal_text(edge_bps(var_bid, lighter_buy)),
            "long_roundtrip_pnl_bps": decimal_text(roundtrip_bps(
                "long_var_short_lighter", var_bid=var_bid, var_ask=var_ask,
                lighter_buy=lighter_buy, lighter_sell=lighter_sell)),
            "short_roundtrip_pnl_bps": decimal_text(roundtrip_bps(
                "short_var_long_lighter", var_bid=var_bid, var_ask=var_ask,
                lighter_buy=lighter_buy, lighter_sell=lighter_sell)),
            "market_availability": (
                "both_feeds_live"
                if self.last_quote_state == "fresh" and self.last_book_state == "fresh"
                else "partial_or_unavailable"
            ),
            "basis_collect_only": True,
            "orders_attempted": False,
            "variational_session_schedule_updated_at": self.session_schedule_updated_at,
            **session_observation,
            "fee_model_note": "indicative market metadata only; actual fills, tier fees, funding and roll cashflows are required for net PnL",
        }
        return row

    def _record_sample(self, row: dict[str, Any]) -> None:
        self.store.append(row)
        self.latest_sample = dict(row)
        self.samples += 1
        self.sample_quality_counts[str(row.get("sample_quality") or "unknown")] += 1
        self.last_sample_at = row["logged_at"]
        self.errors.clear()

    async def collect_fresh_sample(
        self,
        *,
        var_qty: Decimal | None = None,
        lighter_qty: Decimal | None = None,
    ) -> dict[str, Any]:
        """Serialize execution refreshes with the normal collector poll loop."""
        row = await self._collect_once(var_qty=var_qty, lighter_qty=lighter_qty)
        if not isinstance(row, dict):
            raise RuntimeError("wti_fresh_sample_unavailable")
        self._record_sample(row)
        self._write_health(force=True)
        return dict(row)

    @staticmethod
    def _cost_metadata(quote: dict[str, Any]) -> dict[str, Any]:
        raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else quote
        allowed = ("fee", "fund", "roll", "settle", "commission", "financing")
        return {
            str(key): value
            for key, value in raw.items()
            if any(token in str(key).lower() for token in allowed)
            and isinstance(value, (str, int, float, bool, type(None)))
        }

    def _record_error(self, exc: Exception) -> None:
        reason = " ".join(str(exc).split())[:160] or type(exc).__name__
        self.errors[reason] = self.errors.get(reason, 0) + 1
        if self.last_quote_state == "pending":
            self.last_quote_state = "unavailable"
        self.logger.warning("oil_sample_unavailable reason=%s", reason)

    async def _refresh_session_metadata(self) -> None:
        self.last_session_refresh = time.monotonic()
        try:
            message, _elapsed_ms = await self.quote_client.market_metadata(VAR_MARKET)
            result = message.get("result") if isinstance(message, dict) else None
            market = result.get("market") if isinstance(result, dict) else None
            sessions = market.get("trading_sessions") if isinstance(market, dict) else None
            if (
                not message.get("ok")
                or not isinstance(market, dict)
                or market.get("symbol") != VAR_MARKET
                or not isinstance(sessions, list)
                or not sessions
            ):
                raise RuntimeError(
                    str(message.get("error") or "variational_trading_sessions_missing")
                )
            self.trading_sessions = sessions
            self.session_schedule_updated_at = datetime.now(timezone.utc).isoformat()
            self.session_schedule_error = None
        except Exception as exc:
            self.trading_sessions = []
            self.session_schedule_updated_at = None
            self.session_schedule_error = " ".join(str(exc).split())[:160] or type(exc).__name__
            self.logger.warning("variational_session_schedule_unavailable reason=%s", self.session_schedule_error)

    async def run(self) -> None:
        if shutil.disk_usage(self.output_dir).free / (1024**3) < self.args.disk_stop_free_gb:
            raise RuntimeError("disk_free_below_stop_threshold")
        self.store.rotate_closed_days()
        self._write_health(force=True)
        book_task: asyncio.Task[None] | None = None
        status_task: asyncio.Task[None] | None = None
        try:
            status_task = asyncio.create_task(self.market_status_feed.run())
            await self.books.load_markets()
            metadata = self.books.market_metadata[ASSET]
            validate_market_metadata(metadata)
            if str(metadata.get("symbol") or "").upper() != "WTI":
                raise RuntimeError("Lighter market mapping did not resolve to WTI")
            book_task = asyncio.create_task(self.books.run())
            await self.books.wait_ready()
            if not await self.market_status_feed.wait_for_status(timeout=10):
                self.logger.warning("variational_market_status_snapshot_not_received")
            await self._refresh_session_metadata()
            self.logger.info(
                "oil_swap_collector_started run_id=%s market_id=%s size_decimals=%s output=%s",
                self.run_id, metadata.get("market_id"), metadata.get("supported_size_decimals"), self.sample_root,
            )
            while not self.stop:
                started = time.monotonic()
                if time.monotonic() - self.last_session_refresh >= 60:
                    await self._refresh_session_metadata()
                try:
                    row = await self._collect_once()
                    self._record_sample(row)
                    self.logger.info(
                        "oil_sample asset=WTI quality=%s qty=%s basis_bps=%s quote_age_s=%s book_age_s=%s",
                        row["sample_quality"], row["lighter_qty"], row["basis_bps"],
                        row["var_quote_age_seconds"], row["lighter_book_age_seconds"],
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._record_error(exc)
                self._write_health()
                if shutil.disk_usage(self.output_dir).free / (1024**3) < self.args.disk_stop_free_gb:
                    raise RuntimeError("disk_free_below_stop_threshold")
                elapsed = time.monotonic() - started
                await asyncio.sleep(max(0.0, self.args.interval_seconds - elapsed))
                if time.monotonic() - self.last_maintenance >= 60:
                    self.last_maintenance = time.monotonic()
                    self.store.write_manifests()
                    self.store.rotate_closed_days()
                    self._write_health(force=True)
        finally:
            self.stop = True
            self.market_status_feed.stop = True
            self.books.stop = True
            if status_task is not None:
                status_task.cancel()
                await asyncio.gather(status_task, return_exceptions=True)
            if book_task is not None:
                book_task.cancel()
                await asyncio.gather(book_task, return_exceptions=True)
            await self.quote_client.close()
            self.store.write_manifests()
            self._write_health(force=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only Variational USOIL swap / Lighter mainnet WTI sampler."
    )
    parser.add_argument("--reuse-forwarder", action="store_true",
                        help="Connect to the existing live command port; never bind forwarder ports.")
    parser.add_argument("--forwarder-host", default="127.0.0.1")
    parser.add_argument("--forwarder-command-port", type=int, default=8768)
    parser.add_argument("--output-dir", default=str(ROOT / "log" / "oil_swap"))
    parser.add_argument("--notional-usd", type=Decimal, default=Decimal("20"))
    parser.add_argument("--interval-seconds", type=float, default=10.0)
    parser.add_argument("--quote-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-quote-age-seconds", type=float, default=2.0)
    parser.add_argument("--max-book-age-seconds", type=float, default=2.0)
    parser.add_argument("--max-clock-drift-seconds", type=float, default=5.0)
    parser.add_argument("--max-quote-roundtrip-ms", type=float, default=1500.0)
    parser.add_argument("--max-pair-drift-bps", type=float, default=1.5)
    parser.add_argument("--disk-stop-free-gb", type=float, default=3.0)
    args = parser.parse_args(argv)
    if not args.reuse_forwarder:
        parser.error("--reuse-forwarder is required; this collector must not bind live forwarder ports")
    if not args.notional_usd.is_finite() or args.notional_usd <= 0:
        parser.error("--notional-usd must be positive")
    if args.interval_seconds <= 0 or args.quote_timeout_seconds <= 0:
        parser.error("interval and quote timeout must be positive")
    if args.disk_stop_free_gb <= 0:
        parser.error("disk stop threshold must be positive")
    return args


async def _amain(args: argparse.Namespace) -> None:
    collector = OilSwapCollector(args)
    loop = asyncio.get_running_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signame, collector.request_stop)
    await collector.run()


def main() -> int:
    try:
        asyncio.run(_amain(parse_args()))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"OIL_COLLECTOR_STOPPED reason={type(exc).__name__}:{exc}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
