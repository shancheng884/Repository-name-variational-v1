from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import asyncio
import json
import logging
import os
import signal
import sys
import time
import uuid

from .audit import FillAuditor
from .config import CaptureConfig
from .hub import MarketHub
from .probes import LatencyProbe, entropy_probe, lighter_probe, variational_probe
from .storage import TapeWriter, disk_free_gb, gzip_cold_files, prune_old
from .sysmon import LoopLagMonitor, SystemSampler
from .venues.entropy import EntropyBookFeed, EntropyFillFeed
from .venues.lighter import LighterBookFeed, LighterFillFeed, LighterTradePoller
from .venues.variational import VariationalFillFeed, VariationalPriceFeed


def _acquire_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        import fcntl
    except ImportError:
        return handle
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise RuntimeError("market_capture_already_running")
    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


class CaptureRunner:
    def __init__(self, config: CaptureConfig) -> None:
        self.config = config
        self.root = config.output_dir
        self.run_id = "capture-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
        self.started_at = datetime.now(timezone.utc)
        self.logger = logging.getLogger("market_capture")
        self.writer: TapeWriter | None = None
        self.hub: MarketHub | None = None
        self.auditor: FillAuditor | None = None
        self.loop_lag = LoopLagMonitor()
        self.system = SystemSampler(system_seconds=config.system_seconds)
        self.venues: dict[str, Any] = {}
        self.tasks: list[asyncio.Task[Any]] = []
        self.stop_event = asyncio.Event()
        self.last_health: dict[str, Any] = {}
        self._lock_handle = None

    async def run(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock_handle = _acquire_lock(self.root / "capture.lock")
        (self.root / "meta.json").write_text(
            json.dumps({
                "run_id": self.run_id,
                "started_at": self.started_at.isoformat(),
                "config": self.config.public_dict(),
                "orders_attempted": False,
                "execution_enabled": False,
            }, indent=2) + "\n",
            encoding="utf-8",
        )
        self.writer = TapeWriter(self.root).start()
        self.loop_lag.writer = self.writer
        self.hub = MarketHub(self.writer, self.config)
        self.auditor = FillAuditor(
            self.hub, self.writer,
            stall_snapshot=lambda start, end: self.loop_lag.max_in_window(start, end) or {},
        )
        self.loop_lag.start()

        try:
            await self._start_venues()
            self.tasks.append(asyncio.create_task(self._edge_loop(), name="edge-loop"))
            self.tasks.append(asyncio.create_task(self._health_loop(), name="health-loop"))
            self.tasks.append(asyncio.create_task(self._system_loop(), name="system-loop"))
            self.tasks.append(asyncio.create_task(self._retention_loop(), name="retention-loop"))
            for venue in self.venues.values():
                task = getattr(venue, "run", None)
                if task is not None:
                    created = asyncio.create_task(task(), name=f"venue:{getattr(venue, 'name', '?')}")
                    created.add_done_callback(self._task_done)
                    self.tasks.append(created)
            self.logger.info(
                "market_capture_started run_id=%s output=%s venues=%s assets=%s",
                self.run_id, self.root, list(self.config.venue_enabled), [a.name for a in self.config.assets],
            )
            if self.config.duration_seconds > 0:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=self.config.duration_seconds)
                except asyncio.TimeoutError:
                    pass
            else:
                await self.stop_event.wait()
        finally:
            await self._shutdown()

    def _venue_error(self, name: str, exc: BaseException) -> None:
        from .venues.base import VenueHealth
        health = VenueHealth(name)
        health.status = "error"
        health.error(f"{type(exc).__name__}:{exc}")
        from types import SimpleNamespace
        self.venues[name] = SimpleNamespace(health=health)

    async def _start_venues(self) -> None:
        assert self.hub is not None and self.auditor is not None and self.writer is not None
        assets = self.config.assets
        config = self.config
        if config.venue_enabled.get("variational"):
            try:
                self.venues["variational"] = VariationalPriceFeed(
                    ws_url=config.var_ws_url, assets=assets, writer=self.writer, hub=self.hub, logger=self.logger,
                )
                self.venues["variational_fills"] = VariationalFillFeed(
                    command_url=config.var_command_url, assets=assets, writer=self.writer,
                    auditor=self.auditor, interval=max(5.0, config.fill_poll_seconds * 2),
                    accept_after=self.started_at - timedelta(seconds=30), logger=self.logger,
                )
            except Exception as exc:
                self._venue_error("variational", exc)
        if config.venue_enabled.get("lighter_mainnet"):
            try:
                feed = LighterBookFeed(
                    name="lighter_mainnet", rest_url=config.lighter_mainnet_rest, ws_url=config.lighter_mainnet_ws,
                    asset_symbols={a.name: a.symbol("lighter_mainnet") for a in assets},
                    writer=self.writer, hub=self.hub, logger=self.logger,
                    max_levels=config.max_book_levels,
                )
                await feed.load_markets()
                self.venues["lighter_mainnet"] = feed
                self.venues["lighter_mainnet_trades"] = LighterTradePoller(feed=feed, writer=self.writer)
                self.venues["lighter_mainnet_fills"] = LighterFillFeed(
                    name="lighter_mainnet", rest_url=config.lighter_mainnet_rest,
                    asset_symbols={a.name: a.symbol("lighter_mainnet") for a in assets},
                    markets=feed.markets, writer=self.writer, auditor=self.auditor,
                    account_index=config.lighter_mainnet_account_index,
                    api_key_index=config.lighter_mainnet_api_key_index,
                    private_key=config.lighter_mainnet_private_key,
                    interval=config.fill_poll_seconds,
                    accept_after=self.started_at - timedelta(seconds=30), logger=self.logger,
                )
            except Exception as exc:
                self._venue_error("lighter_mainnet", exc)
        if config.venue_enabled.get("lighter_rh"):
            try:
                feed = LighterBookFeed(
                    name="lighter_rh", rest_url=config.lighter_rh_rest, ws_url=config.lighter_rh_ws,
                    asset_symbols={a.name: a.symbol("lighter_rh") for a in assets},
                    writer=self.writer, hub=self.hub, logger=self.logger,
                    max_levels=config.max_book_levels,
                )
                await feed.load_markets()
                self.venues["lighter_rh"] = feed
                self.venues["lighter_rh_trades"] = LighterTradePoller(feed=feed, writer=self.writer)
                self.venues["lighter_rh_fills"] = LighterFillFeed(
                    name="lighter_rh", rest_url=config.lighter_rh_rest,
                    asset_symbols={a.name: a.symbol("lighter_rh") for a in assets},
                    markets=feed.markets, writer=self.writer, auditor=self.auditor,
                    account_index=config.lighter_rh_account_index,
                    api_key_index=config.lighter_rh_api_key_index,
                    private_key=config.lighter_rh_private_key,
                    interval=config.fill_poll_seconds,
                    accept_after=self.started_at - timedelta(seconds=30), logger=self.logger,
                )
            except Exception as exc:
                self._venue_error("lighter_rh", exc)
        if config.venue_enabled.get("entropy"):
            try:
                feed = EntropyBookFeed(
                    info_url=config.entropy_info_url, ws_url=config.entropy_ws_url, dex=config.entropy_dex,
                    asset_symbols={a.name: a.symbol("entropy") for a in assets},
                    writer=self.writer, hub=self.hub, logger=self.logger,
                    max_levels=config.max_book_levels,
                )
                await feed.load_universe()
                self.venues["entropy"] = feed
                self.venues["entropy_fills"] = EntropyFillFeed(
                    info_url=config.entropy_info_url, dex=config.entropy_dex,
                    asset_symbols={a.name: a.symbol("entropy") for a in assets},
                    writer=self.writer, auditor=self.auditor,
                    account_address=config.entropy_account_address,
                    interval=config.fill_poll_seconds,
                    accept_after=self.started_at - timedelta(seconds=30), logger=self.logger,
                )
            except Exception as exc:
                self._venue_error("entropy", exc)
        probes = []
        if config.venue_enabled.get("variational") and "variational" in self.venues:
            probes.append(LatencyProbe(
                venue="variational", target="web", request=variational_probe("NVDA"),
                writer=self.writer, interval=config.latency_seconds,
            ))
        if config.venue_enabled.get("lighter_mainnet") and "lighter_mainnet" in self.venues:
            market = self.venues["lighter_mainnet"].markets
            if market:
                first = next(iter(market.values()))
                probes.append(LatencyProbe(
                    venue="lighter_mainnet", target="recentTrades",
                    request=lighter_probe(config.lighter_mainnet_rest, int(first["market_id"])),
                    writer=self.writer, interval=config.latency_seconds,
                ))
        if config.venue_enabled.get("lighter_rh") and "lighter_rh" in self.venues:
            market = self.venues["lighter_rh"].markets
            if market:
                first = next(iter(market.values()))
                probes.append(LatencyProbe(
                    venue="lighter_rh", target="recentTrades",
                    request=lighter_probe(config.lighter_rh_rest, int(first["market_id"])),
                    writer=self.writer, interval=config.latency_seconds,
                ))
        if config.venue_enabled.get("entropy") and "entropy" in self.venues:
            probes.append(LatencyProbe(
                venue="entropy", target="meta", request=entropy_probe(config.entropy_info_url, config.entropy_dex),
                writer=self.writer, interval=config.latency_seconds,
            ))
        for probe in probes:
            self.venues[f"probe:{probe.venue}"] = probe

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.logger.error("capture_task_failed name=%s error=%r", task.get_name(), exc)

    async def _edge_loop(self) -> None:
        assert self.hub is not None
        while True:
            for asset in self.config.assets:
                self.hub.edge_rows(asset.name)
            await asyncio.sleep(self.config.edge_seconds)

    async def _system_loop(self) -> None:
        assert self.writer is not None
        while True:
            try:
                row = await asyncio.to_thread(self.system.sample)
                self.writer.append("system", "local", "host", row)
                self.writer.append("loop", "local", "event_loop", {
                    "schema": 1, "event": "loop_lag", "at": datetime.now(timezone.utc).isoformat(),
                    **self.loop_lag.snapshot(),
                })
            except Exception as exc:
                self.logger.warning("system_sample_failed error=%s", exc)
            await asyncio.sleep(self.config.system_seconds)

    async def _retention_loop(self) -> None:
        while True:
            await asyncio.sleep(6 * 3600)
            try:
                await asyncio.to_thread(gzip_cold_files, self.root, older_than_hours=6.0)
                await asyncio.to_thread(prune_old, self.root, retention_days=self.config.retention_days)
            except Exception as exc:
                self.logger.warning("retention_failed error=%s", exc)

    async def _health_loop(self) -> None:
        while True:
            try:
                self.write_health()
            except Exception as exc:
                self.logger.warning("health_write_failed error=%s", exc)
            await asyncio.sleep(self.config.health_seconds)

    def write_health(self, *, status: str | None = None) -> None:
        assert self.hub is not None and self.writer is not None
        now = datetime.now(timezone.utc)
        venues = {}
        for name, venue in self.venues.items():
            health = getattr(venue, "health", None)
            if health is not None:
                venues[name] = health.public(now)
            elif hasattr(venue, "samples"):
                venues[name] = {
                    "status": "running" if venue.last_error is None else "degraded",
                    "samples": venue.samples,
                    "last_rtt_ms": round(venue.last_ms, 3) if venue.last_ms is not None else None,
                    "last_error": venue.last_error,
                }
        health = {
            "schema": 1,
            "run_id": self.run_id,
            "status": status or "running",
            "started_at": self.started_at.isoformat(),
            "updated_at": now.isoformat(),
            "orders_attempted": False,
            "execution_enabled": False,
            "order_submission_enabled": False,
            "assets": [asset.name for asset in self.config.assets],
            "venues": venues,
            "freshness": self.hub.freshness(),
            "writer": self.writer.stats(),
            "loop": self.loop_lag.snapshot(),
            "fills_audited": self.auditor.audited if self.auditor else 0,
            "disk_free_gb": round(disk_free_gb(self.root), 2),
            "tasks": len(self.tasks),
        }
        self.last_health = health
        path = self.root / "health.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(health, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)

    async def _shutdown(self) -> None:
        self.write_health(status="stopping")
        if self.auditor:
            await self.auditor.close()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.loop_lag.stop()
        if self.writer:
            self.writer.close()
        self.write_health(status="stopped")
        if self._lock_handle is not None:
            self._lock_handle.close()
            self._lock_handle = None

    def request_stop(self) -> None:
        self.stop_event.set()


async def run_capture(config: CaptureConfig) -> int:
    runner = CaptureRunner(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, runner.request_stop)
        except NotImplementedError:
            pass
    await runner.run()
    return 0
