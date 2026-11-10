import asyncio
import time

import pytest

from main import VariationalToLighterRuntime


class _Logger:
    def __init__(self):
        self.warnings = []

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(msg % args if args else msg)

    def info(self, *args, **kwargs):
        pass


def _runtime():
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.logger = _Logger()
    return runtime


def test_watchdog_reloads_page_when_quotes_go_stale(monkeypatch):
    runtime = _runtime()
    runtime.live_inventory_last_fresh_quote_monotonic = time.monotonic() - 120.0
    reloads = []

    async def fake_command(**kwargs):
        reloads.append(kwargs["payload"]["type"])
        return {"ok": True}

    runtime.send_variational_command = fake_command

    calls = []

    async def fake_sleep(seconds):
        calls.append(seconds)
        if len(calls) > 2:
            raise asyncio.CancelledError

    monkeypatch.setattr("main.asyncio.sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime.watch_variational_quote_freshness())

    assert reloads == ["VAR_API_RELOAD_PAGE"]
    assert any("variational_quote_stale_reload" in w for w in runtime.logger.warnings)


def test_watchdog_stays_quiet_when_quotes_are_fresh(monkeypatch):
    runtime = _runtime()
    runtime.live_inventory_last_fresh_quote_monotonic = time.monotonic()
    reloads = []

    async def fake_command(**kwargs):
        reloads.append(kwargs["payload"]["type"])
        return {"ok": True}

    runtime.send_variational_command = fake_command

    calls = []

    async def fake_sleep(seconds):
        calls.append(seconds)
        if len(calls) > 3:
            raise asyncio.CancelledError

    monkeypatch.setattr("main.asyncio.sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime.watch_variational_quote_freshness())

    assert reloads == []


def test_watchdog_respects_reload_cooldown(monkeypatch):
    runtime = _runtime()
    runtime.live_inventory_last_fresh_quote_monotonic = time.monotonic() - 600.0
    reloads = []

    async def fake_command(**kwargs):
        reloads.append(kwargs["payload"]["type"])
        return {"ok": True}

    runtime.send_variational_command = fake_command

    calls = []

    async def fake_sleep(seconds):
        calls.append(seconds)
        if len(calls) > 3:
            raise asyncio.CancelledError

    monkeypatch.setattr("main.asyncio.sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime.watch_variational_quote_freshness())

    # cooldown 300s：连续三次轮询只允许重载一次
    assert reloads == ["VAR_API_RELOAD_PAGE"]
