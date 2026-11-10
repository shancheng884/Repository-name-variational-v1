import asyncio

import pytest

from main import VariationalToLighterRuntime


class _L:
    def warning(self, *a, **k): pass
    def info(self, *a, **k): pass


def test_watchdog_reloads_even_when_no_fresh_quote_was_ever_recorded(monkeypatch):
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.logger = _L()
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

    # stale_seconds=0：启动即视为过期，验证"从未记录新鲜报价"也会触发重载
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime.watch_variational_quote_freshness(stale_seconds=0.0))

    assert reloads == ["VAR_API_RELOAD_PAGE"]
