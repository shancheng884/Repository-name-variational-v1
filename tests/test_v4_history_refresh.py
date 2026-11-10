import asyncio

import pytest

from main import VariationalToLighterRuntime


class _Logger:
    def __init__(self):
        self.infos = []
        self.warnings = []

    def info(self, msg, *args, **kwargs):
        self.infos.append(msg % args if args else msg)

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(msg)


def test_refresh_loop_reloads_history_periodically(monkeypatch):
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.logger = _Logger()
    calls = []

    def fake_loader(*, asset):
        calls.append(asset)
        return {"samples": len(calls), "effective_seconds": 30 * len(calls)}

    runtime.load_live_inventory_basis_v4_history = fake_loader

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 3:
            raise asyncio.CancelledError

    monkeypatch.setattr("main.asyncio.sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            runtime.refresh_live_inventory_basis_v4_history_loop(
                asset="ETH", interval_seconds=600.0
            )
        )

    assert calls == ["ETH", "ETH"]
    assert sleep_calls and sleep_calls[0] >= 30.0
    assert any("v4_history_refreshed" in message for message in runtime.logger.infos)


def test_refresh_loop_survives_loader_errors(monkeypatch):
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.logger = _Logger()

    def broken_loader(*, asset):
        raise RuntimeError("disk gone")

    runtime.load_live_inventory_basis_v4_history = broken_loader

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 3:
            raise asyncio.CancelledError

    monkeypatch.setattr("main.asyncio.sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            runtime.refresh_live_inventory_basis_v4_history_loop(
                asset="ETH", interval_seconds=600.0
            )
        )

    assert runtime.logger.warnings.count("v4_history_refresh_failed") >= 1
