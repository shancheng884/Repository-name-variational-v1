import asyncio
from types import SimpleNamespace

import pytest

from main import VariationalToLighterRuntime


class _QuietLogger:
    def __init__(self):
        self.records = []

    def _log(self, level, msg, *args, **kwargs):
        self.records.append((level, str(msg)))

    def info(self, msg, *args, **kwargs):
        self._log("info", msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._log("warning", msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._log("error", msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self._log("exception", msg, *args, **kwargs)


class _PrimeRuntime(VariationalToLighterRuntime):
    def __init__(self, frames_sequence, health_payload=None):
        self.logger = _QuietLogger()
        self._frames = list(frames_sequence)
        self._health_payload = health_payload if health_payload is not None else {}
        self.reload_calls = []

    async def variational_captured_price_frames(self):  # type: ignore[override]
        if not self._frames:
            return 0
        return self._frames.pop(0)

    async def send_variational_command(self, **kwargs):  # type: ignore[override]
        self.reload_calls.append(kwargs)
        return {"ok": True}

    async def fetch_variational_stream_health(self):  # type: ignore[override]
        return self._health_payload


def test_prime_skips_reload_when_frames_already_captured():
    runtime = _PrimeRuntime([3])
    asyncio.run(
        runtime.prime_variational_page_streams(
            asset="ETH", timeout_seconds=1, poll_seconds=0.01
        )
    )
    assert runtime.reload_calls == []


def test_prime_reloads_page_until_frames_appear():
    runtime = _PrimeRuntime([0, 0, 5])
    asyncio.run(
        runtime.prime_variational_page_streams(
            asset="ETH", timeout_seconds=2, poll_seconds=0.01
        )
    )
    assert len(runtime.reload_calls) == 2
    assert runtime.reload_calls[0]["payload"]["type"] == "VAR_API_RELOAD_PAGE"
    assert runtime.reload_calls[0]["lane"] == "read"


def test_prime_returns_without_raising_on_timeout():
    runtime = _PrimeRuntime([0, 0])
    asyncio.run(
        runtime.prime_variational_page_streams(
            asset="ETH", timeout_seconds=0.02, poll_seconds=0.01
        )
    )
    assert runtime.reload_calls != []


class _HealthRuntime(VariationalToLighterRuntime):
    def __init__(self, health_payload):
        self.logger = _QuietLogger()
        self._health_payload = health_payload

    async def fetch_variational_stream_health(self):  # type: ignore[override]
        return self._health_payload


def test_captured_price_frames_counts_only_price_streams():
    payload = {
        "ok": True,
        "result": {
            "streams": [
                {"url": "wss://x/events", "frameCount": 4},
                {"url": "wss://x/prices", "frameCount": 7},
                {"matchedPattern": "wss://x/prices", "frameCount": 1},
            ]
        },
    }
    runtime = _HealthRuntime(payload)
    assert asyncio.run(runtime.variational_captured_price_frames()) == 8


def test_captured_price_frames_returns_zero_on_bad_payload():
    runtime = _HealthRuntime({"ok": False, "error": "boom"})
    assert asyncio.run(runtime.variational_captured_price_frames()) == 0


class _PreflightRuntime(VariationalToLighterRuntime):
    def __init__(self, results):
        self.logger = _QuietLogger()
        self._results = list(results)
        self.sent = []
        self.primed = []

    async def send_variational_command(self, **kwargs):  # type: ignore[override]
        self.sent.append(kwargs)
        return self._results.pop(0)

    async def prime_variational_page_streams(self, **kwargs):  # type: ignore[override]
        self.primed.append(kwargs)


def test_preflight_retries_when_extension_disconnects_mid_command():
    runtime = _PreflightRuntime(
        [
            {"ok": False, "error": "Extension disconnected before order result."},
            {"ok": True},
        ]
    )
    asyncio.run(
        runtime.preflight_variational_api_command_client(
            "ETH", extension_wait_seconds=5, retry_interval_seconds=0.01
        )
    )
    assert len(runtime.sent) == 2
    assert runtime.primed == []


def test_preflight_fails_fast_on_unexpected_error():
    runtime = _PreflightRuntime([{"ok": False, "error": "boom"}])
    with pytest.raises(RuntimeError):
        asyncio.run(
            runtime.preflight_variational_api_command_client(
                "ETH", extension_wait_seconds=1, retry_interval_seconds=0.01
            )
        )
    assert runtime.primed == []


def test_preflight_default_wait_is_long_enough_for_extension_reconnect():
    import inspect

    signature = inspect.signature(
        VariationalToLighterRuntime.preflight_variational_api_command_client
    )
    assert signature.parameters["extension_wait_seconds"].default >= 120
