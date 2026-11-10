import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from main import (
    LIVE_INVENTORY_BASIS_PASSIVE_REFERENCE_DUPLICATE_GRACE_SECONDS,
    VariationalToLighterRuntime,
)

ROOT = Path(__file__).resolve().parents[1]


def _runtime():
    return VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)


def test_age_uses_monotonic_stamp():
    runtime = _runtime()
    quote = {"received_monotonic": time.monotonic() - 2.0}
    age = runtime.live_inventory_passive_reference_age_seconds(quote)
    assert age is not None and 1.5 < age < 3.5


def test_age_falls_back_to_received_at():
    runtime = _runtime()
    quote = {"received_at": (datetime.now(timezone.utc) - timedelta(seconds=7)).isoformat()}
    age = runtime.live_inventory_passive_reference_age_seconds(quote)
    assert age is not None and 6.0 < age < 8.5


def test_age_returns_none_without_timestamp():
    runtime = _runtime()
    assert runtime.live_inventory_passive_reference_age_seconds({}) is None


def test_duplicate_grace_window_is_five_seconds():
    assert LIVE_INVENTORY_BASIS_PASSIVE_REFERENCE_DUPLICATE_GRACE_SECONDS == 5.0


def test_skip_condition_requires_the_quote_to_age_out():
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    # 重复报价只有在超过宽限期后才允许跳过，否则健康窗口会被饿死。
    assert "LIVE_INVENTORY_BASIS_PASSIVE_REFERENCE_DUPLICATE_GRACE_SECONDS" in source
    block_start = source.index('sample_skip_reason = "duplicate_passive_reference"')
    window = source[max(0, block_start - 1200):block_start]
    assert "live_inventory_passive_reference_age_seconds" in window
