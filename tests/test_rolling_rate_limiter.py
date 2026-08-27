import asyncio

from tools.lib.rolling_rate_limiter import RollingWindowRateLimiter


def test_rolling_window_reserves_capacity_for_urgent_orders() -> None:
    async def run() -> None:
        now = [100.0]
        limiter = RollingWindowRateLimiter(
            normal_limit=2,
            hard_limit=3,
            window_seconds=60,
            clock=lambda: now[0],
        )
        assert await limiter.acquire() == 0
        assert await limiter.acquire() == 0
        assert limiter.snapshot()["used"] == 2
        assert await limiter.acquire(urgent=True) == 0
        assert limiter.snapshot()["used"] == 3

    asyncio.run(run())


def test_rolling_window_rejects_invalid_limits() -> None:
    try:
        RollingWindowRateLimiter(normal_limit=4, hard_limit=3)
    except ValueError as exc:
        assert "invalid" in str(exc)
    else:
        raise AssertionError("invalid limits must fail")
