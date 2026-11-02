from tools.lib.quote_throttle import (
    ExitQuoteThrottle,
    RfqAttemptLog,
    should_probe_exit_rfq,
)


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


def _throttle(**kwargs):
    clock = FakeClock()
    return ExitQuoteThrottle(clock=clock, **kwargs), clock


def test_allows_first_request():
    throttle, _ = _throttle()

    assert throttle.allow("ETH") is True


def test_success_blocks_for_cooldown_then_allows():
    throttle, clock = _throttle(cooldown_seconds=60.0)

    throttle.record_success("ETH")
    assert throttle.allow("ETH") is False

    clock.advance(59.9)
    assert throttle.allow("ETH") is False

    clock.advance(0.2)
    assert throttle.allow("ETH") is True


def test_failure_walks_the_backoff_ladder():
    throttle, clock = _throttle(backoff_seconds=(5.0, 10.0, 20.0, 60.0))

    waits = []
    for _ in range(5):
        waits.append(throttle.record_failure("ETH"))
        clock.advance(waits[-1])

    assert waits == [5.0, 10.0, 20.0, 60.0, 60.0]


def test_failure_blocks_until_backoff_elapses():
    throttle, clock = _throttle(backoff_seconds=(5.0, 10.0))

    throttle.record_failure("ETH")
    assert throttle.allow("ETH") is False

    clock.advance(5.0)
    assert throttle.allow("ETH") is True


def test_success_resets_the_failure_count():
    throttle, clock = _throttle(backoff_seconds=(5.0, 10.0, 60.0))

    throttle.record_failure("ETH")
    clock.advance(5.0)
    throttle.record_failure("ETH")
    assert throttle.failures("ETH") == 2

    throttle.record_success("ETH")
    assert throttle.failures("ETH") == 0

    clock.advance(60.0)
    assert throttle.record_failure("ETH") == 5.0


def test_keys_are_isolated_and_case_insensitive():
    throttle, clock = _throttle(cooldown_seconds=60.0)

    throttle.record_success("eth")

    assert throttle.allow("ETH") is False
    assert throttle.allow("WTI") is True

    clock.advance(60.0)
    assert throttle.allow("eth") is True


def test_remaining_reports_time_left():
    throttle, clock = _throttle(cooldown_seconds=60.0)

    throttle.record_success("ETH")
    clock.advance(15.0)

    assert round(throttle.remaining("ETH"), 1) == 45.0
    clock.advance(100.0)
    assert throttle.remaining("ETH") == 0.0


def test_reset_single_key_and_all_keys():
    throttle, _ = _throttle()

    throttle.record_success("ETH")
    throttle.record_failure("WTI")
    throttle.reset("ETH")

    assert throttle.allow("ETH") is True
    assert throttle.allow("WTI") is False

    throttle.reset()
    assert throttle.allow("WTI") is True
    assert throttle.failures("WTI") == 0


def test_zero_cooldown_allows_immediately_after_success():
    throttle, _ = _throttle(cooldown_seconds=0.0)

    throttle.record_success("ETH")

    assert throttle.allow("ETH") is True


def test_snapshot_reports_config_and_pending_keys():
    throttle, clock = _throttle(cooldown_seconds=60.0)
    throttle.record_success("ETH")
    clock.advance(10.0)

    snapshot = throttle.snapshot()

    assert snapshot["cooldown_seconds"] == 60.0
    assert snapshot["keys"]["ETH"]["consecutive_failures"] == 0
    assert round(snapshot["keys"]["ETH"]["next_allowed_in_seconds"], 1) == 50.0


def test_throttle_keeps_requests_within_a_one_per_minute_server_budget():
    """Simulates the observed server quota: one accepted RFQ per 60 seconds."""

    throttle, clock = _throttle(cooldown_seconds=60.0, backoff_seconds=(5.0, 10.0, 20.0, 60.0))
    accepted_at = []

    for _ in range(12):  # twelve exit loops, 30 seconds apart
        if throttle.allow("ETH"):
            accepted_at.append(clock.now)
            throttle.record_success("ETH")
        else:
            throttle.record_failure("ETH")
        clock.advance(30.0)

    gaps = [round(b - a, 1) for a, b in zip(accepted_at, accepted_at[1:])]
    assert gaps, "throttle never allowed a request"
    assert min(gaps) >= 60.0


def test_probe_allows_when_threshold_unknown():
    assert should_probe_exit_rfq(passive_pnl_bps=0.0, threshold_bps=None)["probe"] is True


def test_probe_allows_when_passive_pnl_missing():
    result = should_probe_exit_rfq(passive_pnl_bps=None, threshold_bps=4.5)

    assert result["probe"] is True
    assert result["reason"] == "passive_pnl_unavailable"


def test_probe_allows_when_passive_is_near_threshold():
    result = should_probe_exit_rfq(
        passive_pnl_bps=3.2, threshold_bps=4.5, margin_bps=1.5
    )

    assert result["probe"] is True
    assert result["reason"] == "passive_near_threshold"


def test_probe_skips_when_far_below_and_recently_probed():
    result = should_probe_exit_rfq(
        passive_pnl_bps=-0.2,
        threshold_bps=4.5,
        margin_bps=1.5,
        seconds_since_last_probe=10.0,
        probe_interval_seconds=180.0,
    )

    assert result["probe"] is False
    assert result["reason"] == "passive_pnl_far_below_threshold"


def test_probe_fires_heartbeat_even_when_far_below():
    result = should_probe_exit_rfq(
        passive_pnl_bps=-5.0,
        threshold_bps=4.5,
        seconds_since_last_probe=200.0,
        probe_interval_seconds=180.0,
    )

    assert result["probe"] is True
    assert result["reason"] == "heartbeat_probe"


def test_probe_treats_missing_probe_history_as_due():
    result = should_probe_exit_rfq(
        passive_pnl_bps=-5.0, threshold_bps=4.5, seconds_since_last_probe=None
    )

    assert result["probe"] is True


def test_probe_boundary_is_inclusive():
    at_margin = should_probe_exit_rfq(
        passive_pnl_bps=3.0, threshold_bps=4.5, margin_bps=1.5, seconds_since_last_probe=1.0
    )
    below_margin = should_probe_exit_rfq(
        passive_pnl_bps=2.9, threshold_bps=4.5, margin_bps=1.5, seconds_since_last_probe=1.0
    )

    assert at_margin["probe"] is True
    assert below_margin["probe"] is False


def test_attempt_log_counts_outcomes_and_gaps():
    clock = FakeClock()
    log = RfqAttemptLog(window_seconds=600.0, clock=clock)

    log.record(outcome="success", waited_ms=120.0)
    clock.advance(60.0)
    log.record(outcome="unavailable", waited_ms=None)
    clock.advance(60.0)
    log.record(outcome="success", waited_ms=90.0)

    summary = log.summary()

    assert summary["attempts"] == 3
    assert summary["outcomes"] == {"success": 2, "unavailable": 1}
    assert summary["min_success_gap_seconds"] == 120.0
    assert summary["avg_success_gap_seconds"] == 120.0
    assert summary["successes_per_minute"] == 0.2


def test_attempt_log_drops_rows_outside_the_window():
    clock = FakeClock()
    log = RfqAttemptLog(window_seconds=600.0, clock=clock)

    log.record(outcome="success")
    clock.advance(900.0)
    log.record(outcome="success")

    summary = log.summary()

    assert summary["attempts"] == 1


def test_attempt_log_respects_maxlen():
    clock = FakeClock()
    log = RfqAttemptLog(window_seconds=600.0, maxlen=2, clock=clock)

    for outcome in ("success", "unavailable", "throttled"):
        log.record(outcome=outcome)

    assert log.summary()["attempts"] == 2
