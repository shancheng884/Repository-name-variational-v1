from tools.market_capture.venues.base import VenueHealth


def test_error_clears_only_after_three_successful_messages():
    health = VenueHealth("test")
    health.error("transient 429")
    assert health.public()["last_error"] == "transient 429"
    health.message()
    health.message()
    assert health.public()["last_error"] == "transient 429"
    health.message()
    public = health.public()
    assert public["last_error"] is None
    assert public["last_error_at"] is not None
    assert public["success_streak"] == 3
    assert "transient 429" in public["recent_errors"]


def test_repeated_error_resets_success_streak():
    health = VenueHealth("test")
    health.error("first")
    health.message(); health.message()
    health.error("second")
    health.message(); health.message()
    assert health.public()["last_error"] == "second"
    health.message()
    assert health.public()["last_error"] is None
