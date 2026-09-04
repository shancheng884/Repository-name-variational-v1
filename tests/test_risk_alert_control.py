from datetime import datetime, timedelta, timezone

from tools.lib.risk_alert_control import (
    notifications_allowed,
    read_alert_control,
    silence_alerts,
    suppression_reason,
)


def test_timed_silence_expires(tmp_path) -> None:
    now = datetime(2026, 9, 4, tzinfo=timezone.utc)
    path = tmp_path / "alert-control.json"
    silence_alerts(path, minutes=2, now=now)
    control = read_alert_control(path)

    assert not notifications_allowed(control, now=now + timedelta(seconds=30))
    assert suppression_reason(control, now=now + timedelta(seconds=30)) == "operator_silence"
    assert notifications_allowed(control, now=now + timedelta(minutes=3))


def test_missing_expiry_is_permanent_silence(tmp_path) -> None:
    path = tmp_path / "alert-control.json"
    path.write_text(
        '{"notifications_enabled": false, "reason": "manual_review"}',
        encoding="utf-8",
    )

    assert suppression_reason(read_alert_control(path)) == "manual_review"
