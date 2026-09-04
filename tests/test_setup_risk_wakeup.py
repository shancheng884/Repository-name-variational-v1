from __future__ import annotations

import os
import stat

from tools.setup_risk_wakeup import (
    DEFAULT_SETTINGS,
    render_env_updates,
    write_env_updates,
)


def test_render_env_updates_preserves_unrelated_values_and_removes_duplicates() -> None:
    original = """# existing
LIGHTER_PRIVATE_KEY=keep-me
RISK_WAKEUP_POLL_SECONDS=old
RISK_WAKEUP_POLL_SECONDS=duplicate
TELEGRAM_CHAT_ID=123
"""

    rendered = render_env_updates(
        original,
        {
            "RISK_WAKEUP_POLL_SECONDS": "3",
            "RISK_WAKEUP_ENABLED": "true",
        },
    )

    assert "LIGHTER_PRIVATE_KEY=keep-me" in rendered
    assert "TELEGRAM_CHAT_ID=123" in rendered
    assert rendered.count("RISK_WAKEUP_POLL_SECONDS=") == 1
    assert "RISK_WAKEUP_POLL_SECONDS=3" in rendered
    assert "RISK_WAKEUP_ENABLED=true" in rendered


def test_write_env_updates_is_atomic_and_private(tmp_path) -> None:
    path = tmp_path / ".env"
    path.write_text("EXISTING=value\n", encoding="utf-8")

    write_env_updates(path, {"RISK_WAKEUP_MONITOR_STRATEGY": "false"})

    assert "EXISTING=value" in path.read_text(encoding="utf-8")
    assert "RISK_WAKEUP_MONITOR_STRATEGY=false" in path.read_text(
        encoding="utf-8"
    )
    assert not path.with_suffix(".env.risk-wakeup.tmp").exists()
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_defaults_start_in_heartbeat_only_mode() -> None:
    assert DEFAULT_SETTINGS["RISK_WAKEUP_ENABLED"] == "true"
    assert DEFAULT_SETTINGS["RISK_WAKEUP_MONITOR_STRATEGY"] == "false"
    assert DEFAULT_SETTINGS["RISK_WAKEUP_CHANNEL_RETRY_SECONDS"] == "60"
    assert DEFAULT_SETTINGS["RISK_WAKEUP_BACKUP_CHANNEL_RETRY_SECONDS"] == "60"
    assert DEFAULT_SETTINGS["RISK_WAKEUP_MAX_CHANNEL_ATTEMPTS"] == "3"
    assert DEFAULT_SETTINGS["RISK_WAKEUP_MAX_PHONE_ATTEMPTS"] == "1"
