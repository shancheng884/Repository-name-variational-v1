from __future__ import annotations

import os
import stat

import pytest

from tools.setup_risk_wakeup import (
    DEFAULT_SETTINGS,
    collect_updates,
    render_env_updates,
    write_env_updates,
)


def test_render_env_updates_preserves_unrelated_values_and_removes_duplicates() -> None:
    original = """# existing
LIGHTER_PRIVATE_KEY=keep-me
PUSHOVER_APP_TOKEN=old
PUSHOVER_APP_TOKEN=duplicate
TELEGRAM_CHAT_ID=123
"""

    rendered = render_env_updates(
        original,
        {
            "PUSHOVER_APP_TOKEN": "new secret with spaces",
            "RISK_WAKEUP_ENABLED": "true",
        },
    )

    assert "LIGHTER_PRIVATE_KEY=keep-me" in rendered
    assert "TELEGRAM_CHAT_ID=123" in rendered
    assert rendered.count("PUSHOVER_APP_TOKEN=") == 1
    assert 'PUSHOVER_APP_TOKEN="new secret with spaces"' in rendered
    assert "RISK_WAKEUP_ENABLED=true" in rendered


def test_write_env_updates_is_atomic_and_private(tmp_path) -> None:
    path = tmp_path / ".env"
    path.write_text("EXISTING=value\n", encoding="utf-8")

    write_env_updates(path, {"PUSHOVER_APP_TOKEN": "secret"})

    assert "EXISTING=value" in path.read_text(encoding="utf-8")
    assert "PUSHOVER_APP_TOKEN=secret" in path.read_text(encoding="utf-8")
    assert not path.with_suffix(".env.risk-wakeup.tmp").exists()
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_collect_updates_hides_secrets_and_preserves_existing_values() -> None:
    existing = {
        "PUSHOVER_APP_TOKEN": "existing-app-token",
        "PUSHOVER_USER_KEY": "existing-user-key",
        "TENCENTCLOUD_SECRET_ID": "existing-secret-id",
        "TENCENTCLOUD_SECRET_KEY": "existing-secret-key",
        "TENCENT_VMS_SDK_APP_ID": "existing-app-id",
        "TENCENT_VMS_TEMPLATE_ID": "existing-template-id",
        "TENCENT_VMS_CALLED_NUMBER": "+8613800000000",
    }
    prompts = []
    visible_answers = iter(["", "", ""])
    hidden_answers = iter(["", "", "", "", ""])

    def visible(prompt):
        prompts.append(prompt)
        return next(visible_answers)

    def hidden(prompt):
        prompts.append(prompt)
        return next(hidden_answers)

    updates = collect_updates(
        existing=existing,
        input_fn=visible,
        secret_input_fn=hidden,
    )

    assert updates == DEFAULT_SETTINGS
    prompt_text = "\n".join(prompts)
    for value in existing.values():
        assert value not in prompt_text


def test_collect_updates_rejects_non_e164_phone() -> None:
    visible_answers = iter(["", "app-id", "template-id"])
    hidden_answers = iter(
        ["app-token", "user-key", "secret-id", "secret-key", "13800000000"]
    )

    with pytest.raises(ValueError, match="use_E164"):
        collect_updates(
            existing={},
            input_fn=lambda _prompt: next(visible_answers),
            secret_input_fn=lambda _prompt: next(hidden_answers),
        )
