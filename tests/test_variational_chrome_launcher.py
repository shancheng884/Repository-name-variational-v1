from pathlib import Path

from tools.launch_variational_chrome import (
    REQUIRED_BACKGROUND_FLAGS,
    build_chrome_command,
    collect_browser_commands,
    command_has_background_flags,
    default_user_data_dir,
    is_browser_executable,
)


def test_chrome_command_preserves_wallet_profile_and_disables_throttling() -> None:
    command = build_chrome_command(
        binary="/usr/bin/google-chrome",
        extension_dir=Path("/repo/chrome_extension"),
        user_data_dir=Path("/home/ubuntu/.config/google-chrome"),
        profile_directory="Default",
    )

    assert "--user-data-dir=/home/ubuntu/.config/google-chrome" in command
    assert "--profile-directory=Default" in command
    assert "--load-extension=/repo/chrome_extension" in command
    assert all(flag in command for flag in REQUIRED_BACKGROUND_FLAGS)
    assert command_has_background_flags(command) is True


def test_default_profile_matches_chrome_family() -> None:
    home = Path("/home/ubuntu")

    assert default_user_data_dir("google-chrome", home) == (
        home / ".config" / "google-chrome"
    )
    assert default_user_data_dir("chromium", home) == home / ".config" / "chromium"


def test_crashpad_is_not_treated_as_a_browser_process() -> None:
    assert is_browser_executable("chrome") is True
    assert is_browser_executable("chromium-browser") is True
    assert is_browser_executable("chrome_crashpad_handler") is False


def test_ps_fallback_finds_protected_main_process(tmp_path: Path) -> None:
    process = tmp_path / "100"
    process.mkdir()
    (process / "cmdline").write_bytes(
        b"/snap/chromium/current/chrome_crashpad_handler\0--monitor-self\0"
    )
    protected_command = " ".join(
        [
            "/snap/chromium/current/chrome",
            *REQUIRED_BACKGROUND_FLAGS,
            "https://omni.variational.io/",
        ]
    )

    commands = collect_browser_commands(tmp_path, ps_output=protected_command)

    assert len(commands) == 1
    assert command_has_background_flags(commands[0]) is True
