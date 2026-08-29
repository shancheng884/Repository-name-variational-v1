from pathlib import Path

from tools.launch_variational_chrome import (
    REQUIRED_BACKGROUND_FLAGS,
    build_chrome_command,
    command_has_background_flags,
    default_user_data_dir,
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
