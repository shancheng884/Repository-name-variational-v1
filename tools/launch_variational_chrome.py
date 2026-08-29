from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Iterable


VARIATIONAL_URL = "https://omni.variational.io/"
REQUIRED_BACKGROUND_FLAGS = (
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
)


def find_chrome_binary(explicit: str | None = None) -> str | None:
    if explicit:
        return shutil.which(explicit) or (
            explicit if Path(explicit).is_file() else None
        )
    for candidate in (
        "google-chrome-stable",
        "google-chrome",
        "chromium",
        "chromium-browser",
    ):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def default_user_data_dir(binary: str, home: Path) -> Path:
    if "chromium" in Path(binary).name.lower():
        return home / ".config" / "chromium"
    return home / ".config" / "google-chrome"


def build_chrome_command(
    *,
    binary: str,
    extension_dir: Path,
    user_data_dir: Path,
    profile_directory: str,
    url: str = VARIATIONAL_URL,
) -> list[str]:
    return [
        binary,
        f"--user-data-dir={user_data_dir.as_posix()}",
        f"--profile-directory={profile_directory}",
        *REQUIRED_BACKGROUND_FLAGS,
        "--disable-features=CalculateNativeWinOcclusion,IntensiveWakeUpThrottling",
        "--disable-session-crashed-bubble",
        "--no-first-run",
        "--no-default-browser-check",
        "--restore-last-session",
        f"--load-extension={extension_dir.as_posix()}",
        url,
    ]


def iter_browser_commands(proc_root: Path = Path("/proc")) -> Iterable[list[str]]:
    if not proc_root.is_dir():
        return
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            parts = [
                item.decode("utf-8", errors="replace")
                for item in (entry / "cmdline").read_bytes().split(b"\0")
                if item
            ]
        except (OSError, PermissionError):
            continue
        if not parts:
            continue
        executable = Path(parts[0]).name.lower()
        if is_browser_executable(executable):
            yield parts


def is_browser_executable(executable: str) -> bool:
    name = Path(executable).name.lower()
    return name in {"chrome", "chromium", "chromium-browser"} or name.startswith(
        "google-chrome"
    )


def parse_ps_browser_commands(output: str) -> Iterable[list[str]]:
    for line in output.splitlines():
        try:
            parts = shlex.split(line.strip())
        except ValueError:
            continue
        if parts and is_browser_executable(parts[0]):
            yield parts


def command_has_background_flags(command: list[str]) -> bool:
    return all(flag in command for flag in REQUIRED_BACKGROUND_FLAGS)


def collect_browser_commands(
    proc_root: Path = Path("/proc"),
    *,
    ps_output: str | None = None,
) -> list[list[str]]:
    commands = list(iter_browser_commands(proc_root))
    if any(command_has_background_flags(command) for command in commands):
        return commands

    if ps_output is None:
        result = subprocess.run(
            ["ps", "-ww", "-eo", "args="],
            check=False,
            capture_output=True,
            text=True,
        )
        ps_output = result.stdout if result.returncode == 0 else ""

    seen = {tuple(command) for command in commands}
    for command in parse_ps_browser_commands(ps_output):
        key = tuple(command)
        if key not in seen:
            commands.append(command)
            seen.add(key)
    return commands


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Launch the Variational Chrome profile without background throttling. "
            "Run this from the persistent VPS desktop session."
        )
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--chrome-binary")
    parser.add_argument("--user-data-dir")
    parser.add_argument("--profile-directory", default="Default")
    parser.add_argument("--url", default=VARIATIONAL_URL)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    commands = collect_browser_commands()
    if args.check:
        if not commands:
            print("chrome_status=stopped")
            return 1
        protected = [command for command in commands if command_has_background_flags(command)]
        print("chrome_status=running")
        print(f"chrome_processes={len(commands)}")
        print(f"background_protected={bool(protected)}")
        return 0 if protected else 2

    if os.name != "posix":
        raise SystemExit("This launcher is intended for the Linux VPS desktop.")
    if not os.environ.get("DISPLAY"):
        raise SystemExit(
            "DISPLAY is missing. Open a terminal inside the VPS remote desktop first."
        )
    if commands:
        protected = any(command_has_background_flags(command) for command in commands)
        print("REFUSE_START chrome_already_running")
        print(f"background_protected={protected}")
        print("Close every Chrome window while the strategy is safely stopped, then retry.")
        return 2

    binary = find_chrome_binary(args.chrome_binary)
    if binary is None:
        raise SystemExit("Chrome or Chromium was not found.")
    root = Path(__file__).resolve().parents[1]
    extension_dir = root / "chrome_extension"
    user_data_dir = (
        Path(args.user_data_dir).expanduser().resolve()
        if args.user_data_dir
        else default_user_data_dir(binary, Path.home())
    )
    command = build_chrome_command(
        binary=binary,
        extension_dir=extension_dir,
        user_data_dir=user_data_dir,
        profile_directory=args.profile_directory,
        url=args.url,
    )
    log_dir = root / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "variational_chrome.log"
    with log_path.open("ab") as output:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    print(f"chrome_pid={process.pid}")
    print(f"chrome_log={log_path}")
    print(f"extension_dir={extension_dir}")
    print("next=unlock_wallet_open_variational_then_click_forwarder_start_once")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
