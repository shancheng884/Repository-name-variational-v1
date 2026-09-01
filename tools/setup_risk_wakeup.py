from __future__ import annotations

import argparse
import json
import os
import re
import stat
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH = ROOT / ".env"
ENV_KEY_PATTERN = re.compile(
    r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*="
)
UNQUOTED_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9_./:+-]+$")

DEFAULT_SETTINGS = {
    "RISK_WAKEUP_ENABLED": "true",
    "RISK_WAKEUP_MONITOR_STRATEGY": "false",
    "RISK_WAKEUP_ALERT_WHEN_FLAT_STRATEGY_STOPPED": "true",
    "RISK_WAKEUP_POLL_SECONDS": "3",
    "RISK_WAKEUP_HEARTBEAT_MAX_AGE_SECONDS": "45",
    "RISK_WAKEUP_PENDING_MAX_AGE_SECONDS": "30",
    "RISK_WAKEUP_DATA_UNAVAILABLE_CRITICAL_SECONDS": "300",
    "RISK_WAKEUP_CHANNEL_RETRY_SECONDS": "10",
    "RISK_WAKEUP_MAX_PHONE_ATTEMPTS": "3",
}


def quote_env_value(value: str) -> str:
    if value == "":
        return ""
    if UNQUOTED_VALUE_PATTERN.fullmatch(value):
        return value
    return json.dumps(value, ensure_ascii=False)


def render_env_updates(original: str, updates: dict[str, str]) -> str:
    output: list[str] = []
    replaced: set[str] = set()
    for line in original.splitlines():
        match = ENV_KEY_PATTERN.match(line)
        key = match.group(1) if match else None
        if key not in updates:
            output.append(line)
            continue
        if key in replaced:
            continue
        output.append(f"{key}={quote_env_value(updates[key])}")
        replaced.add(key)
    missing = [key for key in updates if key not in replaced]
    if missing:
        if output and output[-1].strip():
            output.append("")
        output.append("# Independent account risk wakeup watchdog")
        output.extend(
            f"{key}={quote_env_value(updates[key])}" for key in missing
        )
    return "\n".join(output).rstrip("\n") + "\n"


def write_env_updates(path: Path, updates: dict[str, str]) -> None:
    try:
        original = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        original = ""
    rendered = render_env_updates(original, updates)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".risk-wakeup.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.chmod(temporary, stat.S_IRUSR | stat.S_IWUSR)
        os.replace(temporary, path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Secure local setup for the risk wakeup watchdog",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_PATH,
        help=argparse.SUPPRESS,
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    env_path = args.env_file.resolve()
    try:
        write_env_updates(env_path, DEFAULT_SETTINGS)
    except OSError as exc:
        print(f"setup_failed={type(exc).__name__}:{exc}")
        return 2
    print("setup_saved=PASS")
    print(f"env_file={env_path}")
    print("env_permissions=600")
    print("strategy_monitor=DISABLED_HEARTBEAT_ONLY")
    print("notification_secrets=unchanged_private_json")
    print("next=python tools/risk_wakeup_watchdog.py --check")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
