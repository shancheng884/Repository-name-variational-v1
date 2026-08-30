from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Callable

from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH = ROOT / ".env"
ENV_KEY_PATTERN = re.compile(
    r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*="
)
UNQUOTED_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9_./:+-]+$")

DEFAULT_SETTINGS = {
    "RISK_WAKEUP_ENABLED": "true",
    "RISK_WAKEUP_ALERT_WHEN_FLAT_STRATEGY_STOPPED": "true",
    "RISK_WAKEUP_POLL_SECONDS": "5",
    "RISK_WAKEUP_HEARTBEAT_MAX_AGE_SECONDS": "45",
    "RISK_WAKEUP_PENDING_MAX_AGE_SECONDS": "30",
    "RISK_WAKEUP_DATA_UNAVAILABLE_CRITICAL_SECONDS": "300",
    "RISK_WAKEUP_VOICE_ESCALATION_SECONDS": "120",
    "RISK_WAKEUP_VOICE_REPEAT_SECONDS": "900",
    "RISK_WAKEUP_MAX_VOICE_CALLS": "3",
    "RISK_WAKEUP_VOICE_ONLY_AT_NIGHT": "true",
    "RISK_WAKEUP_NIGHT_START": "23:00",
    "RISK_WAKEUP_NIGHT_END": "08:00",
    "PUSHOVER_RETRY_SECONDS": "60",
    "PUSHOVER_EXPIRE_SECONDS": "1800",
    "PUSHOVER_EMERGENCY_SOUND": "siren",
    "TENCENT_VMS_REGION": "ap-guangzhou",
    "TENCENT_VMS_PLAY_TIMES": "2",
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
        output.append("# Independent night risk wakeup watchdog")
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


def _prompt_value(
    *,
    key: str,
    label: str,
    existing: dict[str, str | None],
    hidden: bool,
    required: bool,
    input_fn: Callable[[str], str],
    secret_input_fn: Callable[[str], str],
) -> tuple[bool, str]:
    present = bool(existing.get(key))
    suffix = "（已配置，留空保留）" if present else (
        "（必填）" if required else "（可选）"
    )
    prompt = f"{label}{suffix}: "
    value = (secret_input_fn if hidden else input_fn)(prompt).strip()
    if value:
        return True, value
    if present:
        return False, ""
    if required:
        raise ValueError(f"missing_required_value:{key}")
    return False, ""


def collect_updates(
    *,
    existing: dict[str, str | None],
    input_fn: Callable[[str], str] = input,
    secret_input_fn: Callable[[str], str] = getpass.getpass,
) -> dict[str, str]:
    updates = dict(DEFAULT_SETTINGS)
    fields = (
        ("PUSHOVER_APP_TOKEN", "Pushover 应用 Token", True, True),
        ("PUSHOVER_USER_KEY", "Pushover User Key", True, True),
        ("PUSHOVER_DEVICE", "Pushover 设备名", False, False),
        ("TENCENTCLOUD_SECRET_ID", "腾讯云 SecretId", True, True),
        ("TENCENTCLOUD_SECRET_KEY", "腾讯云 SecretKey", True, True),
        ("TENCENT_VMS_SDK_APP_ID", "腾讯云 VoiceSdkAppid", False, True),
        ("TENCENT_VMS_TEMPLATE_ID", "腾讯云语音模板 ID", False, True),
        ("TENCENT_VMS_CALLED_NUMBER", "接听手机号（+86...）", True, True),
    )
    for key, label, hidden, required in fields:
        changed, value = _prompt_value(
            key=key,
            label=label,
            existing=existing,
            hidden=hidden,
            required=required,
            input_fn=input_fn,
            secret_input_fn=secret_input_fn,
        )
        if changed:
            updates[key] = value
    effective_phone = updates.get("TENCENT_VMS_CALLED_NUMBER") or existing.get(
        "TENCENT_VMS_CALLED_NUMBER"
    )
    if effective_phone and not re.fullmatch(r"\+[1-9][0-9]{7,15}", effective_phone):
        raise ValueError("invalid_TENCENT_VMS_CALLED_NUMBER_use_E164")
    return updates


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
    if not sys.stdin.isatty():
        print("setup_failed=interactive_tty_required")
        return 2
    env_path = args.env_file.resolve()
    existing = {
        str(key): value
        for key, value in dotenv_values(env_path).items()
        if key is not None
    }
    print("夜间风险叫醒配置。敏感输入不会回显，也不会写入日志。")
    print("在全部字段校验通过前，不会修改 .env。")
    try:
        updates = collect_updates(existing=existing)
        write_env_updates(env_path, updates)
    except KeyboardInterrupt:
        print("\nsetup_cancelled .env_not_modified")
        return 130
    except (OSError, ValueError) as exc:
        print(f"setup_failed={type(exc).__name__}:{exc}")
        return 2
    print("setup_saved=PASS")
    print(f"env_file={env_path}")
    print("env_permissions=600")
    print("secrets_printed=false")
    print("next=python tools/risk_wakeup_watchdog.py --check")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
