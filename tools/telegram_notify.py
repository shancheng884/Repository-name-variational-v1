#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.telegram_notifier import TelegramNotifier


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Configure and test optional Telegram trade notifications."
    )
    parser.add_argument(
        "--discover-chat-id",
        action="store_true",
        help="List chats that have recently sent the configured bot a message.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(ROOT / ".env")
    notifier = TelegramNotifier.from_env(
        logger=logging.getLogger("telegram_test")
    )
    if args.discover_chat_id:
        chats, detail = notifier.discover_chat_ids()
        if not chats:
            print(f"telegram_chat_discovery=FAIL detail={detail}")
            return 2
        print("telegram_chat_discovery=PASS")
        for chat in chats:
            print(
                "chat_id={chat_id} type={type} name={name}".format(**chat)
            )
        return 0

    ok, detail = notifier.send_now(
        "[Var/Lighter] Telegram test\n"
        f"time={datetime.now(timezone.utc).isoformat()}"
    )
    print(f"telegram_test={'PASS' if ok else 'FAIL'} detail={detail}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
