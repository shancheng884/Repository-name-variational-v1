#!/usr/bin/env python3
"""Send independently verified billing updates without loading the live runtime."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import read_json
from tools.lib.telegram_notifier import TelegramNotifier
from tools.lib.wti_billing_records import load_verified_records, billing_message, publish_billing_updates


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--billing-dir', type=Path, default=ROOT / 'log/wti_billing_records')
    parser.add_argument('--wti-state-path', type=Path, default=ROOT / 'log/wti_live_state.json')
    parser.add_argument('--delivery-state', type=Path, default=ROOT / 'log/wti_billing_telegram_state.json')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    try:
        records = load_verified_records(args.billing_dir, read_json(args.wti_state_path))
        if args.dry_run:
            for record in records.values():
                print(billing_message(record))
            print(json.dumps({'verified_rounds': len(records), 'dry_run': True}))
            return 0
        load_dotenv(ROOT / '.env')
        outcomes = publish_billing_updates(records, args.delivery_state,
            TelegramNotifier.from_env(logger=logging.getLogger('wti_billing_report')))
        print(json.dumps({'billing_notifications': outcomes, 'orders_attempted': False}))
        return 0 if all(v == 'sent' for v in outcomes.values()) else 2
    except Exception as exc:
        print(json.dumps({'status': 'unavailable', 'error_type': type(exc).__name__, 'orders_attempted': False}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
