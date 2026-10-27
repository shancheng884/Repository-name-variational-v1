#!/usr/bin/env python3
"""Read-only exact-order fee verification for the latest closed WTI position."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.wti_fee_audit import audit_latest_closed_position_fees  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read exact closed-position orders and fees; never submits orders or modifies state."
    )
    parser.add_argument("--state-path", type=Path, default=ROOT / "log/wti_live_state.json")
    parser.add_argument("--command-url", default="ws://127.0.0.1:8768")
    parser.add_argument(
        "--lighter-base-url", default="https://mainnet.zklighter.elliot.ai"
    )
    parser.add_argument("--expected-market-id", type=int, default=145)
    parser.add_argument("--timeout-seconds", type=float, default=12)
    return parser


async def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    from dotenv import load_dotenv
    from lighter.signer_client import SignerClient
    from tools.lib.wti_live_gateway import WTIProductionGateway
    from tools.lib.wti_readonly_adapter import VariationalForwarderClient
    from tools.wti_account_probe import (
        _close_client,
        _load_env,
        fetch_lighter_market_metadata,
    )

    load_dotenv(ROOT / ".env", override=False)
    state = json.loads(args.state_path.read_text(encoding="utf-8"))
    account_index, api_key_index, private_key = _load_env()
    market = await asyncio.to_thread(
        fetch_lighter_market_metadata,
        args.lighter_base_url,
        expected_market_id=args.expected_market_id,
    )
    client = SignerClient(
        url=args.lighter_base_url.rstrip("/"),
        account_index=account_index,
        api_private_keys={api_key_index: private_key},
    )
    try:
        check_error = await asyncio.to_thread(client.check_client)
        if check_error is not None:
            raise RuntimeError("lighter_sdk_preflight_failed")
        async with VariationalForwarderClient(
            args.command_url,
            timeout_seconds=args.timeout_seconds,
        ) as variational:
            gateway = WTIProductionGateway(
                loop=asyncio.get_running_loop(),
                variational=variational,
                account_reader=object(),
                lighter_client=client,
                account_index=account_index,
                api_key_index=api_key_index,
                market=market,
                operation_timeout_seconds=args.timeout_seconds,
            )
            return await audit_latest_closed_position_fees(
                state,
                lambda client_order_id, request: gateway._lookup_order(
                    client_order_id, request
                ),
            )
    finally:
        await _close_client(client)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.expected_market_id < 0 or args.timeout_seconds <= 0:
        raise SystemExit("market ID and timeout must be positive")
    try:
        report = asyncio.run(run_audit(args))
    except Exception as exc:
        report = {
            "status": "unverified",
            "reason": f"{type(exc).__name__}:{' '.join(str(exc).split())[:160]}",
            "orders_attempted": False,
            "state_modified": False,
            "fees_usd": None,
            "funding_status": "unverified",
            "legs": [],
        }
    print(json.dumps(report, ensure_ascii=True, sort_keys=True))
    return 0 if report.get("status") == "confirmed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
