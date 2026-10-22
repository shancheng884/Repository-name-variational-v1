#!/usr/bin/env python3
"""Read-only Var USOILP / Lighter WTI account and open-order probe."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.wti_readonly_adapter import (  # noqa: E402
    WTIReadOnlyAccountAdapter,
    WTIReadOnlyError,
    VariationalForwarderClient,
    validate_lighter_market_metadata,
)


DEFAULT_LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"


def _model_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "to_dict"):
        result = value.to_dict()
    elif hasattr(value, "model_dump"):
        result = value.model_dump(mode="json")
    else:
        raise WTIReadOnlyError("lighter_sdk_response_invalid")
    if not isinstance(result, dict):
        raise WTIReadOnlyError("lighter_sdk_response_invalid")
    return result


def fetch_lighter_market_metadata(
    base_url: str,
    *,
    expected_market_id: int | None = None,
) -> dict[str, Any]:
    response = requests.get(
        f"{base_url.rstrip('/')}/api/v1/orderBooks",
        headers={"accept": "application/json"},
        timeout=10,
    )
    response.raise_for_status()
    return validate_lighter_market_metadata(
        response.json(),
        expected_market_id=expected_market_id,
    )


class LighterSDKReadOnlyReader:
    """Uses only account and account-active-orders SDK reads."""

    def __init__(self, *, client: Any, account_index: int, api_key_index: int) -> None:
        self.client = client
        self.account_index = account_index
        self.api_key_index = api_key_index

    async def read_account_and_active_orders(
        self, market_id: int
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        from lighter import AccountApi, OrderApi

        account_result = await AccountApi(self.client.api_client).account(
            by="index",
            value=str(self.account_index),
            _request_timeout=10.0,
        )
        account = _model_dict(account_result)
        token, error = self.client.create_auth_token_with_expiry(
            api_key_index=self.api_key_index
        )
        if error is not None or not token:
            raise WTIReadOnlyError("lighter_read_token_unavailable")
        orders_result = await OrderApi(self.client.api_client).account_active_orders(
            auth=token,
            account_index=self.account_index,
            market_id=market_id,
            _request_timeout=10.0,
        )
        return account, _model_dict(orders_result)


async def _close_client(client: Any) -> None:
    api_client = getattr(client, "api_client", None)
    close = getattr(api_client, "close", None)
    if not callable(close):
        close = getattr(client, "close", None)
    if callable(close):
        result = close()
        if inspect.isawaitable(result):
            await result


def _load_env() -> tuple[int, int, str]:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError as exc:
        raise WTIReadOnlyError("python_dotenv_dependency_missing") from exc
    load_dotenv(ROOT / ".env", override=False)
    try:
        account_index = int(os.environ["LIGHTER_ACCOUNT_INDEX"])
        api_key_index = int(os.environ["LIGHTER_API_KEY_INDEX"])
    except (KeyError, ValueError) as exc:
        raise WTIReadOnlyError("lighter_account_configuration_missing") from exc
    private_key = (
        os.environ.get("API_KEY_PRIVATE_KEY", "").strip()
        or os.environ.get("LIGHTER_PRIVATE_KEY", "").strip()
    )
    if not private_key:
        raise WTIReadOnlyError("lighter_read_key_missing")
    return account_index, api_key_index, private_key


async def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    report: dict[str, Any] = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "scope": "Variational USOILP swap + Lighter mainnet WTI",
        "mode": "read_only",
        "orders_attempted": False,
        "execution_enabled": False,
        "status": "unverified",
    }
    try:
        account_index, api_key_index, private_key = _load_env()
        market = await asyncio.to_thread(
            fetch_lighter_market_metadata,
            args.lighter_base_url,
            expected_market_id=args.expected_market_id,
        )
        report["lighter_market"] = {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in market.items()
        }
        from lighter.signer_client import SignerClient

        client = SignerClient(
            url=args.lighter_base_url.rstrip("/"),
            account_index=account_index,
            api_private_keys={api_key_index: private_key},
        )
        try:
            check_error = client.check_client()
            if check_error is not None:
                raise WTIReadOnlyError("lighter_sdk_preflight_failed")
            lighter_reader = LighterSDKReadOnlyReader(
                client=client,
                account_index=account_index,
                api_key_index=api_key_index,
            )
            async with VariationalForwarderClient(
                args.command_url,
                timeout_seconds=args.timeout_seconds,
            ) as variational:
                adapter = WTIReadOnlyAccountAdapter(
                    variational=variational,
                    lighter=lighter_reader,
                    market=market,
                    max_read_window_seconds=args.max_read_window_seconds,
                    request_timeout_ms=int(args.timeout_seconds * 1000),
                )
                snapshot = await adapter.read_account()
        finally:
            await _close_client(client)
        report.update({
            "status": "verified_read",
            "snapshot": snapshot["normalized"],
            "observed_at": snapshot["observed_at"],
            "read_window_ms": snapshot["read_window_ms"],
            "active_order_count": snapshot["normalized"]["active_order_count"],
            "lighter_market": snapshot["market"],
        })
    except WTIReadOnlyError as exc:
        report["error"] = str(exc)
    except Exception as exc:
        report["error"] = f"read_failed:{type(exc).__name__}"
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only Var USOILP / Lighter WTI account probe; never places orders."
    )
    parser.add_argument("--command-url", default="ws://127.0.0.1:8768")
    parser.add_argument("--lighter-base-url", default=DEFAULT_LIGHTER_URL)
    parser.add_argument("--expected-market-id", type=int)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-read-window-seconds", type=float, default=15.0)
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0 or args.max_read_window_seconds <= 0:
        parser.error("timeouts must be positive")
    if args.expected_market_id is not None and args.expected_market_id < 0:
        parser.error("--expected-market-id must be non-negative")
    return args


def main() -> int:
    report = asyncio.run(run_probe(parse_args()))
    print(json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if report.get("status") == "verified_read" else 1


if __name__ == "__main__":
    raise SystemExit(main())
