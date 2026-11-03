#!/usr/bin/env python3
"""Read-only funding ledger: pull Lighter funding payments and append them.

Runs hourly from a systemd timer. Never touches the trading state or orders.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lighter.api import AccountApi  # noqa: E402
from lighter.api_client import ApiClient  # noqa: E402
from lighter.configuration import Configuration  # noqa: E402
from lighter.signer_client import SignerClient  # noqa: E402

from tools.lib.funding_ledger import append_rows, read_ledger  # noqa: E402

HOST = "https://mainnet.zklighter.elliot.ai"
LEDGER = ROOT / "log" / "funding_ledger.jsonl"
HEALTH = ROOT / "log" / "funding_ledger_health.json"
STATE = ROOT / "log" / "live_inventory_state.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", type=int, default=0)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--lookback-hours", type=float, default=48.0)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--ledger", type=Path, default=LEDGER)
    parser.add_argument("--health", type=Path, default=HEALTH)
    parser.add_argument("--json", action="store_true")
    return parser


def _write_health(path: Path, payload: dict) -> None:
    target = Path(path)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(target)


def _position_size(state_path: Path, asset: str) -> float | None:
    try:
        state = json.loads(Path(state_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    total = 0.0
    for lot in state.get("open_lots") or []:
        if str(lot.get("asset") or asset).upper() != asset.upper():
            continue
        try:
            total += float(lot.get("qty") or 0)
        except (TypeError, ValueError):
            continue
    return total


async def collect(args: argparse.Namespace) -> dict:
    account = int(os.environ["LIGHTER_ACCOUNT_INDEX"])
    key_index = int(os.environ["LIGHTER_API_KEY_INDEX"])
    private_key = os.environ["LIGHTER_PRIVATE_KEY"]
    signer = SignerClient(
        url=HOST, account_index=account, api_private_keys={key_index: private_key}
    )
    token, token_error = signer.create_auth_token_with_expiry(api_key_index=key_index)
    if not token:
        return {"status": "error", "error": f"auth_token_unavailable:{token_error}"}

    client = ApiClient(Configuration(host=HOST))
    try:
        api = AccountApi(client)
        response = await api.position_funding(
            account_index=account,
            limit=args.limit,
            market_id=args.market_id,
            auth=token,
        )
    finally:
        await client.close()

    rows = getattr(response, "position_fundings", None) or []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.lookback_hours)
    records = []
    for row in rows:
        stamp = datetime.fromtimestamp(int(row.timestamp), tz=timezone.utc)
        if stamp < cutoff:
            continue
        records.append(
            {
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "venue": "lighter",
                "asset": args.asset.upper(),
                "market_id": args.market_id,
                "funding_id": row.funding_id,
                "funding_at": stamp.isoformat(),
                "rate": row.rate,
                "position_size": row.position_size,
                "position_side": row.position_side,
                "change_usd": row.change,
                "source": "lighter_position_funding",
            }
        )
    return {"status": "ok", "records": records, "fetched": len(rows)}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload: dict = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "asset": args.asset.upper(),
    }
    try:
        result = asyncio.run(collect(args))
    except Exception as exc:  # noqa: BLE001 - reported, never raised at the timer
        result = {"status": "error", "error": f"{type(exc).__name__}:{exc}"}
    if result.get("status") != "ok":
        payload.update(status=result.get("status"), error=result.get("error"))
        _write_health(args.health, payload)
        print(f"funding_ledger=FAILED {payload.get('error')}")
        return 1

    written = append_rows(args.ledger, result["records"])
    total = read_ledger(args.ledger)
    payload.update(
        status="ok",
        rows_fetched=result["fetched"],
        rows_written=written,
        rows_total=len(total),
        position_qty=_position_size(STATE, args.asset),
        note="variational_funding_unavailable",
        ledger=str(args.ledger),
    )
    _write_health(args.health, payload)
    print(
        f"funding_ledger=PASS fetched={result['fetched']} written={written} "
        f"total_rows={len(total)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())