#!/usr/bin/env python3
"""Read-only proof that every probe venue is flat before a run is allowed."""
from __future__ import annotations
import asyncio, json, os, sys
from decimal import Decimal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.market_capture.config import DEFAULT_ASSETS, load_environment
from tools.lib.venue_probe import (EntropyProbeExecutor, LighterProbeExecutor,
                                   VariationalProbeExecutor, _d)


def executors(env):
    out = {}
    out["variational"] = VariationalProbeExecutor(env.get("VAR_COMMAND_URL", "ws://127.0.0.1:8768"))
    if env.get("LIGHTER_ACCOUNT_INDEX") and env.get("LIGHTER_API_KEY_INDEX"):
        out["lighter_mainnet"] = LighterProbeExecutor(
            name="lighter_mainnet",
            rest_url=env.get("LIGHTER_MAINNET_REST_URL", "https://mainnet.zklighter.elliot.ai"),
            account_index=int(env["LIGHTER_ACCOUNT_INDEX"]),
            api_key_index=int(env["LIGHTER_API_KEY_INDEX"]),
            private_key=env.get("LIGHTER_PRIVATE_KEY", ""))
    if env.get("LIGHTER_RH_ACCOUNT_INDEX") and env.get("LIGHTER_RH_API_KEY_INDEX"):
        out["lighter_rh"] = LighterProbeExecutor(
            name="lighter_rh",
            rest_url=env.get("LIGHTER_RH_REST_URL", "https://api.rh.lighter.xyz"),
            account_index=int(env["LIGHTER_RH_ACCOUNT_INDEX"]),
            api_key_index=int(env["LIGHTER_RH_API_KEY_INDEX"]),
            private_key=env.get("LIGHTER_RH_API_PRIVATE_KEY", ""))
    address = (env.get("ENTROPY_ACCOUNT_ADDRESS") or "").strip()
    key = (env.get("ENTROPY_PRIVATE_KEY") or env.get("HYPERLIQUID_PRIVATE_KEY") or "").strip()
    if address and key:
        out["entropy"] = EntropyProbeExecutor(
            info_url=env.get("ENTROPY_INFO_URL", "https://api.hyperliquid.xyz/info"),
            account_address=address, private_key=key, dex=env.get("ENTROPY_ALLOWED_DEX", "io"))
    return out


async def main():
    env = load_environment()
    ex = executors(env)
    problems = []
    rows = []
    try:
        started = []
        for name, executor in ex.items():
            try:
                await executor.start()
                started.append(executor)
            except Exception as exc:
                problems.append(f"{name}_start_failed:{type(exc).__name__}:{exc}")
        for asset in DEFAULT_ASSETS:
            for name, executor in ex.items():
                if not asset.supports(name):
                    continue
                try:
                    result = await executor.positions(asset)
                except Exception as exc:
                    result = {"ok": False, "verified": False, "error": f"{type(exc).__name__}:{exc}"}
                qty = _d(result.get("qty"))
                orders = result.get("active_orders")
                verified = bool(result.get("verified"))
                flat = bool(verified and qty == Decimal("0") and (orders in (None, 0)))
                rows.append({"asset": asset.name, "venue": name, "verified": verified,
                             "qty": None if qty is None else str(qty),
                             "active_orders": orders, "flat": flat,
                             "error": result.get("error")})
                if not flat:
                    problems.append(f"{asset.name}:{name}:qty={qty}:orders={orders}:verified={verified}")
        for executor in started:
            try:
                await executor.close()
            except Exception:
                pass
    finally:
        pass
    print(json.dumps({"flat": not problems, "problems": problems, "rows": rows}, ensure_ascii=False, indent=2))
    return 0 if not problems else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
