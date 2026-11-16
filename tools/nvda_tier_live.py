#!/usr/bin/env python3
"""NVDA five-tier live runner (entropy vs one Lighter book).

Standalone: main.py is untouched. Observe mode is the default; live orders need
explicit flags. A failed leg is flattened immediately instead of leaving a
naked side.
"""
from __future__ import annotations
import argparse, asyncio, json, os, sys, time, uuid
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec
from tools.lib.venue_probe import EntropyProbeExecutor, LighterProbeExecutor, _d
from tools.market_capture.config import DEFAULT_ASSETS, CaptureConfig, load_environment
from tools.market_capture.runner import CaptureRunner

STATE = ROOT / "log" / "nvda_tier_live_state.json"


def build_executors(env):
    address = (env.get("ENTROPY_ACCOUNT_ADDRESS") or "").strip()
    key = (env.get("ENTROPY_PRIVATE_KEY") or env.get("HYPERLIQUID_PRIVATE_KEY") or "").strip()
    return {
        "entropy": EntropyProbeExecutor(
            info_url=env.get("ENTROPY_INFO_URL", "https://api.hyperliquid.xyz/info"),
            account_address=address, private_key=key, dex=env.get("ENTROPY_ALLOWED_DEX", "io")),
        "lighter_mainnet": LighterProbeExecutor(
            name="lighter_mainnet",
            rest_url=env.get("LIGHTER_MAINNET_REST_URL", "https://mainnet.zklighter.elliot.ai"),
            account_index=int(env["LIGHTER_ACCOUNT_INDEX"]),
            api_key_index=int(env["LIGHTER_API_KEY_INDEX"]),
            private_key=env.get("LIGHTER_PRIVATE_KEY", "")),
        "lighter_rh": LighterProbeExecutor(
            name="lighter_rh",
            rest_url=env.get("LIGHTER_RH_REST_URL", "https://api.rh.lighter.xyz"),
            account_index=int(env["LIGHTER_RH_ACCOUNT_INDEX"]),
            api_key_index=int(env["LIGHTER_RH_API_KEY_INDEX"]),
            private_key=env.get("LIGHTER_RH_API_PRIVATE_KEY", "")),
    }


async def snapshot(ex, asset, venues):
    """Real broker positions. Only a verified read counts as known."""
    out = {}
    for venue in venues:
        try:
            result = await ex[venue].positions(asset)
            out[venue] = _d(result.get("qty")) if result.get("verified") else None
        except Exception:
            out[venue] = None
    return out


async def place_pair(ex, asset, buy, sell, qty, *, opening, quotes):
    async def one(venue, side):
        cid = "nvda-tier-" + uuid.uuid4().hex[:12]
        try:
            return venue, await ex[venue].market_order(asset=asset, side=side, qty=qty,
                                                       quote=quotes.get(venue) or {},
                                                       client_order_id=cid, reduce_only=not opening,
                                                       max_slippage_bps=Decimal("12"))
        except Exception as exc:
            return venue, {"ok": False, "status": "error", "error": "%s:%s" % (type(exc).__name__, exc)}
    tasks = [one(buy, "sell" if opening else "buy"), one(sell, "buy" if opening else "sell")]
    return dict(await asyncio.gather(*tasks))


def write_state(payload):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
    os.replace(tmp, STATE)


async def main_async(args):
    env = load_environment()
    asset = next(a for a in DEFAULT_ASSETS if a.name == args.asset)
    buy, sell = args.pair.split(">")
    venues = (buy, sell)
    thresholds = [float(x) for x in args.tiers.split(",") if x.strip()]
    cfg = TierConfig(asset=args.asset, buy_venue=buy, sell_venue=sell,
                     tiers=tuple(TierSpec(i, t) for i, t in enumerate(thresholds, start=1)),
                     notional_usd=args.notional, timeout_seconds=None, slippage_bps=args.slippage_bps)
    ledger = PositionLedger()
    engine = TierEngine(config=cfg, ledger=ledger)
    ex = build_executors(env)
    for executor in ex.values():
        await executor.start()
    out_dir = ROOT / "log" / "nvda_tier_live_capture"
    cap = CaptureRunner(CaptureConfig(output_dir=out_dir, assets=(asset,), var_quote_seconds=1.0,
                                      book_snapshot_seconds=5.0, health_seconds=2.0, latency_seconds=5.0,
                                      system_seconds=60.0, fill_poll_seconds=2.0, edge_seconds=1.0,
                                      notional_usd=Decimal(str(args.notional)), max_book_levels=20))
    task = asyncio.create_task(cap.run(), name="nvda-tier-capture")
    history: deque = deque(maxlen=3600)
    dev_window: deque = deque(maxlen=3)
    realized = Decimal("0")
    exchange = await snapshot(ex, asset, venues)
    snap_at = time.time()
    try:
        while True:
            await asyncio.sleep(max(0.5, args.poll_seconds))
            qb = cap.hub.latest(buy, asset.name, max_age=6.0 if buy == "entropy" else 3.0)
            qs = cap.hub.latest(sell, asset.name, max_age=6.0 if sell == "entropy" else 3.0)
            if not qb or not qs:
                continue
            eb = _d(qb.get("executable_buy")); es = _d(qs.get("executable_sell"))
            if not eb or not es or eb <= 0:
                continue
            mid = (eb + es) / 2
            edge = float((es - eb) / mid * Decimal("10000"))
            history.append(edge)
            need = max(120, int(args.baseline_minutes * 60 / max(1.0, args.poll_seconds) * 0.5))
            if len(history) < need:
                write_state({"status": "warming", "samples": len(history), "need": need,
                             "pair": args.pair, "updated_at": datetime.now(timezone.utc).isoformat()})
                continue
            ordered = sorted(history)
            baseline = ordered[len(ordered) // 2]
            dev_window.append(edge - baseline)
            if time.time() - snap_at >= 5.0:
                exchange = await snapshot(ex, asset, venues)
                snap_at = time.time()
            actions = engine.on_sample(stamp=time.time(), edge=edge, baseline=baseline,
                                       price_hint=float(mid), window=list(dev_window),
                                       exchange=exchange or {})
            if actions and args.mode == "live":
                for action in actions:
                    if action.get("action") == "open":
                        exchange = await snapshot(ex, asset, venues)
                        snap_at = time.time()
                        qty = Decimal(str(action["qty"]))
                        receipts = await place_pair(ex, asset, buy, sell, qty, opening=True,
                                                     quotes={buy: qb, sell: qs})
                        if not all(isinstance(r, dict) and r.get("status") == "filled" for r in receipts.values()):
                            for venue in receipts:
                                side = "sell" if venue == buy else "buy"
                                try:
                                    await ex[venue].market_order(asset=asset, side=side, qty=qty, quote={},
                                                                 client_order_id="nvda-tier-flat-" + uuid.uuid4().hex[:10],
                                                                 reduce_only=True, max_slippage_bps=Decimal("30"))
                                except Exception:
                                    pass
                            engine.open_tiers.pop(action["tier"], None)
                            write_state({"status": "manual_review", "reason": "leg_not_filled",
                                         "receipts": receipts, "updated_at": datetime.now(timezone.utc).isoformat()})
                            return {"ok": False, "error": "leg_not_filled"}
                    if action.get("action") == "close":
                        qty = Decimal(str(action.get("qty") or 0)) or Decimal(str(args.notional)) / mid
                        await place_pair(ex, asset, buy, sell, qty, opening=False, quotes={buy: qb, sell: qs})
                        realized += Decimal(str(action["net_bps"])) / Decimal("10000") * Decimal(str(args.notional))
            if realized <= -abs(Decimal(str(args.max_loss_usd))):
                write_state({"status": "stopped_max_loss", "realized": str(realized)})
                return {"ok": False, "error": "max_loss"}
            write_state({"status": "running" if args.mode == "live" else "observe",
                         "pair": args.pair, "edge_bps": round(edge, 2), "baseline_bps": round(baseline, 2),
                         "dev_bps": round(edge - baseline, 2), "open_tiers": sorted(engine.open_tiers),
                         "trades": len(engine.trades), "realized_usd": str(realized),
                         "exchange": {k: (None if v is None else str(v)) for k, v in (exchange or {}).items()},
                         "updated_at": datetime.now(timezone.utc).isoformat()})
    finally:
        cap.request_stop()
        try:
            await asyncio.wait_for(task, timeout=10)
        except Exception:
            task.cancel()
        for executor in ex.values():
            try:
                await executor.close()
            except Exception:
                pass


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset", default="NVDA")
    parser.add_argument("--pair", default="entropy>lighter_mainnet")
    parser.add_argument("--tiers", default="7.77,8.36,9.60,11.21,13.84")
    parser.add_argument("--notional", type=float, default=20.0)
    parser.add_argument("--slippage-bps", type=float, default=1.3)
    parser.add_argument("--baseline-minutes", type=float, default=30.0)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--max-loss-usd", type=float, default=3.0)
    parser.add_argument("--mode", choices=("observe", "live"), default="observe")
    parser.add_argument("--confirm-flat", action="store_true")
    parser.add_argument("--i-understand-live-orders", action="store_true")
    args = parser.parse_args(argv)
    if args.mode == "live" and not (args.confirm_flat and args.i_understand_live_orders):
        raise SystemExit("live mode needs --confirm-flat and --i-understand-live-orders")
    result = asyncio.run(main_async(args))
    print(json.dumps(result or {"ok": True}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
