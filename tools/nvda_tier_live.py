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


async def venue_equity(ex, asset, venues):
    """Verified free margin per venue, read before any order goes out.

    Uses the account read rather than preflight: preflight refuses to answer
    while a position is open, and equity has to stay readable across restarts
    with tiers already filled.
    """
    out = {}
    for venue in venues:
        value = None
        try:
            result = await ex[venue].account_state(asset)
            if isinstance(result, dict) and result.get("ok"):
                raw = result.get("available_margin")
                if raw not in (None, ""):
                    value = float(raw)
        except Exception:
            value = None
        out[venue] = value
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
    # Opening: buy on the buy venue, sell on the sell venue.
    # Closing: the reverse, so each leg reduces what it opened.
    tasks = [one(buy, "buy" if opening else "sell"), one(sell, "sell" if opening else "buy")]
    return dict(await asyncio.gather(*tasks))


QTY_TOL = Decimal("0.002")


async def position_qty(ex, asset, venue):
    """Verified position size, or None when the venue could not be read."""
    try:
        result = await ex[venue].positions(asset)
    except Exception:
        return None
    if not isinstance(result, dict) or not result.get("verified"):
        return None
    return _d(result.get("qty"))


async def settle_positions(ex, asset, venues, expected, *, attempts=3):
    """Force the venues to match the ledger and prove it from a fresh read.

    Any difference is closed with a reduce-only order, which can shrink a
    position but never open one, so a correction cannot create a naked leg.
    """
    legs = {}
    for _ in range(max(1, attempts)):
        residual = {}
        for venue in venues:
            qty = await position_qty(ex, asset, venue)
            want = expected[venue]
            if qty is None:
                legs[venue] = {"expected": str(want), "actual": None, "error": "position_unreadable"}
                return {"ok": False, "error": "position_unreadable", "legs": legs}
            delta = qty - want
            legs[venue] = {"expected": str(want), "actual": str(qty), "residual": str(delta)}
            if abs(delta) > QTY_TOL:
                residual[venue] = delta
        if not residual:
            return {"ok": True, "legs": legs}
        for venue, delta in residual.items():
            side = "sell" if delta > 0 else "buy"
            try:
                receipt = await ex[venue].market_order(
                    asset=asset, side=side, qty=abs(delta), quote={},
                    client_order_id="nvda-tier-settle-" + uuid.uuid4().hex[:10],
                    reduce_only=True, max_slippage_bps=Decimal("30"))
                legs[venue]["fix"] = (receipt or {}).get("status")
            except Exception as exc:
                legs[venue]["fix_error"] = "%s:%s" % (type(exc).__name__, exc)
        await asyncio.sleep(0.8)
    return {"ok": False, "error": "position_mismatch", "legs": legs}


def write_state(payload):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
    os.replace(tmp, STATE)


def _tier_list(text):
    return [float(value) for value in text.split(",") if value.strip()]


def _build_engine(asset_name, buy_venue, sell_venue, tiers, equity, args, ledger):
    cfg = TierConfig(asset=asset_name, buy_venue=buy_venue, sell_venue=sell_venue,
                     tiers=tuple(TierSpec(i, t) for i, t in enumerate(tiers, start=1)),
                     child_notional_usd=args.child_notional,
                     equity_usd=equity, notional_usd=args.notional,
                     timeout_seconds=None, slippage_bps=args.slippage_bps)
    return cfg, TierEngine(config=cfg, ledger=ledger)


async def main_async(args):
    env = load_environment()
    asset = next(a for a in DEFAULT_ASSETS if a.name == args.asset)
    buy, sell = args.pair.split(">")
    venues = (buy, sell)
    tiers_forward = _tier_list(args.tiers)
    tiers_reverse = _tier_list(args.tiers_reverse) if args.tiers_reverse else list(tiers_forward)
    ex = build_executors(env)
    for executor in ex.values():
        await executor.start()
    leverage = {}
    if args.mode == "live" and args.rh_leverage > 0 and "lighter_rh" in venues:
        leverage["lighter_rh"] = await ex["lighter_rh"].ensure_leverage(asset, args.rh_leverage)
        if not leverage["lighter_rh"].get("ok"):
            write_state({"status": "blocked_leverage", "pair": args.pair, "leverage": leverage,
                         "updated_at": datetime.now(timezone.utc).isoformat()})
            return {"ok": False, "error": "leverage_not_confirmed", "leverage": leverage}
    equity = await venue_equity(ex, asset, venues)
    ledger = PositionLedger()
    cfg_forward, engine_forward = _build_engine(args.asset, buy, sell, tiers_forward, equity, args, ledger)
    cfg_reverse, engine_reverse = _build_engine(args.asset, sell, buy, tiers_reverse, equity, args, ledger)
    tiers = {"forward": tiers_forward, "reverse": tiers_reverse}
    caps = cfg_forward.slot_caps()
    ladder = [cfg_forward.tier_target_notional_usd(i) for i in range(1, len(tiers_forward) + 1)]
    write_state({"status": "starting", "pair": args.pair, "equity": equity,
                 "slot_caps": caps, "cumulative_notional": ladder, "tiers": tiers,
                 "child_notional": args.child_notional, "leverage": leverage,
                 "updated_at": datetime.now(timezone.utc).isoformat()})
    if caps is None:
        print("warning: venue equity unavailable, falling back to %.0fU per tier" % args.notional)
    out_dir = ROOT / "log" / "nvda_tier_live_capture"
    cap = CaptureRunner(CaptureConfig(output_dir=out_dir, assets=(asset,), var_quote_seconds=1.0,
                                      book_snapshot_seconds=5.0, health_seconds=2.0, latency_seconds=5.0,
                                      system_seconds=60.0, fill_poll_seconds=2.0, edge_seconds=1.0,
                                      notional_usd=Decimal(str(args.notional)), max_book_levels=20))
    task = asyncio.create_task(cap.run(), name="nvda-tier-capture")
    history = {"forward": deque(maxlen=3600), "reverse": deque(maxlen=3600)}
    dev_window = {"forward": deque(maxlen=3), "reverse": deque(maxlen=3)}
    realized = Decimal("0")
    close_failures = {}
    open_block_until = 0.0
    exchange = await snapshot(ex, asset, venues)
    snap_at = time.time()
    need = max(120, int(args.baseline_minutes * 60 / max(1.0, args.poll_seconds) * 0.5))
    try:
        while True:
            await asyncio.sleep(max(0.5, args.poll_seconds))
            q_buy = cap.hub.latest(buy, asset.name, max_age=6.0 if buy == "entropy" else 3.0)
            q_sell = cap.hub.latest(sell, asset.name, max_age=6.0 if sell == "entropy" else 3.0)
            if not q_buy or not q_sell:
                continue
            buy_ask = _d(q_buy.get("executable_buy"))
            buy_bid = _d(q_buy.get("executable_sell"))
            sell_ask = _d(q_sell.get("executable_buy"))
            sell_bid = _d(q_sell.get("executable_sell"))
            if not buy_ask or not buy_bid or not sell_ask or not sell_bid:
                continue
            if buy_ask <= 0 or buy_bid <= 0 or sell_ask <= 0 or sell_bid <= 0:
                continue
            forward_mid = (buy_ask + sell_bid) / 2
            reverse_mid = (sell_ask + buy_bid) / 2
            edge_forward = float((sell_bid - buy_ask) / forward_mid * Decimal("10000"))
            edge_reverse = float((buy_bid - sell_ask) / reverse_mid * Decimal("10000"))
            history["forward"].append(edge_forward)
            history["reverse"].append(edge_reverse)
            if len(history["forward"]) < need or len(history["reverse"]) < need:
                write_state({"status": "warming",
                             "samples": min(len(history["forward"]), len(history["reverse"])),
                             "need": need, "pair": args.pair, "equity": equity,
                             "slot_caps": caps, "cumulative_notional": ladder, "tiers": tiers,
                             "child_notional": args.child_notional, "leverage": leverage,
                             "updated_at": datetime.now(timezone.utc).isoformat()})
                continue
            baseline = {}
            for key in ("forward", "reverse"):
                ordered = sorted(history[key])
                baseline[key] = ordered[len(ordered) // 2]
            dev_window["forward"].append(edge_forward - baseline["forward"])
            dev_window["reverse"].append(edge_reverse - baseline["reverse"])
            if time.time() - snap_at >= 5.0:
                exchange = await snapshot(ex, asset, venues)
                snap_at = time.time()
            stamp = time.time()
            # Both directions always evaluate exits; only the flat side may open,
            # which is the ETH gradient's direction lock until fully flat.
            acts_forward = engine_forward.on_sample(
                stamp=stamp, edge=edge_forward, baseline=baseline["forward"],
                price_hint=float(forward_mid), window=list(dev_window["forward"]),
                exchange=exchange or {}, allow_entry=not engine_reverse.open_tiers)
            acts_reverse = engine_reverse.on_sample(
                stamp=stamp, edge=edge_reverse, baseline=baseline["reverse"],
                price_hint=float(reverse_mid), window=list(dev_window["reverse"]),
                exchange=exchange or {}, allow_entry=not engine_forward.open_tiers)
            for action in acts_forward:
                action["direction"] = "forward"
            for action in acts_reverse:
                action["direction"] = "reverse"
            actions = acts_forward + acts_reverse
            if actions and args.mode == "live":
                for action in actions:
                    side_buy, side_sell = (buy, sell) if action.get("direction") == "forward" else (sell, buy)
                    ob, os_ = (q_buy, q_sell) if action.get("direction") == "forward" else (q_sell, q_buy)
                    engine = engine_forward if action.get("direction") == "forward" else engine_reverse
                    if action.get("action") == "blocked":
                        write_state({"status": "blocked_position_mismatch", "stage": "enter",
                                     "direction": action.get("direction"), "tier": action.get("tier"),
                                     "reason": action.get("reason"), "diffs": action.get("diffs"),
                                     "updated_at": datetime.now(timezone.utc).isoformat()})
                        continue
                    if action.get("action") == "open":
                        if time.time() < open_block_until:
                            engine.drop_tier(action["tier"])
                            continue
                        exchange = await snapshot(ex, asset, venues)
                        snap_at = time.time()
                        qty = Decimal(str(action["qty"]))
                        receipts = await place_pair(ex, asset, side_buy, side_sell, qty, opening=True,
                                                     quotes={side_buy: ob, side_sell: os_})
                        filled = all(isinstance(r, dict) and r.get("status") == "filled"
                                     for r in receipts.values())
                        check = None
                        if filled:
                            check = await settle_positions(ex, asset, venues,
                                                           {v: engine.ledger.net(asset, v) for v in venues})
                        if not filled or not check["ok"]:
                            # A tier only exists if both legs are really on the book.
                            engine.drop_tier(action["tier"])
                            recovery = await settle_positions(ex, asset, venues,
                                                              {v: engine.ledger.net(asset, v) for v in venues})
                            open_block_until = time.time() + 30.0
                            write_state({"status": "open_not_hedged" if recovery["ok"] else "blocked_position_mismatch",
                                         "stage": "open", "direction": action.get("direction"),
                                         "tier": action["tier"], "receipts": receipts,
                                         "settle": check, "recovery": recovery,
                                         "updated_at": datetime.now(timezone.utc).isoformat()})
                            if not recovery["ok"]:
                                return {"ok": False, "error": "open_position_mismatch"}
                    elif action.get("action") == "close":
                        notional = Decimal(str(action.get("notional_usd") or args.notional))
                        qty = Decimal(str(action.get("qty") or 0))
                        if qty <= 0:
                            engine.cancel_close(action["tier"])
                            continue
                        receipts = await place_pair(ex, asset, side_buy, side_sell, qty, opening=False,
                                                    quotes={side_buy: ob, side_sell: os_})
                        expected = {v: engine.ledger.net(asset, v) + (-qty if v == side_buy else qty)
                                    for v in venues}
                        check = await settle_positions(ex, asset, venues, expected)
                        if check["ok"]:
                            trade = engine.confirm_close(action)
                            close_failures.pop(action["tier"], None)
                            if trade:
                                realized += Decimal(str(trade["net_bps"])) / Decimal("10000") * notional
                        else:
                            # Keep the tier: the next sample retries the close and
                            # the residual is measured against the real position.
                            engine.cancel_close(action["tier"])
                            close_failures[action["tier"]] = close_failures.get(action["tier"], 0) + 1
                            write_state({"status": "blocked_position_mismatch", "stage": "close",
                                         "direction": action.get("direction"), "tier": action["tier"],
                                         "receipts": receipts, "settle": check,
                                         "attempts": close_failures[action["tier"]],
                                         "updated_at": datetime.now(timezone.utc).isoformat()})
                            if close_failures[action["tier"]] >= 3:
                                return {"ok": False, "error": "close_position_mismatch"}
            if realized <= -abs(Decimal(str(args.max_loss_usd))):
                write_state({"status": "stopped_max_loss", "realized": str(realized)})
                return {"ok": False, "error": "max_loss"}
            write_state({"status": "running" if args.mode == "live" else "observe",
                         "pair": args.pair, "equity": equity,
                         "slot_caps": caps, "cumulative_notional": ladder, "tiers": tiers,
                         "directions": {
                             "forward": {"pair": "%s>%s" % (buy, sell),
                                         "edge_bps": round(edge_forward, 2),
                                         "baseline_bps": round(baseline["forward"], 2),
                                         "dev_bps": round(edge_forward - baseline["forward"], 2),
                                         "open_tiers": sorted(engine_forward.open_tiers)},
                             "reverse": {"pair": "%s>%s" % (sell, buy),
                                         "edge_bps": round(edge_reverse, 2),
                                         "baseline_bps": round(baseline["reverse"], 2),
                                         "dev_bps": round(edge_reverse - baseline["reverse"], 2),
                                         "open_tiers": sorted(engine_reverse.open_tiers)}},
                         "trades": len(engine_forward.trades) + len(engine_reverse.trades),
                         "realized_usd": str(realized),
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
    parser.add_argument("--tiers-reverse", default="")
    parser.add_argument("--notional", type=float, default=20.0)
    parser.add_argument("--child-notional", type=float, default=20.0)
    parser.add_argument("--slippage-bps", type=float, default=1.3)
    parser.add_argument("--baseline-minutes", type=float, default=30.0)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--max-loss-usd", type=float, default=3.0)
    parser.add_argument("--rh-leverage", type=float, default=20.0,
                        help="leverage to set and verify on lighter_rh before any order")
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
