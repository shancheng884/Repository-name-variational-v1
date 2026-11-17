#!/usr/bin/env python3
"""NVDA five-tier live runner (entropy vs one Lighter book).

Standalone: main.py is untouched. Observe mode is the default; live orders need
explicit flags.  A failed leg is flattened immediately instead of leaving a
naked side, a tier is booked only after both legs are proved on the venue, and
the book survives a restart: tiers, frozen anchors and the rolling baseline are
persisted and reconciled against the real account before any order can go out.
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

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec, asset_symbol
from tools.lib.venue_probe import EntropyProbeExecutor, LighterProbeExecutor, _d
from tools.market_capture.config import DEFAULT_ASSETS, VENUES, CaptureConfig, load_environment
from tools.market_capture.runner import CaptureRunner

STATE = ROOT / "log" / "nvda_tier_live_state.json"
ENGINE_STATE = ROOT / "log" / "nvda_tier_live_engine.json"
BLOCK_LOG = ROOT / "log" / "nvda_tier_blockers.jsonl"
ORDER_LOG = ROOT / "log" / "nvda_tier_orders.jsonl"
ENGINE_STATE_VERSION = 2

LAST_BLOCK: dict = {}
_BLOCK_WRITTEN_AT: dict = {}


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), timezone.utc).isoformat()


def append_jsonl(path: Path, row: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


def record_block(reason: str, *, min_interval: float = 0.0, **fields) -> None:
    """Keep the current blocking reason, and journal it at most every N seconds."""
    global LAST_BLOCK
    now = time.time()
    LAST_BLOCK = {"reason": reason, "at": _iso(now), **fields}
    last = _BLOCK_WRITTEN_AT.get(reason)
    if last is None or now - last >= max(0.0, min_interval):
        _BLOCK_WRITTEN_AT[reason] = now
        append_jsonl(BLOCK_LOG, {"ts": _iso(now), "reason": reason, **fields})


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


async def _positions(ex, venue, asset):
    """Position read that asks the executor to bypass its short cache when it can."""
    reader = ex[venue].positions
    try:
        return await reader(asset, fresh=True)
    except TypeError:
        return await reader(asset)


async def snapshot(ex, asset, venues):
    """Real broker positions. Only a verified read counts as known."""
    out = {}
    for venue in venues:
        try:
            result = await _positions(ex, venue, asset)
            out[venue] = _d(result.get("qty")) if result.get("verified") else None
        except Exception:
            out[venue] = None
    return out


async def position_snapshot(ex, asset, venues):
    """Positions plus resting orders, used to prove a restart is safe."""
    out = {}
    for venue in venues:
        try:
            result = await _positions(ex, venue, asset)
        except Exception as exc:
            out[venue] = {"verified": False, "qty": None, "active_orders": None,
                          "error": "%s:%s" % (type(exc).__name__, exc)}
            continue
        verified = bool(result.get("verified"))
        out[venue] = {"verified": verified,
                      "qty": result.get("qty") if verified else None,
                      "active_orders": result.get("active_orders"),
                      "error": result.get("error")}
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
        result = await _positions(ex, venue, asset)
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
    payload = dict(payload)
    payload.setdefault("updated_at", _iso())
    if LAST_BLOCK:
        payload["last_block"] = LAST_BLOCK
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


def quote_version(*quotes) -> str:
    """Identity of a pair of quotes; two equal versions are the same snapshot."""
    parts = []
    for quote in quotes:
        quote = quote or {}
        parts.append("%s|%s|%s|%s" % (quote.get("sequence"), quote.get("recv_at"),
                                      quote.get("executable_buy"), quote.get("executable_sell")))
    return "||".join(parts)


def trim_history(samples: deque, cutoff: float) -> None:
    while samples and samples[0][0] < cutoff:
        samples.popleft()


def window_ready(samples: deque, window_seconds: float, *, min_samples: int = 120,
                 coverage: float = 0.8) -> bool:
    """A baseline is only usable when it really spans the requested window."""
    if len(samples) < min_samples:
        return False
    if window_seconds <= 0:
        return True
    return (samples[-1][0] - samples[0][0]) >= window_seconds * coverage


def median_of(samples: deque) -> float:
    values = sorted(value for _, value in samples)
    return values[len(values) // 2]


def trade_result_bps(*, entry_buy: float, entry_sell: float, exit_buy: float, exit_sell: float,
                     entry_fee_usd: float = 0.0, exit_fee_usd: float = 0.0,
                     notional_usd: float) -> dict | None:
    """Real four-fill P&L: captured spread minus the fees actually charged."""
    mid = (entry_buy + entry_sell) / 2.0
    if mid <= 0:
        return None
    gross_bps = ((entry_sell - entry_buy) - (exit_sell - exit_buy)) / mid * 10000.0
    fees = float(entry_fee_usd or 0.0) + float(exit_fee_usd or 0.0)
    fee_bps = fees / notional_usd * 10000.0 if notional_usd > 0 else 0.0
    net_bps = gross_bps - fee_bps
    return {"gross_bps": gross_bps, "fee_bps": fee_bps, "net_bps": net_bps,
            "fees_usd": fees, "net_usd": net_bps / 10000.0 * notional_usd}


def receipt_price(receipt) -> float | None:
    if not isinstance(receipt, dict) or receipt.get("status") != "filled":
        return None
    try:
        value = float(receipt.get("avg_price"))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def plausible_fill(receipt, requested: Decimal) -> Decimal | None:
    """A fill size we can trust: positive and not wildly larger than the order."""
    if not isinstance(receipt, dict) or receipt.get("status") != "filled":
        return None
    try:
        filled = _d(receipt.get("filled_qty"))
    except Exception:
        return None
    if not filled or filled <= 0 or filled > requested * Decimal("1.5"):
        return None
    return filled


def receipts_fee(receipts: dict) -> float:
    total = 0.0
    for receipt in receipts.values():
        if not isinstance(receipt, dict):
            continue
        try:
            total += float(receipt.get("fee_usd") or 0.0)
        except (TypeError, ValueError):
            continue
    return total


def entropy_rebate(receipts: dict) -> float:
    """Entropy refunds 110% of the fees it charged; kept separate from P&L."""
    fee = receipts_fee({venue: receipt for venue, receipt in receipts.items() if venue == "entropy"})
    return fee * 1.1


def seed_history_from_edges(history: dict, out_dir: Path, pair: str, asset: str,
                            window_seconds: float) -> None:
    """First run on a new state schema: reuse the capture's own 1 s edge log.

    The values come from the same hub the live loop reads, so seeding the
    baseline does not invent data; it only avoids a second 30 minute warm-up.
    """
    buy, sell = pair.split(">")
    cutoff = time.time() - window_seconds
    rows = {"forward": [], "reverse": []}
    base = out_dir / "edges" / "cross" / asset
    if not base.exists():
        return
    for path in sorted(base.glob("*.jsonl"))[-3:]:
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if row.get("event") != "edge" or row.get("mode") != "executable":
                        continue
                    try:
                        ts = datetime.fromisoformat(str(row.get("at"))).timestamp()
                        value = float(row.get("edge_bps"))
                    except (TypeError, ValueError):
                        continue
                    if ts < cutoff:
                        continue
                    if row.get("buy_venue") == buy and row.get("sell_venue") == sell:
                        rows["forward"].append((ts, value))
                    elif row.get("buy_venue") == sell and row.get("sell_venue") == buy:
                        rows["reverse"].append((ts, value))
        except Exception:
            continue
    for key, items in rows.items():
        items.sort(key=lambda item: item[0])
        for ts, value in items:
            if history[key] and ts <= history[key][-1][0]:
                continue
            history[key].append((ts, value))


def load_engine_state(path: Path) -> dict | None:
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def save_engine_state(*, asset, pair, tiers, engines, realized, rebate, equity, history) -> None:
    payload = {
        "version": ENGINE_STATE_VERSION,
        "asset": asset,
        "pair": pair,
        "tiers": tiers,
        "realized_usd": str(realized),
        "rebate_usd": str(rebate),
        "equity": equity,
        "engines": {key: engine.dump_state() for key, engine in engines.items()},
        "history": {key: [[round(ts, 3), value] for ts, value in list(samples)][-2000:]
                    for key, samples in history.items()},
        "updated_at": _iso(),
    }
    try:
        ENGINE_STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = ENGINE_STATE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        os.replace(tmp, ENGINE_STATE)
    except Exception:
        pass


async def main_async(args):
    env = load_environment()
    asset = next(a for a in DEFAULT_ASSETS if a.name == args.asset)
    symbol = asset_symbol(asset)
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
            write_state({"status": "blocked_leverage", "pair": args.pair, "leverage": leverage})
            record_block("leverage_not_confirmed", leverage=leverage)
            return {"ok": False, "error": "leverage_not_confirmed", "leverage": leverage}
    equity = await venue_equity(ex, asset, venues)
    ledger = PositionLedger()
    cfg_forward, engine_forward = _build_engine(symbol, buy, sell, tiers_forward, equity, args, ledger)
    cfg_reverse, engine_reverse = _build_engine(symbol, sell, buy, tiers_reverse, equity, args, ledger)
    engines = {"forward": engine_forward, "reverse": engine_reverse}
    tiers = {"forward": tiers_forward, "reverse": tiers_reverse}
    caps = cfg_forward.slot_caps()
    ladder = [cfg_forward.tier_target_notional_usd(i) for i in range(1, len(tiers_forward) + 1)]

    realized = Decimal("0")
    rebate = Decimal("0")
    window_seconds = max(60.0, args.baseline_minutes * 60.0)
    min_samples = 120
    history = {"forward": deque(maxlen=20000), "reverse": deque(maxlen=20000)}
    out_dir = ROOT / "log" / "nvda_tier_live_capture"
    saved = load_engine_state(ENGINE_STATE)
    restored = False
    if (saved and saved.get("version") == ENGINE_STATE_VERSION
            and saved.get("asset") == symbol and saved.get("pair") == args.pair
            and (saved.get("tiers") or {}) == tiers):
        for key, engine in engines.items():
            engine.restore_state((saved.get("engines") or {}).get(key))
        try:
            realized = Decimal(str(saved.get("realized_usd") or "0"))
            rebate = Decimal(str(saved.get("rebate_usd") or "0"))
        except Exception:
            realized, rebate = Decimal("0"), Decimal("0")
        cutoff = time.time() - window_seconds
        for key in ("forward", "reverse"):
            for row in (saved.get("history") or {}).get(key) or []:
                if isinstance(row, (list, tuple)) and len(row) >= 2:
                    try:
                        ts = float(row[0])
                    except (TypeError, ValueError):
                        continue
                    if ts >= cutoff:
                        history[key].append((ts, float(row[1])))
        restored = bool(engine_forward.open_tiers or engine_reverse.open_tiers)
    if not history["forward"] and not history["reverse"]:
        seed_history_from_edges(history, out_dir, args.pair, asset.name, window_seconds)

    # A restart must start from the real account, never from an assumed flat book.
    startup = await position_snapshot(ex, asset, venues)
    problems = []
    for venue in venues:
        row = startup.get(venue) or {}
        expected = ledger.net(symbol, venue)
        actual = _d(row.get("qty")) if row.get("verified") else None
        if not row.get("verified") or actual is None:
            problems.append({"venue": venue, "error": row.get("error") or "position_unreadable"})
            continue
        if row.get("active_orders"):
            problems.append({"venue": venue, "error": "active_orders", "count": row.get("active_orders")})
        if abs(actual - expected) > QTY_TOL:
            problems.append({"venue": venue, "error": "ledger_mismatch",
                             "expected": str(expected), "actual": str(actual)})
    write_state({"status": "starting", "pair": args.pair, "equity": equity,
                 "slot_caps": caps, "cumulative_notional": ladder, "tiers": tiers,
                 "child_notional": args.child_notional, "leverage": leverage,
                 "restored": restored, "realized_usd": str(realized),
                 "rebate_usd": str(rebate), "startup": startup})
    if problems:
        write_state({"status": "blocked_restore_mismatch", "pair": args.pair,
                     "restored": restored, "problems": problems, "startup": startup,
                     "realized_usd": str(realized), "rebate_usd": str(rebate)})
        record_block("restore_mismatch", problems=problems, restored=restored)
        return {"ok": False, "error": "restore_mismatch", "problems": problems}

    cap = CaptureRunner(CaptureConfig(output_dir=out_dir, assets=(asset,),
                                      venue_enabled={name: name in venues for name in VENUES},
                                      var_quote_seconds=1.0,
                                      var_bridge_enabled=("variational" in venues),
                                      book_snapshot_seconds=5.0, health_seconds=2.0, latency_seconds=5.0,
                                      system_seconds=60.0, fill_poll_seconds=2.0, edge_seconds=1.0,
                                      notional_usd=Decimal(str(args.notional)), max_book_levels=20))
    task = asyncio.create_task(cap.run(), name="nvda-tier-capture")
    dev_window = {"forward": deque(maxlen=cfg_forward.confirm_window),
                  "reverse": deque(maxlen=cfg_reverse.confirm_window)}
    last_version = None
    close_failures = {}
    open_block_until = 0.0
    last_save = 0.0
    exchange = await snapshot(ex, asset, venues)
    snap_at = time.time()
    try:
        while True:
            await asyncio.sleep(max(0.5, args.poll_seconds))
            q_buy = cap.hub.latest(buy, asset.name, max_age=6.0 if buy == "entropy" else 3.0)
            q_sell = cap.hub.latest(sell, asset.name, max_age=6.0 if sell == "entropy" else 3.0)
            if not q_buy or not q_sell:
                record_block("quotes_missing", min_interval=30, pair=args.pair,
                             buy=bool(q_buy), sell=bool(q_sell))
                continue
            version = quote_version(q_buy, q_sell)
            if version == last_version:
                # Same two quotes as the previous loop: never counts twice.
                continue
            last_version = version
            buy_ask = _d(q_buy.get("executable_buy"))
            buy_bid = _d(q_buy.get("executable_sell"))
            sell_ask = _d(q_sell.get("executable_buy"))
            sell_bid = _d(q_sell.get("executable_sell"))
            if not buy_ask or not buy_bid or not sell_ask or not sell_bid:
                record_block("quotes_incomplete", min_interval=30, pair=args.pair)
                continue
            if buy_ask <= 0 or buy_bid <= 0 or sell_ask <= 0 or sell_bid <= 0:
                continue
            forward_mid = (buy_ask + sell_bid) / 2
            reverse_mid = (sell_ask + buy_bid) / 2
            edge_forward = float((sell_bid - buy_ask) / forward_mid * Decimal("10000"))
            edge_reverse = float((buy_bid - sell_ask) / reverse_mid * Decimal("10000"))
            stamp = time.time()
            history["forward"].append((stamp, edge_forward))
            history["reverse"].append((stamp, edge_reverse))
            cutoff = stamp - window_seconds
            trim_history(history["forward"], cutoff)
            trim_history(history["reverse"], cutoff)
            ready = {key: window_ready(history[key], window_seconds, min_samples=min_samples)
                     for key in history}
            if not all(ready.values()):
                coverage = {key: round(history[key][-1][0] - history[key][0][0], 1)
                            if len(history[key]) > 1 else 0.0 for key in history}
                write_state({"status": "warming", "samples": min(len(history["forward"]), len(history["reverse"])),
                             "need": min_samples, "window_seconds": window_seconds,
                             "coverage_seconds": coverage, "pair": args.pair, "equity": equity,
                             "slot_caps": caps, "cumulative_notional": ladder, "tiers": tiers,
                             "child_notional": args.child_notional, "leverage": leverage,
                             "restored": restored, "realized_usd": str(realized),
                             "rebate_usd": str(rebate)})
                record_block("warming", min_interval=60, samples=len(history["forward"]),
                             need=min_samples, coverage_seconds=coverage)
                continue
            baseline = {key: median_of(history[key]) for key in history}
            dev_window["forward"].append(edge_forward - baseline["forward"])
            dev_window["reverse"].append(edge_reverse - baseline["reverse"])
            if time.time() - snap_at >= 5.0:
                exchange = await snapshot(ex, asset, venues)
                snap_at = time.time()
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
            close_queued = any(action.get("action") == "close" for action in actions)
            block_before = (LAST_BLOCK or {}).get("at")
            if args.mode == "live":
                for action in actions:
                    side_buy, side_sell = (buy, sell) if action.get("direction") == "forward" else (sell, buy)
                    ob, os_ = (q_buy, q_sell) if action.get("direction") == "forward" else (q_sell, q_buy)
                    engine = engine_forward if action.get("direction") == "forward" else engine_reverse
                    if action.get("action") == "blocked":
                        record_block("position_mismatch", min_interval=30,
                                     direction=action.get("direction"), tier=action.get("tier"),
                                     diffs=action.get("diffs"))
                        continue
                    if action.get("action") == "open":
                        if close_queued:
                            record_block("close_in_flight", min_interval=10,
                                         direction=action.get("direction"), tier=action.get("tier"))
                            continue
                        other = engine_reverse if action.get("direction") == "forward" else engine_forward
                        if other.open_tiers:
                            record_block("direction_locked", min_interval=30,
                                         direction=action.get("direction"),
                                         open_tiers=sorted(other.open_tiers))
                            continue
                        if time.time() < open_block_until:
                            record_block("open_cooldown", min_interval=10,
                                         direction=action.get("direction"),
                                         until=_iso(open_block_until))
                            continue
                        exchange = await snapshot(ex, asset, venues)
                        snap_at = time.time()
                        requested = Decimal(str(action["qty"]))
                        receipts = await place_pair(ex, asset, side_buy, side_sell, requested,
                                                    opening=True, quotes={side_buy: ob, side_sell: os_})
                        leg_qty = {}
                        for venue, receipt in receipts.items():
                            if isinstance(receipt, dict) and receipt.get("status") == "filled":
                                leg_qty[venue] = plausible_fill(receipt, requested) or requested
                        check = None
                        hedge_qty = min(leg_qty.values()) if len(leg_qty) == 2 else None
                        if hedge_qty and hedge_qty > 0:
                            expected = {v: ledger.net(symbol, v) for v in venues}
                            expected[side_buy] += hedge_qty
                            expected[side_sell] -= hedge_qty
                            check = await settle_positions(ex, asset, venues, expected)
                        if hedge_qty and hedge_qty > 0 and check and check.get("ok"):
                            prices = {venue: receipt_price(receipts.get(venue)) for venue in venues}
                            pos = engine.confirm_open(action, qty=hedge_qty, entry_prices=prices,
                                                      fee_usd=receipts_fee(receipts))
                            rebate += Decimal(str(entropy_rebate(receipts)))
                            exchange = await snapshot(ex, asset, venues)
                            snap_at = time.time()
                            append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "opened",
                                                     "pair": args.pair, "direction": action.get("direction"),
                                                     "tier": action.get("tier"), "qty": str(hedge_qty),
                                                     "notional_usd": action.get("notional_usd"),
                                                     "entry_prices": prices,
                                                     "fee_usd": receipts_fee(receipts),
                                                     "rebate_usd": entropy_rebate(receipts),
                                                     "dev_bps": action.get("dev_bps"),
                                                     "receipts": receipts, "settle": check})
                            save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                              realized=realized, rebate=rebate, equity=equity, history=history)
                            if pos is None:
                                record_block("open_already_booked", min_interval=10,
                                             direction=action.get("direction"), tier=action.get("tier"))
                        else:
                            # A tier only exists if both legs are really on the book.
                            recovery = await settle_positions(
                                ex, asset, venues, {v: ledger.net(symbol, v) for v in venues})
                            open_block_until = time.time() + 30.0
                            write_state({"status": "open_not_hedged" if recovery.get("ok") else "blocked_position_mismatch",
                                         "stage": "open", "direction": action.get("direction"),
                                         "tier": action.get("tier"), "receipts": receipts,
                                         "settle": check, "recovery": recovery,
                                         "realized_usd": str(realized), "rebate_usd": str(rebate)})
                            record_block("open_not_hedged" if recovery.get("ok") else "open_position_mismatch",
                                         direction=action.get("direction"), tier=action.get("tier"),
                                         receipts=receipts, settle=check, recovery=recovery)
                            append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "open_failed",
                                                     "pair": args.pair, "direction": action.get("direction"),
                                                     "tier": action.get("tier"), "requested_qty": str(requested),
                                                     "receipts": receipts, "settle": check,
                                                     "recovery": recovery})
                            if not recovery.get("ok"):
                                return {"ok": False, "error": "open_position_mismatch"}
                    elif action.get("action") == "close":
                        pos = engine.open_tiers.get(action["tier"])
                        qty = Decimal(str(action.get("qty") or 0))
                        if qty <= 0:
                            engine.cancel_close(action["tier"])
                            continue
                        receipts = await place_pair(ex, asset, side_buy, side_sell, qty, opening=False,
                                                    quotes={side_buy: ob, side_sell: os_})
                        expected = {v: ledger.net(symbol, v) for v in venues}
                        expected[side_buy] -= qty
                        expected[side_sell] += qty
                        check = await settle_positions(ex, asset, venues, expected)
                        if check.get("ok"):
                            trade = engine.confirm_close(action)
                            close_failures.pop(action["tier"], None)
                            exchange = await snapshot(ex, asset, venues)
                            snap_at = time.time()
                            pnl = None
                            if pos is not None and pos.entry_buy_price and pos.entry_sell_price:
                                exit_buy = receipt_price(receipts.get(side_buy))
                                exit_sell = receipt_price(receipts.get(side_sell))
                                if exit_buy and exit_sell:
                                    pnl = trade_result_bps(
                                        entry_buy=pos.entry_buy_price, entry_sell=pos.entry_sell_price,
                                        exit_buy=exit_buy, exit_sell=exit_sell,
                                        entry_fee_usd=pos.entry_fee_usd, exit_fee_usd=receipts_fee(receipts),
                                        notional_usd=pos.notional_usd)
                            if pnl is None:
                                # No four real prices: fall back to the edge estimate and mark it.
                                estimated_bps = float(action.get("net_bps") or 0.0)
                                notional = float((pos.notional_usd if pos else action.get("notional_usd")) or args.notional)
                                pnl = {"gross_bps": estimated_bps + args.slippage_bps,
                                       "fee_bps": args.slippage_bps, "net_bps": estimated_bps,
                                       "fees_usd": 0.0,
                                       "net_usd": estimated_bps / 10000.0 * notional,
                                       "estimated": True}
                            rebate += Decimal(str(entropy_rebate(receipts)))
                            realized += Decimal(str(pnl["net_usd"]))
                            if trade is not None:
                                trade.update(pnl)
                            append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "closed",
                                                     "pair": args.pair, "direction": action.get("direction"),
                                                     "tier": action.get("tier"), "qty": str(qty),
                                                     "notional_usd": (pos.notional_usd if pos else None),
                                                     "pnl": pnl, "rebate_usd": entropy_rebate(receipts),
                                                     "receipts": receipts, "settle": check})
                            save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                              realized=realized, rebate=rebate, equity=equity, history=history)
                        else:
                            # Keep the tier: the next sample retries the close and
                            # the residual is measured against the real position.
                            engine.cancel_close(action["tier"])
                            close_failures[action["tier"]] = close_failures.get(action["tier"], 0) + 1
                            write_state({"status": "blocked_position_mismatch", "stage": "close",
                                         "direction": action.get("direction"), "tier": action["tier"],
                                         "receipts": receipts, "settle": check,
                                         "attempts": close_failures[action["tier"]],
                                         "realized_usd": str(realized), "rebate_usd": str(rebate)})
                            record_block("close_position_mismatch", direction=action.get("direction"),
                                         tier=action["tier"], settle=check,
                                         attempts=close_failures[action["tier"]])
                            if close_failures[action["tier"]] >= 3:
                                return {"ok": False, "error": "close_position_mismatch"}
            cycle_blocked = (LAST_BLOCK or {}).get("at") != block_before
            if not cycle_blocked and not any(engine.open_tiers for engine in engines.values()):
                direction = "forward" if edge_forward >= edge_reverse else "reverse"
                dev = edge_forward - baseline["forward"] if direction == "forward" else edge_reverse - baseline["reverse"]
                tiers_for = tiers_forward if direction == "forward" else tiers_reverse
                record_block("below_threshold", min_interval=60, direction=direction,
                             dev_bps=round(dev, 2), threshold_bps=tiers_for[0] if tiers_for else None,
                             gap_bps=round(tiers_for[0] - dev, 2) if tiers_for else None,
                             edge_bps=round(edge_forward if direction == "forward" else edge_reverse, 2),
                             baseline_bps=round(baseline[direction], 2))
            if realized <= -abs(Decimal(str(args.max_loss_usd))):
                write_state({"status": "stopped_max_loss", "realized": str(realized),
                             "rebate_usd": str(rebate)})
                record_block("max_loss", realized_usd=str(realized))
                save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                  realized=realized, rebate=rebate, equity=equity, history=history)
                return {"ok": False, "error": "max_loss"}
            if time.time() - last_save >= 30.0:
                save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                  realized=realized, rebate=rebate, equity=equity, history=history)
                last_save = time.time()
            write_state({"status": "running" if args.mode == "live" else "observe",
                         "pair": args.pair, "equity": equity,
                         "slot_caps": caps, "cumulative_notional": ladder, "tiers": tiers,
                         "child_notional": args.child_notional, "leverage": leverage,
                         "restored": restored,
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
                         "rebate_usd": str(rebate),
                         "exchange": {k: (None if v is None else str(v)) for k, v in (exchange or {}).items()}})
    finally:
        save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                          realized=realized, rebate=rebate, equity=equity, history=history)
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
