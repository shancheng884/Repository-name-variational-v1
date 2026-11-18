#!/usr/bin/env python3
"""Five-tier live runner (entropy vs one Lighter book).

Standalone: main.py is untouched. Observe mode is the default; live orders need
explicit flags.  A failed leg is flattened immediately instead of leaving a
naked side, a tier is booked only after both legs are proved on the venue, and
the book survives a restart: tiers, frozen anchors and the rolling baseline are
persisted and reconciled against the real account before any order can go out.
"""
from __future__ import annotations

import argparse, asyncio, copy, json, os, sys, time, uuid
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec, asset_symbol
from tools.lib.venue_probe import EntropyProbeExecutor, LighterProbeExecutor, _d
from tools.lib.nvda_quote_alignment import BASIS, quote_clock_issue, quote_max_age, seed_aligned_history
from tools.lib.nvda_fee_updates import finalize_costs, refresh_open_fees
from tools.lib.nvda_execution_stats import (closed_child_result, failed_entry_result, hedged_entry_result,
                                          new_execution_stats, record_attempt)
from tools.market_capture.config import DEFAULT_ASSETS, VENUES, CaptureConfig, load_environment
from tools.market_capture.runner import CaptureRunner, _acquire_lock

STATE = ROOT / "log" / "nvda_tier_live_state.json"
ENGINE_STATE = ROOT / "log" / "nvda_tier_live_engine.json"
BLOCK_LOG = ROOT / "log" / "nvda_tier_blockers.jsonl"
ORDER_LOG = ROOT / "log" / "nvda_tier_orders.jsonl"
CAPTURE_DIR = ROOT / "log" / "nvda_tier_live_capture"
ENGINE_STATE_VERSION = 2

LAST_BLOCK: dict = {}
_BLOCK_WRITTEN_AT: dict = {}


def configure_run_paths(run_dir: Path) -> None:
    global STATE, ENGINE_STATE, BLOCK_LOG, ORDER_LOG, CAPTURE_DIR
    STATE = run_dir / "state.json"
    ENGINE_STATE = run_dir / "engine.json"
    BLOCK_LOG = run_dir / "blockers.jsonl"
    ORDER_LOG = run_dir / "orders.jsonl"
    CAPTURE_DIR = run_dir / "capture"


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
            account_address=address, private_key=key, dex=env.get("ENTROPY_ALLOWED_DEX", "io"),
            fill_ws_url=env.get("ENTROPY_WS_URL", "wss://api.hyperliquid.xyz/ws"),
            enable_fill_stream=True),
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
    async def read(venue):
        try:
            result = await _positions(ex, venue, asset)
            out[venue] = (_d(result.get("qty")) if result.get("verified")
                          and not result.get("active_orders") else None)
        except Exception:
            out[venue] = None
    await asyncio.gather(*(read(venue) for venue in venues))
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


async def verify_leverage(executor, asset, expected):
    observed = await executor.account_state(asset)
    actual = _d(observed.get("leverage"))
    target = _d(expected)
    if not observed.get("ok") or actual is None or target is None or abs(actual - target) >= Decimal("0.05"):
        return {"ok": False, "leverage": observed.get("leverage"), "expected": expected,
                "error": observed.get("error") or "leverage_mismatch"}
    return {"ok": True, "changed": False, "leverage": float(actual), "expected": expected}


def common_qty(ex, asset, venues, qty):
    for _ in range(4):
        normalized = [ex[v].normalize_qty(asset, qty) for v in venues]
        if any(value is None or value <= 0 for value in normalized):
            return None
        if all(value == normalized[0] for value in normalized):
            return normalized[0]
        qty = min(normalized)
    return None


async def place_pair(ex, asset, buy, sell, qty, *, opening, quotes,
                     before_positions=None, token=None, decision_at=None):
    async def one(venue, side):
        cid = "nvda-tier-" + (token or uuid.uuid4().hex)[:20] + "-" + side
        extra = {}
        if venue == "entropy" and before_positions is not None:
            extra["known_position_before"] = before_positions[venue]
        try:
            dispatched = time.time()
            receipt = await ex[venue].market_order(asset=asset, side=side, qty=qty,
                                                       quote=quotes.get(venue) or {},
                                                       client_order_id=cid, reduce_only=not opening,
                                                       max_slippage_bps=Decimal("12"), **extra)
            receipt["requested_qty"] = str(qty)
            receipt["dispatch_started_at"] = _iso(dispatched)
            receipt.setdefault("execution_quote", {}).update(
                {k: (quotes.get(venue) or {}).get(k) for k in ("bid_size", "ask_size", "depth_audit", "time_alignment")})
            if decision_at is not None:
                receipt["signal_to_dispatch_ms"] = round(max(0, dispatched - decision_at) * 1000, 3)
            return venue, receipt
        except Exception as exc:
            return venue, {"ok": False, "status": "error", "error": "%s:%s" % (type(exc).__name__, exc)}
    # Opening: buy on the buy venue, sell on the sell venue.
    # Closing: the reverse, so each leg reduces what it opened.
    tasks = [one(buy, "buy" if opening else "sell"), one(sell, "sell" if opening else "buy")]
    return dict(await asyncio.gather(*tasks))


QTY_TOL = Decimal("0.0000001")


async def position_qty(ex, asset, venue):
    """Verified position size, or None when the venue could not be read."""
    try:
        result = await _positions(ex, venue, asset)
    except Exception:
        return None
    if not isinstance(result, dict) or not result.get("verified") or result.get("active_orders") != 0:
        return None
    return _d(result.get("qty"))


async def settle_positions(ex, asset, venues, expected, *, attempts=3,
                           settlement_grace=0.0, allow_corrections=True):
    """Force the venues to match the ledger and prove it from a fresh read.

    Any difference is closed with a reduce-only order, which can shrink a
    position but never open one, so a correction cannot create a naked leg.
    """
    legs = {}
    corrections = []
    projected = {}
    deadline = time.monotonic() + settlement_grace
    while settlement_grace > 0:
        actual = dict(zip(venues, await asyncio.gather(*(position_qty(ex, asset, v) for v in venues))))
        if all(actual[v] is not None and abs(actual[v] - expected[v]) <= QTY_TOL for v in venues):
            return {"ok": True, "legs": {v: {"expected": str(expected[v]), "actual": str(actual[v]),
                                               "residual": str(actual[v] - expected[v])} for v in venues},
                    "corrections": []}
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(min(.5, max(0, deadline - time.monotonic())))
    max_attempts = max(1, attempts)
    for attempt in range(max_attempts + 1):
        residual = {}
        for venue in venues:
            qty = await position_qty(ex, asset, venue)
            want = expected[venue]
            if qty is None:
                legs[venue] = {"expected": str(want), "actual": None, "error": "position_unreadable"}
                return {"ok": False, "error": "position_unreadable", "legs": legs,
                        "corrections": corrections}
            delta = qty - want
            legs[venue] = {"expected": str(want), "actual": str(qty), "residual": str(delta)}
            if venue in projected and abs(qty - projected[venue]) > QTY_TOL:
                return {"ok": False, "error": "correction_position_not_reflected", "legs": legs,
                        "corrections": corrections}
            if abs(delta) > QTY_TOL:
                residual[venue] = delta
        if not residual:
            return {"ok": True, "legs": legs, "corrections": corrections}
        if not allow_corrections:
            return {"ok": False, "error": "settlement_not_reflected", "legs": legs,
                    "corrections": corrections}
        if attempt == max_attempts:
            break
        for venue, delta in residual.items():
            side = "sell" if delta > 0 else "buy"
            actual = _d(legs[venue]["actual"])
            if actual is None or actual * delta <= 0 or abs(delta) > abs(actual):
                return {"ok": False, "error": "correction_would_increase_position", "legs": legs,
                        "corrections": corrections}
            try:
                extra = {"known_position_before": actual, "retry_rate_limit": False} if venue == "entropy" else {}
                receipt = await ex[venue].market_order(
                    asset=asset, side=side, qty=abs(delta), quote={},
                    client_order_id="nvda-tier-settle-" + uuid.uuid4().hex[:10],
                    reduce_only=True, max_slippage_bps=Decimal("30"), **extra)
                corrections.append({"venue": venue, "side": side,
                                    "qty": str(abs(delta)), "receipt": receipt})
                legs[venue]["fix"] = (receipt or {}).get("status")
                if (receipt or {}).get("status") not in ("filled", "rejected"):
                    return {"ok": False, "error": "unresolved_correction", "legs": legs,
                            "corrections": corrections}
                if receipt["status"] == "filled":
                    # Legacy probe/test receipts can omit size. Do not send a
                    # second correction if the first order's size is unknown.
                    filled = _d(receipt.get("filled_qty"))
                    if filled is None:
                        projected[venue] = expected[venue]
                    elif not filled.is_finite() or not 0 < filled <= abs(delta):
                        return {"ok": False, "error": "unresolved_correction", "legs": legs,
                                "corrections": corrections}
                    else:
                        projected[venue] = actual + (filled if side == "buy" else -filled)
                else:
                    projected[venue] = actual
            except Exception as exc:
                legs[venue]["fix_error"] = "%s:%s" % (type(exc).__name__, exc)
                corrections.append({"venue": venue, "side": side,
                                    "qty": str(abs(delta)), "receipt": {
                                        "status": "unknown", "error": str(exc)}})
                return {"ok": False, "error": "unresolved_correction", "legs": legs,
                        "corrections": corrections}
        if any(row["receipt"]["status"] == "filled" for row in corrections[-len(residual):]):
            await asyncio.sleep(0.8)
    return {"ok": False, "error": "position_mismatch", "legs": legs,
            "corrections": corrections}


async def rollback_entry(ex, asset, venues, before, receipts, buy, sell, *, token, previous=None):
    """Unwind confirmed fills, not a possibly lagging account snapshot."""
    corrections = list((previous or {}).get("corrections", []))
    if any(row.get("receipt", {}).get("status") not in ("filled", "rejected") for row in corrections):
        return {"ok": False, "error": "unresolved_correction", "corrections": corrections}
    deltas = {v: Decimal("0") for v in venues}
    for venue, receipt in receipts.items():
        filled = plausible_fill(receipt, Decimal(receipt.get("requested_qty") or "1e20"))
        if filled:
            deltas[venue] += filled * (1 if venue == buy else -1)
    for row in corrections:
        receipt = row.get("receipt") or {}
        if receipt.get("status") == "filled":
            deltas[row["venue"]] += Decimal(receipt["filled_qty"]) * (1 if row["side"] == "buy" else -1)
    async def unwind(venue, delta):
        rows = []
        for attempt in range(1, 4):
            actual = before[venue] + delta
            # Only the first attempt can act on the fills alone. A retry must
            # match a fresh, order-free account read to the known residual.
            if attempt > 1 or previous:
                verified = await position_qty(ex, asset, venue)
                if verified is None or abs(verified - actual) > QTY_TOL:
                    break
            if abs(delta) <= QTY_TOL or actual * delta <= 0 or abs(delta) > abs(actual):
                break
            side = "sell" if delta > 0 else "buy"
            extra = {"known_position_before": actual, "retry_rate_limit": False} if venue == "entropy" else {}
            cid = f"nvda-tier-rollback-{token[:16]}-{venue}-{attempt}"
            try:
                receipt = await ex[venue].market_order(
                    asset=asset, side=side, qty=abs(delta), quote={}, client_order_id=cid,
                    reduce_only=True, max_slippage_bps=Decimal("30"), **extra)
            except Exception as exc:
                receipt = {"status": "unknown", "error": str(exc)}
            receipt = dict(receipt or {})
            receipt.setdefault("client_order_id", cid)
            filled = _d(receipt.get("filled_qty"))
            status = receipt.get("status")
            if (status == "filled" and (filled is None or not filled.is_finite() or not 0 < filled <= abs(delta))
                    or status == "rejected" and filled not in (None, Decimal("0"))):
                receipt = {**receipt, "status": "unknown", "error": "invalid_rollback_fill_size"}
            rows.append({"venue": venue, "side": side, "qty": str(abs(delta)), "receipt": receipt})
            if receipt.get("status") == "filled":
                delta -= filled if delta > 0 else -filled
                if abs(delta) <= QTY_TOL:
                    break
            elif receipt.get("status") != "rejected":
                break
        return rows

    for rows in await asyncio.gather(*(unwind(v, q) for v, q in deltas.items() if q)):
        corrections.extend(rows)
    if any(row.get("receipt", {}).get("status") not in ("filled", "rejected") for row in corrections):
        return {"ok": False, "error": "unresolved_correction", "corrections": corrections}
    check = await settle_positions(ex, asset, venues, before, settlement_grace=3.0,
                                   allow_corrections=False)
    check["corrections"] = corrections
    return check


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
                     timeout_seconds=None, slippage_bps=args.slippage_bps,
                     exit_cost_bps=getattr(args, "exit_cost_bps", None),
                     profit_target_bps=getattr(args, "profit_target_bps", 1.0))
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


def save_engine_state(*, asset, pair, tiers, engines, realized, rebate, equity, history,
                      execution_stats=None, strict=False) -> None:
    payload = {
        "version": ENGINE_STATE_VERSION,
        "asset": asset,
        "pair": pair,
        "tiers": tiers,
        "realized_usd": str(realized),
        "rebate_usd": str(rebate),
        "equity": equity,
        "execution_stats": execution_stats or new_execution_stats(),
        "quote_basis": BASIS,
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
        if strict:
            raise


async def main_async(args):
    env = load_environment()
    asset = next(a for a in DEFAULT_ASSETS if a.name == args.asset)
    symbol = asset_symbol(asset)
    buy, sell = args.pair.split(">")
    venues = (buy, sell)
    tiers_forward = _tier_list(args.tiers)
    tiers_reverse = _tier_list(args.tiers_reverse) if args.tiers_reverse else list(tiers_forward)
    ex = {v: e for v, e in build_executors(env).items() if v in venues}
    for executor in ex.values():
        await executor.start()
    leverage = {}
    required_leverage = getattr(args, "verify_rh_leverage", None)
    if args.mode == "live" and "lighter_rh" in venues and (required_leverage or args.rh_leverage > 0):
        if required_leverage:
            leverage["lighter_rh"] = await verify_leverage(ex["lighter_rh"], asset, required_leverage)
        else:
            leverage["lighter_rh"] = await ex["lighter_rh"].ensure_leverage(asset, args.rh_leverage)
        if not leverage["lighter_rh"].get("ok"):
            write_state({"status": "blocked_leverage", "pair": args.pair, "leverage": leverage})
            record_block("leverage_not_confirmed", leverage=leverage)
            return {"ok": False, "error": "leverage_not_confirmed", "leverage": leverage}
    equity_actual = await venue_equity(ex, asset, venues)
    equity = dict(equity_actual)
    # The two control groups run on a fixed per-venue margin instead of the
    # live account balance, so both ladders are directly comparable.
    margin_usd = getattr(args, "margin_usd", None)
    if margin_usd is not None:
        fixed = float(margin_usd)
        short = {v: equity_actual.get(v) for v in venues
                 if not (equity_actual.get(v) or 0) >= fixed}
        if short:
            write_state({"status": "blocked_margin", "pair": args.pair,
                         "margin_usd": fixed, "equity_actual": equity_actual})
            record_block("fixed_margin_unavailable", margin_usd=fixed, equity=equity_actual)
            return {"ok": False, "error": "fixed_margin_unavailable", "equity": equity_actual}
        equity = {v: fixed for v in venues}
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
    history.update(aligned_forward=deque(maxlen=20000), aligned_reverse=deque(maxlen=20000))
    aligned_history = {key: history["aligned_" + key] for key in ("forward", "reverse")}
    out_dir = CAPTURE_DIR
    saved = load_engine_state(ENGINE_STATE)
    execution_stats = new_execution_stats((saved or {}).get("execution_stats"))
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
        for key in history:
            if key.startswith("aligned_") and saved.get("quote_basis") != BASIS:
                continue
            for row in (saved.get("history") or {}).get(key) or []:
                if isinstance(row, (list, tuple)) and len(row) >= 2:
                    try:
                        ts = float(row[0])
                    except (TypeError, ValueError):
                        continue
                    if ts >= cutoff:
                        history[key].append((ts, float(row[1])))
        restored = bool(engine_forward.open_tiers or engine_reverse.open_tiers)
    for seed_dir in (out_dir, getattr(args, "seed_capture_dir", None)):
        if seed_dir is None:
            continue
        if not history["forward"] and not history["reverse"]:
            seed_history_from_edges(history, seed_dir, args.pair, asset.name, window_seconds)
        if not all(aligned_history.values()):
            seed_aligned_history(aligned_history, seed_dir, venues, symbol, window_seconds, now=time.time())

    # A restart must start from the real account, never from an assumed flat book.
    startup = await position_snapshot(ex, asset, venues)
    problems = []
    if args.mode == "live" and caps is None:
        problems.append({"error": "equity_unverified", "equity": equity})
    for direction, engine in engines.items():
        if engine.pending_action:
            problems.append({"direction": direction, "error": "unresolved_inflight_order",
                             "action": engine.pending_action})
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
                 "equity_actual": equity_actual,
                 "equity_source": "fixed_margin" if margin_usd is not None else "live",
                 "exit_rule": {"exit_cost_bps": getattr(args, "exit_cost_bps", None),
                               "profit_target_bps": getattr(args, "profit_target_bps", 1.0)},
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
                                      book_snapshot_seconds=.5, entropy_fast_depth=True, quote_alignment=True,
                                      health_seconds=2.0, latency_seconds=5.0,
                                      system_seconds=60.0, fill_poll_seconds=2.0, edge_seconds=1.0,
                                      notional_usd=Decimal(str(args.notional)), max_book_levels=20))
    task = asyncio.create_task(cap.run(), name="nvda-tier-capture")
    dev_window = {"forward": deque(maxlen=cfg_forward.confirm_window),
                  "reverse": deque(maxlen=cfg_reverse.confirm_window)}
    last_version = None
    last_alignment_version = None
    close_failures = {}
    last_save = 0.0
    last_baseline_second = -1
    reconcile_task = None
    reconcile_epoch = 0
    order_epoch = 0
    next_reconcile = time.monotonic() + 5.0
    # Keep a recent incident visible instead of letting the next sample's
    # "running" overwrite it; last_block keeps the detail for the journal.
    degraded = {"status": None, "until": 0.0}
    exchange = {v: _d(startup[v]["qty"]) for v in venues}
    snap_at = time.time()
    try:
        while True:
            if cap.stop_event.is_set():
                break
            if cap.hub is None:
                await asyncio.sleep(0.01)
                continue
            try:
                await asyncio.wait_for(cap.hub.quote_event.wait(), timeout=max(0.1, args.poll_seconds))
            except asyncio.TimeoutError:
                pass
            cap.hub.quote_event.clear()
            if cap.stop_event.is_set():
                break
            rebate += refresh_open_fees(engines, ex, asset)
            cost_delta, rebate_delta, cost_events = finalize_costs(execution_stats, ex, asset, engines)
            realized += cost_delta
            rebate += rebate_delta
            for event in cost_events:
                append_jsonl(ORDER_LOG, {"ts": _iso(), **event})
            if reconcile_task is not None and reconcile_task.done():
                checked = reconcile_task.result()
                reconcile_task = None
                if reconcile_epoch == order_epoch:
                    exchange = checked
            if reconcile_task is None and time.monotonic() >= next_reconcile:
                reconcile_epoch = order_epoch
                reconcile_task = asyncio.create_task(snapshot(ex, asset, venues))
                next_reconcile = time.monotonic() + 5.0
            q_buy = cap.hub.latest(buy, asset.name, max_age=quote_max_age(buy))
            q_sell = cap.hub.latest(sell, asset.name, max_age=quote_max_age(sell))
            if not q_buy or not q_sell:
                last_alignment_version = None
                for window in dev_window.values():
                    window.clear()
                record_block("quotes_missing", min_interval=30, pair=args.pair,
                             buy=bool(q_buy), sell=bool(q_sell))
                continue
            clock_now = time.time()
            issues = {q["venue"]: issue for q in (q_buy, q_sell)
                      if (issue := quote_clock_issue(q, clock_now)) is not None}
            if issues:
                last_alignment_version = None
                for window in dev_window.values():
                    window.clear()
                record_block("quote_clock_invalid", min_interval=10, issues=issues)
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
            raw_forward = float((sell_bid - buy_ask) / forward_mid * Decimal("10000"))
            raw_reverse = float((buy_bid - sell_ask) / reverse_mid * Decimal("10000"))
            stamp = time.time()
            aligned = cap.hub.quote_clock.align(q_buy, q_sell, now=stamp)
            legacy = any(pos.signal_basis != BASIS for engine in engines.values()
                         for pos in engine.open_tiers.values())
            if aligned is None and not legacy:
                last_alignment_version = None
                for window in dev_window.values():
                    window.clear()
                record_block("alignment_unavailable", min_interval=30,
                             buy_source_at=q_buy.get("source_at"), sell_source_at=q_sell.get("source_at"))
                continue
            edge_forward = aligned["edges"]["forward"] if aligned else raw_forward
            edge_reverse = aligned["edges"]["reverse"] if aligned else raw_reverse
            if int(stamp) != last_baseline_second:
                history["forward"].append((stamp, raw_forward))
                history["reverse"].append((stamp, raw_reverse))
                if aligned:
                    aligned_history["forward"].append((stamp, edge_forward))
                    aligned_history["reverse"].append((stamp, edge_reverse))
                last_baseline_second = int(stamp)
            cutoff = stamp - window_seconds
            for samples in history.values():
                trim_history(samples, cutoff)
            ready = {key: window_ready(aligned_history[key], window_seconds, min_samples=min_samples)
                     for key in aligned_history}
            if not all(ready.values()) and not any(engine.open_tiers for engine in engines.values()):
                coverage = {key: round(samples[-1][0] - samples[0][0], 1) if len(samples)>1 else 0.0
                            for key, samples in aligned_history.items()}
                write_state({"status": "warming", "samples": min(map(len, aligned_history.values())),
                             "need": min_samples, "window_seconds": window_seconds,
                             "coverage_seconds": coverage, "pair": args.pair, "equity": equity,
                             "slot_caps": caps, "cumulative_notional": ladder, "tiers": tiers,
                             "child_notional": args.child_notional, "leverage": leverage,
                             "restored": restored, "realized_usd": str(realized),
                             "rebate_usd": str(rebate)})
                record_block("warming", min_interval=60, samples=len(aligned_history["forward"]),
                             need=min_samples, coverage_seconds=coverage)
                continue
            baseline = {key: median_of(samples) if samples else 0.0 for key, samples in aligned_history.items()}
            independent = aligned is not None and aligned["version"] != last_alignment_version
            if independent:
                dev_window["forward"].append(edge_forward - baseline["forward"])
                dev_window["reverse"].append(edge_reverse - baseline["reverse"])
                last_alignment_version = aligned["version"]
            alignment_audit = ({k: v for k, v in aligned.items() if k != "version"} if aligned else {})
            alignment_audit.update(raw_edges={"forward": raw_forward, "reverse": raw_reverse},
                                   baseline=baseline, independent_confirmation=independent,
                                   confirmation_counts={k: sum(v >= tiers[k][0] for v in w)
                                                        for k, w in dev_window.items()})
            for quote in (q_buy, q_sell):
                quote["time_alignment"] = alignment_audit
            if independent and cap.writer is not None:
                cap.writer.append("quote_alignment", "strategy", symbol,
                                  {"event": "quote_alignment", "at": _iso(stamp), **alignment_audit})
            # Both directions always evaluate exits; only the flat side may open,
            # which is the ETH gradient's direction lock until fully flat.
            acts_forward = engine_forward.on_sample(
                stamp=stamp, edge=edge_forward, baseline=baseline["forward"],
                price_hint=float(max(buy_ask, sell_bid)), window=list(dev_window["forward"]),
                exchange=exchange or {}, raw_edge=raw_forward,
                allow_entry=all(ready.values()) and aligned is not None and not legacy and not engine_reverse.open_tiers)
            acts_reverse = engine_reverse.on_sample(
                stamp=stamp, edge=edge_reverse, baseline=baseline["reverse"],
                price_hint=float(max(sell_ask, buy_bid)), window=list(dev_window["reverse"]),
                exchange=exchange or {}, raw_edge=raw_reverse,
                allow_entry=all(ready.values()) and aligned is not None and not legacy and not engine_forward.open_tiers)
            for action in acts_forward:
                action["direction"] = "forward"
            for action in acts_reverse:
                action["direction"] = "reverse"
            actions = acts_forward + acts_reverse
            close_queued = any(action.get("action") == "close" for action in actions)
            # A network round trip invalidates the other proposals' quotes.
            chosen = next((a for a in actions if a.get("action") == "close"), None)
            if chosen is None:
                chosen = max(actions, key=lambda a: a.get("dev_bps", float("-inf")), default=None)
            for discarded in actions:
                if discarded is chosen and args.mode == "live":
                    continue
                owner = engines[discarded["direction"]]
                if discarded.get("action") == "open":
                    owner.cancel_open(discarded)
                elif discarded.get("action") == "close":
                    owner.cancel_close(discarded["tier"])
            actions = [chosen] if chosen is not None else []
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
                            engine.cancel_open(action)
                            record_block("close_in_flight", min_interval=10,
                                         direction=action.get("direction"), tier=action.get("tier"))
                            continue
                        other = engine_reverse if action.get("direction") == "forward" else engine_forward
                        if other.open_tiers:
                            engine.cancel_open(action)
                            record_block("direction_locked", min_interval=30,
                                         direction=action.get("direction"),
                                         open_tiers=sorted(other.open_tiers))
                            continue
                        requested = common_qty(ex, asset, venues, Decimal(str(action["qty"])))
                        if requested is None:
                            engine.cancel_open(action)
                            record_block("quantity_below_minimum", min_interval=30)
                            continue
                        action["qty"] = float(requested)
                        for venue, quote in ((side_buy, ob), (side_sell, os_)):
                            quote["depth_audit"] = cap.hub.depth_audit(venue, symbol, requested)
                        engine.pending_action = dict(action)
                        order_epoch += 1
                        save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                          realized=realized, rebate=rebate, equity=equity, history=history,
                                          execution_stats=execution_stats, strict=True)
                        receipts = await place_pair(ex, asset, side_buy, side_sell, requested,
                                                    opening=True, quotes={side_buy: ob, side_sell: os_},
                                                    before_positions={v: ledger.net(symbol, v) for v in venues},
                                                    token=action["token"], decision_at=action["stamp"])
                        leg_qty = {}
                        for venue, receipt in receipts.items():
                            if isinstance(receipt, dict) and receipt.get("status") == "filled":
                                filled = plausible_fill(receipt, requested)
                                if filled:
                                    leg_qty[venue] = filled
                        check = None
                        hedge_qty = min(leg_qty.values()) if len(leg_qty) == 2 else None
                        if hedge_qty and hedge_qty > 0:
                            expected = {v: ledger.net(symbol, v) for v in venues}
                            expected[side_buy] += hedge_qty
                            expected[side_sell] -= hedge_qty
                            balanced = all(q == hedge_qty for q in leg_qty.values())
                            check = await settle_positions(ex, asset, venues, expected,
                                                           settlement_grace=3.0,
                                                           allow_corrections=not balanced)
                            if balanced and not check.get("ok"):
                                record_attempt(execution_stats, receipts, opening=True, hedged=None)
                                engine.pending_action["receipts"] = receipts
                                engine.pending_action["expected"] = {v: str(q) for v, q in expected.items()}
                                append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "open_unverified",
                                                         "pair": args.pair, "direction": action["direction"],
                                                         "tier": action["tier"], "receipts": receipts,
                                                         "settle": check})
                                write_state({"status": "blocked_settlement_lag", "stage": "open",
                                             "receipts": receipts, "settle": check})
                                return {"ok": False, "error": "settlement_not_reflected"}
                        if hedge_qty and hedge_qty > 0 and check and check.get("ok"):
                            action["notional_usd"] *= float(hedge_qty / requested)
                            retained, excess_pnl = hedged_entry_result(receipts, check, hedge_qty,
                                                                       side_buy, side_sell)
                            prices = {venue: receipt_price(receipts.get(venue)) for venue in venues}
                            pos = engine.confirm_open(action, qty=hedge_qty, entry_prices=prices,
                                                      fee_usd=receipts_fee(retained), entry_receipts=retained)
                            rebate += Decimal(str(entropy_rebate(receipts)))
                            rebate += sum((Decimal(str(entropy_rebate({row["venue"]: row.get("receipt")})))
                                           for row in check.get("corrections", [])), Decimal("0"))
                            if excess_pnl["complete"]:
                                realized += Decimal(excess_pnl["net_usd"])
                            record_attempt(execution_stats, receipts, opening=True, hedged=True,
                                           recovery=check if check.get("corrections") else None,
                                           pnl=excess_pnl if check.get("corrections") else None)
                            if not excess_pnl["complete"] and check.get("corrections"):
                                execution_stats.setdefault("pending_costs", []).append(copy.deepcopy({
                                    "category": "excess", "execution_token": action["token"],
                                    "receipts": receipts, "recovery": check, "buy": side_buy, "sell": side_sell,
                                    "hedge_qty": str(hedge_qty), "rebate_credited": excess_pnl["rebate_usd"]}))
                            exchange = {v: ledger.net(symbol, v) for v in venues}
                            snap_at = time.time()
                            append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "opened",
                                                     "execution_token": action["token"],
                                                     "pair": args.pair, "direction": action.get("direction"),
                                                     "tier": action.get("tier"), "qty": str(hedge_qty),
                                                     "notional_usd": action.get("notional_usd"),
                                                     "entry_prices": prices,
                                                     "fee_usd": receipts_fee(retained),
                                                     "rebate_usd": entropy_rebate(receipts),
                                                     "dev_bps": action.get("dev_bps"),
                                                     "receipts": receipts, "settle": check,
                                                     "excess_pnl": excess_pnl})
                            save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                              realized=realized, rebate=rebate, equity=equity, history=history,
                                              execution_stats=execution_stats)
                            if pos is None:
                                record_block("open_already_booked", min_interval=10,
                                             direction=action.get("direction"), tier=action.get("tier"))
                        else:
                            # A tier only exists if both legs are really on the book.
                            recovery = await rollback_entry(
                                ex, asset, venues, {v: ledger.net(symbol, v) for v in venues}, receipts,
                                side_buy, side_sell, token=action["token"], previous=check)
                            failure_pnl = failed_entry_result(receipts, recovery, side_buy, side_sell)
                            record_attempt(execution_stats, receipts, opening=True, hedged=False,
                                           recovery=recovery, pnl=failure_pnl)
                            if failure_pnl["complete"]:
                                realized += Decimal(failure_pnl["net_usd"])
                                rebate += Decimal(failure_pnl["rebate_usd"])
                            else:
                                execution_stats.setdefault("pending_costs", []).append(copy.deepcopy({
                                    "category": "failed", "execution_token": action["token"],
                                    "receipts": receipts, "recovery": recovery, "buy": side_buy, "sell": side_sell,
                                    "rebate_credited": "0"}))
                            terminal = (all(r.get("status") in ("filled", "rejected")
                                            for r in receipts.values())
                                        and all(row.get("receipt", {}).get("status") in ("filled", "rejected")
                                                for row in recovery.get("corrections", [])))
                            if recovery.get("ok") and terminal:
                                engine.cancel_open(action)
                                exchange = {v: ledger.net(symbol, v) for v in venues}
                            write_state({"status": "open_not_hedged" if recovery.get("ok") else "blocked_position_mismatch",
                                         "stage": "open", "direction": action.get("direction"),
                                         "tier": action.get("tier"), "receipts": receipts,
                                         "settle": check, "recovery": recovery,
                                         "realized_usd": str(realized), "rebate_usd": str(rebate)})
                            degraded = {"status": "open_not_hedged" if recovery.get("ok") else "blocked_position_mismatch",
                                        "until": time.time() + 60.0}
                            record_block("open_not_hedged" if recovery.get("ok") else "open_position_mismatch",
                                         direction=action.get("direction"), tier=action.get("tier"),
                                         receipts=receipts, settle=check, recovery=recovery)
                            append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "open_failed",
                                                     "execution_token": action["token"],
                                                     "pair": args.pair, "direction": action.get("direction"),
                                                     "tier": action.get("tier"), "requested_qty": str(requested),
                                                     "receipts": receipts, "settle": check,
                                                     "recovery": recovery, "pnl": failure_pnl})
                            save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                              realized=realized, rebate=rebate, equity=equity, history=history,
                                              execution_stats=execution_stats)
                            if not recovery.get("ok"):
                                return {"ok": False, "error": "open_position_mismatch"}
                            if not terminal:
                                record_block("unresolved_order", receipts=receipts)
                                write_state({"status": "blocked_unresolved_order", "stage": "open",
                                             "receipts": receipts, "recovery": recovery,
                                             "execution_stats": execution_stats})
                                return {"ok": False, "error": "unresolved_order"}
                    elif action.get("action") == "close":
                        pos = engine.open_tiers.get(action["tier"])
                        qty = Decimal(str(action.get("qty") or 0))
                        if qty <= 0:
                            engine.cancel_close(action["tier"])
                            continue
                        for venue, quote in ((side_buy, ob), (side_sell, os_)):
                            quote["depth_audit"] = cap.hub.depth_audit(venue, symbol, qty)
                        order_epoch += 1
                        engine.pending_action = dict(action)
                        save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                          realized=realized, rebate=rebate, equity=equity, history=history,
                                          execution_stats=execution_stats, strict=True)
                        receipts = await place_pair(ex, asset, side_buy, side_sell, qty, opening=False,
                                                    quotes={side_buy: ob, side_sell: os_},
                                                    before_positions={v: ledger.net(symbol, v) for v in venues},
                                                    token=action["token"], decision_at=stamp)
                        expected = {v: ledger.net(symbol, v) for v in venues}
                        expected[side_buy] -= qty
                        expected[side_sell] += qty
                        close_terminal = all(r.get("status") in ("filled", "rejected") for r in receipts.values())
                        all_filled = all(plausible_fill(r, qty) == qty for r in receipts.values())
                        check = await settle_positions(ex, asset, venues, expected,
                                                       settlement_grace=3.0,
                                                       allow_corrections=close_terminal and not all_filled)
                        record_attempt(execution_stats, receipts, opening=False,
                                       hedged=bool(check.get("ok")),
                                       recovery=check if check.get("corrections") else None)
                        if any(r.get("status") not in ("filled", "rejected") for r in receipts.values()):
                            append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "close_failed", "pair": args.pair,
                                                     "direction": action.get("direction"), "tier": action["tier"],
                                                     "receipts": receipts, "settle": check,
                                                     "pnl": {"complete": False, "missing": ["unresolved_order"]}})
                            record_block("unresolved_order", stage="close", receipts=receipts,
                                         settle=check)
                            write_state({"status": "blocked_unresolved_order", "stage": "close",
                                         "receipts": receipts, "settle": check,
                                         "execution_stats": execution_stats})
                            return {"ok": False, "error": "unresolved_order"}
                        if check.get("ok"):
                            trade = engine.confirm_close(action)
                            close_failures.pop(action["tier"], None)
                            exchange = {v: ledger.net(symbol, v) for v in venues}
                            snap_at = time.time()
                            pnl = closed_child_result(action, receipts, check, side_buy, side_sell)
                            rebate += Decimal(str(entropy_rebate(receipts)))
                            rebate += sum((Decimal(str(entropy_rebate({row["venue"]: row.get("receipt")})))
                                           for row in check.get("corrections", [])), Decimal("0"))
                            if pnl["complete"]:
                                realized += Decimal(pnl["net_usd"])
                            else:
                                execution_stats["unresolved_cost_events"] += 1
                                credited = Decimal(str(action.get("entry_fee_usd") or 0)) * Decimal("1.1")
                                credited += Decimal(str(entropy_rebate(receipts)))
                                credited += sum((Decimal(str(entropy_rebate({row["venue"]: row.get("receipt")})))
                                                 for row in check.get("corrections", [])), Decimal("0"))
                                execution_stats.setdefault("pending_costs", []).append(copy.deepcopy({
                                    "category": "closed", "execution_token": action["token"],
                                    "action": action, "receipts": receipts, "recovery": check,
                                    "buy": side_buy, "sell": side_sell, "rebate_credited": str(credited)}))
                            if trade is not None:
                                trade.update(pnl)
                            append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "closed",
                                                     "execution_token": action["token"],
                                                     "pair": args.pair, "direction": action.get("direction"),
                                                     "tier": action.get("tier"), "qty": str(qty),
                                                     "notional_usd": action.get("notional_usd"),
                                                     "pnl": pnl, "rebate_usd": entropy_rebate(receipts),
                                                     "receipts": receipts, "settle": check})
                            save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                              realized=realized, rebate=rebate, equity=equity, history=history,
                                              execution_stats=execution_stats)
                        else:
                            if (check.get("corrections") or any(r.get("status") == "filled" for r in receipts.values())
                                    or check.get("error") in ("unresolved_correction", "settlement_not_reflected")):
                                engine.pending_action["receipts"] = receipts
                                engine.pending_action["expected"] = {v: str(q) for v, q in expected.items()}
                                write_state({"status": "blocked_settlement_lag", "stage": "close",
                                             "receipts": receipts, "settle": check})
                                append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "close_failed",
                                                         "pair": args.pair, "direction": action["direction"],
                                                         "tier": action["tier"], "receipts": receipts,
                                                         "settle": check,
                                                         "pnl": {"complete": False,
                                                                 "missing": [check["error"]]}})
                                return {"ok": False, "error": check["error"]}
                            # Keep the tier: the next sample retries the close and
                            # the residual is measured against the real position.
                            engine.cancel_close(action["tier"])
                            append_jsonl(ORDER_LOG, {"ts": _iso(), "event": "close_failed", "pair": args.pair,
                                                     "direction": action.get("direction"), "tier": action["tier"],
                                                     "receipts": receipts, "settle": check,
                                                     "pnl": {"complete": False, "missing": ["position_mismatch"]}})
                            close_failures[action["tier"]] = close_failures.get(action["tier"], 0) + 1
                            write_state({"status": "blocked_position_mismatch", "stage": "close",
                                         "direction": action.get("direction"), "tier": action["tier"],
                                         "receipts": receipts, "settle": check,
                                         "attempts": close_failures[action["tier"]],
                                         "realized_usd": str(realized), "rebate_usd": str(rebate)})
                            degraded = {"status": "blocked_position_mismatch", "until": time.time() + 60.0}
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
                                  realized=realized, rebate=rebate, equity=equity, history=history,
                                  execution_stats=execution_stats)
                return {"ok": False, "error": "max_loss"}
            if time.time() - last_save >= 30.0:
                save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                                  realized=realized, rebate=rebate, equity=equity, history=history,
                                  execution_stats=execution_stats)
                last_save = time.time()
            status = "running" if args.mode == "live" else "observe"
            if degraded["status"] and time.time() < degraded["until"]:
                status = degraded["status"]
            fill_stream = getattr(ex.get("entropy"), "_fill_stream", None)
            write_state({"status": status,
                         "pair": args.pair, "equity": equity,
                         "slot_caps": caps, "cumulative_notional": ladder, "tiers": tiers,
                         "child_notional": args.child_notional, "leverage": leverage,
                         "equity_actual": equity_actual,
                         "equity_source": "fixed_margin" if getattr(args, "margin_usd", None) is not None else "live",
                         "exit_rule": {"exit_cost_bps": getattr(args, "exit_cost_bps", None),
                                       "profit_target_bps": getattr(args, "profit_target_bps", 1.0)},
                         "quote_basis": BASIS, "time_alignment": alignment_audit,
                         "legacy_exit_only": legacy,
                         "entropy_fill_stream": ({"connected": fill_stream.connected,
                                                   "messages": fill_stream.messages,
                                                   "last_received_at": fill_stream.last_received_at,
                                                   "last_error": fill_stream.last_error} if fill_stream else None),
                         "restored": restored,
                         "directions": {
                             "forward": {"pair": "%s>%s" % (buy, sell),
                                         "edge_bps": round(edge_forward, 2),
                                         "raw_edge_bps": round(raw_forward, 2),
                                         "baseline_bps": round(baseline["forward"], 2),
                                         "dev_bps": round(edge_forward - baseline["forward"], 2),
                                         "open_tiers": sorted(engine_forward.open_tiers)},
                             "reverse": {"pair": "%s>%s" % (sell, buy),
                                         "edge_bps": round(edge_reverse, 2),
                                         "raw_edge_bps": round(raw_reverse, 2),
                                         "baseline_bps": round(baseline["reverse"], 2),
                                         "dev_bps": round(edge_reverse - baseline["reverse"], 2),
                                         "open_tiers": sorted(engine_reverse.open_tiers)}},
                         "trades": len(engine_forward.trades) + len(engine_reverse.trades),
                         "realized_usd": str(realized),
                         "rebate_usd": str(rebate),
                         "execution_stats": execution_stats,
                         "exchange": {k: (None if v is None else str(v)) for k, v in (exchange or {}).items()}})
    finally:
        if reconcile_task is not None:
            reconcile_task.cancel()
            await asyncio.gather(reconcile_task, return_exceptions=True)
        save_engine_state(asset=symbol, pair=args.pair, tiers=tiers, engines=engines,
                          realized=realized, rebate=rebate, equity=equity, history=history,
                          execution_stats=execution_stats)
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
    parser.add_argument("--run-dir", type=Path, help="isolated state, order journal and capture directory")
    parser.add_argument("--seed-capture-dir", type=Path, help="optional read-only public quote history")
    parser.add_argument("--pair", default="entropy>lighter_mainnet")
    parser.add_argument("--tiers", default="7.77,8.36,9.60,11.21,13.84")
    parser.add_argument("--tiers-reverse", default="")
    parser.add_argument("--notional", type=float, default=20.0)
    parser.add_argument("--child-notional", type=float, default=20.0)
    parser.add_argument("--slippage-bps", type=float, default=1.3)
    parser.add_argument("--exit-cost-bps", type=float, default=None,
                        help="locked exit slippage (P80); closes need this plus the profit target")
    parser.add_argument("--profit-target-bps", type=float, default=1.0)
    parser.add_argument("--margin-usd", type=float, default=None,
                        help="fixed per-venue margin for capacity sizing, verified against the account")
    parser.add_argument("--baseline-minutes", type=float, default=30.0)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--max-loss-usd", type=float, default=3.0)
    parser.add_argument("--rh-leverage", type=float, default=20.0,
                        help="leverage to set and verify on lighter_rh before any order")
    parser.add_argument("--verify-rh-leverage", type=float,
                        help="verify existing leverage without changing it (requires --rh-leverage 0)")
    parser.add_argument("--mode", choices=("observe", "live"), default="observe")
    parser.add_argument("--confirm-flat", action="store_true")
    parser.add_argument("--i-understand-live-orders", action="store_true")
    args = parser.parse_args(argv)
    if args.asset != "NVDA" and args.run_dir is None:
        parser.error("--run-dir is required for non-NVDA assets")
    if args.verify_rh_leverage is not None and not (args.verify_rh_leverage > 0 and args.rh_leverage == 0):
        parser.error("--verify-rh-leverage needs a positive value and --rh-leverage 0")
    if args.mode == "live" and not (args.confirm_flat and args.i_understand_live_orders):
        raise SystemExit("live mode needs --confirm-flat and --i-understand-live-orders")
    run_dir = args.run_dir.resolve() if args.run_dir is not None else None
    if run_dir is not None:
        configure_run_paths(run_dir)
        # One lock per run directory: NVDA and SNDK are independent strategies.
        lock_path = run_dir / "live.lock"
    else:
        lock_path = ROOT / "log" / "nvda_tier_live.lock"
    guard = _acquire_lock(lock_path)
    try:
        result = asyncio.run(main_async(args))
    finally:
        guard.close()
    print(json.dumps(result or {"ok": True}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
