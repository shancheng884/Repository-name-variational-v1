"""Cost decomposition for two-venue probe rounds.

All bps values are adverse-positive unless the field says ``pnl``. Exchange
fees are reported separately and are never silently mixed into the price-path
cost. A probe round contains exactly one entry pair and one exit pair.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable


def _d(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        out = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return out if out.is_finite() else None


def _leg_cost(leg: dict[str, Any], reference: dict[str, Any], *, closing: bool) -> dict[str, Any]:
    side = str(leg.get("side") or "").lower()
    fill = _d(leg.get("avg_price"))
    bid = _d(reference.get("bid"))
    ask = _d(reference.get("ask"))
    mid = _d(reference.get("mid"))
    if mid is None and bid is not None and ask is not None:
        mid = (bid + ask) / 2
    if fill is None or mid is None or mid <= 0:
        return {"ok": False, "reason": "price_or_mid_missing"}
    if side == "buy":
        quote = ask if ask is not None else mid
        half_spread = (quote - mid) / mid * Decimal("10000") if quote is not None else None
        slippage = (fill - quote) / quote * Decimal("10000") if quote is not None and quote > 0 else None
    elif side == "sell":
        quote = bid if bid is not None else mid
        half_spread = (mid - quote) / mid * Decimal("10000") if quote is not None else None
        slippage = (quote - fill) / quote * Decimal("10000") if quote is not None and quote > 0 else None
    else:
        return {"ok": False, "reason": "side_invalid"}
    total = (half_spread or Decimal("0")) + (slippage or Decimal("0"))
    return {
        "ok": True,
        "side": side,
        "reference_mid": str(mid),
        "reference_bbo": str(quote),
        "fill_price": str(fill),
        "half_spread_bps": None if half_spread is None else str(half_spread),
        "slippage_vs_quote_bps": None if slippage is None else str(slippage),
        "price_cost_bps": str(total),
        "closing": closing,
    }


def decompose_round(row: dict[str, Any]) -> dict[str, Any]:
    decision = row.get("decision") or {}
    entry = row.get("entry") or {}
    exit_ = row.get("exit") or {}
    entry_legs = entry.get("legs") or {}
    exit_legs = exit_.get("legs") or {}
    entry_quotes = decision.get("quotes") or {}
    exit_quotes = exit_.get("quotes_at_close") or {}
    components: dict[str, Any] = {}
    for venue, leg in entry_legs.items():
        components[f"entry:{venue}"] = _leg_cost(leg, entry_quotes.get(venue) or {}, closing=False)
    for venue, leg in exit_legs.items():
        components[f"exit:{venue}"] = _leg_cost(leg, exit_quotes.get(venue) or {}, closing=True)
    price_cost = sum(
        (_d(v.get("price_cost_bps")) or Decimal("0"))
        for v in components.values() if v.get("ok")
    )
    fees = sum((_d(v.get("fee_usd")) or Decimal("0")) for v in list(entry_legs.values()) + list(exit_legs.values()))
    buy_leg = next((leg for leg in entry_legs.values() if str(leg.get("side")).lower() == "buy"), None)
    sell_leg = next((leg for leg in entry_legs.values() if str(leg.get("side")).lower() == "sell"), None)
    buy_exit = next((leg for leg in exit_legs.values() if str(leg.get("side")).lower() == "sell"), None)
    sell_exit = next((leg for leg in exit_legs.values() if str(leg.get("side")).lower() == "buy"), None)
    gross_pnl = None
    entry_notional = None
    if all(leg and _d(leg.get("filled_qty")) and _d(leg.get("avg_price")) for leg in (buy_leg, sell_leg, buy_exit, sell_exit)):
        buy_qty, sell_qty = _d(buy_leg["filled_qty"]), _d(sell_leg["filled_qty"])
        qty = min(buy_qty, sell_qty)
        buy_entry_px = _d(buy_leg["avg_price"])
        sell_entry_px = _d(sell_leg["avg_price"])
        buy_exit_px = _d(buy_exit["avg_price"])
        sell_exit_px = _d(sell_exit["avg_price"])
        gross_pnl = (
            (buy_exit_px - buy_entry_px) * qty
            + (sell_entry_px - sell_exit_px) * qty
        )
        entry_notional = (buy_entry_px * qty + sell_entry_px * qty) / 2
        gross_pnl = Decimal(str(gross_pnl))
        entry_notional = Decimal(str(entry_notional))
    pnl_bps = gross_pnl / entry_notional * Decimal("10000") if gross_pnl is not None and entry_notional else None
    hedge_delay_ms = None
    fills = [(_d(leg.get("fill_at_epoch")), leg) for leg in entry_legs.values()]
    fills = [(ts, leg) for ts, leg in fills if ts is not None]
    if len(fills) == 2:
        fills.sort(key=lambda item: item[0])
        hedge_delay_ms = int((fills[1][0] - fills[0][0]) * 1000)
    return {
        "round_id": row.get("round_id"),
        "asset": row.get("asset"),
        "buy_venue": row.get("buy_venue"),
        "sell_venue": row.get("sell_venue"),
        "status": row.get("status"),
        "signal_edge_bps": decision.get("signal_edge_bps"),
        "entry_price_cost_bps": str(sum(
            (_d(v.get("price_cost_bps")) or Decimal("0"))
            for k, v in components.items() if k.startswith("entry:") and v.get("ok")
        )),
        "exit_price_cost_bps": str(sum(
            (_d(v.get("price_cost_bps")) or Decimal("0"))
            for k, v in components.items() if k.startswith("exit:") and v.get("ok")
        )),
        "total_price_cost_bps": str(price_cost),
        "fees_usd": str(fees),
        "gross_pnl_usd": None if gross_pnl is None else str(gross_pnl),
        "gross_pnl_bps": None if pnl_bps is None else str(pnl_bps),
        "net_before_fees_bps": None if pnl_bps is None else str(pnl_bps),
        "hedge_delay_ms": hedge_delay_ms,
        "components": components,
    }


def percentile(values: Iterable[Decimal], p: int) -> Decimal | None:
    data = sorted(v for v in values if v is not None)
    if not data:
        return None
    if len(data) == 1:
        return data[0]
    index = (len(data) - 1) * p / 100
    low = int(index)
    high = min(low + 1, len(data) - 1)
    frac = Decimal(str(index - low))
    return data[low] * (Decimal("1") - frac) + data[high] * frac


def calibrate(rounds: Iterable[dict[str, Any]], *, target_bps: Decimal = Decimal("1")) -> dict[str, Any]:
    by_asset_venue: dict[tuple[str, str], list[Decimal]] = defaultdict(list)
    by_pair: dict[tuple[str, str, str], list[Decimal]] = defaultdict(list)
    summaries = []
    for row in rounds:
        summary = decompose_round(row)
        summaries.append(summary)
        total = _d(summary.get("total_price_cost_bps"))
        pnl = _d(summary.get("gross_pnl_bps"))
        if total is not None:
            for venue in (summary.get("buy_venue"), summary.get("sell_venue")):
                if venue:
                    by_asset_venue[(summary.get("asset"), venue)].append(total / 2)
        if pnl is not None:
            by_pair[(summary.get("asset"), summary.get("buy_venue"), summary.get("sell_venue"))].append(pnl)
    venues = {
        f"{asset}:{venue}": {
            "samples": len(values),
            "p50_cost_bps": str(percentile(values, 50)) if values else None,
            "p80_cost_bps": str(percentile(values, 80)) if values else None,
        }
        for (asset, venue), values in by_asset_venue.items()
    }
    pairs = {
        f"{asset}:{buy}->{sell}": {
            "samples": len(values),
            "p50_pnl_bps": str(percentile(values, 50)) if values else None,
            "p20_pnl_bps": str(percentile(values, 20)) if values else None,
            "p80_pnl_bps": str(percentile(values, 80)) if values else None,
        }
        for (asset, buy, sell), values in by_pair.items()
    }
    recommendations = {}
    for key, values in by_asset_venue.items():
        p80 = percentile(values, 80)
        if p80 is not None:
            recommendations[f"{key[0]}:{key[1]}"] = str(max(Decimal("0"), p80 + target_bps))
    return {
        "schema": 1,
        "target_net_bps": str(target_bps),
        "rounds": len(summaries),
        "venues": venues,
        "pairs": pairs,
        "recommended_trigger_bps": recommendations,
        "fee_policy": "fees_reported_separately_not_subtracted_from_price_cost",
        "summaries": summaries,
    }
