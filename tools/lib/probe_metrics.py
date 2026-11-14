from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

BPS = Decimal("10000")
ZERO = Decimal("0")


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
        half_spread = (quote - mid) / mid * BPS
        sign = Decimal("1")
    elif side == "sell":
        quote = bid if bid is not None else mid
        half_spread = (mid - quote) / mid * BPS
        sign = Decimal("-1")
    else:
        return {"ok": False, "reason": "side_invalid"}

    execution_quote = leg.get("execution_quote") if isinstance(leg.get("execution_quote"), dict) else {}
    exec_ref = _d(execution_quote.get("ask" if side == "buy" else "bid"))
    signal_to_exec = None
    exec_to_fill = None
    if exec_ref is not None and exec_ref > 0 and quote is not None and quote > 0:
        signal_to_exec = (exec_ref - quote) / quote * BPS * sign
        exec_to_fill = (fill - exec_ref) / exec_ref * BPS * sign
    slippage = (fill - quote) / quote * BPS * sign if quote is not None and quote > 0 else None
    if signal_to_exec is not None and exec_to_fill is not None:
        total = half_spread + signal_to_exec + exec_to_fill
        stage = "three_stage"
    else:
        total = half_spread + (slippage or ZERO)
        stage = "two_stage"
    return {
        "ok": True,
        "side": side,
        "stage_split": stage,
        "reference_mid": str(mid),
        "reference_bbo": str(quote),
        "execution_reference": None if exec_ref is None else str(exec_ref),
        "fill_price": str(fill),
        "half_spread_bps": str(half_spread),
        "signal_to_exec_bps": None if signal_to_exec is None else str(signal_to_exec),
        "exec_to_fill_bps": None if exec_to_fill is None else str(exec_to_fill),
        "slippage_vs_quote_bps": None if slippage is None else str(slippage),
        "price_cost_bps": str(total),
        "closing": closing,
    }


def _fill_stamp(leg: dict[str, Any]) -> Decimal | None:
    return _d(leg.get("source_at_epoch")) or _d(leg.get("fill_at_epoch"))


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

    venues = list(dict.fromkeys([str(row.get("buy_venue")), str(row.get("sell_venue"))]))
    entry_cost = sum((_d(v.get("price_cost_bps")) or ZERO) for k, v in components.items() if k.startswith("entry:") and v.get("ok"))
    exit_cost = sum((_d(v.get("price_cost_bps")) or ZERO) for k, v in components.items() if k.startswith("exit:") and v.get("ok"))
    price_cost = entry_cost + exit_cost
    venue_costs = {}
    for venue in venues:
        own_entry = _d((components.get(f"entry:{venue}") or {}).get("price_cost_bps"))
        own_exit = _d((components.get(f"exit:{venue}") or {}).get("price_cost_bps"))
        venue_costs[venue] = None if own_entry is None and own_exit is None else str((own_entry or ZERO) + (own_exit or ZERO))

    legs = list(entry_legs.values()) + list(exit_legs.values())
    fees_present = [_d(leg.get("fee_usd")) for leg in legs]
    fees_verified = [v for v in fees_present if v is not None]
    fees = sum(fees_verified, ZERO)
    fees_status = "verified" if legs and len(fees_verified) == len(legs) else "unverified"

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
        gross_pnl = (buy_exit_px - buy_entry_px) * qty + (sell_entry_px - sell_exit_px) * qty
        entry_notional = (buy_entry_px * qty + sell_entry_px * qty) / 2
    pnl_bps = gross_pnl / entry_notional * BPS if gross_pnl is not None and entry_notional else None

    stamps = sorted(((_fill_stamp(leg), venue) for venue, leg in entry_legs.items() if _fill_stamp(leg) is not None), key=lambda item: item[0])
    hedge_delay_ms = int((stamps[1][0] - stamps[0][0]) * 1000) if len(stamps) == 2 else None
    first_leg = stamps[0][1] if stamps else None

    complete = (
        row.get("status") == "filled"
        and all((components.get(f"entry:{v}") or {}).get("ok") for v in venues)
        and all((components.get(f"exit:{v}") or {}).get("ok") for v in venues)
    )
    return {
        "round_id": row.get("round_id"),
        "run_id": row.get("run_id"),
        "asset": row.get("asset"),
        "buy_venue": row.get("buy_venue"),
        "sell_venue": row.get("sell_venue"),
        "status": row.get("status"),
        "complete": bool(complete),
        "skip_reason": row.get("reason") if row.get("status") == "skipped" else None,
        "signal_edge_bps": decision.get("signal_edge_bps"),
        "entry_price_cost_bps": str(entry_cost),
        "exit_price_cost_bps": str(exit_cost),
        "total_price_cost_bps": str(price_cost),
        "venue_roundtrip_cost_bps": venue_costs,
        "fees_usd": str(fees),
        "fees_status": fees_status,
        "fees_unverified_legs": [f"{'entry' if leg in entry_legs.values() else 'exit'}:{venue}" for venue, leg in list(entry_legs.items()) + list(exit_legs.items()) if _d(leg.get("fee_usd")) is None],
        "gross_pnl_usd": None if gross_pnl is None else str(gross_pnl),
        "gross_pnl_bps": None if pnl_bps is None else str(pnl_bps),
        "hedge_delay_ms": hedge_delay_ms,
        "first_filled_venue": first_leg,
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
    by_venue: dict[tuple[str, str], list[Decimal]] = defaultdict(list)
    by_pair_cost: dict[tuple[str, str, str], list[Decimal]] = defaultdict(list)
    by_pair_pnl: dict[tuple[str, str, str], list[Decimal]] = defaultdict(list)
    excluded: dict[str, int] = defaultdict(int)
    stages: dict[str, list[Decimal]] = {"half_spread": [], "signal_to_exec": [], "exec_to_fill": []}
    delays: list[Decimal] = []
    summaries = []
    for row in rounds:
        summary = decompose_round(row)
        summaries.append(summary)
        if not summary["complete"]:
            excluded[str(summary.get("status"))] += 1
            continue
        for component in (summary.get("components") or {}).values():
            if not component.get("ok"):
                continue
            for stage, field in (("half_spread", "half_spread_bps"), ("signal_to_exec", "signal_to_exec_bps"), ("exec_to_fill", "exec_to_fill_bps")):
                value = _d(component.get(field))
                if value is not None:
                    stages[stage].append(value)
        if summary.get("hedge_delay_ms") is not None:
            delays.append(Decimal(str(summary["hedge_delay_ms"])))
        for venue in (summary.get("buy_venue"), summary.get("sell_venue")):
            value = _d((summary.get("venue_roundtrip_cost_bps") or {}).get(venue))
            if venue and value is not None:
                by_venue[(summary.get("asset"), venue)].append(value)
        key = (summary.get("asset"), summary.get("buy_venue"), summary.get("sell_venue"))
        by_pair_cost[key].append(_d(summary.get("total_price_cost_bps")) or ZERO)
        pnl = _d(summary.get("gross_pnl_bps"))
        if pnl is not None:
            by_pair_pnl[key].append(pnl)

    venues = {}
    venue_recommendations = {}
    for (asset, venue), values in by_venue.items():
        key = f"{asset}:{venue}"
        venues[key] = {
            "samples": len(values),
            "p50_roundtrip_cost_bps": str(percentile(values, 50)),
            "p80_roundtrip_cost_bps": str(percentile(values, 80)),
        }
        venue_recommendations[key] = str(max(ZERO, percentile(values, 80) + target_bps))

    pairs = {}
    pair_recommendations = {}
    for (asset, buy, sell), costs in by_pair_cost.items():
        key = f"{asset}:{buy}->{sell}"
        pnls = by_pair_pnl.get((asset, buy, sell), [])
        p80 = percentile(costs, 80)
        pairs[key] = {
            "samples": len(costs),
            "p50_total_cost_bps": str(percentile(costs, 50)),
            "p80_total_cost_bps": str(p80),
            "p50_pnl_bps": str(percentile(pnls, 50)) if pnls else None,
            "p20_pnl_bps": str(percentile(pnls, 20)) if pnls else None,
            "p80_pnl_bps": str(percentile(pnls, 80)) if pnls else None,
        }
        pair_recommendations[key] = str(max(ZERO, p80 + target_bps))

    return {
        "schema": 2,
        "target_net_bps": str(target_bps),
        "rounds": len(summaries),
        "complete_rounds": sum(1 for s in summaries if s["complete"]),
        "excluded_rounds": dict(excluded),
        "fees_unverified_rounds": sum(1 for s in summaries if s["complete"] and s.get("fees_status") != "verified"),
        "stages": {name: {"samples": len(values), "p50_bps": str(percentile(values, 50)), "p80_bps": str(percentile(values, 80))} for name, values in stages.items()},
        "hedge_delay_ms": {"samples": len(delays), "p50_ms": str(percentile(delays, 50)), "p80_ms": str(percentile(delays, 80))},
        "venues": venues,
        "pairs": pairs,
        "recommended_trigger_bps": venue_recommendations,
        "recommended_pair_trigger_bps": pair_recommendations,
        "fee_policy": "fees_reported_separately_not_subtracted_from_price_cost",
        "summaries": summaries,
    }
