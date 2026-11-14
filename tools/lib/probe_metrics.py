from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

BPS = Decimal("10000")
ZERO = Decimal("0")

# Venues whose published fee schedule is zero for taker flow in the 20U probe
# size.  A missing fee field there is a policy fact, not missing evidence.
FEE_FREE_VENUES = {"variational", "lighter_mainnet", "lighter_rh"}

# Entropy returns 110% of the taker fee it charges, so a 2 bps gross fee is a
# 0.2 bps credit.  Every other venue reports an already-net fee.
FEE_REBATE_MULTIPLIER = {"entropy": Decimal("1.10")}


def _d(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        out = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return out if out.is_finite() else None


def _parse(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_epoch(value: Any) -> datetime | None:
    """Venue timestamps arrive as seconds, milliseconds, microseconds or ns."""
    number = _d(value)
    if number is None:
        return None
    seconds = float(number)
    if abs(seconds) >= 1e17:
        seconds /= 1e9
    elif abs(seconds) >= 1e14:
        seconds /= 1e6
    elif abs(seconds) >= 1e11:
        seconds /= 1e3
    try:
        return datetime.fromtimestamp(seconds, timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _clock_offset_ms(execution_quote: Any) -> Decimal | None:
    """Echo-based clock offset for one venue.

    Every execution quote carries the venue timestamp we were shown plus the
    local receive time of that same message.  Their difference is the venue
    clock offset plus one-way latency; taking the smallest sample over the run
    leaves an offset estimate accurate to a few milliseconds.
    """
    if not isinstance(execution_quote, dict):
        return None
    pairs = (
        ("received_at", "quote_timestamp"),
        ("quote_recv_at", "quote_source_at"),
        ("received_at", "quote_source_at"),
    )
    for local_key, exchange_key in pairs:
        local = _parse(execution_quote.get(local_key))
        exchange = _parse(execution_quote.get(exchange_key)) or _parse_epoch(execution_quote.get(exchange_key))
        if local is not None and exchange is not None:
            return Decimal(str(round((local - exchange).total_seconds() * 1000, 3)))
    return None


def leg_timing(leg: dict[str, Any]) -> dict[str, Any]:
    """Align venue fill timestamps with local submit/ack/complete stamps."""
    submitted = _parse(leg.get("submitted_at"))
    acked = _parse(leg.get("acked_at"))
    completed = _parse(leg.get("completed_at") or leg.get("local_fill_at"))
    exchange_fill = _parse(leg.get("source_at"))
    offset = _d(leg.get("clock_offset_ms"))
    if offset is None:
        offset = _clock_offset_ms(leg.get("execution_quote"))
    aligned = exchange_fill + timedelta(milliseconds=float(offset)) if exchange_fill is not None and offset is not None else None
    out: dict[str, Any] = {
        "exchange_fill_at": exchange_fill.isoformat() if exchange_fill else None,
        "local_fill_at": completed.isoformat() if completed else None,
        "clock_offset_ms": None if offset is None else str(offset),
        "aligned_fill_at": aligned.isoformat() if aligned else None,
        "submit_to_exchange_fill_ms": round((aligned - submitted).total_seconds() * 1000, 3) if aligned is not None and submitted is not None else None,
        "exchange_fill_to_local_ms": round((completed - aligned).total_seconds() * 1000, 3) if aligned is not None and completed is not None else None,
        "submit_to_local_fill_ms": round((completed - submitted).total_seconds() * 1000, 3) if completed is not None and submitted is not None else None,
        "ack_to_local_fill_ms": round((completed - acked).total_seconds() * 1000, 3) if completed is not None and acked is not None else None,
    }
    out["timestamp_anomaly"] = bool(exchange_fill is not None and submitted is not None and exchange_fill < submitted and offset is None)
    return out


def _fee_status(leg: dict[str, Any], venue: str) -> str:
    if _d(leg.get("fee_usd")) is not None:
        return "verified"
    if venue in FEE_FREE_VENUES:
        return "zero_by_policy"
    return "unverified"


def _fee_bps(leg: dict[str, Any], venue: str) -> Decimal | None:
    fee = _d(leg.get("fee_usd"))
    qty = _d(leg.get("filled_qty"))
    price = _d(leg.get("avg_price"))
    if fee is None:
        return ZERO if venue in FEE_FREE_VENUES else None
    if qty is None or price is None or qty <= 0 or price <= 0:
        return None
    return fee / (qty * price) * BPS


def _net_fee_bps(gross: Decimal | None, venue: str) -> Decimal | None:
    if gross is None:
        return None
    multiplier = FEE_REBATE_MULTIPLIER.get(venue)
    if multiplier is None or gross <= ZERO:
        return gross
    return gross * (Decimal("1") - multiplier)


def _net_fee_usd(fee: Decimal | None, venue: str) -> Decimal | None:
    if fee is None:
        return None
    multiplier = FEE_REBATE_MULTIPLIER.get(venue)
    if multiplier is None or fee <= ZERO:
        return fee
    return fee * (Decimal("1") - multiplier)


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
    value = _d(leg.get("source_at_epoch"))
    if value is None:
        value = _d(leg.get("fill_at_epoch"))
    if value is None:
        parsed = _parse(leg.get("source_at"))
        if parsed is not None:
            value = Decimal(str(parsed.timestamp()))
    return value


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

    fees = ZERO
    net_fees = ZERO
    fee_bps = ZERO
    gross_fee_bps = ZERO
    rebate_bps = ZERO
    fee_statuses: dict[str, str] = {}
    fee_unverified: list[str] = []
    for leg_name, legs in (("entry", entry_legs), ("exit", exit_legs)):
        for venue, leg in legs.items():
            status = _fee_status(leg, str(venue))
            fee_statuses[f"{leg_name}:{venue}"] = status
            value = _d(leg.get("fee_usd"))
            if value is not None:
                fees += value
                net_fees += _net_fee_usd(value, str(venue))
            bps = _fee_bps(leg, str(venue))
            if bps is None:
                fee_unverified.append(f"{leg_name}:{venue}")
            else:
                gross_fee_bps += bps
                net = _net_fee_bps(bps, str(venue))
                fee_bps += net
                rebate_bps += net - bps
    statuses = set(fee_statuses.values())
    if "unverified" in statuses or not statuses:
        fees_status = "unverified"
    elif "verified" in statuses:
        fees_status = "verified"
    else:
        fees_status = "zero_by_policy"
    net_cost = None if fee_unverified else price_cost + fee_bps

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

    timing = {
        "entry": {venue: leg_timing(leg) for venue, leg in entry_legs.items()},
        "exit": {venue: leg_timing(leg) for venue, leg in exit_legs.items()},
    }
    entry_timing = timing["entry"]
    aligned = sorted(
        ((_parse((entry_timing[v] or {}).get("aligned_fill_at")), v) for v in entry_legs if _parse((entry_timing[v] or {}).get("aligned_fill_at"))),
        key=lambda item: item[0],
    )
    raw = sorted(((_fill_stamp(leg), venue) for venue, leg in entry_legs.items() if _fill_stamp(leg) is not None), key=lambda item: item[0])
    local = sorted(
        ((_parse(entry_legs[v].get("completed_at") or entry_legs[v].get("acked_at")), v) for v in entry_legs if _parse(entry_legs[v].get("completed_at") or entry_legs[v].get("acked_at"))),
        key=lambda item: item[0],
    )
    hedge_delay_ms = None
    hedge_delay_source = None
    first_leg = None
    if len(aligned) == 2:
        hedge_delay_ms = int((aligned[1][0] - aligned[0][0]).total_seconds() * 1000)
        hedge_delay_source = "exchange_aligned"
        first_leg = aligned[0][1]
    elif len(raw) == 2:
        hedge_delay_ms = int((raw[1][0] - raw[0][0]) * 1000)
        hedge_delay_source = "exchange_raw"
        first_leg = raw[0][1]
    elif len(local) == 2:
        hedge_delay_ms = int((local[1][0] - local[0][0]).total_seconds() * 1000)
        hedge_delay_source = "local_completed"
        first_leg = local[0][1]

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
        "fee_bps": str(fee_bps),
        "gross_fee_bps": str(gross_fee_bps),
        "rebate_bps": str(rebate_bps),
        "net_cost_bps": None if net_cost is None else str(net_cost),
        "venue_roundtrip_cost_bps": venue_costs,
        "fees_usd": str(fees),
        "net_fees_usd": str(net_fees),
        "fees_status": fees_status,
        "fee_leg_status": fee_statuses,
        "fees_unverified_legs": fee_unverified,
        "gross_pnl_usd": None if gross_pnl is None else str(gross_pnl),
        "gross_pnl_bps": None if pnl_bps is None else str(pnl_bps),
        "hedge_delay_ms": hedge_delay_ms,
        "hedge_delay_source": hedge_delay_source,
        "first_filled_venue": first_leg,
        "timing": timing,
        "components": components,
        "reconcile": row.get("reconcile"),
    }


def adverse_move(row: dict[str, Any]) -> dict[str, Any]:
    """Post-fill adverse move per venue and horizon, adverse-positive bps.

    The probe already samples the mid at 0.25/0.5/1/3 seconds after entry; this
    turns those points into the adverse-selection curve without any new data.
    """
    out: dict[str, Any] = {}
    for point in row.get("adverse") or []:
        delay = point.get("delay_s")
        if delay is None:
            continue
        for venue, sample in (point.get("points") or {}).items():
            value = _d(sample.get("adverse_bps"))
            if value is None:
                continue
            out.setdefault(venue, {})[str(delay)] = value
    return out


def split_runs(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group probe rounds by run id, preserving first-seen order.

    Rounds written before run ids existed are grouped under ``legacy`` so a
    recompute can report them separately instead of mixing them into a run.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get("run_id") or "legacy")
        grouped.setdefault(key, []).append(row)
    return grouped


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
    by_pair_net: dict[tuple[str, str, str], list[Decimal]] = defaultdict(list)
    by_pair_pnl: dict[tuple[str, str, str], list[Decimal]] = defaultdict(list)
    excluded: dict[str, int] = defaultdict(int)
    stages: dict[str, list[Decimal]] = {"half_spread": [], "signal_to_exec": [], "exec_to_fill": []}
    delays: list[Decimal] = []
    delay_sources: dict[str, int] = defaultdict(int)
    offsets: dict[str, list[Decimal]] = defaultdict(list)
    adverse: dict[tuple[str, str], list[Decimal]] = defaultdict(list)
    adverse_by_venue: dict[tuple[str, str, str], list[Decimal]] = defaultdict(list)
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
            delay_sources[str(summary.get("hedge_delay_source"))] += 1
        for side_timing in (summary.get("timing") or {}).values():
            for venue, timing in (side_timing or {}).items():
                value = _d((timing or {}).get("clock_offset_ms"))
                if value is not None:
                    offsets[str(venue)].append(value)
        for venue, points in adverse_move(row).items():
            for delay, value in points.items():
                adverse[(summary.get("asset"), delay)].append(value)
                adverse_by_venue[(summary.get("asset"), venue, delay)].append(value)
        for venue in (summary.get("buy_venue"), summary.get("sell_venue")):
            value = _d((summary.get("venue_roundtrip_cost_bps") or {}).get(venue))
            if venue and value is not None:
                by_venue[(summary.get("asset"), venue)].append(value)
        key = (summary.get("asset"), summary.get("buy_venue"), summary.get("sell_venue"))
        by_pair_cost[key].append(_d(summary.get("total_price_cost_bps")) or ZERO)
        net = _d(summary.get("net_cost_bps"))
        if net is not None:
            by_pair_net[key].append(net)
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
        nets = by_pair_net.get((asset, buy, sell), [])
        p80 = percentile(costs, 80)
        pairs[key] = {
            "samples": len(costs),
            "p50_total_cost_bps": str(percentile(costs, 50)),
            "p80_total_cost_bps": str(p80),
            "p50_net_cost_bps": str(percentile(nets, 50)) if nets else None,
            "p80_net_cost_bps": str(percentile(nets, 80)) if nets else None,
            "net_samples": len(nets),
            "p50_pnl_bps": str(percentile(pnls, 50)) if pnls else None,
            "p20_pnl_bps": str(percentile(pnls, 20)) if pnls else None,
            "p80_pnl_bps": str(percentile(pnls, 80)) if pnls else None,
        }
        pair_recommendations[key] = str(max(ZERO, p80 + target_bps))

    return {
        "schema": 3,
        "target_net_bps": str(target_bps),
        "rounds": len(summaries),
        "complete_rounds": sum(1 for s in summaries if s["complete"]),
        "excluded_rounds": dict(excluded),
        "fees_unverified_rounds": sum(1 for s in summaries if s["complete"] and s.get("fees_status") == "unverified"),
        "fee_status_counts": {status: sum(1 for s in summaries if s.get("fees_status") == status) for status in sorted({str(s.get("fees_status")) for s in summaries})},
        "stages": {name: {"samples": len(values), "p50_bps": str(percentile(values, 50)), "p80_bps": str(percentile(values, 80))} for name, values in stages.items()},
        "hedge_delay_ms": {"samples": len(delays), "p50_ms": str(percentile(delays, 50)), "p80_ms": str(percentile(delays, 80)), "sources": dict(delay_sources)},
        "clock_offset_ms": {venue: {"samples": len(values), "min_ms": str(min(values)), "median_ms": str(percentile(values, 50))} for venue, values in sorted(offsets.items())},
        "adverse_selection": {
            "by_horizon": {f"{asset}:{delay}s": {"samples": len(v), "p50_bps": str(percentile(v, 50)), "p80_bps": str(percentile(v, 80))} for (asset, delay), v in sorted(adverse.items())},
            "by_venue": {f"{asset}:{venue}:{delay}s": {"samples": len(v), "p50_bps": str(percentile(v, 50)), "p80_bps": str(percentile(v, 80))} for (asset, venue, delay), v in sorted(adverse_by_venue.items())},
        },
        "venues": venues,
        "pairs": pairs,
        "recommended_trigger_bps": venue_recommendations,
        "recommended_pair_trigger_bps": pair_recommendations,
        "fee_policy": "fees_reported_separately_from_price_cost_zero_fee_venues_marked_zero_by_policy_entropy_rebate_110pct",
        "summaries": summaries,
    }
