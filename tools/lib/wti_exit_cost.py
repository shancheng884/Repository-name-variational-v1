"""Evidence-bound remaining exit costs. Never changes positions or submits orders."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import csv
import io
from urllib.parse import urlsplit, parse_qs

from tools.lib.execution_quality import shortfall_tail

D = Decimal
EXIT_RESERVE_MIN_SAMPLES = 3


def scoped_cost_policy(state, evidence, layer_ids, *, now=None, fallback_bps=D('10')):
    """Revalidate the proof, then charge selected fees and observed exit risk."""
    applied, audit = cost_policy(state, evidence, now=now, fallback_bps=fallback_bps)
    if audit['mode'] != 'real_cost_plus_remaining_reserve':
        return applied
    from tools.lib.wti_continuous import position_subset
    from copy import deepcopy
    try:
        scoped = deepcopy(state)
        scoped['open_position'] = position_subset(state['open_position'], layer_ids)
        var = evidence['fee_proofs']['variational']
        lighter = evidence['fee_proofs']['lighter']
        fees = number(var_fee_capture(scoped, var['raw'], expected_address=var['expected_address'],
                                     now=now or datetime.now(timezone.utc))['fee_usd'])
        fees += number(lighter_fee_capture(scoped, lighter['raw'], account_index=lighter['account_index'])['fee_usd'])
        p = scoped['open_position']
        base = (number(p['var_qty'])*number(p['var_entry_price'])
                +number(p['lighter_qty'])*number(p['lighter_entry_price'])*number(p['multiplier']))/2
        return ((fees+number(evidence['exit_fee_reserve_usd']))
                /base*10000+number(audit['remaining_execution_reserve_bps']))
    except (ValueError, KeyError, TypeError, ArithmeticError):
        return fallback_bps


def var_fee_capture(state, capture, *, expected_address, now):
    """A complete authenticated fee-only response, not a missing fee field."""
    p = state["open_position"]
    entries = {e["action_id"]: e for e in state["execution_history"]}
    ids = {x["entry_execution_id"] for x in p.get("layers", [p])}
    rfqs = {str(entries[i]["legs"]["variational"]["result"]["native_client_order_id"]) for i in ids}
    opened = min(datetime.fromisoformat(entries[i]["intent_at"]) for i in ids)-timedelta(seconds=5)
    latest_entry = max(datetime.fromisoformat(entries[i]["intent_at"]) for i in ids)
    other_rfqs = {str((e.get('legs', {}).get('variational', {}).get('result') or {}).get('native_client_order_id'))
                  for e in state['execution_history'] if e.get('action_id') not in ids}
    found = []
    for entry in capture["log"]["entries"]:
        request, response = entry["request"], entry["response"]
        url = urlsplit(request["url"])
        if url.path != "/api/transfers":
            continue
        query = parse_qs(url.query)
        if "fee" not in query.get("type", [""])[0].split(","):
            continue
        headers = {h["name"].lower(): h["value"] for h in request.get("headers", [])}
        observed = datetime.fromisoformat(entry["startedDateTime"].replace("Z", "+00:00"))
        if (request["method"] != "GET" or response["status"] != 200
                or url.scheme != "https" or url.hostname != "omni.variational.io"
                or headers.get("vr-connected-address", "").lower() != expected_address.lower()
                or not -5 <= (now-observed).total_seconds() <= 900
                or observed < latest_entry
                or datetime.fromisoformat(query["created_at_gte"][0].replace("Z", "+00:00")) > opened
                or datetime.fromisoformat(query["created_at_lte"][0].replace("Z", "+00:00")) < observed):
            raise ValueError("var_fee_response_scope_unverified")
        content = response["content"]
        if content.get("encoding"):
            raise ValueError("var_fee_capture_encoding_unsupported")
        body = json.loads(content["text"])
        rows, page = body["result"], body["pagination"]
        if (not isinstance(rows, list) or page.get("next_page", "missing") is not None
                or page.get("object_count") != len(rows) or len(rows) >= int(query["limit"][0])
                or query.get("offset") != ["0"]):
            raise ValueError("var_fee_pagination_incomplete")
        total, seen = D(0), set()
        for row in rows:
            at = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
            if row.get("transfer_type") != "fee" or not opened <= at <= observed:
                continue
            if row.get("fee_type") in ("deposit", "withdrawal"):
                continue
            if row.get("rfq_id") in other_rfqs:
                continue
            if (row.get("rfq_id") not in rfqs or row.get("status") != "confirmed"
                    or row.get("asset") != "USDC" or not row.get("id") or row["id"] in seen):
                raise ValueError("var_trade_fee_allocation_unverified")
            signed = D(str(row["qty"]))
            if not signed.is_finite() or signed > 0:
                raise ValueError("var_fee_sign_unverified")
            total -= signed
            seen.add(row["id"])
        found.append((observed, total))
    if not found:
        raise ValueError("var_complete_fee_response_missing")
    observed, total = max(found)
    return {"status": "verified", "complete": True, "fee_usd": str(total),
            "observed_at": observed.isoformat(), "position_fingerprint": fingerprint(state),
            "source_sha256": hashlib.sha256(json.dumps(capture, sort_keys=True).encode()).hexdigest(),
            "source": "authenticated_complete_fee_response"}


def lighter_fee_capture(state, raw, *, account_index):
    p = state["open_position"]
    ids = {x["entry_execution_id"] for x in p.get("layers", [p])}
    actions = [e for e in state["execution_history"] if e["action_id"] in ids]
    rows = list(csv.DictReader(io.StringIO(raw["trades_csv"])))
    exported = {str(x["Trade ID"]): x for x in rows}
    if len(exported) != len(rows):
        raise ValueError("lighter_duplicate_fee_trade")
    used, fees = set(), D(0)
    for action in actions:
        leg = action["legs"]["lighter"]
        request, fill = leg["request"], leg["result"]
        reply = raw["trades"][action["action_id"]]
        if reply.get("code") != 200 or reply.get("next_cursor") or not 0 < len(reply["trades"]) < 100:
            raise ValueError("lighter_fee_trade_page_incomplete")
        qty, notional = D(0), D(0)
        prefix = "ask" if request["side"] == "sell" else "bid"
        for trade in reply["trades"]:
            tid = str(trade["trade_id"])
            record = exported.get(tid)
            if (record is None or tid in used or trade.get("market_id") != 145
                    or trade.get(prefix+"_account_id") != account_index
                    or str(trade.get(prefix+"_id")) != str(fill["native_order_id"])
                    or record.get("Market") != "WTI" or number(record["Size"]) != number(trade["size"])
                    or number(record["Price"]) != number(trade["price"])):
                raise ValueError("lighter_fee_trade_binding_mismatch")
            fee = number(record["Fee"])
            if fee and record.get("Fee Currency") not in ("USDC", "USD"):
                raise ValueError("lighter_nonzero_fee_currency_missing")
            fees += fee
            qty += number(trade["size"])
            notional += number(trade["size"])*number(trade["price"])
            used.add(tid)
        if qty != number(fill["filled_qty"]) or abs(notional-qty*number(fill["average_price"])) > D(".000001"):
            raise ValueError("lighter_fee_fill_coverage_incomplete")
    return {"status": "verified", "complete": True, "fee_usd": str(fees),
            "scoped_trade_ids": sorted(used), "out_of_scope_csv_trade_ids": sorted(set(exported)-used),
            "position_fingerprint": fingerprint(state), "source": "authenticated_csv_and_exact_order_trade",
            "source_sha256": hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()}


def number(value):
    if isinstance(value, bool):
        raise ValueError("boolean_cost")
    result = D(str(value))
    if not result.is_finite() or result < 0:
        raise ValueError("invalid_cost")
    return result


def fingerprint(state):
    position = state.get("open_position") or {}
    ids = {x.get("entry_execution_id") for x in position.get("layers", [position])}
    actions = [e for e in state.get("execution_history", []) if e.get("action_id") in ids]
    if not ids or None in ids or len(actions) != len(ids):
        raise ValueError("exit_cost_entry_identity_missing")
    bindings = []
    for action in actions:
        legs = {}
        for venue in ("variational", "lighter"):
            request = action["legs"][venue]["request"]
            fill = action["legs"][venue]["result"]
            if (fill.get("classification") != "filled" or fill.get("terminal_confirmed") is not True
                    or not fill.get("native_order_id") or not fill.get("native_client_order_id")):
                raise ValueError("exit_cost_entry_fill_unconfirmed")
            legs[venue] = {"order_id": str(fill["native_order_id"]),
                           "client_id": str(fill["native_client_order_id"]),
                           "side": request["side"], "qty": str(D(fill["filled_qty"])),
                           "price": str(D(fill["average_price"]))}
        bindings.append({"id": action["action_id"], "legs": legs})
    value = {"pilot_id": position.get("gradient_pilot_id"), "market_id": position.get("market_id"),
             "multiplier": position.get("multiplier"), "direction": position.get("direction"),
             "var_qty": position.get("var_qty"), "lighter_qty": position.get("lighter_qty"),
             "var_entry_price": position.get("var_entry_price"),
             "lighter_entry_price": position.get("lighter_entry_price"),
             "entries": sorted(bindings, key=lambda x: x["id"])}
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def exit_tail(state, *, before):
    """Use only earlier, terminal, paired WTI exits; never ETH or entry slippage."""
    losses, ids = [], set()
    excluded = set(state.get("strategy_evidence_excluded_action_ids") or [])
    for action in state.get("execution_history", []):
        if (action.get("kind") != "close" or action.get("market_id") != 145 or not action.get("action_id")
                or action.get("action_id") in ids or action.get("outcome") != "filled"
                or action.get("strategy_evidence_excluded") is True
                or action.get("action_id") in excluded):
            continue
        try:
            if datetime.fromisoformat(action["completed_at"]) >= before:
                continue
            cost, base, quantities = D(0), D(0), []
            for venue in ("variational", "lighter"):
                leg = action["legs"][venue]
                request, fill = leg["request"], leg["result"]
                expected = "sell" if venue == "variational" else "buy"
                if action.get("direction") == "short_var_long_lighter":
                    expected = "buy" if venue == "variational" else "sell"
                if action.get("direction") not in ("long_var_short_lighter", "short_var_long_lighter") or request.get("side") != expected:
                    raise ValueError("exit_pair_direction_mismatch")
                if (fill.get("classification") != "filled" or fill.get("terminal_confirmed") is not True
                        or not fill.get("native_order_id") or request.get("side") not in ("buy", "sell")):
                    raise ValueError("exit_not_terminal")
                qty, reference, price = (number(fill["filled_qty"]), number(request["reference_price"]),
                                         number(fill["average_price"]))
                if min(qty, reference, price) <= 0 or qty != number(request["qty"]):
                    raise ValueError("exit_quantity_mismatch")
                if venue == "lighter":
                    qty *= number(action["multiplier"])
                quantities.append(qty)
                cost += (price-reference)*qty*(1 if request["side"] == "buy" else -1)
                base += reference*qty/2
            if quantities[0] != quantities[1]:
                continue
            losses.append(max(D(0), cost/base*10000))
            ids.add(action["action_id"])
        except (KeyError, TypeError, ValueError, ArithmeticError):
            continue
    # The reserve is derived only from confirmed paired fills. The caller keeps
    # the legacy 10 bps fallback until the evidence sample is large enough.
    return shortfall_tail(losses)[2], len(losses)


def cost_policy(state, evidence, *, now=None, fallback_bps=D("10")):
    now = now or datetime.now(timezone.utc)
    diagnostic = {"mode": "legacy_fallback", "applied_bps": str(fallback_bps),
                  "reason": "exit_fee_evidence_missing", "entry_slippage_deducted_again": False}
    if not state.get("open_position") or not isinstance(evidence, dict):
        return fallback_bps, diagnostic
    try:
        if evidence.get("schema_version") != 2 or evidence.get("position_fingerprint") != fingerprint(state):
            raise ValueError("exit_cost_position_binding_mismatch")
        if evidence.get("funding_excluded_from_exit_cost") is not True:
            raise ValueError("exit_cost_funding_policy_unverified")
        observed, expires = (datetime.fromisoformat(evidence[k]) for k in ("observed_at", "expires_at"))
        if not observed <= now < expires or not 0 < (expires-observed).total_seconds() <= 900:
            raise ValueError("exit_cost_evidence_expired_or_future")
        if evidence.get("fee_evidence_verified") is not True:
            raise ValueError("exit_fee_evidence_unverified")
        fees = number(evidence["entry_fees_usd"])
        exit_fees = number(evidence["exit_fee_reserve_usd"])
        for venue in ("variational", "lighter"):
            proof = evidence["fee_proofs"][venue]
            if (proof.get("status") != "verified" or not proof.get("source_sha256")
                    or proof.get("position_fingerprint") != evidence["position_fingerprint"]
                    or proof.get("complete") is not True):
                raise ValueError("exit_fee_source_incomplete")
        var_source = evidence["fee_proofs"]["variational"]
        lighter_source = evidence["fee_proofs"]["lighter"]
        rebuilt_var = var_fee_capture(state, var_source["raw"],
            expected_address=var_source["expected_address"], now=now)
        rebuilt_lighter = lighter_fee_capture(state, lighter_source["raw"],
            account_index=lighter_source["account_index"])
        for old, rebuilt in ((var_source, rebuilt_var), (lighter_source, rebuilt_lighter)):
            if old["source_sha256"] != rebuilt["source_sha256"] or old["fee_usd"] != rebuilt["fee_usd"]:
                raise ValueError("exit_fee_source_recomputation_mismatch")
        if fees != number(rebuilt_var["fee_usd"])+number(rebuilt_lighter["fee_usd"]):
            raise ValueError("exit_fee_total_mismatch")
        p = state["open_position"]
        base = (number(p["var_entry_price"])*number(p["var_qty"])
                + number(p["lighter_entry_price"])*number(p["lighter_qty"])*number(p["multiplier"]))/2
        if base <= 0:
            raise ValueError("exit_cost_notional_invalid")
        reserve, count = exit_tail(state, before=now)
        if count < EXIT_RESERVE_MIN_SAMPLES:
            raise ValueError("exit_execution_evidence_insufficient")
        applied = (fees+exit_fees)/base*10000+reserve
        return applied, {**diagnostic, "mode": "real_cost_plus_remaining_reserve", "reason": None,
                         "applied_bps": str(applied), "entry_fees_usd": str(fees),
                         "exit_fee_reserve_usd": str(exit_fees),
                         "remaining_execution_reserve_bps": str(reserve), "exit_tail_samples": count,
                         "execution_reserve_method": "paired_terminal_close_p95_latest",
                         "funding_excluded_from_exit_cost": True,
                         "expires_at": expires.isoformat()}
    except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
        return fallback_bps, {**diagnostic, "reason": str(exc)}
