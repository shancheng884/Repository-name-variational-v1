"""Measure realized round-trip execution slippage from the tier order journal.

Signal edge is the executable top-of-book edge at the submitted quote; fill
edge is computed from the venue's average fill prices.  The difference is the
slippage the strategy actually paid, so entry and exit are not double counted.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.nvda_execution_stats import receipt_order_ids

def _num(value):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _quote(receipt: dict) -> tuple[float | None, float | None]:
    quote = receipt.get("execution_quote") or {}
    return _num(quote.get("bid")), _num(quote.get("ask"))


def _fill(receipt: dict) -> float | None:
    return _num(receipt.get("avg_price"))


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[index]


def executable_close_price(receipt, qty, side):
    quote = receipt.get("execution_quote") or {}
    depth = quote.get("depth_audit") or {}
    touch = "ask" if side == "buy" else "bid"
    if (_num(quote.get(touch + "_size")) or 0) >= qty:
        return _num(quote.get(touch))
    at = quote.get("quote_recv_at")
    if at and depth.get("book_recv_at") == at and _num(depth.get("requested_qty")) == qty:
        if _num(depth.get(side + "_visible_qty_shortfall")) == 0:
            return _num(depth.get(side + "_vwap"))
    return None


def roundtrip_slippage(rows: list[dict]) -> dict:
    pending: dict[tuple[str, int], deque] = defaultdict(deque)
    pairs = []
    for row in rows:
        event = row.get("event")
        key = (row.get("direction"), row.get("tier"))
        if event == "opened":
            pending[key].append(row)
        elif event == "closed" and pending[key]:
            queue = pending[key]
            child, order_ids = row.get("child_id"), row.get("entry_order_ids") or {}
            if not child and not order_ids:
                pairs.append((queue.popleft(), row))
                continue
            candidates = [r for r in queue
                          if (child is not None and child == (r.get("child_id") or r.get("execution_token")))
                          or (order_ids and order_ids == receipt_order_ids(r.get("receipts") or {}))]
            if len(candidates) == 1:
                opened = candidates[0]
                queue.remove(opened)
                pairs.append((opened, row))

    residual = []
    for row in rows:
        qty = _num(row.get("qty"))
        if row.get("event") != "closed" or not qty or qty <= 0 or (row.get("settle") or {}).get("corrections"):
            continue
        long, short = (("lighter_rh", "entropy") if row.get("direction") == "reverse"
                       else ("entropy", "lighter_rh"))
        receipts = row.get("receipts") or {}
        close_long, close_short = receipts.get(long) or {}, receipts.get(short) or {}
        if not all(r.get("status") == "filled" and _num(r.get("filled_qty")) == qty
                   for r in (close_long, close_short)):
            continue
        prices = (executable_close_price(close_long, qty, "sell"),
                  executable_close_price(close_short, qty, "buy"),
                  _fill(close_long), _fill(close_short))
        if all(p is not None and p > 0 for p in prices):
            sell, buy, fill_sell, fill_buy = prices
            residual.append(((sell - fill_sell) + (fill_buy - buy)) / ((sell + buy) / 2) * 1e4)

    entries, exits, total = [], [], []
    for opened, closed in pairs:
        direction = opened["direction"]
        buy_venue, sell_venue = (("lighter_rh", "entropy") if direction == "reverse"
                                 else ("entropy", "lighter_rh"))
        entry_buy_bid, entry_buy_ask = _quote(opened["receipts"][buy_venue])
        entry_sell_bid, entry_sell_ask = _quote(opened["receipts"][sell_venue])
        exit_buy_bid, exit_buy_ask = _quote(closed["receipts"][buy_venue])
        exit_sell_bid, exit_sell_ask = _quote(closed["receipts"][sell_venue])
        values = (entry_buy_ask, entry_sell_bid, exit_buy_bid, exit_sell_ask,
                  _fill(opened["receipts"][buy_venue]), _fill(opened["receipts"][sell_venue]),
                  _fill(closed["receipts"][buy_venue]), _fill(closed["receipts"][sell_venue]))
        if any(value is None for value in values):
            continue
        entry_mid = (entry_buy_ask + entry_sell_bid) / 2
        exit_mid = (exit_buy_bid + exit_sell_ask) / 2
        theoretic_entry = (entry_sell_bid - entry_buy_ask) / entry_mid * 1e4
        realized_entry = (_fill(opened["receipts"][sell_venue])
                          - _fill(opened["receipts"][buy_venue])) / entry_mid * 1e4
        theoretic_exit = (exit_buy_bid - exit_sell_ask) / exit_mid * 1e4
        realized_exit = (_fill(closed["receipts"][buy_venue])
                         - _fill(closed["receipts"][sell_venue])) / exit_mid * 1e4
        entry_slip = theoretic_entry - realized_entry
        exit_slip = theoretic_exit - realized_exit
        entries.append(entry_slip)
        exits.append(exit_slip)
        total.append(entry_slip + exit_slip)

    def stats(values):
        return {"n": len(values),
                "p50": percentile(values, 0.50),
                "p80": percentile(values, 0.80),
                "p95": percentile(values, 0.95)}

    return {"roundtrips": len(total), "entry_slip_bps": stats(entries),
            "exit_slip_bps": stats(exits), "roundtrip_slip_bps": stats(total),
            "exit_after_executable_book_bps": stats(residual)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=Path, required=True)
    args = parser.parse_args(argv)
    rows = [json.loads(line) for line in args.orders.read_text(encoding="utf-8").splitlines()
            if line.strip()]
    result = roundtrip_slippage(rows)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
