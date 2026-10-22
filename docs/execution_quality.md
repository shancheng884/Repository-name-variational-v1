# ETH Exit Execution Quality

Normal V4 profit exits now recheck the complete exit quantity after the intent
has been persisted and before starting either venue order. This gate applies to
single lots, gradient tiers, and aggregate exits. It validates the reused Var
quote, the order budgets, exchange and local book ages, full depth, and the
execution-cost-adjusted PnL. Normal exits require a book age no greater than
250 ms (or the configured book-age limit, if smaller). A rejected preflight
does not submit orders, remove lots, or count a completed cycle.

Lighter book timestamps accept seconds, milliseconds, microseconds, nanoseconds,
or timezone-qualified ISO timestamps. Both the message timestamp and the
optional matching-engine state timestamp are checked. More than 100 ms of
future clock skew is rejected. Local receipt uses a monotonic clock. Missing
exchange timestamps do not pass an enabled normal book-freshness gate.

Exit shortfall calibration counts each confirmed close group once. It uses the
rolling P95 and reacts immediately to the latest loss, without clipping observed
costs at 3 bps. A separate directional reserve envelope decreases by at most
0.25 bps per new confirmed close group. Its floor survives strategy restarts.
Exit costs above the existing 3.5 bps cold-start prior also increase the entry
execution reserve, and new observations invalidate the cached entry thresholds.
This is a conservative execution-cost policy, not a fitted profitability claim.

The profit gate and hedge-completion price budget serve different purposes.
The existing reduce-only completion allowance is anchored to the preflight
Lighter price, and retries cannot widen its absolute boundary. Sell boundaries
round upward to the venue price tick; buy boundaries round downward. A normal
profit gate must not cancel an already-filled counter-leg's recovery just
because the original profit target is no longer achievable. Uncertain outcomes
continue to require reconciliation rather than a duplicate submission. Cross-
venue orders are not atomic and the gate cannot guarantee realized profit.

`exit_lighter_wire_to_fill_report_ms` measures local wire send to receipt of the
Lighter fill notification. It includes execution and notification delays; it is
not pure network latency or the exchange's matching time. Synthetic Var timestamps
are excluded from the old Var-to-Lighter timing field and from latency calibration.
Historical rows without the new timing field have unknown execution latency.

Run the read-only audit on the VPS:

```bash
.venv/bin/python tools/execution_quality_audit.py --asset ETH --since-beijing 2026-10-16
```

It reads current and numbered/gzip metric segments and reports confirmed price
PnL, shortfall, timing coverage, and a strictly-prior cost-gate replay. Earlier
rows warm the cost model before the requested report window. The replay tests
only whether previously logged exit candidates meet the new cost gate; rejected
candidates are not treated as profitable alternative fills. Fees, funding, new
exit times, and hypothetical realized PnL are not inferred. Sparse baseline
samples cannot establish fills during Lighter's taker-delay interval. Actual
delay-aware fill replay needs event-level book data covering that interval.
