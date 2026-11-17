# NVDA fast execution

## Normal path

New BBO/book event -> existing two-of-three distinct quote confirmation -> one
reserved child -> concurrent paired IOC orders -> fill/position verification.
No REST position, order, equity or allMids request runs in front of a normal
paired submission. Entropy prices its IOC from the same side-specific BBO used
by the decision. Startup, periodic background reconciliation and post-fill
verification still read the venues. A background result from before a trade is
discarded rather than overwriting a newer verified position.

Post-fill verification allows up to three seconds for account endpoints to
reflect the confirmed fills, returning immediately when they already match.
Equal, confirmed paired fills are never corrected with another order just
because a position endpoint lags. A correction must strictly reduce absolute
exposure. Failed entries unwind confirmed fill quantities directly, so a stale
flat account response cannot hide a leg that needs unwinding. An unknown
correction is not re-sent. This grace period is after submission, not an entry
confirmation or a delay in front of the order.

The rolling median remains sampled once per wall-clock second. Event-driven
decisions do not overweight venues with faster message rates. Duplicate quote
versions do not count again, and the current deviation must still clear the
existing tier threshold. There is no additional confirmation or 300 ms gate.

## Sizing and recovery

Tier thresholds and cumulative capacities are unchanged. Each normal opening
and closing order is at most 20 USD of target notional (quantity rounded down
to the common venue size step). Multiple children fill the original tier
capacity; the aggregate ladder remains capped at five times the smaller equity.
The first frozen anchor of a tier is retained across its children. Entry costs
are retained per child and allocated once when that child closes.

One in-flight reservation is persisted before submission, and client IDs are
stable for that reservation. An unresolved reservation blocks restart recovery;
it is not silently re-sent. A confirmed reject with a verified unwind does not
add a blanket 30 second cooldown. Unknown order results still stop further
orders. Emergency reduce-only correction orders can exceed a normal child size
to remove an existing exposure promptly; they cannot open a new position.

## Accounting

Every correction receipt, side and filled quantity is kept in the order log.
Rejected entries and excess partial-fill unwinds are valued using actual cash
flows and fees. Verified unwind losses enter the existing realized-loss limit.
Entropy's expected 110% fee rebate remains separate and is not counted as cash
already received. Missing prices/fees or unmatched cashflows are explicitly
incomplete, not fabricated as zero-cost executions or theoretical profits.

Read-only report:

```text
.venv/bin/python tools/nvda_execution_report.py
```

Old journals without unwind fill receipts remain incomplete. The report does
not overwrite strategy state or send orders. ANTH, INTC, the capture service,
watchdogs and auto-rotation are outside this change.

`tools/nvda_reconcile_flat.py` is read-only by default. Its explicit `--apply`
mode can repair a stale single-child close ledger only after two verified flat
account reads, no active orders, and exactly matching paired closing fill
receipts. It makes a backup and never sends an order. It does not guess the
outcome of an unresolved opening order.
