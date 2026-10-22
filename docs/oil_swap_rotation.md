# ETH / WTI Rotation Readiness

## Current Scope

The WTI collector remains read-only. It records indicative Variational USOILP swap
quotes and Lighter WTI order-book snapshots; it does not submit orders or infer
realized fees, funding, or roll cashflows. Session status is extracted only from
explicit status fields in the Variational quote response, with a receive-time
observation stamp, session identity, and timezone-aware future close. Missing,
stale, contradictory, or incomplete evidence remains unknown. Its rotating log
is capped at 10 MiB with five backups.

`tools/lib/wti_execution.py` adds an isolated execution coordinator for adapter
development and simulated-response tests. It is not imported by `main.py` or
`tools/live.py`, has no exchange API implementation, and cannot place real orders
by itself. The coordinator requires an injected gateway, a fresh read-only
sample reader, and a separately verified `SessionWindow`. Its order requests
carry exact USOILP swap / Lighter WTI identities, IOC time-in-force, a marketable
limit boundary, and a hard notional cap. Each leg requires a final full-fill
response; any partial, unknown, or mismatched result is persisted as
`manual_review_required`, blocks retries, and must be resolved by read-only
position and open-order reconciliation. The executor pins the verified Lighter
market ID, exposure multiplier, and size precision at construction; a collector
sample cannot choose the execution contract. The adapter contract must provide
WTI-filtered open orders, exact identity-aware position responses, and an
`observed_at` timestamp for the account snapshot. A snapshot older than five
seconds or taken before the last order attempt cannot confirm a fill or flat
state. Unknown submissions are resolved only through a terminal lookup by the
persisted `client_order_id`; lookup responses must repeat that exact ID.

The exchange session observation and closing time are carried in
`SessionWindow`. Entry cutoff and closing expiry are recalculated immediately
before each submission, so a previously open window cannot authorize an order
after its status expires or entry cutoff passes. Each order intent is flushed
to disk before submission. Confirmed open and close actions retain order IDs,
trade IDs, raw normalized quantities, and fee evidence in `execution_history`;
unreported fees remain unknown instead of being recorded as zero. The
coordinator does not infer funding or realized PnL.

The ETH V4 live runner is explicitly ETH-only. Do not add WTI to its allowlist or
reuse the ETH calibration profile for WTI. Automatic pair switching is not
enabled by this change because the current live runner does not yet reconcile
the USOILP swap identity, swap-specific open-order identity, market session, and
contract exposure as one WTI execution path.

## Read-Only Account Probe

`python tools/wti_account_probe.py` reads Variational positions and pending
orders through the existing extension command port, reads the Lighter account
and market-scoped active orders through the SDK, and fetches current WTI market
metadata from the mainnet order-books endpoint. The optional
`--expected-market-id` rejects a market-ID change. The command prints only
normalized quantities, active-order count, timestamps, and market metadata; it
does not print API keys or raw order records and contains no order-submission
method. It must not be used as a trading-readiness or live-authorization check.

Var pending orders are paginated without an instrument filter and then matched
by explicit underlying/instrument identity. If any row cannot be identified,
or either venue response shape is unknown, the probe fails instead of reporting
zero. A successful read means only that the sampled positions and open orders
were parsed within the configured read window; the operator must still compare
the two signed quantities and verify the live session before any future
execution work.

## Fail-Closed Rotation Rules

- WTI entry is considered only when the exchange explicitly reports the market
  open in a fresh, timezone-stamped status snapshot and supplies a future,
  timezone-aware close time. Status older than 30 seconds, a future-dated status,
  or an expired close time is unknown. Local weekday/time guesses are not an
  exchange-status substitute.
- The entry cutoff begins 30 minutes before the reported close. Existing
  positions may be drained only while the venue confirms that position
  management is available.
- An unknown/closed session blocks new WTI entries. A position is never switched
  to ETH until both venues are confirmed flat, there are no active orders or
  pending actions, and exchange reconciliation succeeds.
- Three qualified exchange session IDs (two training sessions plus a qualified
  latest holdout) only establish that offline analysis may be run. They do not
  authorize live trading. Each session must still meet the existing 120-second
  maximum in-session sample-gap limit. A verified prior close and a separately
  verified subsequent open may delimit distinct sessions, but an open position
  cannot be carried across that boundary in replay.
- Production-rotation readiness additionally requires at least five distinct,
  confirmed WTI roundtrips across at least two dates, exact instrument and
  market identities (the Lighter market ID must match the validated sample
  profile), matched paired exposure, reconciled positions, actual fees
  and funding cashflows, and positive aggregate net PnL.

## Readiness Audit

Run `python tools/oil_swap_readiness.py` against the WTI collector data. For a
production-promotion audit, provide both `--lighter-market-id` and
`--lighter-exposure-multiplier` using the current Lighter market metadata, never
values inferred from historical samples. The JSON
report separates historical `market_data_ready`, recent `data_fresh`,
`ready_for_bounded_live_review`, and `historical_promotion_evidence_ready`.
This read-only audit always returns `ready_for_production_rotation: false` and
adds `live_execution_and_session_gates_not_verified`: historical records cannot
verify live order adapters or the current exchange session. The command returns
exit code 0 when bounded-live review is ready, not when production rotation is
ready. Neither result submits or enables orders. Historical samples without
explicit fresh session status, session ID, and close time are counted as
unverified and excluded from readiness. A single consistent Lighter WTI market ID is required across the
sample profile; roundtrip evidence with another market ID is rejected. The
production gate requires both `--lighter-market-id` and
`--lighter-exposure-multiplier` from current exchange metadata; values inferred
from old samples are not enough. Confirmed roundtrip timestamps must be
timezone-aware and cannot be in the future. A bounded live review means the
recent data is sufficient to start reviewing a separately implemented,
size-limited diagnostic pilot; it does not enable or authorize orders.
Historical promotion evidence additionally needs at least five independently
identified, reconciled roundtrips across two or more sessions, recomputed net
PnL that agrees with the recorded value, positive aggregate net PnL, and a
positive latest validation session. Even when that evidence passes, the audit
does not authorize production rotation; verified live execution adapters,
current exchange-session status, recovery behavior, and end-to-end checks are
still required.

Roundtrip evidence must include actual average entry/exit prices and separate
`var_entry_filled_qty`, `var_exit_filled_qty`, `lighter_entry_filled_qty`, and
`lighter_exit_filled_qty` values. Entry and exit quantities must match on each
venue, and paired exposure must match at both entry and exit. It must also
include both exposure multipliers, per-venue trade IDs, fees, signed funding
cashflows, position reconciliation, exact instrument and market identities,
and the side of each of the four fills. Repeated identical records count once;
conflicting records with the same roundtrip ID or reused trade IDs block
production promotion. The
audit recomputes PnL and rejects duplicate venue trade IDs. A declared PnL value
alone is not accepted as evidence.

The collector does not create roundtrip evidence. Three days of clean, fresh
market data can qualify for operator review, while production-rotation
readiness remains false until a WTI execution path produces verified
roundtrips.

## Indicative Quote-Path Replay

`python tools/wti_strategy_replay.py` replays a user-specified entry edge,
net exit target, roundtrip cost assumption, maximum hold, and maximum sample
gap against saved WTI samples. It uses only the current and prior sample at
each decision point, rejects samples without explicit fresh session evidence,
duplicate/conflicting sample IDs and mixed Lighter market profiles, stops at an
in-session gap above the unchanged configured limit, blocks entry during the
last 30 minutes before reported close, and marks any position crossing a session
boundary unresolved rather than inventing an exit. A long interval is not
treated as closed-market time unless the prior close and next open are both
explicitly evidenced. The replay uses recorded executable quote proxies but
does not establish fills, fees, funding, roll cashflows, or live latency. Its report always has
`live_authorization: false`; do not use this as production promotion evidence.

The rotation state machine in `tools/lib/pair_rotation.py` is a decision
primitive only; it does not stop or start trading processes. Unknown positions
while no active pair is recorded force reconciliation and block new entries.
Production automation still requires production venue adapters, verified live
session-status and account/order response schemas, exchange price-precision
handling, funding/fee reconciliation, close-window draining, and end-to-end tests.
The simulator tests validate only coordinator behavior with synthetic gateway
responses and read-only collector-shaped samples; they do not qualify WTI for
live trading or automatic ETH/WTI rotation.
