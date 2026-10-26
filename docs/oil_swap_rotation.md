# ETH / WTI Rotation Readiness

## Current Scope

The WTI collector remains read-only. It records indicative Variational USOILP swap
quotes and Lighter WTI order-book snapshots; it does not submit orders or infer
realized fees, funding, or roll cashflows. Session status is extracted only from
explicit exchange status and schedule evidence. Missing, stale, contradictory,
or incomplete evidence remains unknown. Its rotating log is capped at 10 MiB
with five backups.

`tools/lib/wti_execution.py` is a fail-closed pair coordinator. The standalone
`tools/wti_live.py` wires it to the production Variational forwarder and Lighter
SDK in an isolated WTI process; it is not imported by the ETH runner and does
not start, stop, or modify ETH. The coordinator pins the validated Lighter
market ID, exposure multiplier, and precision. Each submitted leg has a durable
client ID and requires explicit terminal fill evidence plus a fresh reconciled
account snapshot. Partial, unknown, or mismatched results stop in
`manual_review_required`; there is no blind retry. A single reduce-only
Variational compensation is permitted only after an exact first-leg fill and
confirmed terminal no-fill on Lighter.

Positions record the verified exchange session ID. A different or missing
entry-session identity triggers a close decision when the venue is verifiably
open for position management. Samples and session windows must agree before
execution. Live entry authorization is revoked after an in-process sample gap
over 120 seconds or an execution-cycle failure; an open position is drained on
the next valid manageable sample. Authorization does not recover in that
process. Exchange position reads, active-order reads, final fill lookup, and
state reconciliation are required before a position can be treated as flat or
open.

The ETH V4 live runner remains ETH-only. WTI never reads or reuses ETH market
observations. Its five-tier gradient uses the same rolling, directional
percentile method on WTI-only samples: a 7-day anchor, 97.5/98.25/99/99.5/99.8
percentiles, and a 36-hour effective-history minimum. The existing 30/45 bps
tiers remain hard floors; tiers three through five have strictly increasing
floors and remain unavailable until history matures. Mature thresholds add the
configured 10 bps roundtrip-cost and 1 bps exit-target reserve and cannot move
below their floors. ETH and WTI must both be manually confirmed flat before the
first supervised start; the WTI manager runtime independently reconciles its
own venue positions and orders.

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

If the process stops after a confirmed Variational first-leg fill but before a
Lighter submission was persisted, restart reconciliation may submit only that
missing Lighter hedge leg. It requires the exact terminal Var fill, an account
snapshot showing precisely the Var-only exposure and no active orders, the same
verified exchange session, and a fresh sample. The durable pending action is
written before submission; a leg already marked submitted is only looked up and
is never sent again. If those checks fail, the position remains in manual
review. A terminal no-fill on the missing leg also stops in manual review; it
does not automatically retry that order.

For a Var-first close, an unknown Var receipt is never resubmitted. Recovery may
submit only the missing Lighter reduce-only close when a fresh post-submit
account snapshot proves Var is flat, the original Lighter quantity is unchanged,
and there are no active WTI orders; the exchange session, exact-size depth, and
slippage checks still apply. If Lighter was already marked submitted, it is only
looked up and is never sent again. If both venues are already flat with no active
orders, state may be reconciled flat from that account evidence, while PnL stays
unverified wherever final fill evidence is missing.

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

## Standalone WTI Runtime

`python tools/wti_live.py --mode observe` starts WTI collection and its separate
health file without enabling order submission. It owns the configured forwarder
ports (8766-8768 by default) and cannot share those listeners with another
process. Resolve a port conflict explicitly; do not kill an unknown process.
The read-only `tools/oil_swap_collector.py` also takes an OS-level exclusive
lock for its output directory; a second writer fails closed, and the lock is
released automatically when the process exits.

Live mode requires explicit entry, exit, and cost thresholds, the current
Lighter market ID, confirmation that local ETH state is flat, and explicit
pilot/order acknowledgements. By default it still requires the existing
three-session readiness gate: three qualified sessions, 1,200 valid samples
and six hours of coverage per session, at most 120 seconds between samples,
and at most 120 seconds of latest-sample age.

For a deliberately unqualified-history trial, add
`--allow-unqualified-data-pilot` together with
an explicit bounded-pilot confirmation. This waives only the historical
minimum of three sessions, 1,200 samples per session, and six hours of coverage
per session. It still requires at least one verified historical WTI session
with matching instrument identity and no in-session sample gap above 120
seconds. Every entry still requires a fresh valid two-venue sample, verified
open session, continuity within 120 seconds, exact instrument metadata,
reconciled positions and orders, and passing live account-risk checks. The
runner records the bypass scope in its durable pilot configuration and health
report. The readiness report remains unqualified and this mode is not
production-rotation approval.

The bounded pilot defaults to $20 notional and has a hard $25 maximum, an 8 bps
maximum execution boundary, a six-hour maximum hold, and a minimum 30-minute
pre-close entry cutoff. Account risk retains the existing 40/50/60/75 percent
margin thresholds and 0.82/0.74 cross-venue equity balance thresholds. These
are code guards, not evidence that the strategy is profitable.

The optional `--gradient-live --confirm-bounded-gradient-pilot` path is a
separate bounded diagnostic, not an averaging-down mode. It permits at most five
layers in one exchange session and one completed paired roundtrip. Tier two
retains its 45 bps hard floor; tiers three through five require mature rolling
history and higher thresholds. Each child is capped at $20. Cumulative paired
notional is capped at 1x through 5x the smaller fresh, verified venue equity;
missing, stale, or inconsistent equity blocks additions. The persisted pilot
config, attempt budget, exact-size fresh quotes, current session, account
reconciliation, live account-risk checks, exit logic, and slippage limits remain
enforced. An exit, risk block, session cutoff, sample gap, or execution
uncertainty prevents adding a layer. A completed or exhausted pilot cannot be
reset by restarting with altered settings. The history bypass flag, when used,
skips only historical session/sample/coverage counts; instrument identity,
verified-session and historical-gap evidence, real-time entry checks, live risk,
and production-rotation approval are not bypassed.

`tools/rotation_supervisor.py` coordinates the ETH/WTI handoff. Its default
`--rehearse` mode is read-only. Live supervision requires both explicit
automatic-rotation and live-order confirmations, plus a market ID and
multiplier. It persists a journal and uses a process lock so a restart adopts
the existing transition instead of issuing a duplicate start. The supervisor
requests a PID/run-ID-bound drain, waits for fresh paired-flat and zero-order
proof, then starts only the newly selected runtime. The historical production
gate remains separate from the current live session, account/order, quote,
depth, and risk gates enforced by the WTI runtime.

If a continuous-rotation cold start has no runtime and no verified exchange
session status, it does not wait on the collector that cannot yet start. After
`--confirm-initial-flat` and local flat-state checks, it starts WTI in
management-only mode to establish the live session feed and account proof. This
bootstrap cannot submit entries. Once the verified session is available, the
supervisor either drains to ETH for a closed session or upgrades WTI only after
a fresh dual-flat, zero-order proof. An interrupted bootstrap resumes from the
persisted journal and existing tmux session; recovery never relaunches an
entry-enabled runtime.

For a separately authorized one-round rotation test, both read-only scenarios
can be rehearsed first:

```sh
python tools/rotation_supervisor.py --rehearse --bounded-rotation-test --rehearse-session open --lighter-market-id 145 --lighter-multiplier 1
python tools/rotation_supervisor.py --rehearse --bounded-rotation-test --rehearse-session closed --lighter-market-id 145 --lighter-multiplier 1
```

These simulated session scenarios never write exchange orders. Open-session
planning may drain ETH before WTI; closed-session planning selects ETH when
ready and does not open WTI. A live test requires a separate explicit command:

```sh
python tools/rotation_supervisor.py --execute --bounded-rotation-test --confirm-bounded-rotation-test --confirm-automatic-rotation --i-understand-live-orders --lighter-market-id 145 --lighter-multiplier 1
```

If an older WTI runtime is active, first use the management-only takeover
command below so its next account reconciliation is bound to the new PID/run
ID; do not use it while ETH is active. If neither runtime is active, the
supervisor additionally requires `--confirm-initial-flat` after both accounts
and their orders have been manually verified flat.

The supervisor's separate bounded rotation test persists one test ID, exact
market/multiplier, and fixed budget: one completed round trip, at most two $10
layers ($20 total), 30/45 bps entry tiers, 1 bps exit target, 10 bps assumed
roundtrip cost, 8 bps slippage cap, six-hour maximum hold, and a 30-minute entry
cutoff. This two-layer automatic-rotation test is distinct from the five-tier
manual WTI gradient pilot above. The history qualification
bypass applies only to this explicitly confirmed test; it does not bypass
fresh valid samples, exchange-session status, account reconciliation, zero-order
proof, executable depth/size, slippage, or live risk checks. The test ID is
bound into the persisted gradient pilot. A restart resumes the same budget;
completion/exhaustion consumes it permanently. A management-only WTI runtime
is upgraded only after it is flat, has no pending action, and presents a fresh
account proof bound to its PID, run ID, and market ID. The supervisor requests
its normal verified drain before starting the entry-enabled test runtime.

While ETH is the active runtime, the supervisor adopts one fresh, PID-matched
read-only WTI collector or starts one under its own tmux session after verifying
the ETH command forwarder is listening. Before starting WTI, it waits for that
standalone collector to exit; it never kills an unowned collector. The WTI
runtime then owns collection. Returning to ETH restores the standalone
collector after the ETH runtime and forwarder are healthy. A completed or
exhausted bounded gradient pilot is not renewed automatically; that existing
one-round budget requires explicit operator authorization before another live
pilot.

If runtime identity, heartbeat, local state, account proof, or order status is
ambiguous, the supervisor blocks the handoff and requires review. An orphaned
position can only restart in manage-existing mode with entries disabled. A
closed-session WTI stop additionally requires a fresh read-only account probe
for the configured market ID with both venues flat and zero active orders. The
replay tool can emit input-file hashes, market identity, thresholds, readiness,
and replay results as an evidence JSON; this is audit evidence, not proof of
fills or permission to trade. Code tests and a successful read-only rehearsal
do not establish profitability or production readiness.

The gateway treats a Variational `cleared` order as filled with explicit
executed quantity and fill price. The observed orders-v2 format also supplies
`qty` and `price` as executed values when `clearing_status` is
`success_trades_booked_into_pool`; this requires a native order ID and a valid
execution timestamp at or after submission. Bare requested quantity and price,
failed clearing, and incomplete explicit fill fields do not establish a fill.
Lookup retains unique instrument, side, quantity, time, and available native-ID
matching. Lighter orders use
the pinned market precision and client order index. Unknown outcomes remain
pending for lookup; they are not resubmitted. Gross PnL is computed from
confirmed fills, but net PnL remains unverified unless both-venue fees and
funding are separately confirmed. The runtime writes
`log/wti_live_state.json` and `log/wti_live_risk_health.json`; the watchdog
monitors them independently from ETH. Code tests do not substitute for replay
against qualified real WTI data or venue-side verification.

When recovery must complete a confirmed Var-first open, its fresh market sample
requests the exact Var and Lighter quantities persisted in the pending action.
The collector measures executable Lighter depth and requests the Var quote at
those quantities; it does not resize the hedge from the collector's notional
sampling target. Existing freshness, session, market identity, depth, and
slippage checks still apply. A failed refresh records its specific validation
or collector error code in `manual_review_reason`, and no hedge order is sent
until the full original quantity passes those checks.

Lighter client indexes are restricted to `1..2^48-1`, as enforced by the native
signer ([constants](https://github.com/elliottech/lighter-go/blob/main/types/txtypes/constants.go),
[validation](https://github.com/elliottech/lighter-go/blob/main/types/txtypes/create_order.go)).
New requests persist their `sha256_48_v1` scheme and native index before sending.
Lookup preserves a saved index, including the original 63-bit mapping for legacy
requests; it never silently rehashes an already-submitted identity.

An unversioned legacy request whose original index exceeds that range cannot
have passed the native signer. Only this specific proven local rejection can
replace a missing Lighter hedge after the confirmed Var fill, exact live position
and zero-order reconciliation, original-session checks, exact-size fresh quotes,
and account risk checks. The old leg is archived atomically with the new request
before sending. An accepted, canceled, or ambiguous order is never retried by
this path. Absence from both order lists alone does not prove zero fill.

The original submission result and send transaction hash are retained separately
from later lookups. Health exposes `pending_order_diagnostics` with the failing
lookup stage and reason. SDK `CreateOrder` objects need not support `to_dict()`:
an accepted send receipt survives independently of that signed payload. Trade
history failures leave fees/trade IDs unverified without erasing an independently
confirmed full order fill. Recovery retains the original intent time for maximum
hold limits; restarting does not reset the exposure clock.

Execution session freshness uses the same validated subscription heartbeat as
the collector, retaining the 30-second maximum age. The last market-state-change
timestamp is retained as evidence, but an unchanged open state does not require
repeated state-change messages. Missing/disconnected status feeds, stale or
future heartbeats, and unverified session sources still block execution.

## Readiness Audit

For Linux management takeover, use
`python tools/wti_takeover.py --execute --confirm-eth-flat --i-understand-live-orders`.
Without `--execute` the command only reports local state and process identities.
The helper recognizes both relative and absolute Python command paths, excludes
tmux wrapper processes, accepts zero or one existing WTI runtime, and refuses
multiple runtimes or a running ETH strategy. It sends only SIGTERM and refuses
to force-kill a runtime that has not exited. A nonblocking lock serializes
takeovers, and occupied forwarder ports block a new start.

The WTI runtime owns its forwarder and collector. Stopping it also closes
8766-8768, so an account probe cannot verify the account in the interval between
stop and restart. The helper starts the management runtime first; that runtime
then reconnects the extension, performs fresh account/order reconciliation, and
applies the existing execution guards. It does not delete state or submit orders
itself. Success requires a new PID/run heartbeat, reconciled flat or open state,
no pending action, entry authorization disabled, and fresh collector samples
from the same process while the market is open. During a confirmed closed
session, the management runtime uses fresh account-only reconciliation and the
live market-status heartbeat instead; it accepts only a resolved flat/open local
state with matching venue quantities and zero active orders. It refreshes that
proof periodically, keeps entries disabled, and never resolves a pending order
without normal sample-based evidence. A timeout prints the current blocking
diagnostics and leaves the new runtime running; it does not repeatedly restart
or resubmit.

Explicit existing-position management also accepts an already-flat local state
so that recovery is safe to repeat after a completed close. It never initializes
a new entry pilot. After a completed/exhausted pilot is flat, management mode
keeps the collector and forwarder alive with entries disabled. The separate
bounded-entry pilot retains its existing stop-after-one-round behavior.

After a reconciled completed/exhausted test, explicitly authorize one more round
with `python tools/wti_takeover.py --execute --start-pilot --confirm-eth-flat --i-understand-live-orders`.
This preserves the previous pilot in `single_round_pilot_history` and all prior
execution records. The new runtime receives `--new-pilot-after` with that exact
previous pilot ID; restarting with the same ID resumes the same new budget and
cannot renew it after completion. A pending, open, or nonterminal prior round
blocks this operation. The new test uses the same 20 USD cap, 30 bps entry,
1 bps exit target, 10 bps assumed roundtrip cost and 8 bps slippage cap. Historical
data qualification is explicitly bypassed for this single test only; fresh
account, orders, executable quantity/depth, session and risk checks still apply.
`--keep-collecting-after-pilot` keeps collection alive after its entry budget
finishes, with further entries disabled.

Run `python tools/oil_swap_readiness.py` against the WTI collector data. For a
production-promotion audit, provide both `--lighter-market-id` and
`--lighter-exposure-multiplier` using the current Lighter market metadata, never
values inferred from historical samples. The JSON
report separates historical `market_data_ready`, recent `data_fresh`,
`ready_for_bounded_live_review`, and `historical_promotion_evidence_ready`.
`ready_for_production_rotation` reports only whether strict historical
promotion evidence passes; it cannot verify live order adapters, account state,
or the current exchange session. The runtime checks those independently before
enabling entries. The command returns exit code 0 when bounded-live review is
ready, not when production rotation is ready. Neither result submits or enables
orders. Historical samples without
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

## Continuous Gradient Rotation

The automatic supervisor can run sequential, small-notional WTI gradient rounds
and rotate to ETH when the verified WTI session closes. Enable it explicitly
with `--continuous-gradient-rotation --confirm-continuous-gradient-rotation`
alongside the normal automatic-rotation/live-order confirmations. Its durable
session ID and round counter are stored in `rotation_supervisor_state.json`;
restarting the supervisor resumes that same authorization rather than creating
a fresh budget or submitting an order. A WTI round remains one completed
open/close cycle. The next round is armed only after the runtime confirms a
fresh dual-account zero-position, zero-active-order reconciliation. Pending,
partial, ambiguous, mismatched, or stale state prevents re-arming. The supervisor
does not start a second WTI runtime while the existing round is healthy.

Use `--restart-supervisor` with the normal `--execute` and authorization flags
to replace an older supervisor. It resolves Python script paths through `/proc`,
accepts relative or absolute Python paths, excludes tmux wrappers and restart
helpers, and terminates only the verified supervisor through a Linux pidfd.
Updated supervisors finish their current persisted transition before exiting on SIGTERM.
The replacement must publish a fresh heartbeat with its own PID and instance ID
before takeover is reported ready. Worker processes, rotation authorization,
pending-order identities, and journals are preserved.

If an interrupted ETH start remains in the journal while the live session is
unknown, the supervisor waits for its original tmux session and any starting
worker. Once both are absent, explicit initial-flat confirmation and flat local
states permit a WTI management-only bootstrap. The superseded ETH launch is
retained in `superseded_starts`; the WTI launch and its audit record are written
atomically before starting the process. A subsequent restart reuses that WTI
launch intent. Current exchange-session, account, order, and entry checks still
control the eventual trading runtime.

WTI drain receipts now emit both venue zero-order fields expected by the
supervisor. Legacy WTI receipts remain usable only with exact request/PID/run
identity, zero paired positions, an integer verified total order count of zero,
and an explicit reconciliation timestamp within 20 seconds of completion.
Partial or contradictory venue-order fields cannot use that compatibility path.

When no trading runtime exists and its former session connection is gone, the
supervisor can independently read the public USOILP market-status websocket.
A fresh closed observation still uses the existing 30-second session limit;
an open observation cannot establish WTI entry readiness without the existing
schedule and valid-sample checks. Failed status reads leave a completed drain
waiting for live confirmation instead of repeatedly restarting the bootstrap.
After a verified ETH drain and source-process exit, the supervisor gracefully
stops only the identified read-only standalone collector, then waits for its
exit before launching WTI's embedded collector and execution runtime.

The WTI manual gradient pilot uses five increasing directional thresholds. The
first two hard floors remain 30 bps and 45 bps; tiers three through five have
strictly higher floors and remain unavailable until the same rolling-history
sample-count and effective-time gates mature. Mature thresholds use the existing
conservative roundtrip-cost and exit-target reserve at the 97.5, 98.25, 99, 99.5,
and 99.8 percentiles. No history, session, freshness, risk, depth, reconciliation,
or slippage gate is relaxed.

Each child order remains capped at 20 USD. The paired open-position ceiling is
the lower fresh verified equity of Variational and Lighter multiplied by at most
5, with cumulative tier budgets at 1x through 5x that lower equity. Missing,
stale, nonpositive, or mismatched equity blocks new exposure. Existing inventory
is marked at the current conservative ask; proposed orders use conservative
references padded by maximum slippage. Each new child requires a fresh two-venue
risk read, exact local/exchange position reconciliation, and durable pending
intent before submission. After restart an unresolved intent is looked up and
reconciled; it is never submitted again. Legacy positions under an old tier
budget remain manageable but cannot receive additional layers.

The 1 bps net exit target after the configured 10 bps roundtrip-cost assumption,
8 bps maximum slippage, six-hour maximum hold, and 30-minute pre-close entry
cutoff remain unchanged. A pilot still ends after one completed open/close round.
This change does not start the rotation supervisor or authorize automatic
ETH/WTI switching. Historical replay requires an explicit equity scenario because
the archived samples do not contain verified account-equity snapshots; replay is
indicative only and cannot authorize live orders.

Any separately authorized continuous mode intentionally has no maximum number
of completed rounds or cumulative session-loss cap. The five-times-equity ceiling
limits simultaneous paired WTI exposure, not cumulative turnover or realized
losses. Do not start continuous mode as part of a manual WTI switch. The
indicative replay supports explicit tier-count and equity-scenario inputs; it
remains a no-lookahead simulation, not fill or profitability proof, and never
enables orders. Default replay behavior remains limited to one round.

## Indicative Quote-Path Replay

`python tools/wti_strategy_replay.py` replays a user-specified entry edge,
net exit target, roundtrip cost assumption, maximum hold, and maximum sample
gap against saved WTI samples. It uses only the current and prior sample at
each decision point, rejects samples without explicit fresh session evidence,
duplicate/conflicting sample IDs and mixed Lighter market profiles, stops at an
gap above the configured live limit latches entry authorization off for the
remainder of that replay process, matching the live runtime; a session change
does not by itself prove a reconciled runtime restart. For a supervisor-rotation
scenario, `--assume-verified-flat-session-restarts` explicitly models a new WTI
process after independently verified dual-account flat/order reconciliation;
the quote archive itself does not prove that reconciliation. It blocks entry during the
last 30 minutes before reported close, and marks any position crossing a session
boundary unresolved rather than inventing an exit. A long interval is not
treated as closed-market time unless the prior close and next open are both
explicitly evidenced. The replay uses recorded executable quote proxies but
does not establish fills, fees, funding, roll cashflows, or live latency. Its report always has
`live_authorization: false`; do not use this as production promotion evidence.

Lighter order-fee lookup uses the supported descending trades API and only
confirms a fee when exact order/account/market identities and trade quantities
cover the full terminal fill, with each fee explicitly denominated in USD.
Protocol fee integers with undocumented units remain unverified rather than
being converted by guesswork. See the [Lighter trades API](https://apidocs.lighter.xyz/reference/trades).

Unknown positions while no active pair is recorded force reconciliation and
block new entries. The supervisor and runtime regression tests use synthetic
venue responses; they do not verify live credentials or establish realized
fees/funding. Production qualification remains separate from the explicit
historical-data pilot path described above.

## Booked Variational Order Recovery

For a separately authorized manual five-tier round, use the takeover command
with `--start-gradient-pilot-after` and the exact confirmed terminal gradient
pilot ID. It is separate from `--start-pilot` (single round without gradient)
and never enables the rotation supervisor or continuous rounds. Keep this
predecessor ID fixed when retrying; do not replace it with the newly created
pilot ID. The runtime first reconciles its own two accounts and active orders,
checks live risk/session/freshness and the unchanged configuration, then
atomically archives the terminal predecessor and creates one successor. The
historical-volume exemption remains the existing explicitly bounded pilot mode;
real-time guards and higher-tier history maturity gates are unchanged.

Repeated requests adopt that same successor and preserve submitted order intents.
An already running process with the same request is never restarted by a repeat
command, even while it is waiting for reconciliation. A completed successor is
not renewed by repeating the old request. After completion the runtime keeps
collecting with entries disabled until another separately authorized request.

Management-only takeover never selects the gradient entry strategy, calculates
entry capacity, or arms another round from a saved gradient record. Completed
and exhausted pilots remain historical evidence only. Existing positions still
use the ordinary exit decision and exact-quantity execution/reconciliation path.
Missing or invalid verified equity capacity blocks gradient entries; it cannot
fall back to the legacy aggregate budget. The executor enforces its per-child
notional limit independently of the aggregate equity budget, including quantity
rounding and the existing slippage padding. Replay uses the same sizing cap
(`--max-order-notional-usd`, default 20 USD).

Takeover success requires a current-PID/current-run account proof matching the
local position with no active orders, plus at least one successful non-startup
strategy cycle (or closed-market account-only cycle). Runtime exceptions,
permanently revoked authorization, incomplete/stale proofs and blocked cycles
cannot be reported as recovered merely because a process remains alive.

An orders-v2 `cleared` record with `success_trades_booked_into_pool` may have an
`execution_timestamp` slightly earlier than the local submission clock. Such a
record is accepted only when a persisted native RFQ/order ID matches exactly,
the order is unique, instrument/side/quantity match, and `created_at` falls
between submission and the current time. The execution clock lead must stay
within the existing gateway operation timeout; missing, future or older execution
timestamps remain unconfirmed. Requests without an exact native identity retain
the original execution-time lower bound. This does not relax market freshness,
session, account-risk, size, slippage or position-reconciliation gates.

The persisted result records the clock lead and its evidence source. Unknown
lookups now include a specific failure reason, including missing/ambiguous
records, response errors, incomplete clearing or inconsistent time evidence.
Use management-only takeover for an interrupted layer: it re-queries the existing
Var order, verifies both accounts and zero active orders, and only then submits
the provably missing Lighter hedge. It does not re-submit the Var order or
authorize further gradient entries. A lost hedge acknowledgement is looked up
on the next restart, never blindly repeated. No state/history file is cleared.
