# WTI Decision Evidence

This is an audit trail, never an order outbox, trading state, retry queue,
authorization source, or recovery instruction. Existing entry thresholds,
two-second checks, 120-second abnormal-gap handling, account checks, sizing,
confirmation, execution calls and retry behavior are unchanged.

## Two Evidence Levels

`runtime_exact` events are emitted by the instrumented process after its next
planned normal restart. Deploying Python files does NOT inject these hooks into
an already-running interpreter. Normal parser startup enables them; the
`--disable-decision-audit` option only disables auditing, not risk controls.

Each strategy/startup cycle has one `trace_id` containing the trading run,
input sample ID and a sequence. Evidence includes:

1. Input quote identity, captured prices/ages and quality.
2. Actual directional five-tier thresholds and crossed tiers.
3. Refresh request/result/error, linked signal/refresh IDs and freshness checks.
4. Signal and execution confirmation, counts, mode and explicit rejection.
5. Account reconciliation timestamp/quantities/orders, source run/PID and gates.
6. Pre-submission gate and the existing executor invocation.
7. Newly observed persisted intent/execution IDs and per-leg submission/fill
   evidence, or an explicit unknown after an exception. An executor call is
   not claimed to be an exchange submission or a fill.
8. End state, final decision or exception type. Cancellation propagates normally.

Admission failures before a strategy cycle and abnormal-gap latches use an
`ingest:<sample_id>` trace, linked by that same sample ID rather than inventing
a strategy evaluation that did not run. Pending/recovered executions remain
linked across cycles by their persisted action IDs.

`observed_only` events are produced by the isolated file observer immediately,
without restarting the trading runtime. It reads current source health, state
and newly appended baseline/burst samples, with stable observation hashes and
persisted source offsets. Source files are never written. Sample IDs and
timestamps link observed evidence; unavailable internal confirmation/gate
sequence is explicitly UNKNOWN, never reconstructed as a passed decision.
Initial historical state/tail context is not labelled as a new trade.

`high_frequency_shadow_window` is emitted by the live process every second,
aggregating read-only snapshots of its in-memory Lighter book sampled every
200ms against the latest baseline Variational quote. It makes no additional
API/forwarder requests and is never passed to strategy, confirmation, sizing,
or execution. An edge is scored only when the existing quote-age and session
checks pass, the current book is fresh/continuous, and paired-book drift remains
within 1.5bps. Each window records extrema, tier-crossing observations, source
sample IDs, nonces, and the largest sampling gap. It cannot prove movements
faster than the polling interval; unscored/stale windows are retained as such.

Execution refresh retries are limited to three attempts and a two-second total
deadline. Only quote-age, stale-book, or paired-book-movement failures are
retryable. Session, sequence continuity, resync, clock-drift, slow-quote, and
all other failures remain blocked; every retry must independently pass the
unchanged two-second and 1.5bps checks before the existing strategy can use it.

`observer.status.json` reports `exact_runtime_audit_active=false` and
`exact_hooks_pending_planned_runtime_restart` until matching-run exact hooks
actually appear in the trading health. An alive observer alone does not prove
that the full internal chain is active.

## Isolation and Bounded Writes

The trading thread only captures bounded safe fields and uses `put_nowait`.
Disk writes/rotation occur on an independent daemon thread. Queue saturation,
IO failure and lost audit records are exposed by counters and sequence gaps;
they do not cause order retries, a new order, or a changed trading decision.
Records are best-effort flushed, not synchronously fsynced by the trading
thread. An abrupt crash can leave incomplete cycles. Observer cursor commits
mean queued observations, not guaranteed durable journal commits; crash gaps
must not be interpreted as complete coverage.

Each journal has a 10 MiB current file and seven backups (roughly 80 MiB maximum
per writer, not seven days of retention). Runtime and observer journals use
separate files and locks; JSON records are never split at rotation. Audit health
is separate from trading health/state. Headers, credentials, private keys and
raw exception responses are excluded/redacted.

The observer unit is low-priority with a 10% CPU quota, 128 MiB memory limit,
idle IO priority and no Internet address families. `ProtectSystem=strict`
permits writes ONLY to `log/wti_decision_audit`. It cannot modify strategy,
state, collector or configuration files and never loads account credentials.

No audit reader or writer contains an exchange order endpoint. Journal replay
is not supported as a trading action. A planned restart must use the existing
safe takeover procedure; this audit deployment must not restart the runtime,
collector, watchdog, or automatic rotation.
