# RWA two-venue probe (20 USDC per leg)

This tool is separate from the read-only capture service. It is used to measure
the real price path, quote-to-fill slippage and post-fill adverse move for each
same-symbol venue pair.

## Instruments

Default: `NVDA`, `SNDK`, `CRCL`, `AAPL`, `QQQ` on
`variational`, `lighter_mainnet`, `lighter_rh`, `entropy`.

The first four are available on all four venues. QQQ is present on Var and both
Lighter books. Every asset leg is 20 USDC notional. The plan contains both
directions for every available venue pair: 54 rounds for the default set.

## Hard gates

```bash
# 1. Plan only; no network/order calls
.venv/bin/python tools/venue_probe.py --mode plan

# 2. Read-only account/market preflight; no orders
.venv/bin/python tools/venue_probe.py --mode preflight

# 3. Live, only after explicit confirmation and all preflight keys ok
.venv/bin/python tools/venue_probe.py --mode run   --execute --confirm-live-probes --confirm-flat --i-understand-live-orders
```

Before entry, both venue account positions and active orders must be verified
flat. A round writes its deterministic client order ids to
`log/venue_probe/state.json` before sending. Unknown terminal status moves the
run to `manual_review_required`; it never retries the same round. If one leg
fills and the other does not, the filled leg is closed reduce-only once.

### Per-round position reconciliation

Every round is bracketed by a read-only position/order snapshot on both legs
(`variational` positions + pending orders, Lighter `account` +
`account_active_orders`, Entropy `user_state` + `open_orders`):

- the pre-check must prove `qty == 0` and `active_orders == 0`; an unverified
  snapshot blocks the round with `pre_round_position_not_flat` and no order is
  sent;
- the post-check runs after the exit legs; any residual quantity turns the
  round into `manual_review_required` with `post_round_position_not_flat` and
  the rest of the plan stops.

The snapshot is stored on the round as `reconcile.pre` / `reconcile.post`, so a
later review can see exactly which venue reported which quantity.

### Single-run lock

`log/venue_probe/run.lock` holds `{pid, run_id, acquired_at}` for the live run.
A second process returns `probe_already_running` with the current owner and
sends no orders. A lock left behind by a dead pid is replaced automatically;
the lock is released in a `finally` block, including after exceptions.

The runner records entry quotes, order submit/ack timestamps, final fills,
0.25/0.5/1/3-second adverse marks, and both exit fills. Fees are reported
separately.

## Cost decomposition

`tools/probe_report.py` computes, per leg:

- half-spread paid against the contemporaneous mid;
- fill slippage against the contemporaneous executable BBO;
- entry and exit price cost without double counting;
- gross PnL in USDC and bps; fees separately;
- per-asset/venue p50/p80 and a suggested trigger floor.

```bash
.venv/bin/python tools/probe_report.py
```

The suggestion is research output. It does not change live thresholds.

## Entropy

Entropy quotes and public fills are captured. Live Entropy orders are blocked
until a trading signer private key is provisioned; `ENTROPY_ACCOUNT_ADDRESS`
alone is only sufficient for read-only `userFills`.

## Recovery between runs

`log/venue_probe/state.json` is scoped to one run id. A new run starts with a
fresh cursor; it never inherits the finished cursor of an earlier run. Client
order ids are prefixed with a token derived from the run id, so round 2 cannot
collide with round 1 ids at any venue.

If the previous run left an unresolved round (`current` with a non-terminal
status, or `phase=manual_review_required`), a new live run refuses to start and
returns `unresolved_round_requires_review` with that round id. Reconcile both
venue accounts and clear the round before re-running; the runner will not place
any order while that state is present.

A round that produces no usable quote, or whose size falls under a venue
minimum, is recorded as `skipped` with a reason and the run continues. Skips
never place orders. Only an unknown order status escalates to
`manual_review_required`.

## Instrumentation

Each leg records `submitted_at`, `acked_at`, `completed_at`, the venue fill
timestamp (`source_at` / `source_at_epoch`) and the derived
`submit_to_ack_ms`, `submit_to_complete_ms`, `ack_to_complete_ms` and
`exchange_fill_to_local_ms`. `probe_report.py` aggregates the signed hedge
landing delay (`hedge_delay_ms`) from the two entry fills.

### Fill-time alignment

Venue clocks are not the local clock. For each leg the execution quote echo
(`received_at` vs `quote_timestamp`, or `quote_recv_at` vs `quote_source_at`)
gives a per-venue offset; the leg keeps `clock_offset_ms`, and the fill is
re-based onto the local clock as `aligned_fill_at`. `hedge_delay_ms` is
computed from aligned exchange fills (`hedge_delay_source=exchange_aligned`),
falls back to raw exchange stamps (`exchange_raw`) or local completion stamps
(`local_completed`), and never mixes sources without saying so. A fill stamped
before local submit with no offset available is flagged
`timestamp_anomaly=true`. Lighter timestamps are accepted in seconds,
milliseconds, microseconds or nanoseconds.

## Cost attribution

Only complete four-leg rounds enter the cost distributions. Rolled-back,
partial and skipped rounds are counted in `excluded_rounds` and never averaged
into per-venue or per-pair cost.

Per-venue cost uses that venue's own entry and exit legs; it is never half of
the round total. Each leg is split into half-spread, `signal -> execution
quote` (RFQ reprice where a venue publishes one) and `execution quote -> fill`.
The three stages add up to `price_cost_bps`, so nothing is charged twice.
Lighter and Var now capture an `execution_quote` at submit, so their legs are
`three_stage` like Var RFQ legs instead of collapsing into a single
`signal -> fill` number.

Fees stay outside the price cost. A leg with a reported `fee_usd` is
`verified` (Entropy rebates arrive as a negative fee and reduce `net_cost_bps`);
a leg on a zero-fee venue (`variational`, `lighter_mainnet`, `lighter_rh`)
without a fee field is `zero_by_policy`; anything else is `unverified` and
suppresses `net_cost_bps` rather than contributing a silent zero.

The actionable floor is `recommended_pair_trigger_bps` (p80 of the pair's
total round-trip cost plus the target net). `recommended_trigger_bps` remains
the per-venue round-trip view used for attribution only.
