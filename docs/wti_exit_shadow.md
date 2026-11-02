# Frozen Exit-Basis Model Research

Run `tools/wti_exit_shadow.py --snapshot-log-root <frozen-copy-of-log>`.
The live repository's own `log/` is refused. There are no network, gateway,
notification, deployment, state-write, or order operations in this entry point.
Output goes to stdout; a caller may save it as a separate research artifact.

## Registered Model

Three fixed horizons: 1, 3 and 6 hours. No winning horizon is selected from
holdout performance. Models predict the signed change from entry basis to
future executable exit basis, rather than treating the entire standing basis
as recoverable profit. The upper bound is the worst training-session P95.

Features are direction, exchange-session phase (first 6 hours / next 10 hours /
remaining hours), and expansion above the prior-only 30-minute median (below,
0-5, 5-15, or at least 15 bps). History resets across sessions and abnormal gaps.
Sparse cells back off to state-only, then direction-only. They never borrow
future prices, reverse-direction evidence or a zero-cost assumption.

Each supported cell needs at least 100 labels, three training sessions and six
non-overlapping labels. Overlapping labels are explicitly not counted as
independent evidence. Even supported cells are research models, not validated
profitability or a calibrated 95% guarantee.

Labels require strictly audited two-second captured quotes, matching paired
quantities and an observed endpoint within 20 seconds of the fixed horizon.
They never interpolate, cross a session, bridge a valid-quote gap exceeding
120 seconds, or extend through the 30-minute session entry cutoff. Missing
quotes/sizes are rejected with counts. Short invalid captures do not invent
prices: a later observed quote is usable only within the unchanged gap bound.

## Comparison and Safety

- At least three closed exchange sessions train the frozen model; the last
  two independent completed sessions are holdouts. Every training label must
  end before holdout start minus the 120-second embargo. Checking the label
  end automatically purges its whole forward horizon, up to six hours.
- If only four completed sessions exist, the three-train/one-holdout output is
  explicitly exploratory. The two-holdout acceptance gate remains failed.
  An ongoing session cannot satisfy it.
- Control and model arms reuse the same five-tier replay engine, confirmations,
  existing depth availability checks, two-second capture limit, 120-second gap
  protection, child-order cap, equity tiers, exit target and exit controls.
- The model's midpoint-PnL units are converted to the original price-relative
  edge units. The existing configured floors and adaptive tier spacings remain
  lower bounds. Only research entry schedules differ; missing model evidence
  blocks entries instead of reverting to an easier threshold.
- The fixed 10 bps cost reserve is unchanged and is charged once to replay
  gross PnL. Quotes do not prove actual fees, funding, latency, fills or profit.
- Capacity is an explicitly frozen smaller-venue current-equity scenario, not
  historical account evidence. Sessions are independent simulated flat-start
  scenarios, not proofs of real exchange positions or successful handovers.
  Unresolved positions remain flagged, with their PnL unknown.
- Closed-trade PnL, worst closed return, forced exits, hold time, model coverage
  and signal counts are reported. No path drawdown is claimed from incomplete
  full-position/fill evidence. A larger count of correlated signal captures is
  not a count of independent profitable trades.

The research schedule argument in `replay_wti_gradient` is optional and used
only by offline research. Default production-style replay logic is unchanged;
neither the live runtime nor a live threshold/configuration file is edited.
All outputs retain `eligible_for_live=false` and `actual_pnl_usd=null`.

## Frozen 2026-10-29 Result

The input archive SHA256 is
`2c85c539fb82d7536bde539f7552e7cb50acfe297d2c8bdefe983f2a48927d81`.
The source health snapshot is 2026-10-29 11:58:05 UTC. Strict admission leaves
four completed sessions, so the registered three-train/two-holdout gate FAILS.
The only available unseen completed session (2026-10-27 22:00 UTC through
2026-10-28 21:00 UTC) is reported as exploratory, not accepted validation.

- Control: zero candidate crossings and zero completed trades.
- One-hour model: 10,970 overlapping training labels, 14 supported cells;
  zero additional candidate crossings and zero completed trades. Aggregate
  direction-only conservative exit deltas are +5.9546 bps (long Var) and
  +4.6446 bps (short Var), not a promised contraction covering the 11 bps
  budget-plus-target requirement.
- Three-hour model: 5,324 overlapping training labels, 10 supported cells;
  zero additional crossings and zero trades. Aggregate direction-only bounds
  are +7.1424 / +6.9728 bps.
- Six-hour model: 2,002 labels across directions, but each direction has only
  five non-overlapping labels, below the predeclared six-label minimum.
  No supported model or entries are produced; this is not evidence of zero
  underlying opportunities.
- There are no unresolved simulated positions. No actual PnL or path-drawdown
  validation is claimed. One-hour calibration has only 19 non-overlapping
  labels per direction from a single holdout session; three-hour has five.

Conclusion: this registered conservative model does not demonstrate more
qualified entries on the available unseen session. This does not exclude
shorter-lived intrahour opportunities or justify lowering live thresholds.
At least one further completed holdout is missing, and six-hour training
coverage still needs additional independent matched-quantity observations.
