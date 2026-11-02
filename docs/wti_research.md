# WTI Read-Only Research

`tools/wti_research_audit.py --shadow` prints a JSON report without modifying
execution state, risk settings, or production thresholds. It must never be used
as a live-order authorization. No supervisor or automatic rotation is started.

The report explains each tier using the unchanged production threshold builder:
configured floors, seven-day quantiles plus the existing cost/target reserve,
24-hour P95, and previous-tier spacing. The strict audit additionally checks
capture-time two-second quote/book ages, price-derived edges, instrument and
market identity, session evidence, hedge quantities, duplicate/conflicting IDs,
and ambiguous timestamps. Audit exclusions do not remove production samples.

For exact USD fees, run `tools/wti_fee_audit.py` separately and provide its JSON
output through `--fee-evidence`. The audit only reads persisted orders; it never
submits, cancels, retries orders, or writes execution state. Native order IDs,
fill quantities/prices, terminal states and explicit fee units must agree.
Missing exchange fee or funding evidence remains unknown. Observed slippage is
already included in final-fill gross PnL and is not subtracted a second time.
The live cost budget remains unchanged regardless of the research result.

Shadow validation reserves two completed sessions after at least three training
sessions. Fixed one-hour exit labels must finish before the holdout boundary
minus a 120-second embargo, inside a continuous valid segment. Forecasts use a
frozen training P90 of executable-side exit spreads normalized to entry paired
notional. There is no tuning on holdout returns. Overlapping labels are not
independent observations; minimum label counts alone cannot prove reliability.

The matched control and model replay share a single slot bounded by the smaller
of the existing per-order and layer limits, two-of-three signal confirmation,
the existing cost/target, maximum hold and close-entry cutoff. This intentionally
is not a full five-tier portfolio simulation. Matching quote quantities are
required; absent exact-size exit quotes or a data/session boundary cannot be
assumed to flatten a position. An unresolved position remains reported and the
replay stops. Invalid baselines and unexplained gaps over 120 seconds are barriers.

These are indicative quote-path results, not exchange fills. Historical burst
execution confirmation, actual fees/funding, and independent final-fill outcomes
are not inferred. More shadow signals do not demonstrate safe live profitability.
Reports retain input/training hashes and never mark the model live-eligible.
