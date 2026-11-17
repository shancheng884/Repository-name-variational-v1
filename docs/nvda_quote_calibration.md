# NVDA source-time calibration and execution details

The five thresholds, two-of-three confirmation, 20 USD children, five-times
equity cap, direction lock and exception recovery are unchanged. No priority
fee, maker order, extra RFQ, pre-submit REST request or fixed waiting period is
introduced.

## Quote time

The strategy's hub keeps a bounded source-time index of already-received books
and BBOs. For a pair of latest quotes, the common time is the older source
timestamp. Each leg is looked up at or before that timestamp, with a receive
time no later than the decision. This is an observed historical synchronous
spread, not a prediction of the current Entropy price.

The 30 minute baseline is sampled once per second from these aligned spreads.
The same source versions cannot count twice toward the unchanged two-of-three
confirmation. Both the aligned current deviation and the latest raw deviation
must clear the same existing threshold; the raw quote is never replaced by a
historical or extrapolated limit price. Orders still use current raw BBOs.

On first upgrade, the aligned baseline is replayed from the existing per-quote
files in receive order, without looking at future arrivals or filling gaps.
Subsequent restarts restore its separate aligned history. Old raw history is
never relabelled as aligned. Existing legacy positions keep their original
frozen anchor and raw exit semantics; new entries resume on the calibrated
basis once those legacy positions are flat. No position is flattened merely
to migrate signal basis.

## Fill details

Entropy's filled acknowledgement is parsed and validated by order ID, price
and quantity. A read-only userFills stream provides fee and exchange fill time;
REST is a concurrent fallback rather than the sole poll-based confirmation.
Duplicate snapshots and split fills are deduplicated by trade identity and
aggregated by exchange order ID. ACK time is never invented as exchange fill
time, and missing fees are not assumed to be zero.

Late actual fees update open children without a network query. If a position
has already closed, a persisted pending-cost item is finalized exactly once
when complete actual fee data arrives. The expected 110% Entropy rebate is
credited separately once, not deducted twice from costs or treated as paid cash.
Unresolved execution identities still retain the existing stop/recovery rules.

## Depth audit

Only the strategy enables fast five-level Entropy L2 and half-second book
snapshots; the independent capture service is not restarted or reconfigured.
BBO size is recorded. Each submitted child keeps source/receive times, raw and
aligned quotes, confirmation counts, top-level coverage for its base quantity,
five depth levels and visible VWAP. Book and BBO source timestamps are retained
separately; old depth is not represented as fresh BBO depth. These are audit
fields, not additional entry filters.

The alignment tape is under
`log/nvda_tier_live_capture/quote_alignment/strategy/NVDA/`.
Order receipts keep `time_alignment`, `depth_audit`, `confirmation_source` and
`fill_details_source`. The state exposes stream connectivity and raw versus
aligned edges. These changes do not imply the measured IOC execution delay or
adverse selection has disappeared; real P&L must still be evaluated separately.
