# Frozen Exit-Convergence Pilot

This explicit experimental entry policy is not the default WTI policy. It is
not an arbitrage or profitability guarantee and does not enable rotation.

The fixed model uses one-hour, same-quantity forward exit-gap changes from
completed past observations. Labels cannot cross sessions, a gap over 120s,
the session cutoff, or the purged training boundary. Features use only prior
30-minute observations. Each supported cell requires three sessions, 100 raw
labels and six non-overlapping outcomes. Per-session P75 exit changes are
combined by their median, rather than the prior worst-session P95.

The entry hurdle is predicted executable exit gap plus the unchanged 10bps
roundtrip reserve and 1bps net target. Configured tier floors, tier spacing,
two-of-three/linked-refresh confirmation, 2s quote/book freshness, 1.5bps pairing,
account risk, sizing and persisted order recovery remain independent gates.
Missing predictions do not fall back to a lower entry floor.

The same policy function supplies offline schedules and live thresholds. Two
completed sessions are held out. Both control and candidate replay arms have
the same total paired-notional cap of USD20 and one completed round per
independent simulated flat-session start. Historical quote paths are not
exchange fills or account reconciliation proofs.

Live opt-in requires all three flags:

```text
--experimental-exit-model MODEL.json
--experimental-exit-model-sha256 EXACT_FILE_SHA256
--confirm-experimental-exit-model
```

The model also pins the existing configuration. Rotation and management-only
combinations are rejected. The test caps every tier's aggregate notional at
USD20; it does not expand the original five-tier capacity. The existing pilot
ID, state and pending orders are retained on restart. No new round is silently
created. The model expires 24 hours after creation: new entries stop, while
existing-position exits and reconciliation remain available. A restart with
the same file/hash cannot renew expiry or reset the trading round.

`--confirm-exploratory-entry` is a separate, additional opt-in for a USD20
single-round execution experiment when model-qualified signals are absent.
It uses the configured 30/45bps tier floors plus the unchanged tier spacing
and maturity constraints, not the model's profitability filter. Health/audit
explicitly records `model_would_qualify=false` where applicable. It does not
claim positive expectancy. All capture, account, exposure, session, confirmation,
pending-order, expiry and exit-cost gates still apply. This option must not be
enabled merely because an offline model produced no trades; it requires the
operator to choose exploratory live execution explicitly.

## Bounded Manual WTI Rounds

Exploratory testing may additionally opt into `--manual-wti-trial-id ID`,
`--manual-wti-root-pilot ROOT_ID`, and `--manual-wti-max-rounds 3`. This is
at most three sequential WTI rounds, not asset rotation. The USD20 aggregate
cap applies throughout. A persisted trial binds its exact root, configuration,
model hash and round budget; changing any of them on restart is rejected.

A next round requires no pending action or exposure and a new, current-runtime
dual-account flat/order proof. The transition intent is persisted before the
existing idempotent manual-pilot transition. Restart recovery finalizes that
same successor, never creates an additional round. Completed rounds remain
counted across process restarts. After the third completed round the runtime
keeps collecting but stops opening. Unknown orders, account failures, model
expiry and existing runtime locks do not authorize another round.
