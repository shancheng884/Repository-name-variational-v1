# WTI Entry Diagnostics and Shadow Replay

`tools/wti_entry_audit.py` prints a read-only supplemental health report.
It does not replace `wti_live_risk_health.json`, import a live order gateway,
modify a trading state, or grant order authorization. Running processes need
not be restarted. The existing raw health continues to feed the watchdog.

- `strategy_entry_eligible`: observed policy checks are clear; not permission
  to submit an order. Unknown or stale health fails closed.
- `quote_fresh_now`: quote and book ages recomputed at observation time, with
  the existing two-second bound. No lifetime extension is permitted.
- `observed_live_gate_open`: also requires the raw live authorization and
  its absolute expiry. Raw False is never promoted to True.
- `entry_phase`: policy blocked, awaiting fresh quotes, unavailable raw
  authorization, or authorized while waiting for a signal.
- `refresh`: transport error, attempt age, captured burst quality and the
  particular failing leg/age. Captured evidence is not a new live quote.

The report is a snapshot, not an automatically updating daemon. It is kept
separate so old diagnostic files cannot override risk checks or generate
new trading/watchdog side effects.

## Offline Shadow

`--shadow` reads the saved state configuration and evaluates the last completed
exchange session. Only earlier baseline samples warm the adaptive threshold;
current/future/burst training rows are rejected by the replay engine. No
threshold, cost budget, two-second capture check, 120-second abnormal-gap
gate, per-child limit, tier count, round limit, or confirmation is lowered.

Historical account risk and equities are unavailable in quote samples.
Capacity therefore uses an explicitly labelled frozen current smaller-venue
equity scenario, including the existing 1x-to-5x slot calculation. This is not
a historical account-capacity replay. The CLI refuses unsupported hard-floor
layouts rather than silently replacing them with default tiers.

Signal-crossing counts distinguish the unchanged configured floor from the
unchanged adaptive threshold. They are not executable trade counts, and
quote-path net estimates are not actual profit. A large difference establishes
opportunity filtering, not that relaxing the adaptive rule would be profitable.
No report is eligible to authorize live trading.

## Frozen Evidence: 2026-10-29

Snapshot archive SHA256:
`2c85c539fb82d7536bde539f7552e7cb50acfe297d2c8bdefe983f2a48927d81`.
Health/config snapshot: 2026-10-29 11:58:05 UTC. The completed holdout
session ran from 2026-10-27 22:00 UTC to 2026-10-28 21:00 UTC.

- 7,299 strictly accepted holdout samples; long-direction configured-floor
  crossings: 7,299; adaptive first-tier crossings: zero; short-direction
  crossings: zero. Counts are observations, not independent trade opportunities.
- Full five-tier same-parameter quote replay: zero entries/closed trades,
  no unresolved position and no abnormal-gap stop.
- At snapshot time the production first tier was 81.580943 bps; stricter
  source-quality filtering produced 81.643951 bps, not a lower threshold.
  Thus the audited exclusions did not explain an artificially high threshold.
- The larger adaptive threshold measurably suppresses signals. These results
  do not establish that a lower threshold would improve net profit. Historical
  account capacity, actual fills and a validated alternative remain unproven.
- The capacity scenario froze the smaller current equity at 129.06631 USD:
  tier ceilings 120/240/380/500/640 USD, child order ceiling 20 USD. It did
  not assume that these were the account's actual historical equities.
