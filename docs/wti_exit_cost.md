# Evidence-Bound WTI Exit Costs

This opt-in changes profit-exit costs only. The entry configuration, frozen
model identity, historical median conditioning, legacy tier spacing/maturity,
five-tier position protections and persistent round budget are not changed.
Legacy absolute thresholds remain visible rather than being added a second
time to the convergence model. Exploratory entry authorization stays explicit.

`tools/wti_exit_cost_audit.py` binds complete authenticated Var fee responses
to the forwarding wallet and exact cleared entry orders. An empty, complete
fee response proves zero; missing fee fields or failed exports do not. Lighter
fees require official CSV trades matched to exact order trades and full fill
coverage. This tool does not query funding or modify the execution ledger.
Already known funding debits are retained; credits cannot lower the threshold.

Profit exits use actual-fill paired gross PnL, less verified entry fees,
remaining exit-fee reserve and an execution-risk reserve derived from actual
paired WTI close fills. Funding is excluded from the profit-exit decision; it
remains reflected in account equity and reporting. Actual entry slippage and
executable quote spreads are already in prices and are not separately
subtracted again. The reserve uses the P95/latest tail of earlier terminal,
paired WTI closes and requires at least three observations. With fewer samples,
expired evidence, or invalid bindings, the existing 10bps fallback applies.
There is no fixed 3.50bps floor. ETH fills, future observations, partial and
unpaired closes cannot supply WTI execution evidence. A tail above the old
budget is never capped to make an exit easier.

Activate using `--exit-cost-evidence PATH --confirm-evidence-exit-costs` only
after tests and same-position shadow comparison. Proofs expire within fifteen
minutes and bind all entry fills and current aggregate quantities. Missing,
expired or changed evidence falls back to the legacy budget. The fresh
whole-position quote profit check independently reloads the proof immediately
before creating a close intent. Time/session/risk exits remain independent
of the profit target. No order resend or pilot reset is introduced.

Health contains `exit_cost_policy` and `exit_cost_shadow`; this is a trading
cost estimate, not a claim of independently reconciled final cash PnL.
Do not restart watchdogs, daily reports or automatic asset rotation to use it.

## Automatic Renewal

`wti-exit-evidence.timer` invokes a separate read-only job every five minutes.
It reads only the current wallet's Omni authentication cookies from the local
Chromium profile, in read-only mode. Credentials remain in memory, are never
included in evidence, and are only sent to the fixed HTTPS Omni fee endpoint;
redirects and environment proxies are disabled. It does not require an open
remote-desktop window, refresh a browser page, alter browser security settings,
or attempt login/CAPTCHA. Unsupported cookie protection or expired login causes
fallback, not a security bypass.

Every renewal gets a fresh complete fee response and verifies exact cleared
orders. Immutable Lighter trade/CSV evidence can be reused for the same position
fingerprint; fee metadata is refreshed. Profit-exit proof generation does not
query or allocate funding, so closing a tier does not make renewal depend on
reconstructing historical exposure at funding timestamps. Adjacent previous-
round CSV trades are explicitly out of scope of the exact current-entry order
proofs.

The publisher rechecks the position and pending action after network reads.
Failures invalidate the active proof immediately; a service failure/timeout
also invalidates it through `ExecStopPost`. A valid proof expires after fifteen
minutes even if the renewal timer stops. Only the independent proof cache and
renewal health are written; the live trading ledger and notification outbox
remain untouched. Restore browser login normally if its session genuinely
expires; keeping a remote-desktop window open is not a requirement.
