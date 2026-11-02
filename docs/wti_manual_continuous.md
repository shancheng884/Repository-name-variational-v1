# Manual continuous WTI gradients

This mode is independent of the ETH/WTI rotation supervisor and watchdogs.
It requires `--continuous-manual-gradient --confirm-continuous-manual-gradient`
and the exact original `--adopt-gradient-pilot` ID. It cannot be combined with
bounded trial IDs, rotation IDs, or a new-gradient-pilot predecessor.

The existing position, order identities, and ledger are preserved. Adoption
requires this runtime's fresh exact dual-position reconciliation and zero orders.
Only the approved capacity/continuation policy changes. Entry thresholds, model
hash and validity, 20 USD child orders, 2-second quotes, 1.5 bps pair drift,
8 bps slippage, 120-second gap guard, margin and equity-balance gates remain.

For smaller verified venue equity E and 20 USD children, cumulative tier slots
are floor(E * tier / 20), capped by floor(E * 5 / 20), exactly as ETH. These
are capacities, not a requirement to open five simultaneous orders. An optional
single first-tier elastic slot retains its surplus and two-of-three confirmation.

Profit exits group current children by entry tier, highest tier first. Each
selected group receives a fresh size-specific executable quote and profit check.
Evidence is revalidated at dispatch; expired or failed fee evidence falls back
to 10 bps. All known funding debits are conservatively charged to a selected
group rather than using funding credits. Risk, gap, time and session exits drain
the whole position. Pending partial exits persist original, selected, remaining
positions and stable leg identities. A submitted unknown leg is never retried.

Terminal flat cycles rearm only after fresh dual-flat proof; a restart reuses the
same adopted session and successor chain. No finite round budget is renewed.
The old bounded trial is archived as superseded, not deleted.

WTI trade messages reuse the ETH template and persisted outbox. Daily reports
use the same field layout, with gross, fees, funding, and independently verified
net PnL kept separate. A fresh account snapshot is recorded in reporting storage,
never the trading ledger. An intraday first snapshot is not a daily opening
capital; missing capital or fee evidence stays explicitly unverified.
