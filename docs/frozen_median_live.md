# Frozen median ETH live mode

Enable with `tools/live.py --asset ETH --v4-live --v4-real-gradient
--v4-bidirectional --v4-continuous --frozen-median`. The systemd launcher
selects it only when `VAR_ETH_FROZEN_MEDIAN=1`. Other modes stay unchanged.
Never adopt a legacy open position into this mode. Verify both accounts and
orders flat, preserve the old state, then switch the launcher.

## Signal and sizing

- Signed basis: `(Lighter midpoint - Var midpoint) / average(midpoints) * 10000`.
- Only real passive bid/ask observations qualify. Preserve the original source
  timestamp and indicative quote-size identity; never fabricate old 1s history.
- Journal at most one observation per second in `log/median_samples/`.
- Rolling window 60 minutes, at least 80% occupied seconds, maximum hole 60s.
  The oldest boundary must also meet this hole limit: a cold start needs about
  59 minutes, not just 48 minutes of compact observations. No 30s anchor import.
- Freeze the median when reserving the first intent. Five distances are
  5.6/6.6/7.6/8.6/9.6bps. Require latest plus two distinct source quotes in 3s.
- Cumulative capacities are floor(smaller venue equity * tier / 20), capped
  by the existing 5x per-venue leverage check. Child size remains 20 USDC.
  This launcher does not borrow an elastic tier-1 slot from tier 2.

## Executable confirmation and exits

The passive crossing only authorizes an RFQ, not an order. Recheck quantity,
RFQ freshness, current Lighter depth, account risk, order budget, and capacity.
Prices already contain spread. Map the contemporaneous reference-to-RFQ
midpoint offset into the projected target exit once, not as an added fee.
Use a 4.6bps TOTAL four-leg roundtrip cost budget, not 4.6bps per entry/exit.
The net target is 1bps in both entry and exit checks, including direct main.py
startup. Compute price-embedded cost from the same RFQ/entry and exit midpoint
references and one-leg-average notional; charge only max(0,4.6-embedded_cost).
Actual fill prices replace entry quotes on exit, so actual entry slippage is
not charged twice. Missing cost references receive no cost credit. Costs above
the budget still count in full. This budget is an assumption, not a measured
cost guarantee, and does not replace fresh RFQ/depth checks.

Tier 1 waits for the frozen median; higher tiers can exit at the preceding
tier's distance. Every child must independently meet actual-entry-price PnL
>= 1bps target + the as-yet-unpriced residual of the roundtrip budget. There is
no additional legacy 3bps target, 2+2bps reserve, or separate RFQ-repricing charge.
Signal confirmation is followed by a fresh exact-size exit RFQ, then another
current-depth/freshness preflight before concurrent reduce-only legs.
No ordinary aggregate portfolio-profit exit or maximum-holding-time exit.
Existing account-risk/operator emergency exits remain available, including
bounded-size aggregate exits with component identities preserved.

## Recovery

- Persist model intent together with the native pending intent before either leg.
- Unknown results retain the same order identity and block new submission.
- Only final matched fills resolve an entry/exit. Confirmed entry rollback also
  requires no active orders and must reset/re-cross before rearming.
- A tier closing intent cannot be submitted twice, including after restart.
- Close finalization requires four-leg quantity agreement; partial/unknown fills
  cannot rearm a tier. HTTP-only unwind success is not restored-cost proof and
  remains manual review rather than silently re-opening the order state.
- Re-arm only after a genuine reset below level minus 0.5bps and another crossing.
- Finish an episode only after all intents are closed, both accounts and orders
  are independently flat, and a fresh signal has reset. Old order IDs remain
  tombstones. Never restore quote confirmations after a restart.
- Quote specification changes or sustained median displacement lock new entries,
  not automatic liquidation. All original realtime execution protections remain.

No watchdog, automatic pair rotation, or TG template changes are part of this mode.
