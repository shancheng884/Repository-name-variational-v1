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

## Passive quote wakeups and audit

- A newly accepted, finite, positive Var bid/ask wakes the existing serial
  paper/live loop. Reference-mid WS updates, other assets, invalid pairs, and
  rejected duplicate timestamps do not wake it. The usual timer remains a
  fallback for risk, reconciliation, and exit management during feed silence.
- An update arriving during a cycle is coalesced to the newest version for the
  next cycle. It never starts another execution task or replays an old quote.
  No RFQ or market-data request is added by this scheduling path.
- Execution-command responses are excluded from the passive signal cache:
  suppress admission during that command and remember its quote IDs to reject
  delayed CDP feedback. The ordinary quote cache remains available to legacy
  consumers; it is not the frozen-median strategy's passive signal cache.
  A timed-out command remains quarantined until its late broker result supplies
  the identity; failure is not interpreted as a new independent passive quote.
  Health reports the unresolved count, and no extra request is made to resolve it.
- Confirmation still requires the latest observation plus two independent
  source quotes in the existing 3-second window. Existing one-per-second
  confirmation/history admission, entry/exit gates, cost, and capacity remain.
- Snapshot Lighter prices, version, offset, local receipt, and exchange clocks
  together under the book lock. Check the age of that captured book as well
  as the current book; a later frame cannot make an earlier snapshot fresh.
- `log/median_quote_audit/YYYY-MM-DD.jsonl` records one row per processed Var
  version, with the run/sample pair ID, both bid/ask versions and timestamps,
  receive skew, wakeup reason, coalesced count, rejections, and the model's
  current confirmation window/counts. Timer repeats do not inflate this file.
  Upstream Var timestamps can be absent; local receipt is explicitly separate.
- Health exposes `median_signal_mode=passive_var_event_serial`, the latest
  processed/audited version, and audit time. Audit is not evidence that an RFQ
  or order was accepted. An RFQ still needs all existing signal/execution gates.
