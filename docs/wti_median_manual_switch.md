# Manual WTI median strategy

`tools/wti_median_live.py` runs the same frozen-median executor as ETH.
Only instrument identity, venue specifications, market-session checks, and
storage paths change. It does not run the legacy WTI strategy or rotation.

- Variational: USOILP swap, commodity, USDC, funding interval 0.
- Lighter: WTI perpetual, market 145, multiplier 1; fail closed if changed.
- Levels: 5.6, 6.6, 7.6, 8.6, 9.6 bps from the frozen median.
- Total roundtrip budget 4.6 bps, net target 1 bps, without double charging.
- Child notional 20 USD; existing equity-based five-tier / 5x limits apply.
- Same independent-quote confirmations, RFQ checks, concurrent execution,
  account protections, durable intents, per-tier closing, and rearming.
- Session evidence comes from the existing read-only oil collector. No new
  entries in the last 30 minutes; unknown/closed sessions cannot trade.
- State, median samples, reports, and audits live under `log/wti_median/`.
  ETH journals and legacy oil samples cannot warm the new median window.
- A cold start needs genuine second-level WTI coverage over the existing
  60-minute window. Do not interpolate history or bypass readiness.

Before enabling `var-lighter-wti-median.service`, verify ETH and WTI accounts
and open orders, back up state, drain ETH with its current PID/run-bound
control acknowledgement, then stop/disable ETH. Both runtimes share the same
instance lock and forwarder ports. Do not reset a pending WTI state on restart.

Keep Chrome authenticated and the original collector running. Switch only the
forwarded trading tab and set `VAR_CHROME_MARKET_URL` for future browser starts.
Use the actual market-selector URL, not a guessed route. Watchdogs and automatic
rotation remain off.

For the unchanged ETH-format daily template, invoke `tools/daily_pnl_report.py`
with `--asset WTI --native-inventory` and the isolated WTI baseline/send-state
paths. Never substitute the legacy WTI execution ledger.

## Keep ETH history warm

`var-lighter-eth-passive.service` records qualified ETH pairs every second into
the original `log/median_samples/` journal. It observes an independent ETH tab
through loopback Chrome debugging and Lighter public books, without order or
RFQ commands. Only the ETH observation tab can be reloaded; WTI stays attached.
A missing ETH tab can be recreated in the background after a browser restart.

Real source times, the existing five-second Var signal limit and two-second
Lighter limit are preserved. Missing/stale evidence produces a health reason,
not interpolated records. ETH live ownership pauses this writer automatically.
The normal ETH startup loader reads the same journal, clears old confirmations,
and still requires current valid quotes and sufficient real window coverage.
