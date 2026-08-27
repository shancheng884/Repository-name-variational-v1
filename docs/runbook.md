# Runbook

Use only these two daily commands on the VPS.

## Start Live

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate
python tools/live.py --asset SOL
```

Replace `SOL` with `BTC` or `ETH` when needed.

The command refuses to start when another `python main.py` is running, when `log/live_inventory_state.json` is not flat, or when it contains `open_lots` or `pending_actions`.

Before running it, manually confirm both exchanges have no positions and no open orders.

Startup parameters are read once from `live_config.json`. They are not hot-reloaded into the trading loop.

To inspect the full `main.py` command without starting live:

```bash
python tools/live.py --asset SOL --dry-run --verbose
```

## Analyze Live Data

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate
python tools/analyze.py
```

This reads `log/order_metrics.jsonl`, `log/runtime.log`, and `log/live_inventory_state.json`. It does not start live, stop live, submit orders, or modify state.

## Profit And Account Equity Report

The live process records a non-blocking account snapshot at startup and two
seconds after every confirmed entry and exit. The snapshot uses Variational
`balance + upnl` and Lighter `collateral`; failures are logged as partial
snapshots and never stop trading.

Telegram receives an account snapshot after startup/entry and one `PNL SUMMARY`
after every confirmed exit. The exit summary includes cycle PnL, current-run
PnL, both venue equities, account net change, return, and simple annualized
return. Account-equity change is the preferred return numerator; confirmed
pair-fill PnL is used only when the account snapshot is incomplete.

Show confirmed pair-fill PnL, both venue balances, account-equity change, and
simple annualized return:

```bash
python tools/pnl_report.py --asset ETH --capital-usd 35
```

`--capital-usd` is the amount of capital allocated to this strategy across
both venues, not one leg's order notional. It can be saved outside the command
as `PNL_REPORT_CAPITAL_USD` in `.env`. If neither is provided, the report uses
the first complete flat account snapshot. Use `--since YYYY-MM-DD` to limit the
reporting period.

The confirmed pair-fill PnL is calculated from both venues' final fill prices.
It does not separately deduct fees or funding. Account-equity change includes
fees, funding, deposits, withdrawals, and any other account activity, so the
report keeps these two figures separate.

Resend the latest confirmed close to the configured Telegram chat in the
Chinese PnL-summary format:

```bash
python tools/pnl_report.py --asset ETH --telegram-latest
```

This command only reads logs and sends a message. It never submits orders.

Start a new persistent PnL reporting period, excluding all older trades:

```bash
python tools/pnl_report.py --asset ETH --reset-baseline
```

Run the reset only while the strategy is stopped and local state is flat. The
next startup records the initial combined account equity. Confirmed PnL,
completed cycles, return, and simple annualized return then accumulate from
that checkpoint across later strategy restarts. Historical logs are retained
for audit but excluded from the default report.

## V4 Exit Confirmation

V4 uses the latest executable quote plus two qualifying observations in the
latest three-sample window. A guarded strong-single exit is allowed only when
its mode-specific shortfall reserve, short-window stability, quote latency, and
Lighter depth checks all pass. If a strong-single exit is estimated profitable
but its confirmed final PnL is negative, strong-single is disabled for the rest
of that run and exits fall back to the latest-and-two-of-three policy.

Inspect the active policy and automatic fallback state with:

```bash
python tools/analyze.py --tail 30000 --asset ETH | grep -E \
'^(process=|state=|exit_confirmation_policy=|exit_submit_mode=|exit_block_reasons=|operational_readiness=)'
```

V4 trades continuously on UTC weekends. The same multi-window threshold logic
applies, with a conservative six-hour boundary transition at weekend start and
end. The legacy weekend flag is not needed in new commands.

## Notes

`main.py` remains the trading engine. `tools/live.py` is only a safety wrapper around the existing live command, and `tools/analyze.py` is only an offline live-log analyzer.

## Log Maintenance

`runtime.log` and `basis_collector.log` rotate automatically. Do not use a
generic rotation command on `order_metrics.jsonl`: the strategy reads retained
execution events from that file for calibration.

When `order_metrics.jsonl` exceeds 512 MB, stop the strategy and collector and
confirm both exchanges are flat. Preview the safe compaction first:

```bash
python tools/compact_order_metrics.py
```

Then execute it:

```bash
python tools/compact_order_metrics.py --execute
```

The command creates a gzip archive containing the exact original file. The new
current file keeps every trading, fill, PnL, calibration, fuse, and manual-review
event, while bounding only repetitive diagnostics. Keep the newest full archive
until the compacted file and analyzer have been verified.

Other stopped-process logs can be previewed and archived separately:

```bash
python tools/archive_legacy_logs.py
python tools/archive_legacy_logs.py --execute
```

## Robinhood Chain Lighter Research Sidecar

The Robinhood Chain Lighter basis collector can run alongside live V4 because
it tails the live process's persisted Variational samples and opens only a
public Robinhood Lighter market-data connection. It does not use the extension
command port or any trading credentials. See
`docs/robinhood_lighter_basis_collector.md` for startup and verification.
### V4 parallel shadow-gradient research

`--v4-shadow-gradient` does not submit a second real order. During each real
one-lot episode it independently simulates alternative USD 20 second-tranche
entries after basis improvement of `+0.5`, `+1.0`, `+1.5`, and `+2.0` bps.
Each alternative has its own entry price, exit confirmation state, MFE/MAE, and
PnL. The alternatives are not summed into an USD 80 position.

Use the following analyzer line to compare the alternatives after several real
cycles:

```bash
python tools/analyze.py --tail 50000 --asset ETH | grep '^shadow_gradient_levels='
```

### Five-tier real gradient and account risk

`--v4-real-gradient` is an explicit real-order mode. It uses five dynamic
historical-percentile tiers. Each order remains fixed at USD 20. Tier N permits
cumulative one-sided notional up to N times the smaller venue equity, capped at
5x per venue. Every child order requires a fresh quote and confirmation of the
previous pair of fills. There is no fixed delay between children. Each venue
uses a rolling 60-second request window with normal capacity reserved below the
hard capacity so reduce-only exits remain available. It is mutually exclusive
with `--v4-shadow-gradient`.

Every candidate entry is checked against fresh account data. The hard leverage
limit is 5x on each venue independently, not 5x on combined gross notional.
Operational actions use maintenance-margin usage: 40% warns, 50% blocks new
entries, 60% reduces one child lot, and 75% requests emergency exit. An equity
imbalance near 20% warns and near 30% blocks entries because the smaller venue
limits paired capacity. After balances recover, fresh account data restores
entry permission automatically. Account risk is refreshed every 5 seconds
while a position is open and every 15 seconds while flat. If a venue omits
maintenance-margin data, the engine uses a conservative venue-specific fallback
rate instead of disabling the risk ladder.

When several confirmed child lots jointly reach the executable net target, the
engine quantity-weights their entry fills and submits one total reduce-only
order to each venue. All component IDs remain in the audit payload. If the live
edge falls to a lower tier before the whole basket reaches target, partial
de-tiering may remove the newest, highest-tier excess lots only when the removed
basket itself reaches the normal net target and removed realized plus remaining
executable unrealized PnL is not negative.

V4 has no time-driven exit and does not relax its profit target based on holding
time. Executable profit, the independent unrealized-loss fuse, and account-risk
actions are the only exit triggers. The V4 launcher uses a wider 50 bps
unrealized-loss fuse; this is a last-resort strategy anomaly guard, not the
normal exit condition.

Variational account data must have a valid `published_at` no older than 60
seconds. A stale snapshot is logged as partial, is excluded from account PnL and
capital baselines, and pauses new entries/add-ons without forcing an open
position to close.

External deposits and withdrawals are detected again after every complete,
confirmed flat exit snapshot, not only at startup. The current cycle PnL is
recorded before unexplained equity change is classified as external cash flow.

V4 trades continuously across UTC weekends. A simple six-hour boundary marker
keeps the multi-window threshold conservative around weekend start and end.
The old weekend test flag remains accepted only for command compatibility.

Before increasing either venue to USD 100, stop the strategy and confirm both
venues are flat. Make both deposits, then reset the PnL baseline once so the
cash inflow is not counted as strategy profit:

```bash
python tools/pnl_report.py --asset ETH --reset-baseline
```

Start the five-tier mode only after the new startup account snapshot shows
both venue equities near USD 100:

```bash
python tools/live.py --asset ETH --v4-live --v4-real-gradient \
  --v4-test-skip-recent-health --v4-test-max-cycles 3 \
  --reset-state-after-manual-flat
```
# Automatic margin rebalance

The implementation contract and safety state machine are documented in
`docs/automatic_margin_rebalance.md`. Execution remains disabled until both
venue adapters have verified official transfer and finality paths.

Validate the five percentile tiers on a chronological holdout:

```bash
python tools/validate_gradient_tiers.py --asset ETH
```

The first 70% of samples fits thresholds; the final 30% reports crossings and
six-hour 1/2/4 bps reversion hit rates. The tool is offline and never feeds its
output into the live process.
