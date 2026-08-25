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

V4 blocks new UTC-weekend entries by default. A bounded weekend test must use
both `--v4-test-skip-recent-health` and `--v4-test-allow-weekend`. Never add the
weekend flag to the default daily command; it enables real orders and is logged
as a test bypass.

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
