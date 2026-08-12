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
