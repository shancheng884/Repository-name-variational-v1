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

Before typing `YES` at the prompt, manually confirm both exchanges have no positions and no open orders.

## Analyze Live Data

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate
python tools/analyze.py
```

This reads `log/order_metrics.jsonl`, `log/runtime.log`, and `log/live_inventory_state.json`. It does not start live, stop live, submit orders, or modify state.

## Notes

`main.py` remains the trading engine. `tools/live.py` is only a safety wrapper around the existing live command, and `tools/analyze.py` is only an offline live-log analyzer.
