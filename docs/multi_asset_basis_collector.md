# Multi-asset basis collector

This collector is a read-only research process. It never imports Lighter
trading credentials, never submits orders, and never writes
`live_inventory_state.json`.

## Design

- One Chromium extension command client requests Variational indicative quotes.
- One public Lighter WebSocket subscribes to SOL, BTC, and ETH order books.
- One global quote scheduler avoids multiplying Variational request pressure.
- `baseline` samples are written every 10 seconds per asset and are the only
  samples allowed to form historical quantiles.
- `burst` samples preserve the path around tail events but do not enter the
  quantile history.
- Every sample records quote age, book age, Lighter nonce continuity, clock
  drift, request latency, and pre/post quote book drift.
- Fees are fixed at zero. Executable bid/ask prices contain crossed spread;
  real-fill shortfall remains a separate replay reserve.

Data is stored under `log/basis_samples/<ASSET>/`. Closed UTC days are gzip
compressed without deleting the compressed history. The analyzer reads both
the new store and legacy `order_metrics.jsonl` data.

## Safe startup

Before switching collectors, confirm the existing process, local state, both
venue positions, and both venue open orders. Stop the old collector only after
both venues are manually confirmed flat.

Dry run:

```bash
python tools/live.py --assets SOL,BTC,ETH --collect-only --dry-run --verbose
```

Long runs must be started inside tmux:

```bash
python tools/live.py --assets SOL,BTC,ETH --collect-only
```

Startup is accepted only after all three lines appear:

```text
variational_api_command_client_preflight_passed asset=SOL
variational_api_command_client_preflight_passed asset=BTC
variational_api_command_client_preflight_passed asset=ETH
```

The process stops after three consecutive extension command failures or when
free disk drops below 3 GiB. It warns below 5 GiB.

## Legacy log archive

The archive tool is dry-run by default and refuses to run while `main.py` or
the multi-asset collector is active:

```bash
python tools/archive_legacy_logs.py
python tools/archive_legacy_logs.py --execute
```

Only run `--execute` after the normal process/state/venue checks and after the
collector has stopped. Original content is retained in gzip archives and a new
empty active file is created.
