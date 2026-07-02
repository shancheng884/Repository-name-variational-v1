#!/usr/bin/env bash
set -euo pipefail

CURRENT_ASSET="${1:-SOL}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "== live summary =="
python tools/summarize_live_inventory_recent.py \
  --file log/order_metrics.jsonl \
  --state-file log/live_inventory_state.json \
  --tail 50000 \
  --latest-run-only \
  --top-missed 3

echo
echo "== candidate ranking =="
python tools/watch_live_asset_candidates.py \
  --assets BTC,ETH,SOL \
  --current-asset "${CURRENT_ASSET}" \
  --probe-log log_probe/order_metrics.jsonl \
  --live-log log/order_metrics.jsonl \
  --live-state log/live_inventory_state.json \
  --tail 50000 \
  --latest-run-only \
  --min-samples 120 \
  --lookback-samples 30 \
  --confirm-samples 2 \
  --confirm-consecutive-samples 1 \
  --min-switch-delta-bps 2.0 \
  --min-normalized-edge-bps 1.0 \
  --min-net-score-bps 1.0 \
  --max-sample-move-bps 5 \
  --max-sample-age-seconds 180 \
  --max-abs-basis-bps 100 \
  --max-log-quote-skew-seconds 30 \
  --max-quote-ms-filter 1000 \
  --fallback-shortfall-bps 5.5 \
  --ranking-output log_probe/asset_candidate_rankings.jsonl \
  --print-live-command
