#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

mkdir -p log_probe

echo "Starting probe dry-decisions on ports 8776/8777/8778"
echo "Chrome probe profile extension endpoints:"
echo "  wsEndpoint: ws://127.0.0.1:8776"
echo "  restEndpoint: ws://127.0.0.1:8777"
echo "  commandEndpoint: ws://127.0.0.1:8778"
echo

exec python main.py \
  --mode live \
  --confirm-live \
  --live-allowed-assets BTC,ETH,SOL \
  --forwarder-ws-port 8776 \
  --forwarder-rest-port 8777 \
  --forwarder-command-port 8778 \
  --output-dir log_probe \
  --variational-submit-transport api \
  --lighter-submit-transport ws \
  --lighter-order-mode market-ioc \
  --lighter-prewarm-submit-ws \
  --live-max-notional-usd 25 \
  --live-inventory \
  --live-inventory-dry-decisions \
  --live-inventory-signal-mode basis \
  --live-inventory-basis-entry-mode concurrent \
  --live-inventory-lot-notional-usd 20 \
  --live-inventory-max-cycles 999 \
  --live-inventory-max-lots 1 \
  --live-inventory-max-total-lots 1 \
  --live-inventory-max-lighter-slippage-bps 6 \
  --live-inventory-lighter-submit-slippage-bps 15 \
  --live-inventory-lighter-exit-submit-slippage-bps 30 \
  --live-inventory-basis-min-entry-edge-bps 13 \
  --live-inventory-basis-min-abs-entry-bps 13 \
  --live-inventory-basis-min-exit-pnl-bps 8.0 \
  --live-inventory-basis-min-signal-reverted-exit-pnl-bps 8.0 \
  --live-inventory-basis-profit-take-pnl-bps 10.0 \
  --live-inventory-basis-entry-confirm-samples 2 \
  --live-inventory-basis-max-sample-move-bps 5 \
  --live-inventory-basis-stablecoin-normalization \
  --live-inventory-basis-use-normalized-edge-for-entry \
  --live-inventory-basis-stablecoin-regime-entry \
  --live-inventory-basis-min-normalized-entry-edge-bps 1.0 \
  --live-inventory-basis-min-normalized-filter-edge-bps 0.5 \
  --live-inventory-entry-lighter-fill-timeout-seconds 3 \
  --live-inventory-i-accept-diagnostic-low-entry-bps \
  --live-inventory-ignore-recent-execution-loss-buffer-for-diagnostics \
  --live-inventory-i-confirm-flat-start \
  --live-inventory-reset-state-after-manual-flat
