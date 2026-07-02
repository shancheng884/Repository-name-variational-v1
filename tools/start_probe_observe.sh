#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

mkdir -p log_probe

echo "Starting probe observe on ports 8776/8777/8778"
echo "Chrome probe profile extension endpoints:"
echo "  wsEndpoint: ws://127.0.0.1:8776"
echo "  restEndpoint: ws://127.0.0.1:8777"
echo "  commandEndpoint: ws://127.0.0.1:8778"
echo

exec python main.py \
  --mode observe \
  --live-allowed-assets BTC,ETH,SOL \
  --forwarder-ws-port 8776 \
  --forwarder-rest-port 8777 \
  --forwarder-command-port 8778 \
  --output-dir log_probe
