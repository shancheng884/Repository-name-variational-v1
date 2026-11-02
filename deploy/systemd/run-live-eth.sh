#!/usr/bin/env bash
# Start the ETH live strategy, resuming only when the local state actually holds
# an open position. live.py refuses `--resume-open-position` on a flat state, so
# a fixed flag would put systemd into a restart loop right after any clean exit.
set -euo pipefail

R=/home/ubuntu/Repository-name-variational-v1
cd "$R"

FLAGS=(--asset ETH --v4-live --v4-real-gradient --v4-elastic-capacity --v4-bidirectional --v4-continuous)
STATE=$("$R/.venv/bin/python" - <<'PY' 2>/dev/null || echo "unknown 0"
import json
s = json.load(open("log/live_inventory_state.json"))
print(s.get("status") or "unknown", len(s.get("open_lots") or []))
PY
)
STATUS=${STATE%% *}
LOTS=${STATE##* }

if [ "$STATUS" = "open" ] && [ "$LOTS" != "0" ]; then
  FLAGS+=(--resume-open-position)
fi
if [ "$STATUS" = "pending" ] || [ "$STATUS" = "manual_review_required" ]; then
  FLAGS+=(--resume-open-position)
fi

exec "$R/.venv/bin/python" tools/live.py "${FLAGS[@]}"