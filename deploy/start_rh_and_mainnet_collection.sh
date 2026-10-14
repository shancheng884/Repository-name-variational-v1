#!/usr/bin/env bash
set -euo pipefail

MAINNET_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RH_DIR="/home/ubuntu/var-rh-eth"
echo "RH live now checks exchange book timestamps, bounds normal profit exits, and pauses on abnormal RH fills."
echo "The IOC boundary uses the quoted Var exit price; confirm both venue fills after any uncertain submission."
test -x "$RH_DIR/.venv/bin/python"
test -f "$RH_DIR/.env"
test -d "$RH_DIR/.git"

for repo in "$MAINNET_DIR" "$RH_DIR"; do
  if [ -n "$(git -C "$repo" status --porcelain)" ]; then
    echo "REFUSE: uncommitted changes in $repo"
    exit 1
  fi
  test "$(git -C "$repo" branch --show-current)" = main
  git -C "$repo" pull --ff-only origin main
done

COLLECTOR_PY="$MAINNET_DIR/.venv/bin/python"
if [ ! -x "$COLLECTOR_PY" ]; then
  COLLECTOR_PY="$RH_DIR/.venv/bin/python"
fi
"$COLLECTOR_PY" -c "import sys; sys.path.insert(0, '$MAINNET_DIR'); import tools.lib.mainnet_anchor_follow"
"$COLLECTOR_PY" "$MAINNET_DIR/tools/mainnet_anchor_collect.py" --help >/dev/null
"$RH_DIR/.venv/bin/python" -m py_compile "$RH_DIR/main.py"

mapfile -t PIDS < <(pgrep -f '^/home/ubuntu/var-rh-eth/.venv/bin/python main.py ' || true)
if ((${#PIDS[@]} > 1)); then
  echo "REFUSE: multiple RH strategy processes"
  exit 1
fi
if ((${#PIDS[@]} == 1)); then
  PID="${PIDS[0]}"
  kill -TERM "$PID"
  for _ in $(seq 1 60); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$PID" 2>/dev/null; then
    echo "REFUSE: old strategy has not exited; no force kill"
    exit 1
  fi
fi
if [ -n "$(ss -H -ltn '( sport = :8866 or sport = :8867 or sport = :8868 )')" ]; then
  echo "REFUSE: Var forwarder ports are occupied by another process"
  exit 1
fi

cd "$RH_DIR"
MODE="$(.venv/bin/python - <<'PY'
import json
from pathlib import Path
s = json.loads(Path('log/live_inventory_state.json').read_text())
if s.get('asset') != 'ETH' or s.get('pending_actions'):
    raise SystemExit('REFUSE: asset mismatch or pending actions')
status, lots = s.get('status'), s.get('open_lots') or []
if status == 'flat' and not lots:
    print('flat')
elif status == 'open' and lots:
    print('open')
else:
    raise SystemExit('REFUSE: state requires manual reconciliation')
PY
)"
.venv/bin/python - <<'PY'
import json
from decimal import Decimal
from pathlib import Path
s = json.loads(Path('log/live_inventory_state.json').read_text())
qty = Decimal('0')
for lot in s.get('open_lots') or []:
    q = Decimal(str(lot['qty']))
    if lot.get('direction') == 'long_var_short_lighter':
        qty += q
    elif lot.get('direction') == 'short_var_long_lighter':
        qty -= q
    else:
        raise SystemExit('REFUSE: unknown position direction')
print('state=', s.get('status'), 'lots=', len(s.get('open_lots') or []))
print('expected_Var_ETH=', qty, 'expected_RH_ETH=', -qty)
print('Check actual venue positions match and both venues have no active ETH orders.')
PY
read -r -p "Type CONFIRM_ETH_MATCHED after checking both venues: " CONFIRM </dev/tty
[ "$CONFIRM" = CONFIRM_ETH_MATCHED ] || exit 1

set -a
source .env
set +a
EXTRA=""
if [ "$MODE" = flat ]; then
  .venv/bin/python tools/live.py --asset ETH --reset-local-state-only </dev/tty
  .venv/bin/python - <<'PY'
import json
from pathlib import Path
s = json.loads(Path('log/live_inventory_state.json').read_text())
if s.get('status') != 'flat' or s.get('open_lots') or s.get('pending_actions') or s.get('v4_batch_halted_reason'):
    raise SystemExit('REFUSE: flat-state reset did not complete')
PY
else
  EXTRA="--resume-open-position"
fi
OLD_RUN_ID="$(.venv/bin/python -c 'import json; from pathlib import Path; p=Path("log/live_inventory_risk_health.json"); print(json.loads(p.read_text()).get("run_id") or "" if p.exists() else "")')"
SESSION="var-rh-live-$(date -u +%m%d%H%M%S)"
LOG="$RH_DIR/log/${SESSION}.log"
mkdir -p log
tmux new-session -d -s "$SESSION" \
"cd '$RH_DIR' && source .venv/bin/activate && set -a && source .env && set +a && exec python tools/live.py --asset ETH --v4-live --v4-real-gradient --v4-elastic-capacity --v4-bidirectional --v4-continuous --v4-test-skip-recent-health $EXTRA >> '$LOG' 2>&1"
echo "RH session=$SESSION log=$LOG"

sed -e "s|/home/ubuntu/Repository-name-variational-v1|$MAINNET_DIR|g" \
    -e "s|$MAINNET_DIR/.venv/bin/python|$COLLECTOR_PY|g" \
    "$MAINNET_DIR/deploy/systemd/var-mainnet-anchor.service" |
  sudo tee /etc/systemd/system/var-mainnet-anchor.service >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable var-mainnet-anchor.service
sudo systemctl restart var-mainnet-anchor.service

NEW_RUN_ID=""
for _ in $(seq 1 24); do
  NEW_RUN_ID="$(.venv/bin/python -c 'import json; from pathlib import Path; p=Path("log/live_inventory_risk_health.json"); print(json.loads(p.read_text()).get("run_id") or "" if p.exists() else "")' 2>/dev/null || true)"
  if [ -n "$NEW_RUN_ID" ] && [ "$NEW_RUN_ID" != "$OLD_RUN_ID" ]; then
    break
  fi
  sleep 5
done
if [ -z "$NEW_RUN_ID" ] || [ "$NEW_RUN_ID" = "$OLD_RUN_ID" ]; then
  echo "RH startup not confirmed; do not start a second instance"
  tail -n 50 "$LOG"
  exit 1
fi
if ! pgrep -f '^/home/ubuntu/var-rh-eth/.venv/bin/python main.py ' >/dev/null; then
  echo "RH process exited; check its log before retrying"
  tail -n 50 "$LOG"
  exit 1
fi
.venv/bin/python tools/analyze.py --tail 100 --asset ETH |
  grep -E '^(process=|state=|rows=|account_risk_event=|operational_readiness=)'
.venv/bin/python - <<'PY'
import json
from pathlib import Path
h = json.loads(Path('log/live_inventory_risk_health.json').read_text())
for k in ('updated_at', 'run_id', 'risk_action', 'risk_reason', 'variational_reference_quote_fresh'):
    print(f'{k}={h.get(k)}')
PY

for _ in $(seq 1 12); do
  if "$COLLECTOR_PY" -c 'import json,sys; from pathlib import Path; p=Path(sys.argv[1]); h=json.loads(p.read_text()) if p.exists() else {}; sys.exit(0 if h.get("samples_this_run",0)>0 else 1)' \
      "$MAINNET_DIR/log/mainnet_anchor/anchor_health.json"; then
    break
  fi
  sleep 5
done
systemctl is-active var-mainnet-anchor.service
cat "$MAINNET_DIR/log/mainnet_anchor/anchor_health.json"
"$COLLECTOR_PY" "$MAINNET_DIR/tools/mainnet_anchor_collect.py" --status
