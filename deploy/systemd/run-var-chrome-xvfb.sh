#!/usr/bin/env bash
# 在虚拟显示 :99 上常驻 Chrome：不依赖 RDP 会话，页面不会被冻结。
set -euo pipefail
export DISPLAY=:99
if ! xdpyinfo -display :99 >/dev/null 2>&1; then
  Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp >/tmp/xvfb.log 2>&1 &
  for _ in $(seq 1 20); do
    xdpyinfo -display :99 >/dev/null 2>&1 && break
    sleep 0.5
  done
fi
exec google-chrome \
  --user-data-dir=/home/ubuntu/chrome-var-profile \
  --no-first-run --no-default-browser-check \
  --disable-features=Translate \
  --disable-background-timer-throttling \
  --disable-backgrounding-occluded-windows \
  --disable-renderer-backgrounding \
  --disable-ipc-flooding-protection \
  --disable-gpu \
  --remote-debugging-port=9222 \
  --remote-allow-origins=* \
  "https://omni.variational.io/perpetual/ETH"
