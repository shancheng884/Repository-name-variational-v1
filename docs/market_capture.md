# 多平台 RWA 行情与成交打点（只读）

## 目的

在不影响 ETH / WTI 实盘和现有采集的前提下，记录四个平台、三个 RWA 标的的行情、价差、订单簿、本人成交、延迟、CPU/卡顿，以及成交后 0.25 / 0.5 / 1 / 3 秒的不利波动。所有写入只用于研究，不参与下单。

## 平台与标的

| 平台 | 接入 | 说明 |
|---|---|---|
| Variational | 公共 `/prices` WS | 只有参考中间价，没有公开订单簿；记录 `book_available=false` |
| Lighter 主网 | `mainnet.zklighter.elliot.ai` | L2 订单簿 + 公共成交 + 本人成交 |
| Lighter RH | `api.rh.lighter.xyz` | 同上，独立账户/API key |
| Entropy | Hyperliquid HIP-3 `io` DEX | L2 订单簿 + 公共成交 + `userFills` |

默认三个标的：`OPENAI`、`ANTHROPIC`、`NVDA`。它们同时存在于四个平台，按 Entropy 深度和跨平台可比较性挑选：

- Variational / Lighter 主网 / Lighter RH：同名 `OPENAI`、`ANTHROPIC`、`NVDA`
- Entropy：`io:OAI`、`io:ANTH`、`io:NVDA`

## 安全边界

- 代码内没有下单调用；启动时扫描源码中的 `create_order` / `place_order` / `submit_order` / `send_tx` 等关键字，发现即退出。
- 拒绝 `ALLOW_ORDER_SUBMISSION=true`，健康文件固定 `orders_attempted=false`、`execution_enabled=false`。
- 独立 systemd 服务、独立文件锁、独立输出目录；不重启、不停止 ETH / WTI / 原油采集。
- 只使用公开行情、地址级 `userFills` 和只读 API key；密钥只从环境文件读取，不写入日志、不写进健康文件。

## 数据目录

默认 `log/market_capture/`，按 `stream/venue/symbol/YYYYMMDD-HH.jsonl` 小时切分：

| 目录 | 内容 |
|---|---|
| `quotes/` | 每次 BBO/参考价更新；含 source/recv 时间、延迟、20U 可执行 VWAP、价差 |
| `books/` | 每 5 秒一份前 10 档订单簿快照 |
| `market_trades/` | 公共逐笔成交 |
| `fills/` | 本人成交（四平台） |
| `edges/` | 跨平台两两价差，`mode=executable` 或 `reference` |
| `adverse/` | 成交后固定时点的不利波动 |
| `chains/` | 每笔本人成交的因果链汇总 |
| `latency/` | 各平台 REST 探测 RTT，每 5 秒 |
| `system/` | 每分钟 CPU、steal、iowait、内存、关键进程 CPU/RSS |
| `stalls/` | 事件循环卡顿 >100ms |
| `health.json` | 当前状态、各平台连接/最新报价年龄/错误、丢行计数 |

`quotes` 的 `executable_buy/sell` 是按 `--notional-usd`（默认 20U）吃单的 VWAP；深度不足则为空。`edges` 只在两边都有可执行价时使用 `mode=executable`，否则退回中间价并标 `mode=reference`，不会把参考价当成可成交价。

## 启动与验收

```bash
sudo install -m 0644 deploy/systemd/var-lighter-market-capture.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now var-lighter-market-capture.service
systemctl is-active var-lighter-market-capture.service
python - <<'PY'
import json
h=json.load(open("log/market_capture/health.json"))
print(h["status"], h["orders_attempted"], h["loop"])
for k,v in h["venues"].items():
    print(k, v.get("status"), v.get("connected"), v.get("last_quote_age_s"), v.get("last_error"))
PY
```

只重启本服务，不要动 `var-lighter-eth-live`、`var-lighter-wti-median`、`var-lighter-wti-collector`、`var-chrome`。

## 资源与保留

- systemd 限制：`CPUQuota=60%`、`MemoryMax=700M`、`Nice=10`，避免和实盘抢资源。
- 队列有界；磁盘慢时丢行情行并计数，不阻塞行情线程。
- 每 6 小时压缩 6 小时前的 `.jsonl`；默认保留 14 天，按 `--retention-days` 调整。
- 磁盘低于 2GB 时仍会继续写，但健康文件会显示；建议单独监控 `disk_free_gb`。

## 已知限制

- Variational 公共行情只给参考价，不给 bid/ask；因此 Var 的 `spread_bps` 为空，跨平台价差对 Var 使用 `mode=reference`。要得到 Var 可执行价，需要在 Var 页面产生 RFQ，本采集器不主动发 RFQ，避免影响现有实盘。
- Lighter 本人成交依赖账户 API key；未配置时该平台行情仍采集，`*_fills` 状态为 `disabled_missing_credentials`。
- Entropy 的 `userFills` 需要真实交易账户地址；子账户请填子账户地址。
