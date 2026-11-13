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

默认九个标的：`OPENAI`、`ANTHROPIC`、`NVDA`、`SNDK`、`CRCL`、`AAPL`、`QQQ`、`DRAM`、`COIN`。前六个四平台齐全；QQQ、COIN 缺 Entropy，DRAM 缺 Lighter RH：

- Variational / Lighter 主网：全部九个同名标的；Lighter RH 缺 `DRAM`
- Entropy：`io:OAI`、`io:ANTH`、`io:NVDA`、`io:SNDK`、`io:CRCL`、`io:AAPL`、`io:DRAM`

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


## 归档到 R2

采集进程只写本地 JSONL；归档由独立的 `var-lighter-capture-archive.timer` 每 15 分钟执行：

1. 把已经关闭的 `.jsonl` 压缩成 `.jsonl.gz`（当前小时不动）。
2. 如果配置了 `R2_BUCKET`，用 `rclone copyto --checksum` 上传。
3. 上传后校验远端字节数，写入 `log/capture_archive_state.json`。
4. 只删除 `ARCHIVE_HOT_DAYS` 天以前、且状态为 `verified` 的本地文件。

R2 配置放在 `/home/ubuntu/.config/market-capture/r2.env`，权限 600：

```bash
R2_REMOTE=r2
R2_BUCKET=<bucket>
R2_PREFIX=market_capture
ARCHIVE_HOT_DAYS=3
```

`rclone.conf` 使用 Cloudflare R2 的 S3 endpoint。未配置 R2 时，timer 只做本地压缩，不删除任何文件。

**不要**把 R2 挂载成盘给采集器追加写；对象存储只接收完整文件。


## 磁盘、压缩和上传节奏

9 个标的当前约 **10GB/天原始数据**，gzip 后约 **1.1GB/天**。

建议 VPS 保留：

- 当前小时原始文件：约 0.4–0.6GB
- 最近 3 天压缩文件：约 3–4GB
- 系统、日志、R2 缓冲：2–3GB
- **最低保留 8GB 空闲磁盘**；低于 5GB 应告警并暂停非必要采集

当前定时器：

- 每 **15 分钟**检查一次
- 只压缩 **60 分钟前**已经关闭的 `.jsonl`
- 当前小时不动
- 上传成功后写入 `log/capture_archive_state.json`
- 只有 `verified` 且超过 `ARCHIVE_HOT_DAYS=3` 的文件才会删除

## 回放方式

R2 路径按 stream / venue / symbol / 小时分区：

```text
market_capture/quotes/lighter_mainnet/NVDA/20261113-05.jsonl.gz
market_capture/books/entropy/NVDA/20261113-05.jsonl.gz
market_capture/edges/cross/NVDA/20261113-05.jsonl.gz
```

### 方式 1：用回放脚本（推荐）

脚本按日期、标的、stream 从 R2 拉取到 `~/replay/var-lighter/`，再用 DuckDB 汇总：

```bash
python tools/replay_capture.py \
  --date 2026-11-13 \
  --symbol NVDA,SNDK \
  --streams quotes,edges,market_trades
```

只查某个平台：

```bash
python tools/replay_capture.py \
  --date 2026-11-13 --symbol NVDA \
  --streams quotes --venue lighter_mainnet
```

自定义 SQL，`{files}` 会替换成当天该分区的文件通配符：

```bash
python tools/replay_capture.py \
  --date 2026-11-13 --symbol NVDA \
  --sql 'select count(*) as rows from {files}'
```

脚本默认使用 `nice -n 19`、`ionice -c 3` 和 `--bwlimit 50M`，只读下载，不写采集目录、不重启任何服务。

分析机需要安装 DuckDB：

```bash
pip install duckdb
```

### 方式 1b：手动只拉需要的分区


```bash
mkdir -p ~/replay/20261113
rclone copy   r2:var-lighter-capture/market_capture/quotes/lighter_mainnet/NVDA/20261113-*.jsonl.gz   ~/replay/20261113/
```

然后用 Python / DuckDB 读 gzip JSONL。

### 方式 2：DuckDB 直接查 R2

```sql
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='<ACCOUNT_ID>.r2.cloudflarestorage.com';
SET s3_access_key_id='<ACCESS_KEY>';
SET s3_secret_access_key='<SECRET_KEY>';
SET s3_region='auto';
SET s3_url_style='path';

SELECT *
FROM read_json_auto(
  's3://var-lighter-capture/market_capture/quotes/lighter_mainnet/NVDA/20261113-*.jsonl.gz'
)
LIMIT 20;
```

凭据放在分析机的 DuckDB 配置或环境变量里，不要写进脚本或 Git。

### 方式 3：以后加 Parquet

如果回放变得频繁，建议把每小时 JSONL.gz 再转一份 Parquet。Parquet 列式压缩更好，DuckDB 查询会快很多；JSONL.gz 继续保留用于精确回放。

不要在本交易 VPS 上跑重回放；回放放到本地或单独分析机，避免和实盘抢 CPU/内存。
