# BTC Live Calibration Plan

## 目标

第一轮真实小额校准实验的目标不是先赚钱，而是用最小风险拿到以下真实执行数据：

- `Variational` 成交价
- `Lighter` 对冲成交价
- 提交到成交延迟
- 计划价与实际对冲成交价偏差
- 实际单笔 notional / qty
- 真实失败原因与触发阶段

当前结论是：`paper` 的最保守口径下，`BTC` 最新样本仍未证明 `4-6 bps` 有稳定净收益。因此下一阶段应优先做真实校准，而不是继续堆更多纸面参数。

## 范围

- 资产：仅 `BTC`
- 模式：`live`
- 规模：极小单，目标约 `20u`
- 节奏：低频，单次只验证 1 笔机会
- 目的：校准真实执行成本，不追求样本量和收益

## 启动前检查

1. 确认只保留一个 `main.py` 实例。
2. 确认 `log\main.instance.lock` 不是陈旧锁。
3. 确认 `.env` 中 `Lighter` 真实下单凭证有效。
4. 确认 Chrome 插件已加载，并可正常转发 Variational REST/WS。
5. 确认当前页面资产是 `BTC`。

## 建议护栏

第一轮先尽量复用现有 `live` 参数，不新增复杂逻辑。

- `--live-allowed-assets BTC`
- `--live-max-notional-usd 25`
- `--live-max-qty 0.001`
- `--live-require-min-edge-bps 6`
- `--live-cooldown-seconds 120`

说明：

- `25u` 是为了把真实下单控制在约 `20u` 附近，并给价格波动留一点余量。
- `0.001 BTC` 是额外数量护栏，避免意外放大。
- `6 bps` 不是因为纸面已经证明可赚，而是因为低于这个阈值目前更没有继续测的意义。
- `120s` 冷却是为了避免短时间连续真实下单。

## 建议启动命令

```powershell
Set-Location "D:\my project\arbitrage-system\variational-v1"; .\.venv\Scripts\python.exe main.py --mode live --confirm-live --live-allowed-assets BTC --live-max-notional-usd 25 --live-max-qty 0.001 --live-require-min-edge-bps 6 --live-cooldown-seconds 120
```

## 单笔实验执行顺序

1. 启动程序并确认 dashboard 健康状态正常。
2. 打开 Variational `BTC` 交易页面并启动插件转发。
3. 等待出现满足当前阈值的真实机会。
4. 在 Variational 手动执行小单。
5. 程序在 `Lighter` 自动对冲。
6. 只做 1 笔，先复盘，再决定是否继续第 2 笔。

第一轮建议继续采用“半自动”方式：

- 用户手动在 `Variational` 下单
- 程序只负责 `Lighter` 对冲

这样可以先把最大不确定性缩到最小，不在第一轮就引入双边同时自动 taker 的额外风险。

## 必看日志

第一轮复盘时优先看：

- `log\order_metrics.jsonl`
- `log\trade_records.csv`
- `log\runtime.log`
- `log\rest_events.jsonl`
- `log\ws_events.jsonl`

关键阶段链路应尽量完整：

- `event_received`
- `record_created`
- `variational_filled`
- `live_submit_started`
- `live_submit_sent`
- `lighter_filled`

## 每笔必须记录的字段

第一轮先保证这些字段能回看：

- 资产
- 方向
- `qty`
- `variational_filled_price`
- `lighter_filled_price`
- `variational_notional`
- `lighter_notional`
- `live_notional_usd`
- `live_edge_bps`
- `live_fill_latency_ms`
- `plan_vs_lighter_fill_diff`
- `ref_bid_vs_lighter_fill_diff`
- `ref_ask_vs_lighter_fill_diff`
- `processing_stage`
- `stage_history`
- `failure_reason`
- `record_created_at`
- `last_updated_at`

如果这轮还能额外拿到以下数据，就一起保留：

- 下单前 Variational 页面按钮价
- 下单前 Lighter order book 顶档和深度
- Variational 最终成交确认时间

## 成功标准

第一轮不是看赚不赚钱，而是看是否拿到了足够可信的真实校准数据。

满足以下条件即可算成功：

1. 成功完成至少 1 笔 `BTC` 小额真实对冲。
2. 阶段链路完整可复盘。
3. 能明确看到：
   - `Var` 成交价
   - `Lighter` 成交价
   - 延迟
   - 计划价与实际成交价偏差
4. 能据此反推：
   - `paper` 的 `fee` 是否合理
   - `paper` 的 `latency drift` 是否偏乐观/偏保守
   - `paper` 的 `Lighter depth fill` 是否接近真实

## 立即停止条件

出现以下任一情况就停止继续下单：

- 非 `BTC` 资产被触发
- 实际 notional 超出预期
- 对冲未成交或明显异常延迟
- `failure_reason` 指向风控/参数配置异常
- 看板健康状态异常
- 页面或插件转发不稳定

## 第一轮结束后要回答的问题

1. `Lighter` 实际成交相对计划价偏差有多大？
2. `live_fill_latency_ms` 大概落在什么区间？
3. 当前 `paper` 里默认 `0.5 bps` 的延迟漂移惩罚是否合理？
4. 当前 `paper` 的深度成交估算是否接近真实？
5. `BTC` 是否值得继续做第 2 轮真实校准？
6. 下一轮是否需要加入 `SOL` 作为执行质量对照样本？
