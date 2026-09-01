# 独立账户风险看门狗

`tools/risk_wakeup_watchdog.py` 独立于主策略运行。它只读取本地状态和风险心跳，不请求交易所行情、不提交订单，也不进入开仓和平仓的延迟路径。

## 当前通知链路

- 普通风险：Telegram、Bark、飞书机器人消息。
- 紧急风险：Telegram、Bark 严重警告、飞书消息，并立即调用飞书电话加急。
- 单个通知渠道失败后每 10 秒重试；已经成功的渠道不重复发送。
- 同一事件使用固定事件键和内容签名去重。风险原因改变时视为新的紧急状态并重新报警。
- 风险恢复后发送 Bark、飞书和 Telegram 恢复通知，不再拨打电话。

飞书电话加急无法向程序返回“用户已接听”的可靠确认，因此当前设计不根据接听状态取消其他渠道。一个事件在飞书电话接口返回成功后不会重复拨号；接口失败最多重试 3 次。

## 监控范围

- 主策略停止但仍有未平仓或待确认动作。
- 风险心跳过旧，同时账户仍有仓位。
- 双边成交待确认超时。
- `manual_review_required`、双边仓位不一致、单腿提交失败且尚未重新核对。
- 维持保证金风险进入 `force_reduce` 或 `emergency_exit`。
- 账户数据短暂不可用先普通提醒；持仓期间持续 300 秒后升级为紧急风险。

主策略把已经取得的数据原子写入 `log/live_inventory_risk_health.json`。看门狗读取该文件、`log/live_inventory_state.json` 和最新关键事件，不增加交易 API 压力。

## 两个模式

看门狗安装后默认进入 `heartbeat_only`：进程持续运行并写自己的健康文件，但不判断主策略是否停止，适合更新和维护。

明确开始实盘监听：

```bash
python tools/risk_wakeup_watchdog.py --enable-strategy-monitor
```

更新策略前关闭监听，但不停止看门狗：

```bash
python tools/risk_wakeup_watchdog.py --disable-strategy-monitor
```

开关保存在 `log/risk_wakeup_watchdog_control.json`，服务重启后仍保留。

## 私密配置

Bark 配置保存到：

```text
~/.config/var-risk-alarm-a/bark.json
```

内容字段：`server`、`key`、`sound`、`volume`、`group`。

飞书配置保存到：

```text
~/.config/var-risk-alarm-a/feishu.json
```

内容字段：`app_id`、`app_secret`、`open_id`。

两个文件必须设置为当前用户私有：

```bash
chmod 600 ~/.config/var-risk-alarm-a/bark.json
chmod 600 ~/.config/var-risk-alarm-a/feishu.json
chmod 600 .env
```

不要把完整 Key、Secret 或 Open ID 放进聊天、截图、日志或 Git。

## 初始化

以下命令只补充看门狗的非敏感参数，不修改 Bark、飞书或交易凭证：

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate
python tools/setup_risk_wakeup.py
set -a
source .env
set +a
python tools/risk_wakeup_watchdog.py --check
```

默认参数：

```dotenv
RISK_WAKEUP_ENABLED=true
RISK_WAKEUP_MONITOR_STRATEGY=false
RISK_WAKEUP_ALERT_WHEN_FLAT_STRATEGY_STOPPED=true
RISK_WAKEUP_POLL_SECONDS=3
RISK_WAKEUP_HEARTBEAT_MAX_AGE_SECONDS=45
RISK_WAKEUP_PENDING_MAX_AGE_SECONDS=30
RISK_WAKEUP_DATA_UNAVAILABLE_CRITICAL_SECONDS=300
RISK_WAKEUP_CHANNEL_RETRY_SECONDS=10
RISK_WAKEUP_MAX_PHONE_ATTEMPTS=3
```

`--check` 必须显示：

```text
bark_configured=True
feishu_configured=True
configuration_ready=True
```

## 测试

先做完全不联网的逻辑测试，不消耗飞书电话额度：

```bash
python tools/risk_wakeup_watchdog.py --dry-run --test-alert
cat log/risk_wakeup_watchdog_health.json
```

再做一次真实端到端测试。它会发送一条 Bark、一个飞书消息并使用一次飞书电话加急额度：

```bash
python tools/risk_wakeup_watchdog.py --test-alert
```

预期结果：

```text
bark_test=PASS
feishu_message_test=PASS
feishu_phone_test=PASS
```

## 安装独立服务

```bash
cd ~/Repository-name-variational-v1
sudo cp deploy/risk-wakeup-watchdog.service /etc/systemd/system/risk-wakeup-watchdog.service
sudo systemctl daemon-reload
sudo systemctl enable --now risk-wakeup-watchdog.service
sudo systemctl status risk-wakeup-watchdog.service --no-pager
cat log/risk_wakeup_watchdog_health.json
```

服务独立于主策略。主策略退出、tmux 断开或远程桌面关闭不会停止看门狗；VPS 重启后由 systemd 自动恢复。确认服务正常后，再显式开启策略监听。

## 验收条件

- `heartbeat_only` 模式下停止或更新策略不会触发电话。
- 开启策略监听后，紧急测试同时收到 Bark、飞书消息和飞书电话。
- 相同事件连续轮询不会重复拨号。
- 单个渠道失败时仅重试失败渠道。
- 风险恢复后收到恢复通知且不拨电话。
- 健康文件持续更新，且不含任何密钥。
