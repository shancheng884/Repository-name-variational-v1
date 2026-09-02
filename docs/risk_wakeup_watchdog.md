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

## VPS B 备用报警

本机看门狗只读取本机文件。VPS B 不能直接复制本服务后期待它看到 VPS A 的状态；B
需要运行独立的 `tools/risk_wakeup_backup.py` 接收 A 的签名心跳。B 不需要交易所
API、Variational 浏览器、Lighter 私钥或任何下单配置。

心跳只携带运行摘要：节点状态、仓位层数、待确认动作、风险动作以及 A 端关键通知
渠道的送达状态。请求使用共享随机令牌进行 HMAC 签名，B 拒绝错误节点、篡改内容和
过期请求。优先使用 VPS 内网、WireGuard/Tailscale 或 HTTPS 反向代理；如果暂时使用
公网 HTTP，必须把 B 的端口防火墙限制为仅允许 VPS A 的公网地址访问。

### B 端配置

在 B 端只安装代码和通知配置，不复制 A 的完整 `.env`：

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate

cat >> .env <<'EOF'
RISK_WAKEUP_BACKUP_ENABLED=true
RISK_WAKEUP_BACKUP_BIND=0.0.0.0
RISK_WAKEUP_BACKUP_PORT=8769
RISK_WAKEUP_BACKUP_TOKEN=CHANGE_ME_TO_THE_SAME_RANDOM_TOKEN
RISK_WAKEUP_BACKUP_NODE_ID=vps-a
RISK_WAKEUP_BACKUP_MAX_AGE_SECONDS=45
RISK_WAKEUP_BACKUP_DELIVERY_GRACE_SECONDS=15
RISK_WAKEUP_BACKUP_POLL_SECONDS=3
EOF
chmod 600 .env
python tools/risk_wakeup_backup.py --check
```

将 A 端的 Bark/飞书私密 JSON 通过安全方式复制到 B，并在 B 的 `.env` 中设置
`BARK_CONFIG_FILE`、`FEISHU_CONFIG_FILE`；只复制 Telegram 的通知变量，不复制交易
凭证。两个 JSON 文件和 `.env` 均必须是 `600` 权限。

安装 B 端服务：

```bash
sudo cp deploy/systemd/risk-wakeup-backup.service \
  /etc/systemd/system/risk-wakeup-backup.service
sudo systemctl daemon-reload
sudo systemctl enable --now risk-wakeup-backup.service
sudo systemctl status risk-wakeup-backup.service --no-pager
```

放行端口时只允许 A 访问，例如：

```bash
sudo ufw allow from VPS_A_PUBLIC_IP to any port 8769 proto tcp
```

### A 端配置

在 A 端 `.env` 增加 B 的心跳地址和同一个随机令牌。地址必须指向固定的 B
端点，例如 `https://backup.example.com/v1/risk-heartbeat`：

```dotenv
RISK_WAKEUP_BACKUP_URL=https://backup.example.com/v1/risk-heartbeat
RISK_WAKEUP_BACKUP_TOKEN=同一个随机令牌
RISK_WAKEUP_BACKUP_NODE_ID=vps-a
RISK_WAKEUP_BACKUP_INTERVAL_SECONDS=10
RISK_WAKEUP_BACKUP_TIMEOUT_SECONDS=4
```

重启 A 端看门狗即可，不要重启交易策略：

```bash
sudo systemctl restart risk-wakeup-watchdog.service
python tools/risk_wakeup_watchdog.py --check
```

### 验收

先在 B 端运行 `python tools/risk_wakeup_backup.py --check`，再让 A 端看门狗运行约
20 秒。B 的 `log/risk_wakeup_remote_heartbeat.json` 应出现最近心跳。然后在 B 端执行：

```bash
python tools/risk_wakeup_backup.py --once
```

必须没有备用事件。模拟 A 停止发送后，超过 `45` 秒 B 才发送一次备用紧急报警；A
恢复后发送恢复通知。同一事件不会重复拨打超过 `RISK_WAKEUP_MAX_PHONE_ATTEMPTS`。

B 端服务只监听心跳和发送报警，永远不运行 `main.py`、`tools/live.py` 或行情采集器。

## 验收条件

- `heartbeat_only` 模式下停止或更新策略不会触发电话。
- 开启策略监听后，紧急测试同时收到 Bark、飞书消息和飞书电话。
- 相同事件连续轮询不会重复拨号。
- 单个渠道失败时仅重试失败渠道。
- 风险恢复后收到恢复通知且不拨电话。
- 健康文件持续更新，且不含任何密钥。
