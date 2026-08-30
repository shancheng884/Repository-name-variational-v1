# 夜间账户风险叫醒系统

`tools/risk_wakeup_watchdog.py` 是独立于主策略的只读监控进程。它不会请求交易所行情、不会提交订单，也不会进入开仓和平仓的延迟路径。

## 监控范围

- 主策略停止但本地仍有未平仓或待确认动作。
- 风险心跳过旧，同时账户仍有仓位。
- 双边成交待确认超时。
- `manual_review_required`、双边仓位不一致、单腿提交失败且尚未成功重新核对。
- 维持保证金风险进入 `force_reduce` 或 `emergency_exit`。
- `warning` 和 `block_entry` 只发普通提醒，不触发电话。

主策略原有账户风险循环把已经取得的数据写入 `log/live_inventory_risk_health.json`。这只是一次小型本地原子写入，不增加任何交易所请求。watchdog 读取该文件、`log/live_inventory_state.json` 和最新关键事件。

同一次故障若同时命中多个条件，只建立一个紧急事件，避免多组 Pushover 铃声和电话同时触发。

## 通知顺序

1. 紧急事件立即发送 Pushover Emergency。Pushover 按 `retry` 周期重复提醒，直到用户确认或达到 `expire`。
2. watchdog 轮询 Emergency 回执。用户已确认后，不再拨打电话。
3. 未确认且持续超过升级时间时，腾讯云语音电话开始拨打。默认只在北京时间 `23:00-08:00` 启用电话。
4. 风险恢复后，watchdog 取消尚未确认的 Pushover Emergency，并发送恢复消息。

Pushover Emergency 参数遵循官方约束：`retry` 不低于 30 秒，`expire` 不高于 10800 秒。腾讯云使用 `SendTtsVoice`，号码使用 E.164 格式，单次播放次数不高于 3 次。

- Pushover API：<https://pushover.net/api>
- 腾讯云语音通知 `SendTtsVoice`：<https://cloud.tencent.com/document/product/1128/51558>
- 腾讯云 Python SDK：<https://cloud.tencent.com/document/product/1128/37716>

## 需要准备

### Pushover

1. 在 Pushover 创建应用，取得应用 Token。
2. 从账户页面取得 User Key。
3. 手机安装 Pushover、登录并允许紧急通知、声音和后台运行。

### 腾讯云语音通知

1. 开通语音消息服务并完成适用的账号认证。
2. 创建语音应用，取得 `VoiceSdkAppid`。
3. 提交并通过一个 TTS 模板。模板参数按顺序接收资产和风险说明。
4. 准备接听号码，使用 `+86...` 形式。

服务是否可用以及模板审核要求以腾讯云控制台当前显示为准。

## VPS 安全配置

所有密钥只写到 VPS 的 `.env`。不要在聊天、截图、Git 提交或命令输出中发送密钥。推荐使用本地交互向导；敏感字段不会回显，全部校验通过后才会原子更新文件，并自动设置权限为 `600`：

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate
python tools/setup_risk_wakeup.py
```

向导只更新本节列出的风险叫醒字段，不改变现有交易所、钱包和 Telegram 配置。已配置的字段可直接留空保留。

如需手工配置：

```bash
cd ~/Repository-name-variational-v1
nano .env
chmod 600 .env
```

加入以下配置，等号右侧由用户在 VPS 本地填写：

```dotenv
RISK_WAKEUP_ENABLED=true
RISK_WAKEUP_ALERT_WHEN_FLAT_STRATEGY_STOPPED=true
RISK_WAKEUP_POLL_SECONDS=5
RISK_WAKEUP_HEARTBEAT_MAX_AGE_SECONDS=45
RISK_WAKEUP_PENDING_MAX_AGE_SECONDS=30
RISK_WAKEUP_DATA_UNAVAILABLE_CRITICAL_SECONDS=300
RISK_WAKEUP_VOICE_ESCALATION_SECONDS=120
RISK_WAKEUP_VOICE_REPEAT_SECONDS=900
RISK_WAKEUP_MAX_VOICE_CALLS=3
RISK_WAKEUP_VOICE_ONLY_AT_NIGHT=true
RISK_WAKEUP_NIGHT_START=23:00
RISK_WAKEUP_NIGHT_END=08:00

PUSHOVER_APP_TOKEN=
PUSHOVER_USER_KEY=
PUSHOVER_DEVICE=
PUSHOVER_RETRY_SECONDS=60
PUSHOVER_EXPIRE_SECONDS=1800
PUSHOVER_EMERGENCY_SOUND=siren

TENCENTCLOUD_SECRET_ID=
TENCENTCLOUD_SECRET_KEY=
TENCENT_VMS_REGION=ap-guangzhou
TENCENT_VMS_SDK_APP_ID=
TENCENT_VMS_TEMPLATE_ID=
TENCENT_VMS_CALLED_NUMBER=+86
TENCENT_VMS_PLAY_TIMES=2
```

watchdog 只在健康文件中记录各渠道是否配置完成，不记录 Token、Secret、完整手机号。

## 分阶段端到端测试

安装最新依赖并检查配置：

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate
pip install -r requirements.txt
set -a
source .env
set +a
python tools/risk_wakeup_watchdog.py --check
```

预期 watchdog、Pushover、腾讯云语音配置、腾讯云 SDK 和
`configuration_ready` 均为 `True`。任一项缺失时，正式守护服务拒绝伪装成
“已保护”状态。

先进行完全不联网的逻辑测试：

```bash
python tools/risk_wakeup_watchdog.py --dry-run --test-alert
cat log/risk_wakeup_watchdog_health.json
```

再发真实 Pushover Emergency，不拨电话：

```bash
python tools/risk_wakeup_watchdog.py --test-alert
```

命令必须显示 `pushover_test=PASS`。确认手机会持续发声，然后在 Pushover
中点确认。用以下命令现场验证回执被 watchdog 收到，并且电话未被触发：

```bash
python tools/risk_wakeup_watchdog.py --test-alert --wait-for-ack-seconds 180 --include-voice
```

在 180 秒内点击 Pushover 确认，命令必须显示
`pushover_ack_test=PASS` 和 `tencent_voice_suppression_test=PASS`。

最后显式测试一次语音电话：

```bash
python tools/risk_wakeup_watchdog.py --test-alert --include-voice
```

命令必须同时显示 `pushover_test=PASS` 和 `tencent_voice_test=PASS`。

测试消息会明确标注“不代表真实账户风险”。只有以上两项都收到，才进入守护服务部署。

## 安装独立服务

```bash
cd ~/Repository-name-variational-v1
sudo cp deploy/risk-wakeup-watchdog.service /etc/systemd/system/risk-wakeup-watchdog.service
sudo systemctl daemon-reload
sudo systemctl enable --now risk-wakeup-watchdog.service
sudo systemctl status risk-wakeup-watchdog.service --no-pager
cat log/risk_wakeup_watchdog_health.json
```

服务独立于主策略。主策略退出、tmux 断开或远程桌面关闭都不会自动停止 watchdog。VPS 重启后由 systemd 自动恢复。

## 验收条件

- 主策略正常且空仓时无紧急通知。
- 测试紧急事件能触发 Pushover Emergency 重复提醒。
- Pushover 确认后不会升级语音电话。
- 未确认测试可显式触发腾讯云语音电话。
- 恢复后 Emergency 被取消并收到恢复消息。
- 健康文件持续更新，且不含任何密钥和完整手机号。
