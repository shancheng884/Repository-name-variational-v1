import asyncio
import logging

from tools.lib.telegram_notifier import (
    TelegramNotifier,
    format_telegram_trade_message,
)


def test_telegram_entry_message_contains_execution_details() -> None:
    message = format_telegram_trade_message(
        "live_inventory_entered",
        {
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "qty": "0.01",
            "lot_id": 1,
            "edge_bps": "-7.1",
            "var_price": "1800",
            "lighter_price": "1798",
            "run_id": "live-1",
            "variational_maintenance_margin_usage_pct": "12.5",
            "lighter_maintenance_margin_usage_pct": "11.0",
        },
    )

    assert "[Var/Lighter] 开仓/加仓确认" in message
    assert "资产：ETH｜方向：做空 Variational / 做多 Lighter" in message
    assert "开仓价差：-7.1 bps" in message
    assert "Variational 保证金使用率：12.5%" in message
    assert "Lighter 保证金使用率：11.0%" in message
    assert "运行编号" not in message


def test_telegram_pushes_final_pnl_and_throttles_repeated_failures() -> None:
    notifier = TelegramNotifier(
        bot_token="secret-token",
        chat_id="123",
        logger=logging.getLogger("test_telegram"),
        throttle_seconds=300,
    )

    assert notifier.enqueue("live_inventory_final_pnl", {}) is True
    assert notifier.enqueue(
        "live_inventory_basis_quote_failed",
        {"asset": "ETH", "error": "HTTP 503"},
    ) is True
    assert notifier.enqueue(
        "live_inventory_basis_quote_failed",
        {"asset": "ETH", "error": "HTTP 503"},
    ) is False
    assert notifier.enqueue(
        "live_inventory_basis_quote_failed",
        {"asset": "ETH", "error": "No extension"},
    ) is True
    notifier.throttle_seconds = 0
    assert notifier.enqueue(
        "live_inventory_basis_quote_failed",
        {"asset": "ETH", "error": "HTTP 503"},
    ) is True
    queued_event, queued_payload = notifier.queue.get_nowait()
    assert queued_event == "live_inventory_final_pnl"
    notifier.queue.task_done()
    queued_event, queued_payload = notifier.queue.get_nowait()
    assert queued_event == "live_inventory_basis_quote_failed"
    assert queued_payload["telegram_suppressed_repeats"] == 0
    notifier.queue.task_done()
    queued_event, queued_payload = notifier.queue.get_nowait()
    assert queued_event == "live_inventory_basis_quote_failed"
    notifier.queue.task_done()
    queued_event, queued_payload = notifier.queue.get_nowait()
    assert queued_event == "live_inventory_basis_quote_failed"
    assert queued_payload["telegram_suppressed_repeats"] == 1
    notifier.queue.task_done()


def test_telegram_final_pnl_message_contains_final_values() -> None:
    message = format_telegram_trade_message(
        "live_inventory_final_pnl",
        {
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "lot_id": 1,
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "final_pnl_usd": "0.01",
            "final_pnl_bps": "5.0",
            "run_id": "live-1",
            "market_gradient_tier": 2,
            "variational_margin_usage_pct": "10.1",
            "lighter_margin_usage_pct": "9.9",
        },
    )

    assert "[Var/Lighter] 档位平仓完成" in message
    assert "实际盈亏：0.01 U" in message
    assert "实际收益：5.0 bps" in message
    assert "当前市场档位：2 / 5" in message
    assert "Variational 保证金使用率：10.1%" in message
    assert "Lighter 保证金使用率：9.9%" in message


def test_telegram_account_snapshot_contains_both_venues() -> None:
    message = format_telegram_trade_message(
        "live_inventory_account_snapshot",
        {
            "asset": "ETH",
            "lot_id": 2,
            "snapshot_stage": "exit_confirmed_flat",
            "snapshot_status": "complete",
            "variational_equity_usd": "14.85",
            "lighter_equity_usd": "20.25",
            "combined_equity_usd": "35.10",
        },
    )

    assert "[Var/Lighter] 账户权益快照" in message
    assert "阶段：平仓确认" in message
    assert "Variational 权益：14.85 U" in message
    assert "Lighter 权益：20.25 U" in message
    assert "双边总权益：35.10 U" in message


def test_telegram_pnl_summary_contains_profit_and_annualized_return() -> None:
    message = format_telegram_trade_message(
        "live_inventory_pnl_summary",
        {
            "asset": "ETH",
            "lot_id": 3,
            "cycle_actual_pnl_usd": "0.005",
            "beijing_day": "2026-08-28",
            "beijing_day_actual_pnl_usd": "0.008",
            "beijing_day_return_pct": "0.02286",
            "beijing_day_completed_cycles": 2,
            "run_actual_pnl_usd": "0.012",
            "account_net_change_usd": "0.010",
            "variational_equity_usd": "14.85",
            "lighter_equity_usd": "20.25",
            "combined_equity_usd": "35.10",
            "capital_usd": "35.00",
            "return_pct": "0.02857",
            "annualized_simple_pct": "10.43",
            "annualized_reliability": "sample_under_30_days",
            "summary_status": "complete",
            "return_pnl_source": "account_equity_delta",
        },
    )

    assert "[Var/Lighter] 平仓收益汇总" in message
    assert "本轮实际盈亏：0.005 U" in message
    assert "统计日期：2026-08-28（北京时间）" in message
    assert "北京时间今日累计盈亏：0.008 U" in message
    assert "本统计周期累计盈亏：0.012 U" in message
    assert "账户权益净变化：0.01 U" in message
    assert "北京时间当日收益率：0.0229%" in message
    assert "统计周期累计收益率：0.0286%" in message
    assert "统计周期简单年化收益率：10.43%" in message
    assert "收益口径：账户权益净变化" in message
    assert "年化说明：样本不足30天，仅供参考" in message


def test_telegram_pnl_summary_formats_long_decimals_and_missing_values() -> None:
    message = format_telegram_trade_message(
        "live_inventory_pnl_summary",
        {
            "asset": "ETH",
            "lot_id": 3,
            "summary_status": "partial",
            "cycle_actual_pnl_usd": "0.0045270000000000000000000009",
            "run_actual_pnl_usd": "0.0246960000000000000000000009",
            "return_pnl_source": "confirmed_pair_fills",
            "annualized_reliability": "unavailable",
        },
    )

    assert "本轮实际盈亏：0.004527 U" in message
    assert "本统计周期累计盈亏：0.024696 U" in message
    assert "账户权益净变化：暂不可用" in message
    assert "双边总权益：暂不可用" in message
    assert "北京时间当日收益率：暂不可用" in message
    assert "统计周期累计收益率：暂不可用" in message
    assert "统计周期简单年化收益率：暂不可用" in message
    assert "年化说明：暂不可计算" in message
    assert "- U" not in message
    assert "-%" not in message


def test_telegram_skips_duplicate_exit_account_snapshot() -> None:
    notifier = TelegramNotifier(
        bot_token="secret-token",
        chat_id="123",
        logger=logging.getLogger("test_telegram"),
    )

    assert notifier.enqueue(
        "live_inventory_account_snapshot",
        {"snapshot_stage": "exit_confirmed_flat"},
    ) is False


def test_telegram_strong_single_fallback_message_contains_action() -> None:
    message = format_telegram_trade_message(
        "live_inventory_v4_strong_single_auto_disabled",
        {
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "reason": "estimated_profitable_actual_loss",
            "actual_pnl_bps": "-2.50",
            "strong_single_shortfall_reserve_bps": "6.00",
            "action": "fallback_to_latest_and_2_of_3",
            "run_id": "live-1",
        },
    )

    assert "[Var/Lighter] 平仓确认模式已降级" in message
    assert "实际收益：-2.50 bps" in message
    assert "强单次确认预留：6.00 bps" in message
    assert "动作：fallback_to_latest_and_2_of_3" in message


def test_telegram_account_risk_alert_is_chinese() -> None:
    message = format_telegram_trade_message(
        "live_inventory_account_risk_alert",
        {
            "asset": "ETH",
            "risk_reason": "venue_equity_imbalance_warning",
            "risk_action": "warning",
            "variational_equity_usd": "100",
            "lighter_equity_usd": "70",
            "equity_balance_ratio": "0.70",
            "max_projected_venue_leverage": "2.0",
            "max_maintenance_margin_usage_pct": "35",
        },
    )

    assert "[Var/Lighter] 账户风险提醒" in message
    assert "双边权益不均衡" in message
    assert "动作：仅提醒" in message
    assert "最高单边预计杠杆：2.0x / 5x" in message


def test_telegram_account_risk_throttle_distinguishes_risk_reasons() -> None:
    notifier = TelegramNotifier(
        bot_token="secret-token",
        chat_id="123",
        logger=logging.getLogger("test_telegram"),
        throttle_seconds=300,
    )

    assert notifier.enqueue(
        "live_inventory_account_risk_alert",
        {"asset": "ETH", "risk_reason": "venue_equity_imbalance_warning"},
    ) is True
    assert notifier.enqueue(
        "live_inventory_account_risk_alert",
        {"asset": "ETH", "risk_reason": "venue_equity_imbalance_warning"},
    ) is False
    assert notifier.enqueue(
        "live_inventory_account_risk_alert",
        {"asset": "ETH", "risk_reason": "maintenance_margin_usage_warning"},
    ) is True


def test_telegram_only_pushes_critical_exit_cost_block() -> None:
    notifier = TelegramNotifier(
        bot_token="secret-token",
        chat_id="123",
        logger=logging.getLogger("test_telegram"),
        throttle_seconds=300,
    )

    assert notifier.enqueue(
        "live_inventory_exit_blocked",
        {
            "asset": "ETH",
            "reason": "basis_exit_refresh_pnl_below_threshold",
        },
    ) is False
    assert notifier.enqueue(
        "live_inventory_exit_blocked",
        {
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "reason": "entry_final_fill_cost_pending",
            "pnl_bps": "4.37",
            "effective_min_exit_pnl_bps": "1.50",
        },
    ) is True
    assert notifier.enqueue(
        "live_inventory_exit_blocked",
        {
            "asset": "ETH",
            "reason": "entry_final_fill_cost_pending",
        },
    ) is False

    event_type, payload = notifier.queue.get_nowait()
    message = format_telegram_trade_message(event_type, payload)
    assert "[Var/Lighter] 平仓条件未满足" in message
    assert "原因：entry_final_fill_cost_pending" in message
    assert "当前预计收益：4.37 bps" in message


def test_telegram_worker_sends_queued_event_without_exposing_token(
    monkeypatch,
) -> None:
    async def run() -> None:
        calls = []

        class Response:
            status_code = 200

        def fake_post(url, **kwargs):
            calls.append((url, kwargs))
            return Response()

        monkeypatch.setattr(
            "tools.lib.telegram_notifier.requests.post",
            fake_post,
        )
        notifier = TelegramNotifier(
            bot_token="secret-token",
            chat_id="123",
            logger=logging.getLogger("test_telegram"),
        )

        await notifier.start()
        queued = notifier.enqueue(
            "live_inventory_final_pnl",
            {
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "pnl_usd": "0.01",
                "run_id": "live-1",
            },
        )
        await notifier.stop()

        assert queued is True
        assert len(calls) == 1
        assert calls[0][1]["json"]["chat_id"] == "123"

    asyncio.run(run())


def test_telegram_suppresses_internal_gradient_noise() -> None:
    notifier = TelegramNotifier(
        bot_token="secret-token",
        chat_id="123",
        logger=logging.getLogger("test_telegram"),
    )

    assert notifier.enqueue(
        "live_inventory_entry_blocked",
        {"asset": "ETH", "reason": "v4_real_gradient_tier_capacity_reached"},
    ) is False
    assert notifier.enqueue(
        "live_inventory_v4_entry_blocked",
        {"asset": "ETH", "reason": "v4_waiting_for_episode_rearm"},
    ) is False
    assert notifier.enqueue(
        "live_inventory_exited",
        {"asset": "ETH", "exit_reason": "v4_tier_net_target_reached"},
    ) is False
    assert notifier.enqueue(
        "live_inventory_pnl_summary",
        {"asset": "ETH", "account_snapshot_flat": False},
    ) is False


def test_discover_chat_ids_deduplicates_updates(monkeypatch) -> None:
    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "ok": True,
                "result": [
                    {
                        "message": {
                            "chat": {
                                "id": 123,
                                "type": "private",
                                "first_name": "Trader",
                            }
                        }
                    },
                    {
                        "message": {
                            "chat": {
                                "id": 123,
                                "type": "private",
                                "first_name": "Trader",
                            }
                        }
                    },
                ],
            }

    monkeypatch.setattr(
        "tools.lib.telegram_notifier.requests.get",
        lambda *args, **kwargs: Response(),
    )
    notifier = TelegramNotifier(
        bot_token="secret-token",
        chat_id="",
        logger=logging.getLogger("test_telegram"),
    )

    chats, detail = notifier.discover_chat_ids()

    assert detail == "found"
    assert chats == [
        {"chat_id": "123", "type": "private", "name": "Trader"}
    ]


def test_invalid_throttle_env_does_not_break_runtime(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "secret-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("TELEGRAM_ALERT_THROTTLE_SECONDS", "invalid")

    notifier = TelegramNotifier.from_env(
        logger=logging.getLogger("test_telegram")
    )

    assert notifier.enabled is True
    assert notifier.throttle_seconds == 1800.0


def test_request_failure_is_contained(monkeypatch) -> None:
    def fail_post(*args, **kwargs):
        raise TimeoutError("network unavailable")

    monkeypatch.setattr(
        "tools.lib.telegram_notifier.requests.post",
        fail_post,
    )
    notifier = TelegramNotifier(
        bot_token="secret-token",
        chat_id="123",
        logger=logging.getLogger("test_telegram"),
    )

    ok, detail = notifier.send_now("test")

    assert ok is False
    assert detail == "request_failed:TimeoutError"
