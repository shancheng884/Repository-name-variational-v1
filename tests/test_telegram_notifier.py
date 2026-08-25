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
        },
    )

    assert "[Var/Lighter] OPEN" in message
    assert "asset=ETH direction=short_var_long_lighter" in message
    assert "edge_bps=-7.1" in message
    assert "run_id=" not in message


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
        },
    )

    assert "[Var/Lighter] FINAL PNL" in message
    assert "pnl_usd=0.01" in message
    assert "pnl_bps=5.0" in message


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

    assert "[Var/Lighter] ACCOUNT SNAPSHOT" in message
    assert "stage=exit_confirmed_flat" in message
    assert "variational_equity_usd=14.85" in message
    assert "lighter_equity_usd=20.25" in message
    assert "combined_equity_usd=35.10" in message


def test_telegram_pnl_summary_contains_profit_and_annualized_return() -> None:
    message = format_telegram_trade_message(
        "live_inventory_pnl_summary",
        {
            "asset": "ETH",
            "lot_id": 3,
            "cycle_actual_pnl_usd": "0.005",
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
    assert "本次运行累计盈亏：0.012 U" in message
    assert "账户权益净变化：0.01 U" in message
    assert "简单年化收益率：10.43%" in message
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
    assert "本次运行累计盈亏：0.024696 U" in message
    assert "账户权益净变化：暂不可用" in message
    assert "双边总权益：暂不可用" in message
    assert "本次运行收益率：暂不可用" in message
    assert "简单年化收益率：暂不可用" in message
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

    assert "[Var/Lighter] EXIT MODE FALLBACK" in message
    assert "actual_pnl_bps=-2.50" in message
    assert "strong_reserve_bps=6.00" in message
    assert "action=fallback_to_latest_and_2_of_3" in message


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
    assert "[Var/Lighter] EXIT BLOCKED" in message
    assert "reason=entry_final_fill_cost_pending" in message
    assert "pnl_bps=4.37" in message


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
            "live_inventory_exited",
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
