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
