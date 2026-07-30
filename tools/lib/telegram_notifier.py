from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import requests


TELEGRAM_EVENT_TYPES = {
    "live_inventory_entered",
    "live_inventory_exited",
    "live_inventory_final_pnl",
    "live_inventory_entry_blocked",
    "live_inventory_v4_entry_blocked",
    "live_inventory_manual_review_required",
    "live_inventory_runtime_fuse_triggered",
    "live_inventory_basis_quote_failed",
}

TELEGRAM_THROTTLED_EVENT_TYPES = {
    "live_inventory_entry_blocked",
    "live_inventory_v4_entry_blocked",
    "live_inventory_basis_quote_failed",
}


def _value(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    blocked_context = payload.get("blocked_context")
    if isinstance(blocked_context, dict):
        for key in keys:
            value = blocked_context.get(key)
            if value not in (None, ""):
                return value
    return "-"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def format_telegram_trade_message(
    event_type: str,
    payload: dict[str, Any],
) -> str:
    asset = _value(payload, "asset")
    direction = _value(payload, "direction")
    run_id = _value(payload, "run_id")
    lot_id = _value(payload, "lot_id", "cycle_id")

    if event_type == "live_inventory_entered":
        return "\n".join(
            [
                "[Var/Lighter] OPEN",
                f"asset={asset} direction={direction}",
                f"qty={_value(payload, 'qty')} lot={lot_id}",
                f"edge_bps={_value(payload, 'edge_bps')}",
            ]
        )
    if event_type == "live_inventory_exited":
        return "\n".join(
            [
                "[Var/Lighter] CLOSE",
                f"asset={asset} direction={direction}",
                f"qty={_value(payload, 'qty')} lot={lot_id}",
                f"reason={_value(payload, 'exit_reason')}",
                f"estimated_pnl_usd={_value(payload, 'pnl_usd')}",
                f"estimated_pnl_bps={_value(payload, 'pnl_bps')}",
                f"holding_seconds={_value(payload, 'holding_seconds')}",
            ]
        )
    if event_type == "live_inventory_final_pnl":
        return "\n".join(
            [
                "[Var/Lighter] FINAL PNL",
                f"asset={asset} direction={direction}",
                f"lot={lot_id} status={_value(payload, 'final_pnl_status')}",
                f"pnl_usd={_value(payload, 'final_pnl_usd')}",
                f"pnl_bps={_value(payload, 'final_pnl_bps')}",
                f"spread_capture_bps={_value(payload, 'final_spread_capture_bps')}",
            ]
        )
    if event_type in {
        "live_inventory_entry_blocked",
        "live_inventory_v4_entry_blocked",
    }:
        return "\n".join(
            [
                "[Var/Lighter] ENTRY BLOCKED",
                f"asset={asset} direction={direction}",
                f"reason={_value(payload, 'reason')}",
                f"edge_bps={_value(payload, 'edge_bps', 'short_edge_bps')}",
                "threshold_bps="
                f"{_value(payload, 'v4_entry_threshold_bps', 'entry_threshold_bps')}",
                f"suppressed_repeats={_value(payload, 'telegram_suppressed_repeats')}",
            ]
        )
    if event_type == "live_inventory_basis_quote_failed":
        return "\n".join(
            [
                "[Var/Lighter] QUOTE FAILED",
                f"asset={asset}",
                f"reason={_value(payload, 'reason', 'error')}",
                f"suppressed_repeats={_value(payload, 'telegram_suppressed_repeats')}",
            ]
        )
    if event_type == "live_inventory_manual_review_required":
        return "\n".join(
            [
                "[Var/Lighter] MANUAL REVIEW",
                f"asset={asset} reason={_value(payload, 'reason')}",
                f"open_lots={_value(payload, 'open_lots_total')}",
                f"run_id={run_id}",
            ]
        )
    if event_type == "live_inventory_runtime_fuse_triggered":
        return "\n".join(
            [
                "[Var/Lighter] RUNTIME FUSE",
                f"asset={asset} reason={_value(payload, 'reason')}",
                f"action={_value(payload, 'action')}",
                f"open_lots={_value(payload, 'open_lots_total')}",
                f"run_id={run_id}",
            ]
        )
    raise ValueError(f"Unsupported Telegram event: {event_type}")


class TelegramNotifier:
    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str,
        logger: logging.Logger,
        queue_size: int = 100,
        throttle_seconds: float = 1800.0,
    ) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.logger = logger
        self.throttle_seconds = max(0.0, throttle_seconds)
        self.enabled = bool(self.bot_token and self.chat_id)
        self.last_enqueued_at: dict[tuple[str, str, str], float] = {}
        self.suppressed_counts: dict[tuple[str, str, str], int] = {}
        self.queue: asyncio.Queue[tuple[str, dict[str, Any]] | None] = (
            asyncio.Queue(maxsize=queue_size)
        )
        self.worker_task: asyncio.Task[None] | None = None

    @classmethod
    def from_env(cls, *, logger: logging.Logger) -> "TelegramNotifier":
        return cls(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            logger=logger,
            throttle_seconds=_env_float(
                "TELEGRAM_ALERT_THROTTLE_SECONDS",
                1800.0,
            ),
        )

    async def start(self) -> None:
        if not self.enabled or (
            self.worker_task is not None and not self.worker_task.done()
        ):
            return
        self.worker_task = asyncio.create_task(
            self._worker(),
            name="telegram_notifier",
        )
        self.logger.info("telegram_notifications_enabled")

    def enqueue(self, event_type: str, payload: dict[str, Any]) -> bool:
        if not self.enabled or event_type not in TELEGRAM_EVENT_TYPES:
            return False
        if event_type in TELEGRAM_THROTTLED_EVENT_TYPES:
            throttle_key = (
                event_type,
                str(payload.get("asset") or "-"),
                str(payload.get("reason") or payload.get("error") or "-"),
            )
            now = time.monotonic()
            previous = self.last_enqueued_at.get(throttle_key)
            if (
                previous is not None
                and now - previous < self.throttle_seconds
            ):
                self.suppressed_counts[throttle_key] = (
                    self.suppressed_counts.get(throttle_key, 0) + 1
                )
                return False
            self.last_enqueued_at[throttle_key] = now
            payload = dict(payload)
            payload["telegram_suppressed_repeats"] = (
                self.suppressed_counts.pop(throttle_key, 0)
            )
        try:
            self.queue.put_nowait((event_type, dict(payload)))
        except asyncio.QueueFull:
            self.logger.warning(
                "telegram_notification_dropped event=%s reason=queue_full",
                event_type,
            )
            return False
        return True

    async def stop(self, *, flush_timeout_seconds: float = 10.0) -> None:
        task = self.worker_task
        if task is None:
            return
        try:
            await asyncio.wait_for(
                self.queue.join(),
                timeout=flush_timeout_seconds,
            )
        except TimeoutError:
            self.logger.warning("telegram_notification_flush_timeout")
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            while True:
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                else:
                    self.queue.task_done()
            self.worker_task = None
            return
        if not task.done():
            self.queue.put_nowait(None)
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.worker_task = None

    def send_now(self, text: str) -> tuple[bool, str]:
        if not self.enabled:
            return False, "missing_TELEGRAM_BOT_TOKEN_or_TELEGRAM_CHAT_ID"
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
                timeout=(3.0, 5.0),
            )
        except Exception as exc:
            return False, f"request_failed:{type(exc).__name__}"
        if response.status_code != 200:
            return False, f"http_status_{response.status_code}"
        return True, "sent"

    def discover_chat_ids(self) -> tuple[list[dict[str, str]], str]:
        if not self.bot_token:
            return [], "missing_TELEGRAM_BOT_TOKEN"
        try:
            response = requests.get(
                f"https://api.telegram.org/bot{self.bot_token}/getUpdates",
                timeout=(3.0, 5.0),
            )
        except Exception as exc:
            return [], f"request_failed:{type(exc).__name__}"
        if response.status_code != 200:
            return [], f"http_status_{response.status_code}"
        try:
            body = response.json()
        except Exception:
            return [], "invalid_json_response"
        if not body.get("ok"):
            return [], "telegram_api_error"

        chats: dict[str, dict[str, str]] = {}
        for update in body.get("result", []):
            message = (
                update.get("message")
                or update.get("edited_message")
                or update.get("channel_post")
                or {}
            )
            chat = message.get("chat") or {}
            chat_id = chat.get("id")
            if chat_id is None:
                continue
            key = str(chat_id)
            chats[key] = {
                "chat_id": key,
                "type": str(chat.get("type") or "-"),
                "name": str(
                    chat.get("username")
                    or chat.get("title")
                    or chat.get("first_name")
                    or "-"
                ),
            }
        if not chats:
            return [], "no_chat_found_send_the_bot_a_message_then_retry"
        return list(chats.values()), "found"

    async def _worker(self) -> None:
        while True:
            item = await self.queue.get()
            try:
                if item is None:
                    return
                event_type, payload = item
                try:
                    message = format_telegram_trade_message(
                        event_type,
                        payload,
                    )
                    ok, detail = await asyncio.to_thread(
                        self.send_now,
                        message,
                    )
                except Exception as exc:
                    ok = False
                    detail = f"worker_failed:{type(exc).__name__}"
                if not ok:
                    self.logger.warning(
                        "telegram_notification_failed event=%s detail=%s",
                        event_type,
                        detail,
                    )
                else:
                    self.logger.info(
                        "telegram_notification_sent event=%s",
                        event_type,
                    )
            finally:
                self.queue.task_done()
