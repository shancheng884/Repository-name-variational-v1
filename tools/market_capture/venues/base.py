from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Any


class VenueHealth:
    def __init__(self, name: str) -> None:
        self.name = name
        self.status = "starting"
        self.connected = False
        self.reconnects = 0
        self.messages = 0
        self.last_message_at: datetime | None = None
        self.last_quote_at: datetime | None = None
        self.last_error: str | None = None
        self.last_error_at: datetime | None = None
        self.last_success_at: datetime | None = None
        self.success_streak = 0
        self.errors: deque[str] = deque(maxlen=5)
        self.subscribed: list[str] = []

    def error(self, message: str) -> None:
        self.last_error = str(message)[:300]
        self.last_error_at = datetime.now(timezone.utc)
        self.success_streak = 0
        self.errors.append(self.last_error)

    def message(self) -> None:
        now = datetime.now(timezone.utc)
        self.messages += 1
        self.last_message_at = now
        self.last_success_at = now
        self.success_streak += 1
        # A single heartbeat after an error is not proof of recovery.  Keep the
        # error visible until three consecutive successful messages arrive.
        if self.last_error is not None and self.success_streak >= 3:
            self.last_error = None

    def public(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        return {
            "status": self.status,
            "connected": self.connected,
            "reconnects": self.reconnects,
            "messages": self.messages,
            "last_message_age_s": round((now - self.last_message_at).total_seconds(), 3)
            if self.last_message_at else None,
            "last_quote_age_s": round((now - self.last_quote_at).total_seconds(), 3)
            if self.last_quote_at else None,
            "last_error": self.last_error,
            "last_error_at": self.last_error_at.isoformat() if self.last_error_at else None,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "success_streak": self.success_streak,
            "recent_errors": list(self.errors),
            "subscribed": list(self.subscribed),
        }
