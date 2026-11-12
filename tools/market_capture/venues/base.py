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
        self.errors: deque[str] = deque(maxlen=5)
        self.subscribed: list[str] = []

    def error(self, message: str) -> None:
        self.last_error = str(message)[:300]
        self.errors.append(self.last_error)

    def message(self) -> None:
        self.messages += 1
        self.last_message_at = datetime.now(timezone.utc)

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
            "recent_errors": list(self.errors),
            "subscribed": list(self.subscribed),
        }
