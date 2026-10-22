"""Fail-closed session classification based on exchange-provided status."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from math import isfinite
from typing import Any
from zoneinfo import ZoneInfo

import websockets


NEW_YORK = ZoneInfo("America/New_York")
VARIATIONAL_MARKET_STATUS_WS = (
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/market_status"
)
VARIATIONAL_MARKET_STATUS_SYMBOL = "USOILP"
VARIATIONAL_MARKET_STATUS_VALUES = frozenset({"open", "closed", "halted_trading"})


@dataclass(frozen=True, slots=True)
class SessionObservation:
    session_id: str
    observed_at: datetime
    sampled_at: datetime
    closing_at: datetime


class OilSession(StrEnum):
    OPEN = "open"
    ENTRY_CUTOFF = "entry_cutoff"
    EXCHANGE_CLOSED = "exchange_closed"
    STATUS_UNKNOWN = "status_unknown"


@dataclass(frozen=True, slots=True)
class SessionWindow:
    state: OilSession
    local_time: str
    next_transition_at: str | None
    allow_new_entries: bool
    allow_position_management: bool
    reason: str
    observed_at: str | None = None
    status_max_age_seconds: float = 30
    exchange_open: bool | None = None
    entry_cutoff_at: str | None = None
    closing_at: str | None = None


def _aware_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def parse_market_status_update(
    payload: Any,
    *,
    symbol: str = VARIATIONAL_MARKET_STATUS_SYMBOL,
) -> str | None:
    """Parse the app's /market_status frame: [symbol, status] or a batch of pairs."""
    if isinstance(payload, (str, bytes, bytearray)):
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError, UnicodeDecodeError):
            return None
    if not isinstance(payload, list):
        return None
    entries = [payload] if payload and isinstance(payload[0], str) else payload
    matched: list[str] = []
    for entry in entries:
        if not isinstance(entry, list) or len(entry) < 2 or entry[0] != symbol:
            continue
        raw = entry[1]
        status = raw.strip().lower() if isinstance(raw, str) else ""
        matched.append(status if status in VARIATIONAL_MARKET_STATUS_VALUES else "unknown")
    if not matched:
        return None
    return matched[0] if all(value == matched[0] for value in matched) else "unknown"


class VariationalMarketStatusFeed:
    """Maintain the public market-status subscription; disconnects clear the status."""

    def __init__(
        self,
        *,
        symbol: str = VARIATIONAL_MARKET_STATUS_SYMBOL,
        url: str = VARIATIONAL_MARKET_STATUS_WS,
    ) -> None:
        self.symbol = symbol
        self.url = url
        self.stop = False
        self.connected = False
        self.status: str | None = None
        self.status_observed_at: datetime | None = None
        self.heartbeat_at: datetime | None = None
        self.last_frame_at: datetime | None = None
        self.error: str | None = None

    async def wait_for_status(self, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.connected and self.status in VARIATIONAL_MARKET_STATUS_VALUES:
                return True
            await asyncio.sleep(0.1)
        return False

    async def run(self) -> None:
        backoff = 1.0
        while not self.stop:
            try:
                async with websockets.connect(
                    self.url,
                    open_timeout=10,
                    close_timeout=3,
                    ping_interval=None,
                ) as socket:
                    self.connected = True
                    self.status = None
                    self.status_observed_at = None
                    self.heartbeat_at = datetime.now(timezone.utc)
                    self.error = None
                    backoff = 1.0
                    while not self.stop:
                        try:
                            raw = await asyncio.wait_for(socket.recv(), timeout=10)
                            now = datetime.now(timezone.utc)
                            self.last_frame_at = now
                            update = parse_market_status_update(raw, symbol=self.symbol)
                            if update is not None:
                                self.status = update
                                self.status_observed_at = now
                            self.heartbeat_at = now
                        except asyncio.TimeoutError:
                            pong = await socket.ping()
                            await asyncio.wait_for(pong, timeout=5)
                            self.heartbeat_at = datetime.now(timezone.utc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.status = None
                self.status_observed_at = None
                self.heartbeat_at = None
                self.error = " ".join(str(exc).split())[:160] or type(exc).__name__
                if not self.stop:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
        self.connected = False
        self.status = None
        self.heartbeat_at = None


def session_observation_for_sample(
    row: dict[str, Any],
    *,
    maximum_status_age_seconds: float = 30,
    maximum_future_skew_seconds: float = 5,
) -> SessionObservation | None:
    """Validate explicit, fresh exchange session evidence on one market sample."""
    if (
        row.get("session_status") != OilSession.OPEN.value
        or row.get("market_status") != "open"
        or row.get("session_status_source") != "variational_market_status_websocket"
        or row.get("session_schedule_source") != "variational_supported_assets_api"
        or row.get("session_status_feed_connected") is not True
    ):
        return None
    sampled_at = _aware_timestamp(row.get("logged_at"))
    observed_at = _aware_timestamp(row.get("session_status_observed_at"))
    heartbeat_at = _aware_timestamp(row.get("session_status_feed_heartbeat_at"))
    closing_at = _aware_timestamp(row.get("session_closing_at"))
    session_id = str(row.get("session_id") or "").strip()
    if (
        sampled_at is None
        or observed_at is None
        or heartbeat_at is None
        or closing_at is None
        or not session_id
        or closing_at <= sampled_at
    ):
        return None
    status_age = (sampled_at - heartbeat_at).total_seconds()
    if (
        status_age > maximum_status_age_seconds
        or status_age < -maximum_future_skew_seconds
        or observed_at > sampled_at + timedelta(seconds=maximum_future_skew_seconds)
    ):
        return None
    return SessionObservation(session_id, observed_at, sampled_at, closing_at)


def extract_exchange_session_observation(
    market_status: Any,
    trading_sessions: Any,
    *,
    observed_at: datetime,
    status_observed_at: datetime | None,
    feed_connected: bool,
    feed_heartbeat_at: datetime | None,
) -> dict[str, str | bool | None]:
    """Combine actual market_status WS data with supported_assets trading_sessions."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must include a timezone")
    observed = observed_at.astimezone(timezone.utc)
    status = str(market_status or "").strip().lower()
    if status not in VARIATIONAL_MARKET_STATUS_VALUES:
        status = "unknown"
    heartbeat = feed_heartbeat_at
    healthy = feed_connected is True
    status_observed = status_observed_at
    if (
        status_observed is None
        or status_observed.tzinfo is None
        or status_observed.utcoffset() is None
    ):
        healthy = False
        status_observed = None
    elif (
        status_observed.astimezone(timezone.utc) - observed
    ).total_seconds() > 5:
        healthy = False
    if heartbeat is None or heartbeat.tzinfo is None or heartbeat.utcoffset() is None:
        healthy = False
        heartbeat = None
    else:
        heartbeat_age = (observed - heartbeat.astimezone(timezone.utc)).total_seconds()
        if heartbeat_age > 30 or heartbeat_age < -5:
            healthy = False
    if not healthy:
        status = "unknown"

    if status == "halted_trading":
        session_status = "closed"
    elif status in {"open", "closed"}:
        session_status = status
    else:
        session_status = "unknown"
    sessions: list[tuple[datetime, datetime]] = []
    schedule_valid = isinstance(trading_sessions, list)
    if schedule_valid:
        for entry in trading_sessions:
            if not isinstance(entry, dict):
                schedule_valid = False
                break
            session_open = _aware_timestamp(entry.get("open"))
            session_close = _aware_timestamp(entry.get("close"))
            if session_open is None or session_close is None or session_close <= session_open:
                schedule_valid = False
                break
            sessions.append((session_open, session_close))
        sessions.sort()
        if any(sessions[i][1] > sessions[i + 1][0] for i in range(len(sessions) - 1)):
            schedule_valid = False

    active_sessions = [
        pair for pair in sessions if pair[0] <= observed < pair[1]
    ] if schedule_valid else []
    active = active_sessions[0] if len(active_sessions) == 1 else None
    if session_status == "open" and active is not None:
        session_open, closing_at = active
        session_id = f"{session_open.isoformat()}|{closing_at.isoformat()}"
        schedule_source: str | None = "variational_supported_assets_api"
    else:
        closing_at = None
        session_id = None
        schedule_source = None
    return {
        "market_status": status,
        "session_status": session_status,
        "session_status_observed_at": (
            None if status_observed is None or not healthy
            else status_observed.astimezone(timezone.utc).isoformat()
        ),
        "session_status_source": (
            "variational_market_status_websocket" if status != "unknown" else "unverified"
        ),
        "session_status_feed_connected": feed_connected is True,
        "session_status_feed_heartbeat_at": (
            None if heartbeat is None else heartbeat.astimezone(timezone.utc).isoformat()
        ),
        "session_schedule_source": schedule_source,
        "session_id": session_id,
        "session_closing_at": None if closing_at is None else closing_at.isoformat(),
    }


def classify_usoil_session(
    now: datetime,
    *,
    exchange_open: bool | None,
    exchange_status_observed_at: datetime | None = None,
    exchange_closing_at: datetime | None = None,
    entry_cutoff_minutes: int = 30,
    status_max_age_seconds: float = 30,
    status_future_skew_seconds: float = 5,
) -> SessionWindow:
    """Use live exchange status; do not infer holidays or hours from weekday."""
    if entry_cutoff_minutes < 0:
        raise ValueError("entry_cutoff_minutes must be non-negative")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must include a timezone")
    if (
        not isfinite(status_max_age_seconds)
        or not isfinite(status_future_skew_seconds)
        or status_max_age_seconds <= 0
        or status_future_skew_seconds < 0
    ):
        raise ValueError("status age must be positive and future skew non-negative")
    local = now.astimezone(NEW_YORK)

    def unknown(reason: str) -> SessionWindow:
        return SessionWindow(
            OilSession.STATUS_UNKNOWN,
            local.isoformat(),
            None,
            False,
            False,
            reason,
        )

    if type(exchange_open) is not bool:
        return unknown("exchange_session_status_unconfirmed")
    if (
        exchange_status_observed_at is None
        or exchange_status_observed_at.tzinfo is None
        or exchange_status_observed_at.utcoffset() is None
    ):
        return unknown("exchange_session_status_timestamp_unconfirmed")
    now_utc = now.astimezone(ZoneInfo("UTC"))
    observed_at = exchange_status_observed_at.astimezone(ZoneInfo("UTC"))
    status_age = (now_utc - observed_at).total_seconds()
    if status_age > status_max_age_seconds or status_age < -status_future_skew_seconds:
        return unknown("exchange_session_status_stale_or_future")
    if not exchange_open:
        return SessionWindow(
            OilSession.EXCHANGE_CLOSED,
            local.isoformat(),
            None,
            False,
            False,
            "exchange_reports_closed",
            observed_at.isoformat(),
            status_max_age_seconds,
            False,
        )
    if exchange_closing_at is None:
        return unknown("exchange_close_time_unconfirmed")
    if exchange_closing_at.tzinfo is None or exchange_closing_at.utcoffset() is None:
        return unknown("exchange_close_time_timezone_unconfirmed")

    minutes_to_close = (
        exchange_closing_at.astimezone(ZoneInfo("UTC")) - now_utc
    ).total_seconds() / 60
    if minutes_to_close <= 0:
        return unknown("exchange_close_time_expired")
    if minutes_to_close <= entry_cutoff_minutes:
        return SessionWindow(
            OilSession.ENTRY_CUTOFF,
            local.isoformat(),
            exchange_closing_at.astimezone(NEW_YORK).isoformat(),
            False,
            True,
            "exchange_close_within_entry_cutoff",
            observed_at.isoformat(),
            status_max_age_seconds,
            True,
            (exchange_closing_at.astimezone(timezone.utc) - timedelta(minutes=entry_cutoff_minutes)).isoformat(),
            exchange_closing_at.astimezone(timezone.utc).isoformat(),
        )
    return SessionWindow(
        OilSession.OPEN,
        local.isoformat(),
        exchange_closing_at.astimezone(NEW_YORK).isoformat(),
        True,
        True,
        "exchange_open_and_close_time_confirmed",
        observed_at.isoformat(),
        status_max_age_seconds,
        True,
        (exchange_closing_at.astimezone(timezone.utc) - timedelta(minutes=entry_cutoff_minutes)).isoformat(),
        exchange_closing_at.astimezone(timezone.utc).isoformat(),
    )
