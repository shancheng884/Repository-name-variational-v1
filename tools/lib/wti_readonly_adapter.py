"""Read-only WTI account adapter; deliberately has no order submission API."""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from tools.lib.hedge_instruments import (
    WTI_HEDGE,
    decimal_value,
    integer_value,
    lighter_position_qty,
    variational_position_qty,
)


class WTIReadOnlyError(RuntimeError):
    """A read failed or its response could not prove the requested WTI state."""


class VariationalCommandClient(Protocol):
    async def request(self, payload: dict[str, Any]) -> dict[str, Any]: ...


class LighterAccountReader(Protocol):
    async def read_account_and_active_orders(
        self, market_id: int
    ) -> tuple[dict[str, Any], dict[str, Any]]: ...


def _as_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if hasattr(value, "to_dict"):
        converted = value.to_dict()
        return converted if isinstance(converted, dict) else None
    if hasattr(value, "model_dump"):
        converted = value.model_dump(mode="json")
        return converted if isinstance(converted, dict) else None
    return None


def _require_var_response(message: Any, *, expected_type: str, key: str) -> Any:
    if not isinstance(message, dict) or message.get("type") != expected_type:
        raise WTIReadOnlyError("variational_command_response_type_mismatch")
    result = message.get("result")
    if message.get("ok") is not True or not isinstance(result, dict) or result.get("ok") is not True:
        raise WTIReadOnlyError("variational_read_failed")
    status = integer_value(result.get("httpStatus"))
    if status is None or not 200 <= status < 300:
        raise WTIReadOnlyError("variational_read_http_status_invalid")
    if key not in result:
        raise WTIReadOnlyError(f"variational_{key}_missing")
    return result[key]


def _order_rows(payload: Any) -> tuple[dict[str, Any], ...] | None:
    current = payload
    for _ in range(10):
        if isinstance(current, list):
            return tuple(current) if all(isinstance(row, dict) for row in current) else None
        if not isinstance(current, dict):
            return None
        next_value = None
        for key in ("orders", "result", "data"):
            if key in current:
                next_value = current[key]
                break
        if next_value is None:
            return None
        current = next_value
    return None


def _var_order_underlying(order: dict[str, Any]) -> str:
    candidates: list[Any] = [
        order.get("underlying"),
        order.get("symbol"),
        order.get("asset"),
        order.get("market"),
    ]
    instrument = order.get("instrument")
    if isinstance(instrument, dict):
        candidates.extend(
            instrument.get(key) for key in ("underlying", "symbol", "asset", "market")
        )
    elif isinstance(instrument, str):
        token = instrument.strip().upper()
        if re.search(r"(^|[-_/])USOILP($|[-_/])", token):
            candidates.append("USOILP")
        parts = token.split("-")
        if (
            "USOILP" not in candidates
            and len(parts) >= 3
            and parts[0] in {"P", "S", "SWAP", "PERP"}
        ):
            candidates.append(parts[1])
    order_info = order.get("order_info")
    if isinstance(order_info, dict):
        candidates.extend(
            order_info.get(key) for key in ("underlying", "symbol", "asset", "market")
        )
        nested = order_info.get("instrument")
        if isinstance(nested, dict):
            candidates.extend(
                nested.get(key) for key in ("underlying", "symbol", "asset", "market")
            )
    named = [str(value).strip().upper() for value in candidates if value not in (None, "")]
    if not named:
        raise WTIReadOnlyError("variational_order_instrument_unidentifiable")
    distinct = set(named)
    if len(distinct) != 1:
        raise WTIReadOnlyError("variational_order_instrument_ambiguous")
    return named[0]


async def fetch_variational_wti_pending_orders(
    client: VariationalCommandClient,
    *,
    timeout_ms: int = 10000,
    page_size: int = 100,
    max_pages: int = 50,
) -> tuple[dict[str, Any], ...]:
    """Read all pending orders; unknown instrument rows make the result unusable."""
    if timeout_ms <= 0 or page_size <= 0 or max_pages <= 0:
        raise ValueError("pagination and timeout limits must be positive")
    matched: list[dict[str, Any]] = []
    for page in range(max_pages):
        request_id = uuid.uuid4().hex
        message = await client.request({
            "type": "VAR_API_ORDERS",
            "requestId": request_id,
            "status": "pending",
            "limit": page_size,
            "offset": page * page_size,
            "orderBy": "created_at",
            "order": "desc",
            "requestTimeoutMs": timeout_ms,
        })
        if not isinstance(message, dict) or str(message.get("requestId")) != request_id:
            raise WTIReadOnlyError("variational_orders_request_id_mismatch")
        payload = _require_var_response(
            message, expected_type="VAR_API_ORDERS_RESULT", key="orders"
        )
        rows = _order_rows(payload)
        if rows is None:
            raise WTIReadOnlyError("variational_orders_shape_unrecognized")
        for row in rows:
            if _var_order_underlying(row) == WTI_HEDGE.var.symbol:
                matched.append(row)
        if len(rows) < page_size:
            return tuple(matched)
    raise WTIReadOnlyError("variational_orders_pagination_limit_reached")


def validate_lighter_market_metadata(
    payload: Any,
    *,
    expected_market_id: int | None = None,
) -> dict[str, Any]:
    root = _as_dict(payload)
    rows = root.get("order_books") if root else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise WTIReadOnlyError("lighter_orderbooks_shape_unrecognized")
    matches = [row for row in rows if str(row.get("symbol") or "").upper() == "WTI"]
    if len(matches) != 1:
        raise WTIReadOnlyError("lighter_wti_market_missing_or_ambiguous")
    market = matches[0]
    market_id = integer_value(market.get("market_id"))
    size_decimals = integer_value(market.get("supported_size_decimals"))
    multiplier = decimal_value(market.get("multiplier"))
    min_base = decimal_value(market.get("min_base_amount"))
    min_quote = decimal_value(market.get("min_quote_amount"))
    if (
        market_id is None
        or market_id < 0
        or size_decimals is None
        or size_decimals < 0
        or multiplier is None
        or multiplier <= 0
        or min_base is None
        or min_base < 0
        or min_quote is None
        or min_quote < 0
        or str(market.get("status") or "").lower() != "active"
        or str(market.get("market_type") or "").lower() != "perp"
        or market.get("is_frozen") is True
    ):
        raise WTIReadOnlyError("lighter_wti_market_metadata_invalid")
    if expected_market_id is not None and market_id != expected_market_id:
        raise WTIReadOnlyError("lighter_wti_market_id_changed")
    return {
        "symbol": "WTI",
        "market_id": market_id,
        "market_type": "perp",
        "status": "active",
        "multiplier": multiplier,
        "supported_size_decimals": size_decimals,
        "min_base_amount": min_base,
        "min_quote_amount": min_quote,
    }


def normalize_lighter_account(payload: Any) -> dict[str, Any]:
    account = _as_dict(payload)
    if account is None:
        raise WTIReadOnlyError("lighter_account_response_invalid")
    code = account.get("code")
    if code is not None and integer_value(code) not in {0, 200}:
        raise WTIReadOnlyError("lighter_account_read_failed")
    if not isinstance(account.get("accounts"), list) or not account["accounts"]:
        raise WTIReadOnlyError("lighter_account_rows_missing")
    normalized = dict(account)
    normalized["code"] = 200
    return normalized


def normalize_lighter_active_orders(payload: Any, *, market_id: int) -> tuple[dict[str, Any], ...]:
    response = _as_dict(payload)
    if response is None:
        raise WTIReadOnlyError("lighter_active_orders_response_invalid")
    code = response.get("code")
    if code is not None and integer_value(code) not in {0, 200}:
        raise WTIReadOnlyError("lighter_active_orders_read_failed")
    orders = response.get("orders")
    if not isinstance(orders, list) or any(not isinstance(order, dict) for order in orders):
        raise WTIReadOnlyError("lighter_active_orders_shape_unrecognized")
    output: list[dict[str, Any]] = []
    for order in orders:
        raw_market_id = order.get("market_id", order.get("market_index"))
        if raw_market_id is not None and integer_value(raw_market_id) != market_id:
            raise WTIReadOnlyError("lighter_active_order_market_id_mismatch")
        symbol = str(order.get("symbol") or "").strip().upper()
        if symbol and symbol != WTI_HEDGE.lighter.symbol:
            raise WTIReadOnlyError("lighter_active_order_symbol_mismatch")
        output.append(dict(order))
    return tuple(output)


class WTIReadOnlyAccountAdapter:
    """Collect fresh positions and scoped active orders; provides no trading methods."""

    def __init__(
        self,
        *,
        variational: VariationalCommandClient,
        lighter: LighterAccountReader,
        market: dict[str, Any],
        clock: Any = lambda: datetime.now(timezone.utc),
        max_read_window_seconds: float = 15,
        request_timeout_ms: int = 10000,
    ) -> None:
        self.variational = variational
        self.lighter = lighter
        self.market = validate_lighter_market_metadata({"order_books": [market]})
        self.clock = clock
        self.max_read_window_seconds = max_read_window_seconds
        self.request_timeout_ms = request_timeout_ms
        if (
            not math.isfinite(max_read_window_seconds)
            or max_read_window_seconds <= 0
            or request_timeout_ms <= 0
        ):
            raise ValueError("read window and timeout must be positive")

    def _now_utc(self) -> datetime:
        value = self.clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise WTIReadOnlyError("account_snapshot_clock_timezone_missing")
        return value.astimezone(timezone.utc)

    async def read_account(self) -> dict[str, Any]:
        started = self._now_utc()
        position_id = uuid.uuid4().hex
        position_message = await self.variational.request({
            "type": "VAR_API_POSITIONS",
            "requestId": position_id,
            "requestTimeoutMs": self.request_timeout_ms,
        })
        if not isinstance(position_message, dict) or str(position_message.get("requestId")) != position_id:
            raise WTIReadOnlyError("variational_positions_request_id_mismatch")
        raw_positions = _require_var_response(
            position_message,
            expected_type="VAR_API_POSITIONS_RESULT",
            key="positions",
        )
        var_positions = {"ok": True, "result": raw_positions}
        var_qty = variational_position_qty(var_positions, WTI_HEDGE)
        if var_qty is None:
            raise WTIReadOnlyError("variational_wti_position_unverifiable")

        var_orders = await fetch_variational_wti_pending_orders(
            self.variational,
            timeout_ms=self.request_timeout_ms,
        )
        lighter_positions_raw, lighter_orders_raw = await self.lighter.read_account_and_active_orders(
            self.market["market_id"]
        )
        lighter_positions = normalize_lighter_account(lighter_positions_raw)
        lighter_orders = normalize_lighter_active_orders(
            lighter_orders_raw,
            market_id=self.market["market_id"],
        )
        lighter_qty = lighter_position_qty(
            lighter_positions,
            market_id=self.market["market_id"],
            symbol=WTI_HEDGE.lighter.symbol,
        )
        if lighter_qty is None:
            raise WTIReadOnlyError("lighter_wti_position_unverifiable")

        completed = self._now_utc()
        window = (completed - started).total_seconds()
        if window < 0 or window > self.max_read_window_seconds:
            raise WTIReadOnlyError("account_snapshot_read_window_exceeded")
        active_orders = [
            {"venue": "variational", "order": order} for order in var_orders
        ] + [{"venue": "lighter", "order": order} for order in lighter_orders]
        return {
            "observed_at": completed.isoformat(),
            "read_started_at": started.isoformat(),
            "read_window_ms": round(window * 1000, 3),
            "market": {
                key: str(value) if isinstance(value, Decimal) else value
                for key, value in self.market.items()
            },
            "variational_positions": var_positions,
            "lighter_positions": lighter_positions,
            "active_orders": {"ok": True, "orders": active_orders},
            "normalized": {
                "variational_qty": str(var_qty),
                "lighter_qty": str(lighter_qty),
                "active_order_count": len(active_orders),
            },
        }


class VariationalForwarderClient:
    """One-request-at-a-time client for the existing local command forwarder."""

    def __init__(self, url: str, timeout_seconds: float = 15) -> None:
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.websocket: Any = None

    async def __aenter__(self) -> "VariationalForwarderClient":
        try:
            import websockets
        except ModuleNotFoundError as exc:
            raise WTIReadOnlyError("websockets_dependency_missing") from exc
        self.websocket = await websockets.connect(
            self.url,
            ping_interval=20,
            ping_timeout=20,
            open_timeout=min(5, self.timeout_seconds),
        )
        return self

    async def __aexit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        if self.websocket is not None:
            await self.websocket.close()
            self.websocket = None

    async def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.websocket is None:
            raise WTIReadOnlyError("variational_forwarder_not_connected")
        request_id = str(payload.get("requestId") or "")
        if not request_id:
            raise WTIReadOnlyError("variational_request_id_missing")
        await self.websocket.send(json.dumps(payload, ensure_ascii=True))
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WTIReadOnlyError("variational_forwarder_timeout")
            try:
                raw = await asyncio.wait_for(self.websocket.recv(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise WTIReadOnlyError("variational_forwarder_timeout") from exc
            try:
                message = json.loads(raw)
            except (TypeError, ValueError) as exc:
                raise WTIReadOnlyError("variational_forwarder_invalid_json") from exc
            if isinstance(message, dict) and str(message.get("requestId")) == request_id:
                return message
