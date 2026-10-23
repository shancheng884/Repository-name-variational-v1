"""Production WTI venue adapters with explicit native-order reconciliation.

The gateway intentionally treats an accepted submission as unknown until an
exchange order record proves a terminal full fill or terminal zero fill.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from concurrent.futures import Future, TimeoutError as FutureTimeout
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from tools.lib.hedge_instruments import WTI_HEDGE, integer_value
from tools.lib.wti_readonly_adapter import (
    WTIReadOnlyAccountAdapter,
    WTIReadOnlyError,
    VariationalForwarderClient,
)


VARIATIONAL_RESULT_TYPES = {
    "VAR_API_ORDER": "VAR_API_ORDER_RESULT",
    "VAR_API_ORDERS": "VAR_API_ORDERS_RESULT",
}
TERMINAL_VAR_NO_FILL = {"canceled", "cancelled", "rejected", "expired"}
TERMINAL_VAR_FILLED = {"cleared", "filled", "executed", "complete", "completed"}
# The orders-v2 endpoint accepts the extension's established status filter;
# additional aliases such as "cancelled" and "filled" make it return HTTP 400.
VARIATIONAL_ORDERS_STATUS_FILTER = "pending,canceled,cleared,rejected"
TERMINAL_LIGHTER_NO_FILL = {
    "canceled", "canceled-post-only", "canceled-reduce-only",
    "canceled-position-not-allowed", "canceled-margin-not-allowed",
    "canceled-too-much-slippage", "canceled-not-enough-liquidity",
    "canceled-self-trade", "canceled-expired", "canceled-oco",
    "canceled-child", "canceled-liquidation", "canceled-invalid-balance",
}


def _decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def lighter_client_order_index(client_order_id: str) -> int:
    digest = hashlib.sha256(client_order_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1) or 1


def _model_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    for method_name in ("to_dict", "model_dump"):
        method = getattr(value, method_name, None)
        if callable(method):
            converted = method() if method_name == "to_dict" else method(mode="json")
            return converted if isinstance(converted, dict) else None
    return None


def _rows(value: Any, key: str) -> list[dict[str, Any]] | None:
    root = _model_dict(value)
    if root is None or integer_value(root.get("code")) not in {0, 200}:
        return None
    items = root.get(key)
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        return None
    return items


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        try:
            numeric = float(value)
            if numeric > 100_000_000_000:
                numeric /= 1000
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


class WTIProductionGateway:
    """Synchronous executor gateway; async venue clients stay on one event loop."""

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        variational: VariationalForwarderClient,
        account_reader: WTIReadOnlyAccountAdapter,
        lighter_client: Any,
        account_index: int,
        api_key_index: int,
        market: dict[str, Any],
        max_order_wait_seconds: float = 4.0,
        operation_timeout_seconds: float = 12.0,
    ) -> None:
        if max_order_wait_seconds <= 0 or operation_timeout_seconds <= 0:
            raise ValueError("order and operation timeouts must be positive")
        self.loop = loop
        self.variational = variational
        self.account_reader = account_reader
        self.lighter_client = lighter_client
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.market = dict(market)
        self.max_order_wait_seconds = max_order_wait_seconds
        self.operation_timeout_seconds = operation_timeout_seconds
        self.current_notional_usd = Decimal("0")
        self.proposed_notional_usd: Decimal | None = None
        self.latest_risk: dict[str, Any] = {}

    def _call(self, coroutine: Any) -> dict[str, Any]:
        if self.loop.is_closed() or not self.loop.is_running():
            coroutine.close()
            raise WTIReadOnlyError("wti_gateway_event_loop_unavailable")
        future: Future[dict[str, Any]] = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        try:
            result = future.result(timeout=self.operation_timeout_seconds)
        except FutureTimeout as exc:
            future.cancel()
            raise WTIReadOnlyError("wti_gateway_operation_timeout") from exc
        if not isinstance(result, dict):
            raise WTIReadOnlyError("wti_gateway_result_invalid")
        return result

    def read_account(self) -> dict[str, Any]:
        return self._call(self._read_account())

    def submit_order(self, request: dict[str, Any]) -> dict[str, Any]:
        return self._call(self._submit_order(dict(request)))

    def lookup_order(
        self, client_order_id: str, request: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self._call(self._lookup_order(client_order_id, dict(request or {})))

    def set_current_notional(self, value: Decimal) -> None:
        if not value.is_finite() or value < 0:
            raise ValueError("current WTI notional must be finite and non-negative")
        self.current_notional_usd = value

    def set_proposed_notional(self, value: Decimal | None) -> None:
        if value is not None and (not value.is_finite() or value <= 0):
            raise ValueError("proposed WTI notional must be finite and positive")
        self.proposed_notional_usd = value

    async def _read_account(self) -> dict[str, Any]:
        snapshot = await self.account_reader.read_account()
        from main import (
            account_risk_context,
            extract_lighter_account_metrics,
            extract_variational_account_metrics,
        )

        var_metrics = extract_variational_account_metrics(snapshot.get("variational_portfolio"))
        lighter_metrics = extract_lighter_account_metrics(snapshot.get("lighter_positions"))
        risk = account_risk_context(
            variational_metrics=var_metrics,
            lighter_metrics=lighter_metrics,
            current_notional_usd=self.current_notional_usd,
            proposed_notional_usd=self.proposed_notional_usd,
            max_venue_leverage=Decimal("5"),
            margin_warning_pct=Decimal("40"),
            margin_block_entry_pct=Decimal("50"),
            margin_reduce_pct=Decimal("60"),
            margin_emergency_pct=Decimal("75"),
            balance_warning_ratio=Decimal("0.82"),
            balance_block_ratio=Decimal("0.74"),
        )
        risk.update({
            "observed_at": snapshot["observed_at"],
            "variational_initial_margin_usage_pct": var_metrics.get("initial_margin_usage_pct"),
            "variational_maintenance_margin_usage_pct": var_metrics.get("maintenance_margin_usage_pct"),
            "lighter_initial_margin_usage_pct": lighter_metrics.get("initial_margin_usage_pct"),
            "lighter_maintenance_margin_usage_pct": lighter_metrics.get("maintenance_margin_usage_pct"),
            "risk_data_complete": (
                var_metrics.get("equity_usd") is not None
                and lighter_metrics.get("equity_usd") is not None
            ),
        })
        if not risk["risk_data_complete"]:
            risk["risk_action"] = "block_entry"
            risk["risk_reason"] = "wti_account_risk_data_incomplete"
        self.latest_risk = risk
        return {**snapshot, "risk": risk}

    async def _submit_order(self, request: dict[str, Any]) -> dict[str, Any]:
        if request.get("venue") not in {"variational", "lighter"}:
            return {"ok": False, "final": False, "client_order_id": request.get("client_order_id")}
        if request.get("reduce_only") is not True:
            account = await self._read_account()
            risk = account.get("risk") or {}
            observed_at = _timestamp(risk.get("observed_at"))
            age = None if observed_at is None else (datetime.now(timezone.utc) - observed_at).total_seconds()
            if (
                risk.get("risk_action") not in {"normal", "warning"}
                or risk.get("risk_data_complete") is not True
                or age is None or age < -5 or age > 15
            ):
                return {
                    "ok": False,
                    "final": True,
                    "client_order_id": request.get("client_order_id"),
                    "status": "rejected",
                    "filled_qty": "0",
                    "risk_reason": risk.get("risk_reason") or "wti_entry_risk_gate_blocked",
                }
        if request["venue"] == "variational":
            request_id = str(request["client_order_id"])
            message = await self.variational.request({
                "type": "VAR_API_ORDER",
                "requestId": request_id,
                "market": WTI_HEDGE.var.symbol,
                "instrumentType": WTI_HEDGE.var.instrument_type,
                "settlementAsset": WTI_HEDGE.var.settlement_asset,
                "kind": WTI_HEDGE.var.kind,
                "fundingIntervalS": WTI_HEDGE.var.funding_interval_s,
                "side": request["side"].upper(),
                "amount": str(request["qty"]),
                "confirm": True,
                "maxSlippage": float(Decimal(str(request["max_slippage_bps"])) / Decimal("10000")),
                "reduceOnly": request["reduce_only"] is True,
                "requestTimeoutMs": int(self.operation_timeout_seconds * 1000),
                "operationTimeoutMs": int(self.operation_timeout_seconds * 1000),
            })
            if (
                not isinstance(message, dict)
                or message.get("type") != VARIATIONAL_RESULT_TYPES["VAR_API_ORDER"]
                or str(message.get("requestId")) != request_id
                or message.get("ok") is not True
                or not isinstance(message.get("result"), dict)
                or message["result"].get("ok") is not True
            ):
                return {
                    "ok": False,
                    "final": False,
                    "client_order_id": request_id,
                    "status": "unknown",
                    "response_error": (message.get("error") if isinstance(message, dict) else None),
                }
            result = message["result"]
            http_status = integer_value(result.get("httpStatus"))
            if http_status is None or not 200 <= http_status < 300:
                return {
                    "ok": False, "final": False, "client_order_id": request_id,
                    "status": "unknown",
                }
            rfq_id = result.get("rfqId")
            if rfq_id not in (None, ""):
                request["rfq_id"] = str(rfq_id)
        else:
            await self._submit_lighter(request)
        deadline = time.monotonic() + self.max_order_wait_seconds
        while time.monotonic() < deadline:
            status = await self._lookup_order(request["client_order_id"], request)
            if status.get("final") is True:
                status.setdefault("native_client_order_id", request.get("rfq_id"))
                return status
            await asyncio.sleep(0.2)
        return {
            "ok": False,
            "final": False,
            "client_order_id": request["client_order_id"],
            "status": "unknown",
            "native_client_order_id": request.get("rfq_id"),
        }

    async def _submit_lighter(self, request: dict[str, Any]) -> None:
        from lighter import OrderApi

        size_decimals = int(self.market["supported_size_decimals"])
        price_decimals = int(self.market["supported_price_decimals"])
        base_units = Decimal(str(request["qty"])) * (Decimal(10) ** size_decimals)
        price = Decimal(str(request["limit_price"]))
        price_units = price * (Decimal(10) ** price_decimals)
        if (
            base_units <= 0
            or base_units != base_units.to_integral_value()
            or price_units != price_units.to_integral_value()
        ):
            raise WTIReadOnlyError("lighter_order_precision_invalid")
        base_amount = int(base_units)
        client_index = lighter_client_order_index(str(request["client_order_id"]))
        create_order, response, error = await self.lighter_client.create_order(
            market_index=int(self.market["market_id"]),
            client_order_index=client_index,
            base_amount=base_amount,
            price=int(price_units),
            is_ask=request["side"] == "sell",
            order_type=self.lighter_client.ORDER_TYPE_MARKET,
            time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
            order_expiry=self.lighter_client.DEFAULT_IOC_EXPIRY,
            reduce_only=request["reduce_only"] is True,
            api_key_index=self.api_key_index,
        )
        if error is not None:
            raise WTIReadOnlyError("lighter_order_submission_rejected")
        response_dict = _model_dict(response)
        if response_dict is None or integer_value(response_dict.get("code")) not in {0, 200}:
            raise WTIReadOnlyError("lighter_order_submission_unconfirmed")
        order_dict = _model_dict(create_order)
        if order_dict is None:
            raise WTIReadOnlyError("lighter_order_intent_response_invalid")

    async def _lookup_order(
        self, client_order_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        venue = str(request.get("venue") or client_order_id.rsplit("-", 1)[-1])
        if venue == "variational":
            return await self._lookup_variational(client_order_id, request)
        if venue == "lighter":
            return await self._lookup_lighter(client_order_id, request)
        return {"ok": False, "final": False, "client_order_id": client_order_id}

    async def _lookup_variational(
        self, client_order_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        submitted = _timestamp(request.get("submitted_at") or request.get("intent_at"))
        created_at_gte = None if submitted is None else submitted.isoformat()
        matching: list[dict[str, Any]] = []
        for offset in range(0, 1000, 100):
            request_id = f"lookup-{hashlib.sha256((client_order_id + str(offset)).encode()).hexdigest()[:24]}"
            message = await self.variational.request({
                "type": "VAR_API_ORDERS",
                "requestId": request_id,
                "status": VARIATIONAL_ORDERS_STATUS_FILTER,
                "createdAtGte": created_at_gte,
                "limit": 100,
                "offset": offset,
                "orderBy": "created_at",
                "order": "desc",
                "requestTimeoutMs": int(self.operation_timeout_seconds * 1000),
            })
            rows = self._var_order_rows(message, request_id)
            if rows is None:
                return {"ok": False, "final": False, "client_order_id": client_order_id}
            for row in rows:
                identity = self._var_identity(row)
                if identity == "ambiguous":
                    return {"ok": False, "final": False, "client_order_id": client_order_id}
                if identity not in {WTI_HEDGE.var.symbol, "WTI"}:
                    continue
                if self._var_order_matches(row, request):
                    matching.append(row)
            if len(rows) < 100:
                break
        explicit_rfq = str(request.get("native_client_order_id") or request.get("rfq_id") or "")
        explicit_order_id = str(request.get("native_order_id") or "")
        if explicit_rfq:
            matching = [row for row in matching if str(self._var_order_value(row, "rfq_id") or "") == explicit_rfq]
        if explicit_order_id:
            matching = [
                row for row in matching
                if str(self._var_order_value(row, "order_id") or "") == explicit_order_id
            ]
        if len(matching) != 1:
            return {"ok": False, "final": False, "client_order_id": client_order_id}
        row = matching[0]
        status = str(self._var_order_value(row, "status") or "").strip().lower().replace("-", "_")
        filled_raw = next((self._var_order_value(row, key) for key in (
            "filled_qty", "executed_qty", "filled_amount", "executed_amount"
        ) if self._var_order_value(row, key) is not None), None)
        fill_price_raw = next((self._var_order_value(row, key) for key in (
            "average_fill_price", "average_price", "avg_fill_price", "avg_price", "fill_price"
        ) if self._var_order_value(row, key) is not None), None)
        qty = _decimal(filled_raw)
        price = _decimal(fill_price_raw)
        order_id = self._var_order_value(row, "order_id") or self._var_order_value(row, "rfq_id")
        if status in TERMINAL_VAR_FILLED and qty is not None and qty > 0 and price is not None and price > 0 and order_id:
            result = {
                "ok": True, "final": True, "client_order_id": client_order_id,
                "order_id": str(order_id), "native_order_id": str(self._var_order_value(row, "order_id") or ""),
                "native_client_order_id": str(self._var_order_value(row, "rfq_id") or ""),
                "status": "filled", "filled_qty": str(qty), "average_price": str(price),
                "trade_ids": [], "fee_usd": self._explicit_fee_usd(row),
            }
            return result
        if status in TERMINAL_VAR_NO_FILL and qty == 0:
            return {
                "ok": True, "final": True, "client_order_id": client_order_id,
                "order_id": str(order_id or ""), "status": "canceled",
                "filled_qty": "0", "no_order_confirmed": False,
            }
        return {"ok": False, "final": False, "client_order_id": client_order_id}

    @staticmethod
    def _var_order_rows(message: Any, expected_id: str) -> list[dict[str, Any]] | None:
        if (
            not isinstance(message, dict)
            or message.get("requestId") != expected_id
            or message.get("type") != VARIATIONAL_RESULT_TYPES["VAR_API_ORDERS"]
            or message.get("ok") is not True
        ):
            return None
        result = message.get("result")
        if not isinstance(result, dict) or result.get("ok") is not True:
            return None
        payload = result.get("orders")
        for _ in range(8):
            if isinstance(payload, list):
                return payload if all(isinstance(row, dict) for row in payload) else None
            if not isinstance(payload, dict):
                return None
            payload = payload.get("orders", payload.get("result", payload.get("data")))
        return None

    @staticmethod
    def _var_identity(row: dict[str, Any]) -> str | None:
        keys = ("underlying", "symbol", "asset", "market")
        values = [row.get(key) for key in keys]
        containers = [row.get("order_info"), row.get("instrument")]
        for container in containers:
            if isinstance(container, dict):
                values.extend(container.get(key) for key in keys)
            elif isinstance(container, str):
                token = container.strip().upper()
                if token in {"USOILP", "WTI", "ETH", "BTC", "SOL"}:
                    values.append(token)
                else:
                    parts = token.replace("_", "-").replace("/", "-").split("-")
                    if len(parts) >= 2 and parts[0] in {"P", "S", "SWAP", "PERP"}:
                        values.append(parts[1])
        order_info = row.get("order_info")
        if isinstance(order_info, dict) and isinstance(order_info.get("instrument"), dict):
            values.extend(order_info["instrument"].get(key) for key in keys)
        found = {str(value).strip().upper() for value in values if value not in (None, "")}
        found = {value for value in found if value in {"USOILP", "WTI", "ETH", "BTC", "SOL"}}
        if found == {"USOILP", "WTI"}:
            return WTI_HEDGE.var.symbol
        if len(found) == 1:
            identity = next(iter(found))
            return WTI_HEDGE.var.symbol if identity == "WTI" else identity
        return "ambiguous" if len(found) > 1 else None

    @staticmethod
    def _var_order_value(row: dict[str, Any], key: str) -> Any:
        value = row.get(key)
        if value is not None:
            return value
        order_info = row.get("order_info")
        if isinstance(order_info, dict):
            value = order_info.get(key)
            if value is not None:
                return value
            nested_order = order_info.get("order")
            if isinstance(nested_order, dict):
                return nested_order.get(key)
        instrument = row.get("instrument")
        if isinstance(instrument, dict):
            return instrument.get(key)
        return None

    @staticmethod
    def _var_order_matches(row: dict[str, Any], request: dict[str, Any]) -> bool:
        side = str(WTIProductionGateway._var_order_value(row, "side") or "").lower()
        qty_raw = next((WTIProductionGateway._var_order_value(row, key) for key in (
            "qty", "quantity", "base_amount", "amount", "requested_qty"
        ) if WTIProductionGateway._var_order_value(row, key) is not None), None)
        qty = _decimal(qty_raw)
        wanted_qty = _decimal(request.get("qty"))
        wanted_side = str(request.get("side") or "").lower()
        created = _timestamp(
            WTIProductionGateway._var_order_value(row, "created_at")
            or WTIProductionGateway._var_order_value(row, "createdAt")
        )
        submitted = _timestamp(request.get("submitted_at") or request.get("intent_at"))
        return (
            side in {wanted_side, wanted_side.upper()}
            and qty is not None and wanted_qty is not None and qty == wanted_qty
            and created is not None and submitted is not None
            and submitted <= created <= datetime.now(timezone.utc)
        )

    @staticmethod
    def _explicit_fee_usd(row: dict[str, Any]) -> str | None:
        for key in ("fee_usd", "actual_fee_usd", "commission_usd"):
            value = _decimal(row.get(key))
            if value is not None and value >= 0:
                return str(value)
        return None

    async def _lookup_lighter(
        self, client_order_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        from lighter import OrderApi

        token, error = self.lighter_client.create_auth_token_with_expiry(
            api_key_index=self.api_key_index
        )
        if error is not None or not token:
            return {"ok": False, "final": False, "client_order_id": client_order_id}
        client_index = lighter_client_order_index(client_order_id)
        api = OrderApi(self.lighter_client.api_client)
        try:
            inactive = await api.account_inactive_orders(
                account_index=self.account_index,
                limit=100,
                auth=token,
                market_id=int(self.market["market_id"]),
                _request_timeout=5.0,
            )
            inactive_rows = _rows(inactive, "orders")
            if inactive_rows is None:
                return {"ok": False, "final": False, "client_order_id": client_order_id}
            matches = [row for row in inactive_rows if integer_value(row.get("client_order_index")) == client_index]
            if len(matches) > 1:
                return {"ok": False, "final": False, "client_order_id": client_order_id}
            if not matches:
                active = await api.account_active_orders(
                    account_index=self.account_index,
                    market_id=int(self.market["market_id"]),
                    auth=token,
                    _request_timeout=5.0,
                )
                active_rows = _rows(active, "orders")
                if active_rows is None or any(
                    integer_value(row.get("client_order_index")) == client_index
                    for row in active_rows
                ):
                    return {"ok": False, "final": False, "client_order_id": client_order_id}
                return {"ok": False, "final": False, "client_order_id": client_order_id}
            order = matches[0]
            if (
                integer_value(order.get("market_index")) != int(self.market["market_id"])
                or integer_value(order.get("client_order_index")) != client_index
                or bool(order.get("is_ask")) != (request.get("side") == "sell")
            ):
                return {"ok": False, "final": False, "client_order_id": client_order_id}
            status = str(order.get("status") or "").lower()
            filled = _decimal(order.get("filled_base_amount"))
            quote = _decimal(order.get("filled_quote_amount"))
            wanted = _decimal(request.get("qty"))
            if filled is None or quote is None or wanted is None or filled < 0:
                return {"ok": False, "final": False, "client_order_id": client_order_id}
            if status == "filled" and filled == wanted and filled > 0 and quote > 0:
                trades = await api.trades(
                    sort_by="timestamp", limit=100, auth=token,
                    market_id=int(self.market["market_id"]),
                    account_index=self.account_index,
                    order_index=integer_value(order.get("order_index")),
                    sort_dir="asc", _request_timeout=5.0,
                )
                trade_rows = _rows(trades, "trades")
                trade_ids: list[str] = []
                fee_total = Decimal("0")
                fee_confirmed = bool(trade_rows)
                if trade_rows is not None:
                    for trade in trade_rows:
                        if integer_value(trade.get("market_id")) != int(self.market["market_id"]):
                            fee_confirmed = False
                            continue
                        trade_ids.append(str(trade.get("trade_id")))
                        explicit_fee = _decimal(trade.get("fee_usd"))
                        if explicit_fee is None:
                            fee_confirmed = False
                        else:
                            fee_total += explicit_fee
                return {
                    "ok": True, "final": True, "client_order_id": client_order_id,
                    "order_id": str(order.get("order_index")),
                    "native_order_id": str(order.get("order_id") or order.get("order_index")),
                    "native_client_order_id": str(client_index), "status": "filled",
                    "filled_qty": str(filled), "average_price": str(quote / filled),
                    "trade_ids": trade_ids, "fee_usd": str(fee_total) if fee_confirmed else None,
                }
            if status in TERMINAL_LIGHTER_NO_FILL and filled == 0:
                return {
                    "ok": True, "final": True, "client_order_id": client_order_id,
                    "order_id": str(order.get("order_index")), "status": "canceled",
                    "filled_qty": "0", "trade_ids": [], "fee_usd": "0",
                }
            return {
                "ok": True, "final": True, "client_order_id": client_order_id,
                "order_id": str(order.get("order_index")), "status": status,
                "filled_qty": str(filled), "trade_ids": [],
            }
        except Exception:
            return {"ok": False, "final": False, "client_order_id": client_order_id}
