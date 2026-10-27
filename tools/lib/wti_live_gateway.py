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
from math import ceil
from typing import Any

from tools.lib.hedge_instruments import WTI_HEDGE, integer_value
from tools.lib.lighter_order_identity import (
    LEGACY_INDEX_REJECTION,
    legacy_lighter_index_was_unsendable,
    lighter_client_order_index as lighter_client_order_index,
    lighter_order_identity,
    request_lighter_client_order_index,
)
from tools.lib.wti_readonly_adapter import (
    WTIReadOnlyAccountAdapter,
    WTIReadOnlyError,
    VariationalForwarderClient,
)
from tools.lib.wti_sizing import DEFAULT_MAX_EQUITY_MULTIPLE, equity_position_cap_usd


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
        max_equity_multiple: Decimal = DEFAULT_MAX_EQUITY_MULTIPLE,
    ) -> None:
        if (
            max_order_wait_seconds <= 0 or operation_timeout_seconds <= 0
            or not max_equity_multiple.is_finite()
            or max_equity_multiple <= 0 or max_equity_multiple > Decimal("5")
        ):
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
        self.max_equity_multiple = max_equity_multiple
        self.current_notional_usd = Decimal("0")
        self.proposed_notional_usd: Decimal | None = None
        self.proposed_equity_multiple: Decimal | None = None
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

    def set_proposed_notional(
        self,
        value: Decimal | None,
        *,
        equity_multiple: Decimal | None = None,
    ) -> None:
        if value is not None and (not value.is_finite() or value <= 0):
            raise ValueError("proposed WTI notional must be finite and positive")
        if equity_multiple is not None and (
            not equity_multiple.is_finite()
            or equity_multiple <= 0
            or equity_multiple > self.max_equity_multiple
        ):
            raise ValueError("proposed WTI equity multiple must be within the hard cap")
        self.proposed_notional_usd = value
        self.proposed_equity_multiple = equity_multiple if value is not None else None

    async def _read_account(self) -> dict[str, Any]:
        snapshot = await self.account_reader.read_account()
        from main import (
            account_risk_context,
            extract_lighter_account_metrics,
            extract_variational_account_metrics,
        )

        var_metrics = extract_variational_account_metrics(snapshot.get("variational_portfolio"))
        lighter_metrics = extract_lighter_account_metrics(snapshot.get("lighter_positions"))
        var_equity = _decimal(var_metrics.get("equity_usd"))
        lighter_equity = _decimal(lighter_metrics.get("equity_usd"))
        projected_wti_notional = self.current_notional_usd + (
            self.proposed_notional_usd or Decimal("0")
        )
        active_equity_multiple = (
            self.proposed_equity_multiple
            if self.proposed_notional_usd is not None
            and self.proposed_equity_multiple is not None
            else self.max_equity_multiple
        )
        equity_cap = equity_position_cap_usd(
            var_equity, lighter_equity,
            max_equity_multiple=active_equity_multiple,
        )
        risk = account_risk_context(
            variational_metrics=var_metrics,
            lighter_metrics=lighter_metrics,
            current_notional_usd=self.current_notional_usd,
            # account_risk_context treats proposed_notional as the projected total
            # target, not an increment. Keep that existing margin model unchanged.
            proposed_notional_usd=(
                projected_wti_notional if self.proposed_notional_usd is not None else None
            ),
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
                var_equity is not None and lighter_equity is not None
                and var_equity.is_finite() and lighter_equity.is_finite()
                and var_equity > 0 and lighter_equity > 0
            ),
            "wti_equity_position_cap_usd": None if equity_cap is None else str(equity_cap),
            "wti_projected_position_notional_usd": str(projected_wti_notional),
            "wti_max_equity_multiple": str(active_equity_multiple),
        })
        if not risk["risk_data_complete"]:
            risk["risk_action"] = "block_entry"
            risk["risk_reason"] = "wti_account_risk_data_incomplete"
        elif self.proposed_notional_usd is not None and (
            equity_cap is None or projected_wti_notional > equity_cap
        ):
            risk["risk_action"] = "block_entry"
            risk["risk_reason"] = "wti_equity_position_cap_exceeded"
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
                    "ok": True,
                    "final": True,
                    "client_order_id": request.get("client_order_id"),
                    "status": "rejected",
                    "filled_qty": "0",
                    "risk_reason": risk.get("risk_reason") or "wti_entry_risk_gate_blocked",
                    "error_code": "wti_entry_risk_gate_blocked",
                    "source": "local_account_risk_preflight",
                    "submission_stage": "account_risk_preflight",
                    "no_order_confirmed": True,
                    "native_client_order_id": request.get("native_client_order_id"),
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
            receipt = await self._submit_lighter(request)
            if receipt.get("ok") is not True:
                return receipt
            request.update({key: receipt[key] for key in (
                "native_client_order_id", "transaction_hash", "submission_stage",
                "submission_acknowledged",
            ) if key in receipt})
        deadline = time.monotonic() + self.max_order_wait_seconds
        status: dict[str, Any] = {}
        while time.monotonic() < deadline:
            status = await self._lookup_order(request["client_order_id"], request)
            if status.get("final") is True:
                status.setdefault("native_client_order_id", request.get("rfq_id"))
                for key in ("transaction_hash", "submission_stage", "submission_acknowledged"):
                    if key in request:
                        status.setdefault(key, request[key])
                return status
            await asyncio.sleep(0.2)
        return {
            "ok": False,
            "final": False,
            "client_order_id": request["client_order_id"],
            "status": "unknown",
            "native_client_order_id": request.get("native_client_order_id") or request.get("rfq_id"),
            **{key: request[key] for key in (
                "transaction_hash", "submission_stage", "submission_acknowledged",
            ) if key in request},
            **{key: status[key] for key in ("lookup_stage", "lookup_reason", "error_type") if key in status},
        }

    async def _submit_lighter(self, request: dict[str, Any]) -> dict[str, Any]:
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
        client_order_id = str(request["client_order_id"])
        client_index = request_lighter_client_order_index(client_order_id, request, submitting=True)
        if request.get("native_client_order_id") in (None, ""):
            request.update(lighter_order_identity(client_order_id))
        receipt = {
            "ok": False, "final": False, "status": "unknown",
            "client_order_id": client_order_id, "native_client_order_id": str(client_index),
            "submission_stage": "sdk_create_order",
        }
        _create_order, response, error = await self.lighter_client.create_order(
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
            return {**receipt, "error_code": "lighter_sdk_submission_failed", "error_type": type(error).__name__}
        response_dict = _model_dict(response)
        if response_dict is None or integer_value(response_dict.get("code")) not in {0, 200}:
            return {**receipt, "submission_stage": "send_tx_response", "error_code": "lighter_order_submission_unconfirmed"}
        # SDK CreateOrder exposes to_json(), not to_dict()/model_dump(). An accepted
        # send_tx receipt must survive independently of that signed payload object.
        return {
            **receipt, "ok": True, "submission_acknowledged": True,
            "submission_stage": "send_tx_accepted",
            "transaction_hash": response_dict.get("tx_hash"),
        }

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
        explicit_rfq = str(request.get("native_client_order_id") or request.get("rfq_id") or "")
        explicit_order_id = str(request.get("native_order_id") or "")

        def unknown(reason: str) -> dict[str, Any]:
            return {
                "ok": False, "final": False, "status": "unknown",
                "client_order_id": client_order_id,
                "native_client_order_id": explicit_rfq or None,
                "native_order_id": explicit_order_id or None,
                "lookup_stage": "variational_orders", "lookup_reason": reason,
            }

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
                return unknown("variational_orders_response_invalid")
            for row in rows:
                identity = self._var_identity(row)
                if identity == "ambiguous":
                    return unknown("variational_order_instrument_ambiguous")
                if identity not in {WTI_HEDGE.var.symbol, "WTI"}:
                    continue
                if self._var_order_matches(row, request):
                    matching.append(row)
            if len(rows) < 100:
                break
        else:
            return unknown("variational_orders_pagination_exhausted")
        if explicit_rfq:
            matching = [row for row in matching if str(self._var_order_value(row, "rfq_id") or "") == explicit_rfq]
        if explicit_order_id:
            matching = [
                row for row in matching
                if str(self._var_order_value(row, "order_id") or "") == explicit_order_id
            ]
        if len(matching) != 1:
            return unknown(
                "variational_order_not_found" if not matching
                else "variational_order_identity_ambiguous"
            )
        row = matching[0]
        status = str(self._var_order_value(row, "status") or "").strip().lower().replace("-", "_")
        filled_raw = next((self._var_order_value(row, key) for key in (
            "filled_qty", "executed_qty", "filled_amount", "executed_amount"
        ) if self._var_order_value(row, key) is not None), None)
        fill_price_raw = next((self._var_order_value(row, key) for key in (
            "average_fill_price", "average_price", "avg_fill_price", "avg_price", "fill_price"
        ) if self._var_order_value(row, key) is not None), None)
        clearing_status = self._var_order_value(row, "clearing_status")
        if status == "cleared" and clearing_status not in (None, "success_trades_booked_into_pool"):
            return unknown("variational_clearing_status_unconfirmed")
        clock_evidence: dict[str, Any] = {}
        if filled_raw is None and fill_price_raw is None:
            execution_at = _timestamp(self._var_order_value(row, "execution_timestamp"))
            # In orders-v2, successfully booked cleared records carry fills as
            # qty/price. A pending order's qty/price are not execution evidence.
            if (
                status == "cleared"
                and clearing_status == "success_trades_booked_into_pool"
                and submitted is not None
                and self._var_order_value(row, "order_id")
            ):
                if execution_at is None:
                    return unknown("variational_execution_timestamp_missing")
                if execution_at > datetime.now(timezone.utc):
                    return unknown("variational_execution_timestamp_future")
                if execution_at < submitted:
                    if not (explicit_rfq or explicit_order_id):
                        return unknown("variational_execution_clock_lead_without_exact_identity")
                    lead_seconds = (submitted - execution_at).total_seconds()
                    if lead_seconds > self.operation_timeout_seconds:
                        return unknown("variational_execution_timestamp_outside_request_window")
                    # Exact native identity and post-submit created_at were checked
                    # above. Booked records may carry an earlier exchange clock;
                    # only this stronger proof admits a lead within the RPC window.
                    clock_evidence = {
                        "source": "variational_booked_exact_native_identity",
                        "lookup_stage": "variational_orders",
                        "lookup_reason": "variational_booked_execution_clock_lead",
                        "execution_timestamp_lead_ms": ceil(lead_seconds * 1000),
                    }
                filled_raw = self._var_order_value(row, "qty")
                fill_price_raw = self._var_order_value(row, "price")
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
                "fee_evidence": self._fee_evidence(
                    row,
                    venue="variational",
                    source="variational_exact_order_record",
                    native_order_id=self._var_order_value(row, "order_id"),
                    native_client_order_id=self._var_order_value(row, "rfq_id"),
                ),
                **clock_evidence,
            }
            return result
        if status in TERMINAL_VAR_NO_FILL and qty == 0:
            return {
                "ok": True, "final": True, "client_order_id": client_order_id,
                "order_id": str(order_id or ""), "status": "canceled",
                "filled_qty": "0", "no_order_confirmed": False,
            }
        return unknown(
            "variational_order_not_terminal" if status not in TERMINAL_VAR_FILLED | TERMINAL_VAR_NO_FILL
            else "variational_fill_evidence_unverifiable"
        )

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
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _raw_fee_fields(row: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "fee_usd", "actual_fee_usd", "commission_usd", "fee", "fees",
            "commission", "commission_amount", "fee_amount", "maker_fee",
            "taker_fee", "fee_currency", "fee_asset", "fee_token", "fee_unit",
            "currency", "fee_type", "fee_rate", "maker_fee_rate", "taker_fee_rate",
        )
        values: dict[str, Any] = {}
        for key in keys:
            value = row.get(key)
            if value is None or isinstance(value, (dict, list, tuple, set)):
                continue
            if isinstance(value, (str, int, bool)):
                values[key] = value
            elif isinstance(value, float):
                values[key] = value if _decimal(value) is not None else str(value)
            else:
                values[key] = str(value)
        return values

    @classmethod
    def _fee_evidence(
        cls,
        row: dict[str, Any],
        *,
        venue: str,
        source: str,
        native_order_id: Any = None,
        native_client_order_id: Any = None,
        trade_ids: list[str] | None = None,
        fill_coverage_complete: bool | None = None,
        lookup_error_type: str | None = None,
    ) -> dict[str, Any]:
        fee_usd = cls._explicit_fee_usd(row)
        return {
            "venue": venue,
            "source": source,
            "native_order_id": None if native_order_id is None else str(native_order_id),
            "native_client_order_id": (
                None if native_client_order_id is None else str(native_client_order_id)
            ),
            "trade_ids": list(trade_ids or []),
            "fill_coverage_complete": fill_coverage_complete,
            "fee_usd": fee_usd,
            "status": "confirmed_usd" if fee_usd is not None else "unverified",
            "raw_fee_unit": "USD" if fee_usd is not None else "unverified",
            "raw_fields": cls._raw_fee_fields(row),
            "lookup_error_type": lookup_error_type,
        }

    async def _lookup_lighter(
        self, client_order_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        from lighter import OrderApi

        unknown = {"ok": False, "final": False, "status": "unknown", "client_order_id": client_order_id}
        try:
            client_index = request_lighter_client_order_index(client_order_id, request)
        except ValueError as exc:
            return {**unknown, "lookup_stage": "identity", "lookup_reason": str(exc)}
        unknown["native_client_order_id"] = str(client_index)
        if legacy_lighter_index_was_unsendable(client_order_id, request):
            return {
                **unknown, "ok": True, "final": True, "status": "rejected",
                "filled_qty": "0", "trade_ids": [], "fee_usd": "0",
                "source": "local_signer_validation", "error_code": LEGACY_INDEX_REJECTION,
                "no_order_confirmed": True, "lookup_stage": "identity",
            }
        stage = "auth"
        try:
            token, error = self.lighter_client.create_auth_token_with_expiry(
                api_key_index=self.api_key_index
            )
            if error is not None or not token:
                return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_auth_token_unavailable"}
            api = OrderApi(self.lighter_client.api_client)
            stage = "inactive_orders"
            inactive = await api.account_inactive_orders(
                account_index=self.account_index,
                limit=100,
                auth=token,
                market_id=int(self.market["market_id"]),
                _request_timeout=5.0,
            )
            inactive_rows = _rows(inactive, "orders")
            if inactive_rows is None:
                return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_inactive_orders_response_invalid"}
            matches = [row for row in inactive_rows if integer_value(row.get("client_order_index")) == client_index]
            if len(matches) > 1:
                return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_order_identity_ambiguous"}
            if not matches:
                stage = "active_orders"
                active = await api.account_active_orders(
                    account_index=self.account_index,
                    market_id=int(self.market["market_id"]),
                    auth=token,
                    _request_timeout=5.0,
                )
                active_rows = _rows(active, "orders")
                if active_rows is None:
                    return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_active_orders_response_invalid"}
                if any(
                    integer_value(row.get("client_order_index")) == client_index
                    for row in active_rows
                ):
                    return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_order_still_active"}
                return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_order_absent_from_active_and_inactive"}
            order = matches[0]
            stage = "order_record"
            if (
                integer_value(order.get("market_index")) != int(self.market["market_id"])
                or integer_value(order.get("client_order_index")) != client_index
                or not isinstance(order.get("is_ask"), bool)
                or request.get("side") not in {"buy", "sell"}
                or order["is_ask"] != (request["side"] == "sell")
            ):
                return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_order_identity_mismatch"}
            order_index = integer_value(order.get("order_index"))
            if order_index is None or order_index <= 0:
                return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_native_order_id_missing"}
            status = str(order.get("status") or "").lower()
            filled = _decimal(order.get("filled_base_amount"))
            quote = _decimal(order.get("filled_quote_amount"))
            wanted = _decimal(request.get("qty"))
            if filled is None or quote is None or wanted is None or filled < 0:
                return {**unknown, "lookup_stage": stage, "lookup_reason": "lighter_order_amounts_unverifiable"}
            if status == "filled" and filled == wanted and filled > 0 and quote > 0:
                # An unavailable fee query cannot erase a proven terminal order fill.
                trade_error = None
                try:
                    trades = await api.trades(
                        sort_by="timestamp", limit=100, auth=token,
                        market_id=int(self.market["market_id"]),
                        account_index=self.account_index,
                        order_index=order_index,
                        sort_dir="desc", _request_timeout=5.0,
                    )
                    trade_rows = _rows(trades, "trades")
                except Exception as exc:
                    trade_rows = None
                    trade_error = type(exc).__name__
                trade_ids: list[str] = []
                trade_fee_evidence: list[dict[str, Any]] = []
                fee_total = Decimal("0")
                matched_trade_qty = Decimal("0")
                all_trade_rows_match = bool(trade_rows)
                all_fees_explicit = bool(trade_rows)
                seen_trade_ids: set[str] = set()
                if trade_rows is not None:
                    for trade in trade_rows:
                        trade_id = trade.get("trade_id")
                        ask_order_id = integer_value(trade.get("ask_id"))
                        bid_order_id = integer_value(trade.get("bid_id"))
                        ask_account_id = integer_value(trade.get("ask_account_id"))
                        bid_account_id = integer_value(trade.get("bid_account_id"))
                        requested_side = request.get("side")
                        expected_order_id, expected_account_id = (
                            (ask_order_id, ask_account_id)
                            if requested_side == "sell"
                            else (bid_order_id, bid_account_id)
                        )
                        order_identity_ok = expected_order_id == order_index
                        account_identity_ok = expected_account_id == self.account_index
                        identity_ok = (
                            integer_value(trade.get("market_id")) == int(self.market["market_id"])
                            and trade_id is not None
                            and str(trade_id) not in seen_trade_ids
                            and requested_side in {"buy", "sell"}
                            and order_identity_ok
                            and account_identity_ok
                        )
                        explicit_fee = self._explicit_fee_usd(trade)
                        trade_qty = _decimal(
                            trade.get("size", trade.get("filled_qty", trade.get("base_amount")))
                        )
                        trade_fee_evidence.append({
                            "trade_id": None if trade_id is None else str(trade_id),
                            "market_id": trade.get("market_id"),
                            "ask_id": trade.get("ask_id"),
                            "bid_id": trade.get("bid_id"),
                            "ask_account_id": trade.get("ask_account_id"),
                            "bid_account_id": trade.get("bid_account_id"),
                            "requested_side": requested_side,
                            "matched_order_id": expected_order_id,
                            "matched_account_id": expected_account_id,
                            "is_maker_ask": trade.get("is_maker_ask"),
                            "maker_fee_raw": trade.get("maker_fee"),
                            "taker_fee_raw": trade.get("taker_fee"),
                            "raw_fee_fields": self._raw_fee_fields(trade),
                            "fee_usd": explicit_fee,
                            "fee_evidence_status": (
                                "confirmed_usd" if explicit_fee is not None and identity_ok
                                else "unverified"
                            ),
                            "trade_qty": None if trade_qty is None else str(trade_qty),
                            "identity_verified": identity_ok,
                            "raw_fee_unit": "unverified",
                        })
                        if not identity_ok:
                            all_trade_rows_match = False
                            continue
                        if trade_qty is None or trade_qty <= 0:
                            all_trade_rows_match = False
                        else:
                            matched_trade_qty += trade_qty
                        seen_trade_ids.add(str(trade_id))
                        trade_ids.append(str(trade_id))
                        if explicit_fee is None:
                            all_fees_explicit = False
                        else:
                            fee_value = _decimal(explicit_fee)
                            if fee_value is None:
                                all_fees_explicit = False
                            else:
                                fee_total += fee_value
                fill_coverage_complete = (
                    trade_rows is not None
                    and 0 < len(trade_rows) < 100
                    and matched_trade_qty == filled
                    and all_trade_rows_match
                )
                fee_confirmed = fill_coverage_complete and all_fees_explicit
                return {
                    **unknown, "ok": True, "final": True,
                    "order_id": str(order.get("order_index")),
                    "native_order_id": str(order.get("order_id") or order.get("order_index")),
                    "native_client_order_id": str(client_index), "status": "filled",
                    "filled_qty": str(filled), "average_price": str(quote / filled),
                    "trade_ids": trade_ids if trade_rows else None,
                    "matched_trade_qty": str(matched_trade_qty),
                    "trade_fill_coverage_complete": fill_coverage_complete,
                    "fee_usd": str(fee_total) if fee_confirmed else None,
                    "trade_fee_evidence": trade_fee_evidence if trade_rows is not None else None,
                    "fee_evidence": {
                        "venue": "lighter",
                        "source": "lighter_exact_order_trades",
                        "native_order_id": str(order_index),
                        "native_client_order_id": str(client_index),
                        "trade_ids": list(trade_ids),
                        "fill_coverage_complete": fill_coverage_complete,
                        "fee_usd": str(fee_total) if fee_confirmed else None,
                        "status": "confirmed_usd" if fee_confirmed else "unverified",
                        "raw_fee_unit": "USD" if fee_confirmed else "unverified",
                        "raw_fields_by_trade": [
                            item["raw_fee_fields"] for item in trade_fee_evidence
                        ],
                        "lookup_error_type": trade_error,
                    },
                    **({
                        "lookup_stage": "trades",
                        "lookup_reason": (
                            "lighter_trade_evidence_unavailable"
                            if not trade_rows
                            else "lighter_trade_fill_coverage_incomplete"
                            if not fill_coverage_complete
                            else "lighter_trade_fee_usd_unverified"
                        ),
                        **({"error_type": trade_error} if trade_error else {}),
                    } if not fee_confirmed else {}),
                }
            if status in TERMINAL_LIGHTER_NO_FILL and filled == 0:
                return {
                    **unknown, "ok": True, "final": True,
                    "order_id": str(order.get("order_index")), "status": "canceled",
                    "filled_qty": "0", "trade_ids": [], "fee_usd": "0",
                }
            return {
                **unknown, "ok": True, "final": True,
                "order_id": str(order.get("order_index")), "status": status,
                "filled_qty": str(filled), "trade_ids": [],
            }
        except Exception as exc:
            return {
                **unknown, "lookup_stage": stage,
                "lookup_reason": "lighter_lookup_request_failed", "error_type": type(exc).__name__,
            }
