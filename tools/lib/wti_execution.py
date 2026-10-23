"""Fail-closed WTI pair execution coordinator with injectable venue adapters.

This module deliberately contains no exchange client. Production wiring must
provide an adapter that confirms final fills and exact WTI account snapshots.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from copy import deepcopy
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from math import isfinite
from pathlib import Path
from typing import Any, Callable, Iterator, Protocol

from tools.lib.hedge_instruments import (
    HedgeInstrument,
    WTI_HEDGE,
    decimal_value,
    integer_value,
    lighter_position_qty,
    paired_exposure_matches,
    paired_realized_pnl,
    variational_position_qty,
)
from tools.lib.market_sessions import (
    OilSession,
    SessionWindow,
    session_observation_for_sample,
)
from tools.lib.lighter_order_identity import (
    LEGACY_INDEX_REJECTION,
    legacy_lighter_index_was_unsendable,
    lighter_order_identity,
)


SCHEMA_VERSION = 2
VALID_DIRECTIONS = {"long_var_short_lighter", "short_var_long_lighter"}
ALLOWED_STATES = {"flat", "pending", "open", "manual_review_required"}


class WTIExecutionError(RuntimeError):
    """An unsafe or unverified execution condition; no blind retry is allowed."""


class ExecutionBusy(WTIExecutionError):
    pass


class CorruptExecutionState(WTIExecutionError):
    pass


class WTIExecutionGateway(Protocol):
    """Adapter contract: timestamps reflect completed live reads, never cache time."""

    def read_account(self) -> dict[str, Any]: ...

    def submit_order(self, request: dict[str, Any]) -> dict[str, Any]: ...

    def lookup_order(
        self, client_order_id: str, request: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class MarketContext:
    instrument: HedgeInstrument
    market_id: int
    multiplier: Decimal
    session_id: str
    session_observed_at: datetime
    session_closing_at: datetime
    lighter_qty_step: Decimal
    lighter_price_decimals: int
    var_bid: Decimal
    var_ask: Decimal
    lighter_bid: Decimal
    lighter_ask: Decimal
    lighter_buy_price: Decimal
    lighter_sell_price: Decimal
    quoted_var_qty: Decimal
    quoted_lighter_qty: Decimal
    lighter_min_base: Decimal
    lighter_min_quote: Decimal
    sampled_at: datetime
    validated_at: datetime
    var_quote_age_seconds: Decimal
    lighter_book_age_seconds: Decimal

    @property
    def position_tolerance(self) -> Decimal:
        return max(self.lighter_qty_step * self.multiplier / Decimal("2"), Decimal("0.00000001"))

    @property
    def lighter_position_tolerance(self) -> Decimal:
        return self.lighter_qty_step / Decimal("2")


def _positive_decimal(value: Any, name: str) -> Decimal:
    parsed = decimal_value(value)
    if parsed is None or parsed <= 0:
        raise WTIExecutionError(f"invalid_{name}")
    return parsed


def _sample_time(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise WTIExecutionError("invalid_market_sample_timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WTIExecutionError("market_sample_timestamp_timezone_missing")
    return parsed.astimezone(timezone.utc)


def validate_market_sample(
    sample: dict[str, Any],
    *,
    expected_lighter_market_id: int,
    expected_lighter_multiplier: Decimal,
    expected_lighter_size_decimals: int,
    expected_lighter_price_decimals: int | None = None,
    now: datetime | None = None,
    max_sample_age_seconds: float = 20,
    max_quote_age_seconds: float = 2,
    max_book_age_seconds: float = 2,
) -> MarketContext:
    """Validate one collector row as read-only evidence; it never places orders."""
    if not isinstance(sample, dict):
        raise WTIExecutionError("market_sample_invalid")
    if (
        not isfinite(max_sample_age_seconds)
        or not isfinite(max_quote_age_seconds)
        or not isfinite(max_book_age_seconds)
        or max_sample_age_seconds <= 0
        or max_quote_age_seconds <= 0
        or max_book_age_seconds <= 0
    ):
        raise ValueError("market sample age limits must be finite and positive")
    if sample.get("asset") != "WTI" or sample.get("sample_quality") != "valid":
        raise WTIExecutionError("market_sample_not_valid_wti")
    if sample.get("market_availability") != "both_feeds_live":
        raise WTIExecutionError("market_feeds_not_both_live")
    session = session_observation_for_sample(sample)
    if session is None:
        raise WTIExecutionError("exchange_session_observation_unverified")
    if (
        sample.get("variational_underlying") != WTI_HEDGE.var.symbol
        or sample.get("variational_instrument_type") != WTI_HEDGE.var.instrument_type
        or sample.get("variational_instrument_kind") != WTI_HEDGE.var.kind
        or str(sample.get("variational_funding_interval_s")) != str(WTI_HEDGE.var.funding_interval_s)
        or sample.get("lighter_symbol") != WTI_HEDGE.lighter.symbol
    ):
        raise WTIExecutionError("wti_instrument_identity_mismatch")
    if str(sample.get("lighter_market_type") or "").lower() != WTI_HEDGE.lighter.instrument_type:
        raise WTIExecutionError("lighter_wti_market_type_mismatch")
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    current_utc = current.astimezone(timezone.utc)
    age = (current_utc - _sample_time(sample.get("logged_at"))).total_seconds()
    if age < -5 or age > max_sample_age_seconds:
        raise WTIExecutionError("market_sample_stale_or_future")

    sampled_at = _sample_time(sample.get("logged_at"))
    elapsed = Decimal(str(max(0.0, age)))
    quote_age_at_sample = decimal_value(sample.get("var_quote_age_seconds"))
    book_age_at_sample = decimal_value(sample.get("lighter_book_age_seconds"))
    quote_age = None if quote_age_at_sample is None else quote_age_at_sample + elapsed
    book_age = None if book_age_at_sample is None else book_age_at_sample + elapsed
    if quote_age is None or quote_age_at_sample < -1 or quote_age > Decimal(str(max_quote_age_seconds)):
        raise WTIExecutionError("variational_quote_stale_or_invalid")
    if book_age is None or book_age_at_sample < 0 or book_age > Decimal(str(max_book_age_seconds)):
        raise WTIExecutionError("lighter_book_stale_or_invalid")

    if str(sample.get("lighter_market_status") or "").lower() != "active":
        raise WTIExecutionError("lighter_market_not_confirmed_active")
    market_id = integer_value(sample.get("lighter_market_id"))
    decimals = integer_value(sample.get("lighter_supported_size_decimals"))
    price_decimals = integer_value(sample.get("lighter_supported_price_decimals"))
    if (
        market_id is None
        or market_id < 0
        or decimals is None
        or decimals < 0
        or market_id != expected_lighter_market_id
        or decimals != expected_lighter_size_decimals
        or price_decimals is None
        or price_decimals < 0
        or (
            expected_lighter_price_decimals is not None
            and price_decimals != expected_lighter_price_decimals
        )
    ):
        raise WTIExecutionError("lighter_market_identity_or_precision_invalid")
    multiplier = _positive_decimal(sample.get("lighter_multiplier"), "lighter_multiplier")
    if multiplier != expected_lighter_multiplier:
        raise WTIExecutionError("lighter_market_multiplier_mismatch")
    qty_step = Decimal(1).scaleb(-decimals)
    var_bid = _positive_decimal(sample.get("var_bid"), "var_bid")
    var_ask = _positive_decimal(sample.get("var_ask"), "var_ask")
    lighter_bid = _positive_decimal(sample.get("lighter_bid"), "lighter_bid")
    lighter_ask = _positive_decimal(sample.get("lighter_ask"), "lighter_ask")
    lighter_buy_price = _positive_decimal(sample.get("lighter_buy_price"), "lighter_buy_price")
    lighter_sell_price = _positive_decimal(sample.get("lighter_sell_price"), "lighter_sell_price")
    quoted_var_qty = _positive_decimal(sample.get("variational_qty"), "variational_qty")
    quoted_lighter_qty = _positive_decimal(sample.get("lighter_qty"), "lighter_qty")
    if var_bid > var_ask or lighter_bid > lighter_ask:
        raise WTIExecutionError("crossed_market_sample")
    instrument = replace(
        WTI_HEDGE,
        lighter=replace(WTI_HEDGE.lighter, exposure_multiplier=multiplier),
    )
    min_quote = decimal_value(sample.get("lighter_min_quote_amount"))
    if min_quote is None or min_quote < 0:
        raise WTIExecutionError("lighter_min_quote_invalid")
    return MarketContext(
        instrument=instrument,
        market_id=market_id,
        multiplier=multiplier,
        session_id=session.session_id,
        session_observed_at=_sample_time(sample["session_status_feed_heartbeat_at"]),
        session_closing_at=session.closing_at,
        lighter_qty_step=qty_step,
        lighter_price_decimals=price_decimals,
        var_bid=var_bid,
        var_ask=var_ask,
        lighter_bid=lighter_bid,
        lighter_ask=lighter_ask,
        lighter_buy_price=lighter_buy_price,
        lighter_sell_price=lighter_sell_price,
        quoted_var_qty=quoted_var_qty,
        quoted_lighter_qty=quoted_lighter_qty,
        lighter_min_base=_positive_decimal(sample.get("lighter_min_base_amount"), "lighter_min_base"),
        lighter_min_quote=min_quote,
        sampled_at=sampled_at,
        validated_at=current_utc,
        var_quote_age_seconds=quote_age,
        lighter_book_age_seconds=book_age,
    )


def _size_for_notional(notional: Decimal, context: MarketContext) -> tuple[Decimal, Decimal]:
    if notional <= 0:
        raise WTIExecutionError("notional_must_be_positive")
    reference = (context.lighter_bid + context.lighter_ask) / Decimal("2")
    required = max(
        notional / (reference * context.multiplier),
        context.lighter_min_base,
        context.lighter_min_quote / (reference * context.multiplier),
    )
    steps = (required / context.lighter_qty_step).to_integral_value(rounding=ROUND_CEILING)
    lighter_qty = steps * context.lighter_qty_step
    return lighter_qty * context.multiplier, lighter_qty


def _active_order_count(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("ok") is not True:
        return None
    orders = payload.get("orders")
    if not isinstance(orders, list):
        return None
    if any(not isinstance(order, dict) for order in orders):
        return None
    return len(orders)


def _signed_target(direction: str, var_qty: Decimal, lighter_qty: Decimal) -> tuple[Decimal, Decimal]:
    if direction == "long_var_short_lighter":
        return var_qty, -lighter_qty
    if direction == "short_var_long_lighter":
        return -var_qty, lighter_qty
    raise WTIExecutionError("unsupported_wti_direction")


def _account_positions(
    payload: Any,
    context: MarketContext,
    *,
    now: datetime,
    minimum_observed_at: datetime | None = None,
    max_age_seconds: float = 5,
) -> AccountSnapshot | None:
    if not isinstance(payload, dict):
        return None
    try:
        observed_at = _sample_time(payload.get("observed_at"))
    except WTIExecutionError:
        return None
    age = (now.astimezone(timezone.utc) - observed_at).total_seconds()
    if age < -5 or age > max_age_seconds:
        return None
    if minimum_observed_at is not None and observed_at < minimum_observed_at.astimezone(timezone.utc):
        return None
    var_qty = variational_position_qty(payload.get("variational_positions"), context.instrument)
    lighter_qty = lighter_position_qty(
        payload.get("lighter_positions"), market_id=context.market_id, symbol=WTI_HEDGE.lighter.symbol
    )
    active_count = _active_order_count(payload.get("active_orders"))
    if var_qty is None or lighter_qty is None or active_count is None:
        return None

    return AccountSnapshot(var_qty, lighter_qty, active_count, observed_at)


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    variational_qty: Decimal
    lighter_qty: Decimal
    active_order_count: int
    observed_at: datetime


def _matches(
    actual: AccountSnapshot,
    expected: tuple[Decimal, Decimal],
    context: MarketContext,
) -> bool:
    return (
        actual.active_order_count == 0
        and abs(actual.variational_qty - expected[0]) <= Decimal("0.00000001")
        and abs(actual.lighter_qty - expected[1]) <= context.lighter_position_tolerance
    )


def _normalized_order_result(
    payload: Any,
    requested_qty: Decimal,
    *,
    expected_client_order_id: str,
) -> dict[str, Any]:
    """Normalize only explicit terminal fills; ambiguous replies remain unknown."""
    if (
        not isinstance(payload, dict)
        or str(payload.get("client_order_id") or "") != expected_client_order_id
    ):
        return {"classification": "unknown"}
    diagnostics = {key: payload[key] for key in (
        "source", "error_code", "error_type", "submission_stage", "submission_acknowledged",
        "transaction_hash", "lookup_stage", "lookup_reason", "risk_reason",
    ) if key in payload and isinstance(payload[key], (str, bool, int, type(None)))}
    if payload.get("ok") is not True or payload.get("final") is not True:
        return {
            **diagnostics,
            "classification": "unknown",
            "client_order_id": expected_client_order_id,
            "native_order_id": payload.get("native_order_id"),
            "native_client_order_id": payload.get("native_client_order_id"),
            "status": str(payload.get("status") or "unknown").lower(),
            "terminal_confirmed": False,
        }
    order_id = payload.get("order_id")
    status = str(payload.get("status") or "").lower()
    filled_qty = decimal_value(payload.get("filled_qty"))
    if filled_qty is None or filled_qty < 0:
        return {"classification": "unknown"}
    fee = decimal_value(payload.get("fee_usd"))
    trade_ids = payload.get("trade_ids")
    evidence = {
        **diagnostics,
        "client_order_id": expected_client_order_id,
        "order_id": str(order_id) if order_id else None,
        "native_order_id": str(payload.get("native_order_id") or order_id) if (payload.get("native_order_id") or order_id) else None,
        "native_client_order_id": payload.get("native_client_order_id"),
        "status": status,
        "filled_qty": str(filled_qty),
        "trade_ids": [str(value) for value in trade_ids] if isinstance(trade_ids, list) else [],
        "trade_ids_confirmed": isinstance(trade_ids, list),
        "fee_usd": str(fee) if fee is not None else None,
        "fee_confirmed": fee is not None,
        "average_price": (
            str(decimal_value(payload.get("average_price")))
            if decimal_value(payload.get("average_price")) is not None
            else None
        ),
        "terminal_confirmed": True,
        "no_order_confirmed": payload.get("no_order_confirmed") is True,
    }
    if (
        filled_qty == 0
        and (
            status in {"rejected", "cancelled", "canceled", "expired"}
            or (status == "not_found" and payload.get("no_order_confirmed") is True)
        )
    ):
        return {"classification": "no_fill", **evidence}
    avg_price = decimal_value(payload.get("average_price"))
    if status == "filled" and order_id and filled_qty == requested_qty and avg_price is not None and avg_price > 0:
        return {
            "classification": "filled",
            "average_price": str(avg_price),
            **evidence,
        }
    return {"classification": "partial_or_inconsistent", **evidence}


def _session_allows(
    session: SessionWindow | None,
    *,
    opening: bool,
    now: datetime,
) -> bool:
    if session is None or now.tzinfo is None or now.utcoffset() is None:
        return False
    try:
        observed_at = _sample_time(session.observed_at)
        now_utc = now.astimezone(timezone.utc)
        closing_at = _sample_time(session.closing_at) if session.closing_at else None
        entry_cutoff_at = _sample_time(session.entry_cutoff_at) if session.entry_cutoff_at else None
    except WTIExecutionError:
        return False
    try:
        max_status_age = float(session.status_max_age_seconds)
    except (TypeError, ValueError):
        return False
    status_age = (now_utc - observed_at).total_seconds()
    if (
        session.exchange_open is not True
        or not isfinite(max_status_age)
        or max_status_age <= 0
        or status_age < -5
        or status_age > max_status_age
        or closing_at is None
        or now_utc >= closing_at
    ):
        return False
    if opening:
        return (
            session.state == OilSession.OPEN
            and session.allow_new_entries
            and entry_cutoff_at is not None
            and now_utc < entry_cutoff_at
        )
    return (
        session.state in {OilSession.OPEN, OilSession.ENTRY_CUTOFF}
        and session.allow_position_management
    )


def _session_matches_sample(
    sample: dict[str, Any], session: SessionWindow | None
) -> str | None:
    observation = session_observation_for_sample(sample)
    if observation is None or session is None or session.exchange_open is not True:
        return None
    try:
        heartbeat_at = _sample_time(sample["session_status_feed_heartbeat_at"])
        if (
            abs((_sample_time(session.observed_at) - heartbeat_at).total_seconds()) > 30
            or abs((_sample_time(session.closing_at) - observation.closing_at).total_seconds()) > 1
        ):
            return None
    except WTIExecutionError:
        return None
    return observation.session_id


def _fresh_session_window(
    session: SessionWindow, context: MarketContext
) -> SessionWindow:
    return replace(
        session,
        observed_at=context.session_observed_at.isoformat(),
        closing_at=context.session_closing_at.isoformat(),
        exchange_open=True,
    )


class WTIExecutionStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            if os.name == "nt":
                import msvcrt

                if os.fstat(fd).st_size == 0:
                    os.write(fd, b"0")
                    os.lseek(fd, 0, os.SEEK_SET)
                try:
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    raise ExecutionBusy("another WTI executor holds the state lock") from exc
            else:
                import fcntl

                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    raise ExecutionBusy("another WTI executor holds the state lock") from exc
            yield
        finally:
            try:
                if os.name == "nt":
                    import msvcrt

                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {
                "schema_version": SCHEMA_VERSION,
                "asset": "WTI",
                "instrument_key": WTI_HEDGE.key,
                "status": "flat",
                "open_position": None,
                "pending_action": None,
                "execution_history": [],
                "updated_at": _utc_now(),
            }
        try:
            state = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CorruptExecutionState("WTI execution state is unreadable; refusing to overwrite") from exc
        if (
            not isinstance(state, dict)
            or state.get("schema_version") != SCHEMA_VERSION
            or state.get("asset") != "WTI"
            or state.get("instrument_key") != WTI_HEDGE.key
            or state.get("status") not in ALLOWED_STATES
        ):
            raise CorruptExecutionState("WTI execution state schema or identity is invalid")
        if "execution_history" not in state:
            state["execution_history"] = []
        if not isinstance(state["execution_history"], list):
            raise CorruptExecutionState("WTI execution history is invalid; refusing to overwrite")
        return state

    def save(self, state: dict[str, Any]) -> None:
        state["updated_at"] = _utc_now()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(state, handle, ensure_ascii=True, sort_keys=True, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, self.path)
            if os.name != "nt":
                directory_fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass

    def initialize_single_round_pilot(
        self,
        *,
        market_id: int,
        multiplier: Decimal,
        config: dict[str, Any],
        readiness: dict[str, Any],
        max_entry_attempts: int = 3,
        new_pilot_after: str | None = None,
    ) -> dict[str, Any]:
        """Create or resume one durable pilot; never reset its completed budget."""
        if not isinstance(max_entry_attempts, int) or max_entry_attempts < 1:
            raise ValueError("single_round_pilot_max_entry_attempts_invalid")
        with self.exclusive():
            state = self.load()
            pilot = state.get("single_round_pilot")
            if new_pilot_after is not None:
                if not isinstance(pilot, dict):
                    raise WTIExecutionError("previous_single_round_pilot_missing")
                if pilot.get("pilot_id") == new_pilot_after:
                    if (
                        state.get("status") != "flat" or state.get("open_position") is not None
                        or state.get("pending_action") is not None
                        or pilot.get("status") not in {"completed", "exhausted"}
                        or pilot.get("schema_version") != 1
                        or (pilot.get("status") == "completed" and pilot.get("completed_roundtrips") != 1)
                        or (pilot.get("status") == "exhausted" and (
                            pilot.get("completed_roundtrips") != 0
                            or not isinstance(pilot.get("max_entry_attempts"), int)
                            or pilot.get("max_entry_attempts", 0) < 1
                            or pilot.get("entry_attempts") != pilot.get("max_entry_attempts")
                        ))
                        or pilot.get("market_id") != market_id
                        or decimal_value(pilot.get("multiplier")) != multiplier
                    ):
                        raise WTIExecutionError("new_single_round_pilot_requires_terminal_flat_previous")
                    state.setdefault("single_round_pilot_history", []).append(deepcopy(pilot))
                    pilot = None
                elif pilot.get("previous_pilot_id") != new_pilot_after:
                    raise WTIExecutionError("previous_single_round_pilot_identity_mismatch")
            if pilot is None:
                if (
                    state.get("status") != "flat"
                    or state.get("open_position") is not None
                    or state.get("pending_action") is not None
                ):
                    raise WTIExecutionError("single_round_pilot_requires_flat_wti_state")
                pilot = {
                    "schema_version": 1,
                    "pilot_id": uuid.uuid4().hex,
                    "status": "available",
                    "market_id": market_id,
                    "multiplier": str(multiplier),
                    "config": deepcopy(config),
                    "readiness_at_start": deepcopy(readiness),
                    "entry_attempts": 0,
                    "max_entry_attempts": max_entry_attempts,
                    "completed_roundtrips": 0,
                    "reservation_id": None,
                    "entry_action_id": None,
                    "started_at": _utc_now(),
                    **({"previous_pilot_id": new_pilot_after} if new_pilot_after is not None else {}),
                }
                state["single_round_pilot"] = pilot
                self.save(state)
            else:
                if (
                    not isinstance(pilot, dict)
                    or pilot.get("schema_version") != 1
                    or not pilot.get("pilot_id")
                    or pilot.get("market_id") != market_id
                    or decimal_value(pilot.get("multiplier")) != multiplier
                    or pilot.get("config") != config
                    or not isinstance(pilot.get("entry_attempts"), int)
                    or not isinstance(pilot.get("completed_roundtrips"), int)
                    or not isinstance(pilot.get("max_entry_attempts"), int)
                    or pilot.get("max_entry_attempts", 0) < 1
                    or pilot.get("entry_attempts", 0) < 0
                    or pilot.get("entry_attempts", 0) > pilot.get("max_entry_attempts", 0)
                    or pilot.get("completed_roundtrips", 0) not in {0, 1}
                ):
                    raise CorruptExecutionState("single-round pilot state is invalid or config changed")
                if pilot["completed_roundtrips"] >= 1 or pilot.get("status") == "completed":
                    raise WTIExecutionError("single_round_pilot_already_completed")
                if pilot.get("status") == "exhausted":
                    raise WTIExecutionError("single_round_pilot_entry_attempts_exhausted")
                if pilot.get("status") == "entry_reserved":
                    if (
                        state.get("status") == "flat"
                        and state.get("open_position") is None
                        and state.get("pending_action") is None
                    ):
                        # Executor persists a pending intent before any venue submit.
                        pilot["status"] = (
                            "exhausted"
                            if pilot["entry_attempts"] >= pilot["max_entry_attempts"]
                            else "available"
                        )
                        pilot["reservation_id"] = None
                        pilot["entry_action_id"] = None
                        self.save(state)
                    else:
                        raise WTIExecutionError("single_round_pilot_entry_requires_reconciliation")
                elif pilot.get("status") == "in_position":
                    position = state.get("open_position")
                    if (
                        state.get("status") != "open"
                        or not isinstance(position, dict)
                        or position.get("single_round_pilot_id") != pilot.get("pilot_id")
                        or position.get("entry_execution_id") != pilot.get("entry_action_id")
                    ):
                        raise WTIExecutionError("single_round_pilot_position_requires_reconciliation")
                elif pilot.get("status") != "available":
                    raise CorruptExecutionState("single-round pilot status is invalid")
            return deepcopy(pilot)

    def reserve_single_round_pilot_entry(self, pilot_id: str) -> str:
        with self.exclusive():
            state = self.load()
            pilot = state.get("single_round_pilot")
            if (
                not isinstance(pilot, dict)
                or pilot.get("pilot_id") != pilot_id
                or pilot.get("status") != "available"
                or pilot.get("completed_roundtrips") != 0
                or pilot.get("entry_attempts", 0) >= pilot.get("max_entry_attempts", 0)
                or state.get("status") != "flat"
                or state.get("open_position") is not None
                or state.get("pending_action") is not None
            ):
                raise WTIExecutionError("single_round_pilot_entry_budget_unavailable")
            reservation_id = uuid.uuid4().hex
            pilot["status"] = "entry_reserved"
            pilot["entry_attempts"] += 1
            pilot["reservation_id"] = reservation_id
            pilot["reserved_at"] = _utc_now()
            self.save(state)
            return reservation_id

    def release_unsubmitted_single_round_pilot_entry(
        self, pilot_id: str, reservation_id: str
    ) -> dict[str, Any]:
        """Release only when executor state proves no order intent was persisted."""
        with self.exclusive():
            state = self.load()
            pilot = state.get("single_round_pilot")
            if (
                not isinstance(pilot, dict)
                or pilot.get("pilot_id") != pilot_id
                or pilot.get("reservation_id") != reservation_id
                or pilot.get("status") != "entry_reserved"
                or state.get("status") != "flat"
                or state.get("open_position") is not None
                or state.get("pending_action") is not None
            ):
                return state
            pilot["status"] = (
                "exhausted"
                if pilot["entry_attempts"] >= pilot["max_entry_attempts"]
                else "available"
            )
            pilot["reservation_id"] = None
            pilot["entry_action_id"] = None
            pilot["last_no_submit_at"] = _utc_now()
            self.save(state)
            return state


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class WTIPairExecutor:
    """Coordinates two venue orders; an unresolved leg blocks all new requests."""

    def __init__(
        self,
        *,
        state_path: Path,
        gateway: WTIExecutionGateway,
        max_slippage_bps: Decimal,
        market_reader: Callable[[], dict[str, Any]],
        market_reader_for_qty: Callable[[Decimal, Decimal], dict[str, Any]] | None = None,
        expected_lighter_market_id: int,
        expected_lighter_multiplier: Decimal,
        expected_lighter_size_decimals: int,
        expected_lighter_price_decimals: int | None = None,
        max_notional_usd: Decimal = Decimal("25"),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if max_slippage_bps <= 0 or max_slippage_bps > Decimal("100"):
            raise ValueError("max_slippage_bps must be in (0, 100]")
        if max_notional_usd <= 0:
            raise ValueError("max_notional_usd must be positive")
        trusted_market_id = integer_value(expected_lighter_market_id)
        trusted_size_decimals = integer_value(expected_lighter_size_decimals)
        trusted_multiplier = decimal_value(expected_lighter_multiplier)
        if (
            trusted_market_id is None
            or trusted_market_id < 0
            or trusted_size_decimals is None
            or trusted_size_decimals < 0
        ):
            raise ValueError("trusted Lighter market identity and precision must be non-negative")
        if trusted_multiplier is None or trusted_multiplier <= 0:
            raise ValueError("trusted Lighter exposure multiplier must be positive")
        self.store = WTIExecutionStore(state_path)
        self.gateway = gateway
        self.max_slippage_bps = max_slippage_bps
        self.market_reader = market_reader
        self.market_reader_for_qty = market_reader_for_qty
        self.expected_lighter_market_id = trusted_market_id
        self.expected_lighter_multiplier = trusted_multiplier
        self.expected_lighter_size_decimals = trusted_size_decimals
        self.expected_lighter_price_decimals = (
            None
            if expected_lighter_price_decimals is None
            else integer_value(expected_lighter_price_decimals)
        )
        if self.expected_lighter_price_decimals is not None and self.expected_lighter_price_decimals < 0:
            raise ValueError("trusted Lighter price precision must be non-negative")
        self.max_notional_usd = max_notional_usd
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        current = self.clock()
        if current.tzinfo is None or current.utcoffset() is None:
            raise WTIExecutionError("executor_clock_must_be_timezone_aware")
        return current.astimezone(timezone.utc)

    def _validate_sample(self, sample: dict[str, Any], *, now: datetime | None = None) -> MarketContext:
        return validate_market_sample(
            sample,
            expected_lighter_market_id=self.expected_lighter_market_id,
            expected_lighter_multiplier=self.expected_lighter_multiplier,
            expected_lighter_size_decimals=self.expected_lighter_size_decimals,
            expected_lighter_price_decimals=self.expected_lighter_price_decimals,
            now=now or self._now(),
        )

    @staticmethod
    def _context_is_fresh(context: MarketContext, now: datetime) -> bool:
        elapsed = Decimal(str((now.astimezone(timezone.utc) - context.validated_at).total_seconds()))
        quote_age = context.var_quote_age_seconds + elapsed
        book_age = context.lighter_book_age_seconds + elapsed
        return (
            elapsed >= Decimal("-5")
            and quote_age >= Decimal("-1")
            and quote_age <= Decimal("2")
            and book_age >= 0
            and book_age <= Decimal("2")
        )

    def _fresh_context(
        self,
        initial: MarketContext,
        *,
        var_qty: Decimal,
        lighter_qty: Decimal,
    ) -> MarketContext:
        try:
            sample = (
                self.market_reader_for_qty(var_qty, lighter_qty)
                if self.market_reader_for_qty is not None
                else self.market_reader()
            )
        except WTIExecutionError:
            raise
        except Exception as exc:
            detail = str(exc).strip()
            if not detail or len(detail) > 120 or not all(
                char.isalnum() or char in "_.:-" for char in detail
            ):
                detail = type(exc).__name__
            raise WTIExecutionError(
                f"fresh_readonly_market_sample_unavailable:{detail}"
            ) from exc
        context = self._validate_sample(sample, now=self._now())
        if context.market_id != initial.market_id or context.multiplier != initial.multiplier:
            raise WTIExecutionError("wti_market_metadata_changed_during_pair_execution")
        if (
            context.session_id != initial.session_id
            or abs((context.session_closing_at - initial.session_closing_at).total_seconds()) > 1
        ):
            raise WTIExecutionError("wti_session_changed_during_pair_execution")
        if context.quoted_var_qty < var_qty or context.quoted_lighter_qty < lighter_qty:
            raise WTIExecutionError("readonly_sample_depth_below_order_quantity")
        limit = self.max_slippage_bps / Decimal("10000")
        for before, after in (
            (initial.var_bid, context.var_bid),
            (initial.var_ask, context.var_ask),
            (initial.lighter_bid, context.lighter_bid),
            (initial.lighter_ask, context.lighter_ask),
        ):
            if abs(after - before) / before > limit:
                raise WTIExecutionError("readonly_market_moved_beyond_slippage_guard")
        return context

    def _read(
        self,
        context: MarketContext,
        *,
        minimum_observed_at: datetime | None = None,
    ) -> AccountSnapshot:
        try:
            parsed = _account_positions(
                self.gateway.read_account(),
                context,
                now=self._now(),
                minimum_observed_at=minimum_observed_at,
            )
        except Exception as exc:
            raise WTIExecutionError("account_snapshot_unavailable") from exc
        if parsed is None:
            raise WTIExecutionError("account_snapshot_unverifiable")
        return parsed

    def _order_request(
        self,
        *, context: MarketContext, venue: str, side: str, qty: Decimal,
        reduce_only: bool, action_id: str,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "client_order_id": f"{action_id}-{venue}",
            "instrument_key": WTI_HEDGE.key,
            "venue": venue,
            "side": side,
            "qty": str(qty),
            "reduce_only": reduce_only,
            "time_in_force": "IOC",
            "order_type": "marketable_limit",
            "max_slippage_bps": str(self.max_slippage_bps),
            "limit_price": None,
        }
        if venue == "variational":
            reference = context.var_ask if side == "buy" else context.var_bid
        elif venue == "lighter":
            reference = context.lighter_buy_price if side == "buy" else context.lighter_sell_price
        else:
            raise WTIExecutionError("unsupported_venue")
        move = self.max_slippage_bps / Decimal("10000")
        boundary = reference * (Decimal("1") + move if side == "buy" else Decimal("1") - move)
        if venue == "lighter":
            tick = Decimal(1).scaleb(-context.lighter_price_decimals)
            rounding = ROUND_CEILING if side == "buy" else ROUND_FLOOR
            boundary = (boundary / tick).to_integral_value(rounding=rounding) * tick
        request["limit_price"] = str(boundary)
        request["reference_price"] = str(reference)
        if venue == "variational":
            request.update({
                "symbol": WTI_HEDGE.var.symbol,
                "instrument_type": WTI_HEDGE.var.instrument_type,
                "settlement_asset": WTI_HEDGE.var.settlement_asset,
                "kind": WTI_HEDGE.var.kind,
                "funding_interval_s": WTI_HEDGE.var.funding_interval_s,
            })
        elif venue == "lighter":
            request.update({
                "symbol": WTI_HEDGE.lighter.symbol,
                "instrument_type": WTI_HEDGE.lighter.instrument_type,
                "market_id": context.market_id,
                "settlement_asset": WTI_HEDGE.lighter.settlement_asset,
                **lighter_order_identity(request["client_order_id"]),
            })
        return request

    def _submit_leg(
        self, state: dict[str, Any], *, context: MarketContext, venue: str,
        side: str, qty: Decimal, reduce_only: bool,
        session: SessionWindow,
        opening: bool,
    ) -> dict[str, Any]:
        pending = state["pending_action"]
        previous = pending.get("superseded_legs", [])
        if not isinstance(previous, list) or any(not isinstance(item, dict) for item in previous):
            raise WTIExecutionError("pending_leg_replacement_history_invalid")
        replacements = sum(1 for item in previous if item.get("venue") == venue)
        action_id = pending["action_id"]
        if replacements:
            action_id = f"{action_id}-replacement-{replacements}"
        request = self._order_request(
            context=context, venue=venue, side=side, qty=qty,
            reduce_only=reduce_only, action_id=action_id,
        )
        request["intent_at"] = pending.get("intent_at")
        leg = {
            "request": request,
            "intent_at": self._now().isoformat(),
            "submission_started": False,
            "result": None,
        }
        pending["legs"][venue] = leg
        self.store.save(state)
        now = self._now()
        fresh_session = _fresh_session_window(session, context)
        if not _session_allows(fresh_session, opening=opening, now=now):
            normalized = {
                "classification": "no_fill",
                "source": "local_pre_submit_guard",
                "reason": "exchange_session_expired_before_submit",
                "filled_qty": "0",
                "terminal_confirmed": True,
            }
            leg["result"] = normalized
            self.store.save(state)
            return normalized
        if not self._context_is_fresh(context, now):
            normalized = {
                "classification": "no_fill",
                "source": "local_pre_submit_guard",
                "reason": "market_sample_expired_before_submit",
                "filled_qty": "0",
                "terminal_confirmed": True,
            }
            leg["result"] = normalized
            self.store.save(state)
            return normalized
        leg["submitted_at"] = now.isoformat()
        request["submitted_at"] = leg["submitted_at"]
        leg["submission_started"] = True
        self.store.save(state)
        try:
            result = self.gateway.submit_order(request)
        except Exception as exc:
            code = str(exc)
            result = {
                "ok": False, "final": False, "client_order_id": request["client_order_id"],
                "error_type": type(exc).__name__, "submission_stage": "gateway_call",
                "error_code": code if code.startswith("lighter_") and code.replace("_", "").isalnum()
                else "order_submission_exception",
            }
        normalized = _normalized_order_result(
            result, qty, expected_client_order_id=request["client_order_id"]
        )
        for source_key, target_key in (
            ("native_order_id", "native_order_id"),
            ("native_client_order_id", "native_client_order_id"),
        ):
            value = normalized.get(source_key)
            if value not in (None, ""):
                request[target_key] = value
        leg["result"] = normalized
        leg["submission_result"] = deepcopy(normalized)
        if normalized.get("transaction_hash"):
            request["transaction_hash"] = normalized["transaction_hash"]
        leg["response_received_at"] = self._now().isoformat()
        self.store.save(state)
        return normalized

    def _latest_order_time(self, pending: dict[str, Any]) -> datetime:
        timestamps = [pending.get("intent_at")]
        timestamps.extend(
            leg.get("submitted_at") or leg.get("intent_at")
            for leg in (pending.get("legs") or {}).values()
            if isinstance(leg, dict)
        )
        parsed = [_sample_time(value) for value in timestamps if value]
        if not parsed:
            raise WTIExecutionError("pending_action_timestamp_missing")
        return max(parsed)

    @staticmethod
    def _all_expected_fills(pending: dict[str, Any]) -> bool:
        legs = pending.get("legs", {})
        if not isinstance(legs, dict):
            return False
        return set(legs) == {"variational", "lighter"} and all(
            isinstance(legs.get(venue), dict)
            and (legs[venue].get("result") or {}).get("classification") == "filled"
            and (legs[venue].get("result") or {}).get("terminal_confirmed") is True
            for venue in ("variational", "lighter")
        )

    def _all_known_no_fill(self, pending: dict[str, Any]) -> bool:
        legs = pending.get("legs", {})
        if not isinstance(legs, dict) or any(not isinstance(leg, dict) for leg in legs.values()):
            return False
        for leg in legs.values():
            result = leg.get("result") or {}
            if leg.get("submission_started") is False:
                if result and (
                    result.get("classification") != "no_fill"
                    or result.get("source") != "local_pre_submit_guard"
                    or result.get("terminal_confirmed") is not True
                ):
                    return False
                continue
            if leg.get("submission_started") is not True:
                return False
            if result.get("classification") != "no_fill" or result.get("terminal_confirmed") is not True:
                return False
        return True

    @staticmethod
    def _execution_record(pending: dict[str, Any], outcome: str, *, recovered: bool = False) -> dict[str, Any]:
        return {
            "action_id": pending["action_id"],
            "kind": pending["kind"],
            "direction": pending["direction"],
            "outcome": outcome,
            "intent_at": pending.get("intent_at"),
            "completed_at": _utc_now(),
            "market_id": pending["market_id"],
            "multiplier": pending["multiplier"],
            "legs": deepcopy(pending.get("legs") or {}),
            "superseded_legs": deepcopy(pending.get("superseded_legs") or []),
            "recovered": recovered,
        }

    def _archive_execution(
        self,
        state: dict[str, Any],
        pending: dict[str, Any],
        outcome: str,
        *,
        recovered: bool = False,
    ) -> dict[str, Any]:
        record = self._execution_record(pending, outcome, recovered=recovered)
        state.setdefault("execution_history", []).append(record)
        if pending.get("kind") == "open" and pending.get("single_round_pilot_id"):
            pilot = state.get("single_round_pilot")
            if (
                not isinstance(pilot, dict)
                or pilot.get("pilot_id") != pending.get("single_round_pilot_id")
                or pilot.get("reservation_id") != pending.get("pilot_reservation_id")
            ):
                raise CorruptExecutionState("single-round pilot reservation does not match entry")
            if outcome == "filled":
                pilot["status"] = "in_position"
                pilot["entry_action_id"] = pending["action_id"]
                pilot["position_opened_at"] = record["completed_at"]
            elif outcome in {"no_fill", "compensated"}:
                pilot["status"] = (
                    "exhausted"
                    if pilot.get("entry_attempts", 0) >= pilot.get("max_entry_attempts", 0)
                    else "available"
                )
                pilot["reservation_id"] = None
                pilot["entry_action_id"] = None
                pilot["last_no_fill_at"] = record["completed_at"]
        return record

    @staticmethod
    def _complete_single_round_pilot(
        state: dict[str, Any],
        position: dict[str, Any],
        execution: dict[str, Any],
    ) -> None:
        pilot_id = position.get("single_round_pilot_id")
        if not pilot_id:
            return
        pilot = state.get("single_round_pilot")
        if (
            not isinstance(pilot, dict)
            or pilot.get("pilot_id") != pilot_id
            or pilot.get("status") != "in_position"
            or pilot.get("entry_action_id") != position.get("entry_execution_id")
            or pilot.get("completed_roundtrips") != 0
        ):
            raise CorruptExecutionState("single-round pilot close does not match its confirmed entry")
        pilot["status"] = "completed"
        pilot["completed_roundtrips"] = 1
        pilot["reservation_id"] = None
        pilot["exit_action_id"] = execution["action_id"]
        pilot["completed_at"] = execution["completed_at"]

    @staticmethod
    def _closed_pnl_evidence(
        position: dict[str, Any],
        *,
        var_exit: dict[str, Any],
        lighter_exit: dict[str, Any],
    ) -> dict[str, Any]:
        var_entry = decimal_value(position.get("var_entry_price"))
        lighter_entry = decimal_value(position.get("lighter_entry_price"))
        var_qty = decimal_value(position.get("var_qty"))
        lighter_qty = decimal_value(position.get("lighter_qty"))
        multiplier = decimal_value(position.get("multiplier"))
        var_exit_price = decimal_value(var_exit.get("average_price"))
        lighter_exit_price = decimal_value(lighter_exit.get("average_price"))
        output: dict[str, Any] = {
            "var_exit_price": var_exit.get("average_price"),
            "lighter_exit_price": lighter_exit.get("average_price"),
            "var_exit_fee_usd": var_exit.get("fee_usd"),
            "lighter_exit_fee_usd": lighter_exit.get("fee_usd"),
            "funding_cashflow_usd": None,
            "funding_cashflow_confirmed": False,
            "actual_net_pnl_usd": None,
        }
        if None not in (
            var_entry, lighter_entry, var_qty, lighter_qty, multiplier,
            var_exit_price, lighter_exit_price,
        ):
            instrument = replace(
                WTI_HEDGE,
                lighter=replace(WTI_HEDGE.lighter, exposure_multiplier=multiplier),
            )
            gross = paired_realized_pnl(
                direction=str(position.get("direction")),
                var_entry=var_entry,
                var_exit=var_exit_price,
                var_qty=var_qty,
                lighter_entry=lighter_entry,
                lighter_exit=lighter_exit_price,
                lighter_qty=lighter_qty,
                instrument=instrument,
            )
            output["actual_gross_pnl_usd"] = str(gross)
            output["actual_gross_pnl_status"] = "confirmed_from_final_fills"
        else:
            output["actual_gross_pnl_usd"] = None
            output["actual_gross_pnl_status"] = "fill_price_or_quantity_unverified"
        fee_values = [
            decimal_value(position.get("var_entry_fee_usd")),
            decimal_value(position.get("lighter_entry_fee_usd")),
            decimal_value(var_exit.get("fee_usd")),
            decimal_value(lighter_exit.get("fee_usd")),
        ]
        output["actual_fees_usd"] = (
            str(sum(fee_values, Decimal("0"))) if all(value is not None for value in fee_values) else None
        )
        output["actual_net_pnl_status"] = (
            "funding_unverified"
            if output["actual_gross_pnl_usd"] is not None and output["actual_fees_usd"] is not None
            else "fees_unverified"
        )
        return output

    def _resolve_pending_orders(self, state: dict[str, Any], pending: dict[str, Any]) -> bool:
        changed = False
        legs = pending.get("legs", {})
        if not isinstance(legs, dict) or any(not isinstance(leg, dict) for leg in legs.values()):
            return False
        for venue, leg in legs.items():
            if leg.get("submission_started") is False:
                result = leg.get("result") or {}
                if result and (
                    result.get("classification") != "no_fill"
                    or result.get("source") != "local_pre_submit_guard"
                    or result.get("terminal_confirmed") is not True
                ):
                    return False
                continue
            if leg.get("submission_started") is not True:
                return False
            result = leg.get("result") or {}
            if result.get("terminal_confirmed") is True:
                continue
            client_order_id = (leg.get("request") or {}).get("client_order_id")
            if not client_order_id:
                return False
            try:
                lookup_request = deepcopy(leg.get("request") or {})
                prior_result = leg.get("result") or {}
                for key in ("native_order_id", "native_client_order_id", "transaction_hash"):
                    if key not in lookup_request and prior_result.get(key) not in (None, ""):
                        lookup_request[key] = prior_result[key]
                submission = leg.get("submission_result") or {}
                for key in ("native_order_id", "native_client_order_id", "transaction_hash"):
                    if key not in lookup_request and submission.get(key) not in (None, ""):
                        lookup_request[key] = submission[key]
                try:
                    lookup = self.gateway.lookup_order(
                        str(client_order_id), request=lookup_request
                    )
                except TypeError:
                    # Keep compatibility with test and third-party adapters implementing v1.
                    lookup = self.gateway.lookup_order(str(client_order_id))
            except Exception as exc:
                lookup = {
                    "ok": False, "final": False, "client_order_id": str(client_order_id),
                    "lookup_stage": "gateway_call", "lookup_reason": "order_lookup_exception",
                    "error_type": type(exc).__name__,
                }
            requested_qty = decimal_value((leg.get("request") or {}).get("qty"))
            if requested_qty is None:
                return False
            normalized = _normalized_order_result(
                lookup,
                requested_qty,
                expected_client_order_id=str(client_order_id),
            )
            leg["result"] = normalized
            leg["lookup_checked_at"] = self._now().isoformat()
            changed = True
        if changed:
            self.store.save(state)
        return all(
            (leg.get("submission_started") is False and (
                not leg.get("result")
                or (
                    leg["result"].get("classification") == "no_fill"
                    and leg["result"].get("source") == "local_pre_submit_guard"
                    and leg["result"].get("terminal_confirmed") is True
                )
            ))
            or (leg.get("submission_started") is True
                and (leg.get("result") or {}).get("terminal_confirmed") is True)
            for leg in legs.values()
        )

    def _manual_review(self, state: dict[str, Any], reason: str) -> dict[str, Any]:
        state["status"] = "manual_review_required"
        state["manual_review_reason"] = reason
        self.store.save(state)
        return state

    def open(
        self,
        *,
        sample: dict[str, Any],
        direction: str,
        notional_usd: Decimal,
        session: SessionWindow | None,
        pilot_id: str | None = None,
        pilot_reservation_id: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        operation_now = now or self._now()
        if direction not in VALID_DIRECTIONS:
            raise WTIExecutionError("unsupported_wti_direction")
        if not _session_allows(session, opening=True, now=operation_now):
            raise WTIExecutionError("exchange_session_does_not_allow_entry")
        if session is None:
            raise WTIExecutionError("exchange_session_does_not_allow_entry")
        if notional_usd <= 0 or notional_usd > self.max_notional_usd:
            raise WTIExecutionError("notional_outside_executor_limit")
        context = self._validate_sample(sample, now=operation_now)
        session_id = _session_matches_sample(sample, session)
        if session_id is None:
            raise WTIExecutionError("exchange_session_observation_unverified")
        if bool(pilot_id) != bool(pilot_reservation_id):
            raise WTIExecutionError("single_round_pilot_reservation_incomplete")
        var_qty, lighter_qty = _size_for_notional(notional_usd, context)
        reference = (context.lighter_bid + context.lighter_ask) / Decimal("2")
        planned_notional = lighter_qty * context.multiplier * reference
        if planned_notional > self.max_notional_usd:
            # A target equal to the cap cannot be rounded up past that cap.
            capped_steps = (
                self.max_notional_usd / (reference * context.multiplier)
                / context.lighter_qty_step
            ).to_integral_value(rounding=ROUND_FLOOR)
            lighter_qty = capped_steps * context.lighter_qty_step
            var_qty = lighter_qty * context.multiplier
            planned_notional = lighter_qty * context.multiplier * reference
            if (
                lighter_qty <= 0
                or lighter_qty < context.lighter_min_base
                or planned_notional < context.lighter_min_quote
                or planned_notional > self.max_notional_usd
            ):
                raise WTIExecutionError("rounded_order_exceeds_executor_notional_limit")
        if context.quoted_var_qty < var_qty or context.quoted_lighter_qty < lighter_qty:
            raise WTIExecutionError("readonly_sample_depth_below_order_quantity")
        if not context.instrument or not paired_exposure_matches(
            var_qty=var_qty, lighter_qty=lighter_qty,
            instrument=context.instrument, tolerance=context.position_tolerance,
        ):
            raise WTIExecutionError("planned_pair_exposure_mismatch")
        expected = _signed_target(direction, var_qty, lighter_qty)
        var_side, lighter_side = (
            ("buy", "sell") if direction == "long_var_short_lighter" else ("sell", "buy")
        )

        with self.store.exclusive():
            state = self.store.load()
            if state["status"] != "flat" or state.get("pending_action") or state.get("open_position"):
                raise WTIExecutionError("existing_wti_state_blocks_entry")
            if pilot_id:
                pilot = state.get("single_round_pilot")
                if (
                    not isinstance(pilot, dict)
                    or pilot.get("pilot_id") != pilot_id
                    or pilot.get("status") != "entry_reserved"
                    or pilot.get("reservation_id") != pilot_reservation_id
                    or pilot.get("completed_roundtrips") != 0
                ):
                    raise WTIExecutionError("single_round_pilot_reservation_invalid")
            baseline = self._read(context)
            if not _matches(baseline, (Decimal("0"), Decimal("0")), context):
                raise WTIExecutionError("wti_account_not_flat_or_has_active_orders")
            action_id = uuid.uuid4().hex
            state.update({
                "status": "pending",
                "pending_action": {
                    "action_id": action_id,
                    "kind": "open",
                    "direction": direction,
                    "expected_var_qty": str(expected[0]),
                    "expected_lighter_qty": str(expected[1]),
                    "var_qty": str(var_qty),
                    "lighter_qty": str(lighter_qty),
                    "multiplier": str(context.multiplier),
                    "market_id": context.market_id,
                    "session_id": session_id,
                    "single_round_pilot_id": pilot_id,
                    "pilot_reservation_id": pilot_reservation_id,
                    "previous_position": None,
                    "intent_at": self._now().isoformat(),
                    "legs": {},
                },
                "manual_review_reason": None,
            })
            self.store.save(state)

            try:
                first_context = self._fresh_context(
                    context, var_qty=var_qty, lighter_qty=lighter_qty
                )
            except WTIExecutionError as exc:
                return self._manual_review(state, str(exc))
            var_result = self._submit_leg(
                state, context=first_context, venue="variational", side=var_side,
                qty=var_qty, reduce_only=False, session=session, opening=True,
            )
            if var_result["classification"] != "filled":
                return self._manual_review(state, f"variational_open_{var_result['classification']}")
            try:
                refreshed = self._fresh_context(
                    context, var_qty=var_qty, lighter_qty=lighter_qty
                )
            except WTIExecutionError as exc:
                return self._manual_review(state, str(exc))
            lighter_result = self._submit_leg(
                state, context=refreshed, venue="lighter", side=lighter_side,
                qty=lighter_qty, reduce_only=False, session=session, opening=True,
            )
            if lighter_result["classification"] != "filled":
                if lighter_result["classification"] == "no_fill":
                    compensated = self._compensate_var_only_open(
                        state,
                        context=refreshed,
                        session=session,
                        side="sell" if var_side == "buy" else "buy",
                        qty=var_qty,
                    )
                    if compensated:
                        return state
                return self._manual_review(state, f"lighter_open_{lighter_result['classification']}")

            try:
                actual = self._read(
                    context,
                    minimum_observed_at=self._latest_order_time(state["pending_action"]),
                )
            except WTIExecutionError:
                return self._manual_review(state, "post_open_account_snapshot_unavailable")
            if not _matches(actual, expected, context):
                return self._manual_review(state, "post_open_exchange_position_mismatch")
            execution = self._archive_execution(state, state["pending_action"], "filled")
            state["status"] = "open"
            state["open_position"] = {
                "direction": direction,
                "var_qty": str(var_qty),
                "lighter_qty": str(lighter_qty),
                "expected_var_qty": str(expected[0]),
                "expected_lighter_qty": str(expected[1]),
                "var_entry_price": var_result["average_price"],
                "lighter_entry_price": lighter_result["average_price"],
                "var_entry_fee_usd": var_result.get("fee_usd"),
                "lighter_entry_fee_usd": lighter_result.get("fee_usd"),
                "market_id": context.market_id,
                "multiplier": str(context.multiplier),
                "session_id": session_id,
                "opened_at": _utc_now(),
                "entry_execution_id": execution["action_id"],
                **({"single_round_pilot_id": pilot_id} if pilot_id else {}),
            }
            state["pending_action"] = None
            state["manual_review_reason"] = None
            self.store.save(state)
            return state

    def _compensate_var_only_open(
        self,
        state: dict[str, Any],
        *,
        context: MarketContext,
        session: SessionWindow,
        side: str,
        qty: Decimal,
    ) -> bool:
        """Flatten a proven first-leg-only entry; never compensate an unknown fill."""
        pending = state.get("pending_action")
        if not isinstance(pending, dict):
            return False
        var_leg = (pending.get("legs") or {}).get("variational") or {}
        var_fill = var_leg.get("result") or {}
        lighter_result = ((pending.get("legs") or {}).get("lighter") or {}).get("result") or {}
        if (
            var_fill.get("classification") != "filled"
            or var_fill.get("terminal_confirmed") is not True
            or lighter_result.get("classification") != "no_fill"
            or lighter_result.get("terminal_confirmed") is not True
            or not _session_allows(
                _fresh_session_window(session, context), opening=False, now=self._now()
            )
        ):
            return False
        try:
            before = self._read(context)
        except WTIExecutionError:
            return False
        expected_var = Decimal(str(pending.get("expected_var_qty")))
        if (
            before.active_order_count != 0
            or abs(before.variational_qty - expected_var) > Decimal("0.00000001")
            or abs(before.lighter_qty) > context.lighter_position_tolerance
        ):
            return False

        action_id = str(pending.get("action_id") or "")
        compensation_request = self._order_request(
            context=context,
            venue="variational",
            side=side,
            qty=qty,
            reduce_only=True,
            action_id=f"{action_id}-compensation",
        )
        compensation_request["intent_at"] = self._now().isoformat()
        pending["compensation"] = {
            "request": compensation_request,
            "intent_at": compensation_request["intent_at"],
            "submission_started": False,
            "result": None,
        }
        self.store.save(state)
        if not self._context_is_fresh(context, self._now()):
            return False
        compensation = pending["compensation"]
        compensation["submitted_at"] = self._now().isoformat()
        compensation["submission_started"] = True
        self.store.save(state)
        try:
            response = self.gateway.submit_order(compensation_request)
        except Exception as exc:
            response = {"exception": type(exc).__name__}
        result = _normalized_order_result(
            response,
            qty,
            expected_client_order_id=compensation_request["client_order_id"],
        )
        for key in ("native_order_id", "native_client_order_id"):
            if result.get(key) not in (None, ""):
                compensation_request[key] = result[key]
        compensation["result"] = result
        compensation["response_received_at"] = self._now().isoformat()
        self.store.save(state)
        if result.get("classification") != "filled":
            return False
        try:
            after = self._read(context, minimum_observed_at=self._now())
        except WTIExecutionError:
            return False
        if not _matches(after, (Decimal("0"), Decimal("0")), context):
            return False
        self._archive_execution(state, pending, "compensated")
        state["status"] = "flat"
        state["open_position"] = None
        state["pending_action"] = None
        state["manual_review_reason"] = None
        state["last_action_resolution"] = "one_leg_open_confirmed_and_reduce_only_compensated"
        self.store.save(state)
        return True

    def close(
        self,
        *,
        sample: dict[str, Any],
        session: SessionWindow | None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        operation_now = now or self._now()
        if not _session_allows(session, opening=False, now=operation_now):
            raise WTIExecutionError("exchange_session_does_not_allow_position_management")
        if session is None:
            raise WTIExecutionError("exchange_session_does_not_allow_position_management")
        context = self._validate_sample(sample, now=operation_now)
        session_id = _session_matches_sample(sample, session)
        if session_id is None:
            raise WTIExecutionError("exchange_session_observation_unverified")
        with self.store.exclusive():
            state = self.store.load()
            position = state.get("open_position")
            if state["status"] != "open" or not isinstance(position, dict) or state.get("pending_action"):
                raise WTIExecutionError("no_reconciled_open_wti_position")
            if (
                int(position.get("market_id", -1)) != context.market_id
                or decimal_value(position.get("multiplier")) != context.multiplier
            ):
                raise WTIExecutionError("wti_market_metadata_changed_since_entry")
            close_var_qty = Decimal(str(position["var_qty"]))
            close_lighter_qty = Decimal(str(position["lighter_qty"]))
            if (
                context.quoted_var_qty < close_var_qty
                or context.quoted_lighter_qty < close_lighter_qty
            ):
                try:
                    context = self._fresh_context(
                        context,
                        var_qty=close_var_qty,
                        lighter_qty=close_lighter_qty,
                    )
                except WTIExecutionError as exc:
                    if str(exc) == "readonly_sample_depth_below_order_quantity":
                        raise WTIExecutionError(
                            "readonly_sample_depth_below_close_quantity"
                        ) from exc
                    raise
            before = self._read(context)
            expected_open = (
                Decimal(str(position["expected_var_qty"])),
                Decimal(str(position["expected_lighter_qty"])),
            )
            if not _matches(before, expected_open, context):
                return self._manual_review(state, "pre_close_exchange_position_mismatch")

            direction = str(position["direction"])
            var_close_side, lighter_close_side = (
                ("sell", "buy") if direction == "long_var_short_lighter" else ("buy", "sell")
            )
            action_id = uuid.uuid4().hex
            state["status"] = "pending"
            state["pending_action"] = {
                "action_id": action_id,
                "kind": "close",
                "direction": direction,
                "expected_var_qty": "0",
                "expected_lighter_qty": "0",
                "var_qty": str(position["var_qty"]),
                "lighter_qty": str(position["lighter_qty"]),
                "multiplier": str(context.multiplier),
                "market_id": context.market_id,
                "session_id": session_id,
                "previous_position": position,
                "intent_at": self._now().isoformat(),
                "legs": {},
            }
            self.store.save(state)

            try:
                first_context = self._fresh_context(
                    context,
                    var_qty=Decimal(str(position["var_qty"])),
                    lighter_qty=Decimal(str(position["lighter_qty"])),
                )
            except WTIExecutionError as exc:
                return self._manual_review(state, str(exc))
            var_result = self._submit_leg(
                state, context=first_context, venue="variational", side=var_close_side,
                qty=Decimal(str(position["var_qty"])), reduce_only=True,
                session=session, opening=False,
            )
            if var_result["classification"] != "filled":
                return self._manual_review(state, f"variational_close_{var_result['classification']}")
            try:
                refreshed = self._fresh_context(
                    context,
                    var_qty=Decimal(str(position["var_qty"])),
                    lighter_qty=Decimal(str(position["lighter_qty"])),
                )
            except WTIExecutionError as exc:
                return self._manual_review(state, str(exc))
            lighter_result = self._submit_leg(
                state, context=refreshed, venue="lighter", side=lighter_close_side,
                qty=Decimal(str(position["lighter_qty"])), reduce_only=True,
                session=session, opening=False,
            )
            if lighter_result["classification"] != "filled":
                return self._manual_review(state, f"lighter_close_{lighter_result['classification']}")
            try:
                actual = self._read(
                    context,
                    minimum_observed_at=self._latest_order_time(state["pending_action"]),
                )
            except WTIExecutionError:
                return self._manual_review(state, "post_close_account_snapshot_unavailable")
            if not _matches(actual, (Decimal("0"), Decimal("0")), context):
                return self._manual_review(state, "post_close_exchange_position_mismatch")
            execution = self._archive_execution(state, state["pending_action"], "filled")
            state["status"] = "flat"
            state["open_position"] = None
            state["pending_action"] = None
            state["manual_review_reason"] = None
            state["last_closed_position"] = {
                **position,
                **self._closed_pnl_evidence(
                    position, var_exit=var_result, lighter_exit=lighter_result
                ),
                "closed_at": _utc_now(),
                "exit_execution_id": execution["action_id"],
            }
            self._complete_single_round_pilot(state, position, execution)
            self.store.save(state)
            return state

    def _resume_pending_open_missing_lighter(
        self,
        state: dict[str, Any],
        pending: dict[str, Any],
        *,
        context: MarketContext,
        actual: AccountSnapshot,
        sample: dict[str, Any],
        session: SessionWindow | None,
    ) -> dict[str, Any] | None:
        """Continue a Var-first open only when the missing hedge was provably never sent."""
        if pending.get("kind") != "open" or actual.active_order_count != 0:
            return None
        legs = pending.get("legs") or {}
        var_leg = legs.get("variational") or {}
        var_fill = var_leg.get("result") or {}
        var_qty = decimal_value(pending.get("var_qty"))
        lighter_qty = decimal_value(pending.get("lighter_qty"))
        expected_var = decimal_value(pending.get("expected_var_qty"))
        expected_lighter = decimal_value(pending.get("expected_lighter_qty"))
        direction = pending.get("direction")
        if (
            direction not in VALID_DIRECTIONS
            or None in (var_qty, lighter_qty, expected_var, expected_lighter)
            or var_qty <= 0 or lighter_qty <= 0
            or (expected_var, expected_lighter)
            != _signed_target(direction, var_qty, lighter_qty)
            or var_leg.get("submission_started") is not True
            or var_fill.get("classification") != "filled"
            or var_fill.get("terminal_confirmed") is not True
            or decimal_value(var_fill.get("filled_qty")) != var_qty
            or not _matches(actual, (expected_var, Decimal("0")), context)
        ):
            return None

        lighter_leg = legs.get("lighter")
        replace_unsendable = False
        if isinstance(lighter_leg, dict):
            if lighter_leg.get("submission_started") is True:
                lighter_result = lighter_leg.get("result") or {}
                if (
                    lighter_result.get("classification") != "no_fill"
                    or lighter_result.get("terminal_confirmed") is not True
                ):
                    return None
                old_request = lighter_leg.get("request") or {}
                replace_unsendable = (
                    lighter_result.get("source") == "local_signer_validation"
                    and lighter_result.get("error_code") == LEGACY_INDEX_REJECTION
                    and lighter_result.get("no_order_confirmed") is True
                    and decimal_value(lighter_result.get("filled_qty")) == 0
                    and old_request.get("client_order_id") == f"{pending['action_id']}-lighter"
                    and old_request.get("side") == ("sell" if direction == "long_var_short_lighter" else "buy")
                    and decimal_value(old_request.get("qty")) == lighter_qty
                    and integer_value(old_request.get("market_id")) == context.market_id
                    and old_request.get("reduce_only") is False
                    and legacy_lighter_index_was_unsendable(
                        str(old_request.get("client_order_id") or ""), old_request
                    )
                )
                if not replace_unsendable:
                    return self._manual_review(state, "pending_open_lighter_no_fill_requires_operator")
            elif lighter_leg.get("submission_started") is not False:
                return None
            prior = lighter_leg.get("result") or {}
            if not replace_unsendable and prior and (
                prior.get("classification") != "no_fill"
                or prior.get("source") != "local_pre_submit_guard"
                or prior.get("terminal_confirmed") is not True
            ):
                return None

        pilot_id = pending.get("single_round_pilot_id")
        if pilot_id:
            pilot = state.get("single_round_pilot")
            if (
                not isinstance(pilot, dict)
                or pilot.get("pilot_id") != pilot_id
                or pilot.get("status") != "entry_reserved"
                or pilot.get("reservation_id") != pending.get("pilot_reservation_id")
            ):
                return self._manual_review(state, "single_round_pilot_reservation_invalid")

        if (
            _session_matches_sample(sample, session) != pending.get("session_id")
            or not _session_allows(session, opening=True, now=self._now())
        ):
            return self._manual_review(state, "pending_open_var_only_requires_position_management")
        try:
            fresh = self._fresh_context(context, var_qty=var_qty, lighter_qty=lighter_qty)
        except WTIExecutionError as exc:
            return self._manual_review(
                state, f"pending_open_missing_lighter_{exc}"
            )

        lighter_side = "sell" if direction == "long_var_short_lighter" else "buy"
        if replace_unsendable:
            previous = pending.setdefault("superseded_legs", [])
            if not isinstance(previous, list) or previous:
                return self._manual_review(state, "pending_open_lighter_replacement_history_invalid")
            previous.append({
                "venue": "lighter", "reason": LEGACY_INDEX_REJECTION,
                "archived_at": self._now().isoformat(), "leg": deepcopy(lighter_leg),
            })
            # _submit_leg atomically saves this archive and the new 48-bit identity
            # before any SDK call. A crash cannot replay the superseded identity.
        lighter_result = self._submit_leg(
            state,
            context=fresh,
            venue="lighter",
            side=lighter_side,
            qty=lighter_qty,
            reduce_only=False,
            session=session,
            opening=True,
        )
        if lighter_result.get("classification") != "filled":
            if lighter_result.get("classification") == "no_fill":
                return self._manual_review(state, "pending_open_lighter_no_fill_requires_operator")
            return self._manual_review(
                state, f"pending_open_lighter_{lighter_result.get('classification', 'unknown')}"
            )

        try:
            after = self._read(context, minimum_observed_at=self._latest_order_time(pending))
        except WTIExecutionError:
            return self._manual_review(state, "post_open_account_snapshot_unavailable")
        expected = (expected_var, expected_lighter)
        if after.active_order_count != 0 or not _matches(after, expected, context):
            return self._manual_review(state, "post_open_exchange_position_mismatch")

        execution = self._archive_execution(state, pending, "filled", recovered=True)
        var_fill = (pending.get("legs") or {}).get("variational", {}).get("result") or {}
        lighter_fill = (pending.get("legs") or {}).get("lighter", {}).get("result") or {}
        state["open_position"] = {
            "direction": direction,
            "var_qty": str(var_qty),
            "lighter_qty": str(lighter_qty),
            "expected_var_qty": str(expected_var),
            "expected_lighter_qty": str(expected_lighter),
            "var_entry_price": var_fill.get("average_price"),
            "lighter_entry_price": lighter_fill.get("average_price"),
            "var_entry_fee_usd": var_fill.get("fee_usd"),
            "lighter_entry_fee_usd": lighter_fill.get("fee_usd"),
            "market_id": pending["market_id"],
            "multiplier": pending["multiplier"],
            "session_id": pending.get("session_id"),
            "opened_at": pending.get("intent_at") or _utc_now(),
            "recovered_at": _utc_now(),
            "recovered_from_pending": True,
            "entry_execution_id": execution["action_id"],
            **({"single_round_pilot_id": pilot_id} if pilot_id else {}),
        }
        state["status"] = "open"
        state["pending_action"] = None
        state["manual_review_reason"] = None
        state["last_action_resolution"] = "pending_open_missing_lighter_leg_completed"
        self.store.save(state)
        return state

    def _finalize_reconciled_close(
        self,
        state: dict[str, Any],
        pending: dict[str, Any],
    ) -> dict[str, Any]:
        position = pending.get("previous_position") or {}
        legs = pending.get("legs") or {}
        var_result = (legs.get("variational") or {}).get("result") or {}
        lighter_result = (legs.get("lighter") or {}).get("result") or {}
        var_exit = var_result if var_result.get("classification") == "filled" else {}
        lighter_exit = lighter_result if lighter_result.get("classification") == "filled" else {}
        execution = self._archive_execution(
            state, pending, "positions_flat_confirmed", recovered=True
        )
        state["status"] = "flat"
        state["open_position"] = None
        state["pending_action"] = None
        state["manual_review_reason"] = None
        state["last_action_resolution"] = "close_confirmed_flat_by_account_snapshot"
        state["last_closed_position"] = {
            **position,
            **self._closed_pnl_evidence(
                position, var_exit=var_exit, lighter_exit=lighter_exit
            ),
            "closed_at": _utc_now(),
            "recovered_from_pending": True,
            "exit_execution_id": execution["action_id"],
        }
        self._complete_single_round_pilot(state, position, execution)
        self.store.save(state)
        return state

    def _resume_pending_close_missing_lighter(
        self,
        state: dict[str, Any],
        pending: dict[str, Any],
        *,
        context: MarketContext,
        actual: AccountSnapshot,
        sample: dict[str, Any],
        session: SessionWindow | None,
    ) -> dict[str, Any] | None:
        """Recover a Var-first close without ever retrying an uncertain submitted leg."""
        if pending.get("kind") != "close" or actual.active_order_count != 0:
            return None
        previous = pending.get("previous_position")
        legs = pending.get("legs")
        if not isinstance(previous, dict) or not isinstance(legs, dict):
            return None
        var_leg = legs.get("variational")
        if not isinstance(var_leg, dict) or var_leg.get("submission_started") is not True:
            return None

        var_request = var_leg.get("request") or {}
        var_result = var_leg.get("result") or {}
        direction = str(pending.get("direction") or "")
        var_qty = decimal_value(previous.get("var_qty"))
        lighter_qty = decimal_value(previous.get("lighter_qty"))
        expected_var = decimal_value(previous.get("expected_var_qty"))
        expected_lighter = decimal_value(previous.get("expected_lighter_qty"))
        lighter_side = "buy" if expected_lighter is not None and expected_lighter < 0 else "sell"
        if (
            direction not in VALID_DIRECTIONS
            or None in (var_qty, lighter_qty, expected_var, expected_lighter)
            or var_qty <= 0 or lighter_qty <= 0
            or (expected_var, expected_lighter)
            != _signed_target(direction, var_qty, lighter_qty)
            or decimal_value(pending.get("var_qty")) != var_qty
            or decimal_value(pending.get("lighter_qty")) != lighter_qty
            or decimal_value(pending.get("expected_var_qty")) != 0
            or decimal_value(pending.get("expected_lighter_qty")) != 0
            or var_request.get("venue") != "variational"
            or var_request.get("client_order_id") != f"{pending.get('action_id')}-variational"
            or var_request.get("side") != ("sell" if direction == "long_var_short_lighter" else "buy")
            or decimal_value(var_request.get("qty")) != var_qty
            or var_request.get("reduce_only") is not True
            or var_request.get("symbol") != WTI_HEDGE.var.symbol
            or var_request.get("instrument_type") != WTI_HEDGE.var.instrument_type
            or integer_value(pending.get("market_id")) != context.market_id
            or decimal_value(pending.get("multiplier")) != context.multiplier
        ):
            return None

        var_classification = var_result.get("classification")
        if var_classification == "filled":
            if (
                var_result.get("terminal_confirmed") is not True
                or decimal_value(var_result.get("filled_qty")) != var_qty
            ):
                return None
        elif var_classification != "unknown" or var_result.get("terminal_confirmed") is True:
            return None

        lighter_leg = legs.get("lighter")
        superseded = pending.get("superseded_legs") or []
        if not isinstance(superseded, list) or any(
            not isinstance(item, dict) for item in superseded
        ):
            return None
        lighter_replacements = sum(
            1 for item in superseded if item.get("venue") == "lighter"
        )
        lighter_action_id = str(pending.get("action_id") or "")
        if lighter_replacements:
            lighter_action_id = f"{lighter_action_id}-replacement-{lighter_replacements}"
        if lighter_leg is not None:
            if not isinstance(lighter_leg, dict):
                return None
            lighter_request = lighter_leg.get("request") or {}
            if (
                lighter_request.get("venue") != "lighter"
                or lighter_request.get("client_order_id") != f"{lighter_action_id}-lighter"
                or lighter_request.get("side") != lighter_side
                or decimal_value(lighter_request.get("qty")) != lighter_qty
                or lighter_request.get("reduce_only") is not True
                or integer_value(lighter_request.get("market_id")) != context.market_id
            ):
                return None

        snapshot_evidence = {
            "observed_at": actual.observed_at.isoformat(),
            "variational_qty": str(actual.variational_qty),
            "lighter_qty": str(actual.lighter_qty),
            "active_order_count": actual.active_order_count,
        }
        if _matches(actual, (Decimal("0"), Decimal("0")), context):
            pending["recovery_account_snapshot"] = snapshot_evidence
            return self._finalize_reconciled_close(state, pending)

        if (
            abs(actual.variational_qty) > Decimal("0.00000001")
            or abs(actual.lighter_qty - expected_lighter) > context.lighter_position_tolerance
            or _session_matches_sample(sample, session) != pending.get("session_id")
            or not _session_allows(session, opening=False, now=self._now())
        ):
            return None

        if lighter_leg is not None:
            if lighter_leg.get("submission_started") is not False:
                return None
            old_result = lighter_leg.get("result") or {}
            if old_result and (
                old_result.get("classification") != "no_fill"
                or old_result.get("source") != "local_pre_submit_guard"
                or old_result.get("terminal_confirmed") is not True
            ):
                return None

        pending["recovery_account_snapshot"] = {
            **snapshot_evidence,
            "action": "submit_only_missing_lighter_reduce_only_close",
        }
        self.store.save(state)
        try:
            fresh = self._fresh_context(
                context, var_qty=var_qty, lighter_qty=lighter_qty
            )
        except WTIExecutionError as exc:
            return self._manual_review(
                state, f"pending_close_missing_lighter_{exc}"
            )

        if lighter_leg is not None:
            superseded = pending.setdefault("superseded_legs", [])
            client_id = (lighter_leg.get("request") or {}).get("client_order_id")
            archived = any(
                item.get("venue") == "lighter"
                and ((item.get("leg") or {}).get("request") or {}).get("client_order_id") == client_id
                for item in superseded if isinstance(item, dict)
            )
            if not archived:
                superseded.append({
                    "venue": "lighter",
                    "reason": "prior_close_lighter_leg_not_submitted",
                    "archived_at": self._now().isoformat(),
                    "leg": deepcopy(lighter_leg),
                })

        lighter_result = self._submit_leg(
            state,
            context=fresh,
            venue="lighter",
            side=lighter_side,
            qty=lighter_qty,
            reduce_only=True,
            session=session,
            opening=False,
        )
        if lighter_result.get("classification") != "filled":
            return self._manual_review(
                state,
                f"pending_close_lighter_{lighter_result.get('classification', 'unknown')}",
            )
        try:
            after = self._read(
                context,
                minimum_observed_at=self._latest_order_time(pending),
            )
        except WTIExecutionError:
            return self._manual_review(state, "post_close_account_snapshot_unavailable")
        if not _matches(after, (Decimal("0"), Decimal("0")), context):
            return self._manual_review(state, "post_close_exchange_position_mismatch")
        pending["recovery_account_snapshot_after"] = {
            "observed_at": after.observed_at.isoformat(),
            "variational_qty": str(after.variational_qty),
            "lighter_qty": str(after.lighter_qty),
            "active_order_count": after.active_order_count,
        }
        result = self._finalize_reconciled_close(state, pending)
        result["last_action_resolution"] = (
            "pending_close_missing_lighter_leg_completed_from_verified_account_snapshot"
        )
        self.store.save(result)
        return result

    def reconcile(
        self,
        *,
        sample: dict[str, Any],
        now: datetime | None = None,
        session: SessionWindow | None = None,
    ) -> dict[str, Any]:
        """Recover only from exact venue evidence; pending client IDs are never resubmitted."""
        context = self._validate_sample(sample, now=now or self._now())
        with self.store.exclusive():
            state = self.store.load()
            active = state.get("open_position") or state.get("pending_action") or {}
            if active and (
                integer_value(active.get("market_id")) != context.market_id
                or decimal_value(active.get("multiplier")) != context.multiplier
            ):
                return self._manual_review(state, "wti_market_metadata_changed_during_recovery")

            pending = state.get("pending_action")
            if pending is not None and (
                not isinstance(pending, dict)
                or state["status"] not in {"pending", "manual_review_required"}
            ):
                return self._manual_review(state, "execution_state_pending_action_mismatch")
            if pending is None and state["status"] == "pending":
                return self._manual_review(state, "pending_state_missing_action")
            if state["status"] == "flat" and state.get("open_position") is not None:
                return self._manual_review(state, "flat_state_contains_open_position")
            if state["status"] == "open" and not isinstance(state.get("open_position"), dict):
                return self._manual_review(state, "open_state_position_missing")
            orders_resolved = True
            if isinstance(pending, dict):
                orders_resolved = self._resolve_pending_orders(state, pending)
                try:
                    actual = self._read(
                        context,
                        minimum_observed_at=self._latest_order_time(pending),
                    )
                except WTIExecutionError:
                    return self._manual_review(state, "post_order_account_snapshot_unavailable")
            else:
                actual = self._read(context)
            if actual.active_order_count != 0:
                return self._manual_review(state, "active_wti_orders_require_reconciliation")
            if isinstance(pending, dict):
                resumed_close = self._resume_pending_close_missing_lighter(
                    state,
                    pending,
                    context=context,
                    actual=actual,
                    sample=sample,
                    session=session,
                )
                if resumed_close is not None:
                    return resumed_close
                if not orders_resolved:
                    return self._manual_review(state, "pending_order_terminal_status_unconfirmed")
                resumed = self._resume_pending_open_missing_lighter(
                    state,
                    pending,
                    context=context,
                    actual=actual,
                    sample=sample,
                    session=session,
                )
                if resumed is not None:
                    return resumed
            zero = (Decimal("0"), Decimal("0"))
            if state["status"] == "flat":
                if _matches(actual, zero, context):
                    state["manual_review_reason"] = None
                    self.store.save(state)
                    return state
                return self._manual_review(state, "flat_local_state_exchange_position_mismatch")
            if state["status"] == "open":
                position = state.get("open_position") or {}
                expected = (
                    decimal_value(position.get("expected_var_qty")),
                    decimal_value(position.get("expected_lighter_qty")),
                )
                if None not in expected and _matches(actual, (expected[0], expected[1]), context):
                    return state
                return self._manual_review(state, "open_local_state_exchange_position_mismatch")

            if not isinstance(pending, dict):
                return self._manual_review(state, "pending_state_missing_action")
            expected = (
                decimal_value(pending.get("expected_var_qty")),
                decimal_value(pending.get("expected_lighter_qty")),
            )
            if (
                None not in expected
                and _matches(actual, (expected[0], expected[1]), context)
                and self._all_expected_fills(pending)
            ):
                execution = self._archive_execution(state, pending, "filled", recovered=True)
                if pending.get("kind") == "open":
                    legs = pending.get("legs") or {}
                    var_fill = (legs.get("variational") or {}).get("result", {}).get("average_price")
                    lighter_fill = (legs.get("lighter") or {}).get("result", {}).get("average_price")
                    state["open_position"] = {
                        "direction": pending["direction"],
                        "var_qty": pending["var_qty"],
                        "lighter_qty": pending["lighter_qty"],
                        "expected_var_qty": str(expected[0]),
                        "expected_lighter_qty": str(expected[1]),
                        "var_entry_price": var_fill,
                        "lighter_entry_price": lighter_fill,
                        "market_id": pending["market_id"],
                        "multiplier": pending["multiplier"],
                        "session_id": pending.get("session_id"),
                        "opened_at": pending.get("intent_at") or _utc_now(),
                        "recovered_at": _utc_now(),
                        "recovered_from_pending": True,
                        "entry_execution_id": execution["action_id"],
                        **({
                            "single_round_pilot_id": pending.get("single_round_pilot_id")
                        } if pending.get("single_round_pilot_id") else {}),
                    }
                    state["status"] = "open"
                elif pending.get("kind") == "close":
                    state["open_position"] = None
                    state["status"] = "flat"
                    legs = pending.get("legs") or {}
                    state["last_closed_position"] = {
                        **(pending.get("previous_position") or {}),
                        **self._closed_pnl_evidence(
                            pending.get("previous_position") or {},
                            var_exit=(legs.get("variational") or {}).get("result") or {},
                            lighter_exit=(legs.get("lighter") or {}).get("result") or {},
                        ),
                        "closed_at": _utc_now(),
                        "recovered_from_pending": True,
                        "exit_execution_id": execution["action_id"],
                    }
                    self._complete_single_round_pilot(
                        state,
                        pending.get("previous_position") or {},
                        execution,
                    )
                else:
                    return self._manual_review(state, "pending_action_kind_invalid")
                state["pending_action"] = None
                state["manual_review_reason"] = None
                self.store.save(state)
                return state

            previous = pending.get("previous_position")
            if (
                pending.get("kind") == "open"
                and _matches(actual, zero, context)
                and self._all_known_no_fill(pending)
            ):
                execution = self._archive_execution(state, pending, "no_fill", recovered=True)
                state["status"] = "flat"
                state["open_position"] = None
                state["pending_action"] = None
                state["manual_review_reason"] = None
                state["last_action_resolution"] = "open_confirmed_no_fill"
                state["last_action_execution_id"] = execution["action_id"]
                self.store.save(state)
                return state
            if (
                pending.get("kind") == "close"
                and isinstance(previous, dict)
                and self._all_known_no_fill(pending)
            ):
                expected_previous = (
                    decimal_value(previous.get("expected_var_qty")),
                    decimal_value(previous.get("expected_lighter_qty")),
                )
                if None not in expected_previous and _matches(actual, (expected_previous[0], expected_previous[1]), context):
                    execution = self._archive_execution(state, pending, "no_fill", recovered=True)
                    state["status"] = "open"
                    state["open_position"] = previous
                    state["pending_action"] = None
                    state["manual_review_reason"] = None
                    state["last_action_resolution"] = "close_confirmed_no_fill"
                    state["last_action_execution_id"] = execution["action_id"]
                    self.store.save(state)
                    return state
            return self._manual_review(state, "pending_execution_not_exactly_reconciled")
