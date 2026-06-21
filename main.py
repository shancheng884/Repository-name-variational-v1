import argparse
import asyncio
import contextlib
import csv
import json
import logging
import math
import os
import signal
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from types import SimpleNamespace
from statistics import median
from typing import Any, Iterable

import requests
import websockets
from dotenv import load_dotenv
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from variational.listener import (
    CommandBroker,
    HEARTBEAT_STALE_SECONDS,
    EventSink,
    VariationalMonitor,
    run_command_server,
    run_receiver_server,
)
from paper_engine import (
    PaperEntryCandidate,
    PaperPositionState,
    percent_to_bps,
    paper_direction_values,
    paper_entry_candidate,
    paper_entry_execution_prices,
    paper_exit_execution_prices,
    paper_fee_cost_usd,
    paper_latency_drift_cost_usd,
    paper_lighter_taker_cost_usd,
    paper_var_spread_cost_usd,
)
from inventory_engine import DIRECTION_LONG_VAR_SHORT_LIGHTER, DIRECTION_SHORT_VAR_LONG_LIGHTER, PaperInventoryEngine

MODE_OBSERVE = "observe"
MODE_DRY_RUN = "dry-run"
MODE_LIVE = "live"
MODE_PAPER = "paper"
MODE_CHOICES = (MODE_OBSERVE, MODE_DRY_RUN, MODE_LIVE, MODE_PAPER)
LIGHTER_SUBMIT_TRANSPORT_HTTP = "http"
LIGHTER_SUBMIT_TRANSPORT_WS = "ws"
LIGHTER_SUBMIT_TRANSPORT_CHOICES = (LIGHTER_SUBMIT_TRANSPORT_HTTP, LIGHTER_SUBMIT_TRANSPORT_WS)
LIGHTER_ORDER_MODE_LIMIT_GTT = "limit-gtt"
LIGHTER_ORDER_MODE_MARKET_IOC = "market-ioc"
LIGHTER_ORDER_MODE_CHOICES = (LIGHTER_ORDER_MODE_LIMIT_GTT, LIGHTER_ORDER_MODE_MARKET_IOC)
VARIATIONAL_SUBMIT_TRANSPORT_DOM = "dom"
VARIATIONAL_SUBMIT_TRANSPORT_API = "api"
VARIATIONAL_SUBMIT_TRANSPORT_CHOICES = (VARIATIONAL_SUBMIT_TRANSPORT_DOM, VARIATIONAL_SUBMIT_TRANSPORT_API)
LIVE_INVENTORY_SIGNAL_SNAPSHOT = "snapshot"
LIVE_INVENTORY_SIGNAL_BASIS = "basis"
LIVE_INVENTORY_SIGNAL_CHOICES = (LIVE_INVENTORY_SIGNAL_SNAPSHOT, LIVE_INVENTORY_SIGNAL_BASIS)

STAGE_EVENT_RECEIVED = "event_received"
STAGE_EVENT_FILTERED = "event_filtered"
STAGE_RECORD_CREATED = "record_created"
STAGE_VARIATIONAL_FILLED = "variational_filled"
STAGE_DRY_RUN_PENDING = "dry_run_pending"
STAGE_DRY_RUN_PLANNED = "dry_run_planned"
STAGE_BLOCKED_BY_MODE = "blocked_by_mode"
STAGE_LIVE_SUBMIT_STARTED = "live_submit_started"
STAGE_LIVE_SUBMIT_SENT = "live_submit_sent"
STAGE_LIVE_SUBMIT_FAILED = "live_submit_failed"
STAGE_LIVE_SUBMIT_TIMED_OUT = "live_submit_timed_out"
STAGE_LIGHTER_FILLED = "lighter_filled"

FAILURE_STAGE_FILTER = "filter"
FAILURE_STAGE_HEDGE_PLAN = "hedge_plan"
FAILURE_STAGE_DRY_RUN_PLAN = "dry_run_plan"
FAILURE_STAGE_LIVE_SUBMIT = "live_submit"
FAILURE_STAGE_MODE_GUARD = "mode_guard"
RISK_GUARD_FAILURE_REASONS = {
    "lighter_order_book_not_ready",
    "lighter_order_book_stale",
    "hedge_base_amount_rounds_to_zero",
    "hedge_base_amount_exceeds_risk_limit",
    "hedge_below_lighter_min_base_amount",
    "hedge_below_lighter_min_quote_amount",
    "hedge_price_deviation_exceeds_risk_limit",
    "live_asset_not_allowed",
    "live_side_not_allowed",
    "live_qty_exceeds_limit",
    "live_notional_exceeds_limit",
    "live_edge_bps_below_threshold",
    "live_cooldown_active",
}

VARIATIONAL_TICKER_OVERRIDES = {
    "LIT": "LIGHTER",
}
VARIATIONAL_ASSET_TO_LIGHTER_TICKER = {v: k for k, v in VARIATIONAL_TICKER_OVERRIDES.items()}

FORWARDER_HOST = "127.0.0.1"
FORWARDER_WS_PORT = 8766
FORWARDER_REST_PORT = 8767
FORWARDER_COMMAND_PORT = 8768
LOG_DIR = Path("./log")
OUTPUT_DIR = LOG_DIR
APP_LOG_FILE = LOG_DIR / "runtime.log"
TRADE_RECORDS_CSV_FILE = LOG_DIR / "trade_records.csv"
MARKET_SAMPLES_FILE = LOG_DIR / "market_samples.jsonl"
INVENTORY_PAPER_FILE = LOG_DIR / "inventory_paper.jsonl"
INSTANCE_LOCK_FILE = LOG_DIR / "main.instance.lock"
AUTO_LIVE_STATE_FILE = LOG_DIR / "auto_live_state.json"
LIVE_INVENTORY_STATE_FILE = LOG_DIR / "live_inventory_state.json"
READY_TIMEOUT_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 0.05
HEDGE_SLIPPAGE_BPS = 100.0
RISK_GUARD_MAX_BASE_AMOUNT = 1000
RISK_GUARD_MAX_PRICE_DEVIATION_BPS = Decimal("500")
DEFAULT_LIVE_MAX_NOTIONAL_USD = Decimal("0")
DEFAULT_LIVE_MAX_QTY = Decimal("0")
DEFAULT_LIVE_REQUIRE_MIN_EDGE_BPS = Decimal("0")
DEFAULT_LIVE_COOLDOWN_SECONDS = 3.0
DEFAULT_LIVE_SUBMIT_TIMEOUT_SECONDS = 30.0
DEFAULT_PAPER_NOTIONAL_USD = Decimal("30")
DEFAULT_PAPER_ENTRY_DEVIATION_BPS = Decimal("3")
DEFAULT_PAPER_EXIT_DEVIATION_BPS = Decimal("0.5")
DEFAULT_PAPER_MAX_VAR_HALF_SPREAD_BPS = Decimal("2")
DEFAULT_PAPER_MAX_HOLDING_SECONDS = 1800.0
DEFAULT_PAPER_COOLDOWN_SECONDS = 10.0
DEFAULT_PAPER_MIN_SAMPLES = 30
DEFAULT_PAPER_INTERVAL_SECONDS = 1.0
DEFAULT_PAPER_LATENCY_DRIFT_BPS = Decimal("0.5")
DEFAULT_AUTO_LIVE_COMMAND_TIMEOUT_SECONDS = 15.0
DEFAULT_AUTO_LIVE_MATCH_WINDOW_SECONDS = 10.0
DEFAULT_AUTO_LIVE_MIN_HOLDING_SECONDS = 15.0
DEFAULT_AUTO_LIVE_COOLDOWN_SECONDS = 60.0
DEFAULT_AUTO_LIVE_MAX_CYCLES = 1
DEFAULT_VARIATIONAL_API_MAX_SLIPPAGE = 0.005
LIGHTER_INIT_RETRY_ATTEMPTS = 3
LIGHTER_INIT_RETRY_DELAY_SECONDS = 1.0
VAR_QUOTE_DIAGNOSTIC_INTERVAL_SECONDS = 30.0
VARIATIONAL_METADATA_STATS_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
VARIATIONAL_METADATA_STATS_TTL_SECONDS = 30.0
VARIATIONAL_METADATA_QUOTE_SIZE = "size_1k"
DASHBOARD_REFRESH_SECONDS = 1.0
DASHBOARD_ORDERS = 8
SPREAD_HISTORY_SECONDS = 3600.0
ASSET_SWITCH_CONFIRM_TICKS = 3
LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_WS_PING_INTERVAL_SECONDS = 30
LIGHTER_WS_PING_TIMEOUT_SECONDS = 30
HEALTH_VARIATIONAL_HEARTBEAT_DEGRADED_SECONDS = HEARTBEAT_STALE_SECONDS
HEALTH_VARIATIONAL_HEARTBEAT_STALE_SECONDS = HEARTBEAT_STALE_SECONDS * 2
HEALTH_QUOTE_DEGRADED_SECONDS = 30.0
HEALTH_QUOTE_STALE_SECONDS = 90.0
HEALTH_TRADE_EVENT_DEGRADED_SECONDS = 60.0
HEALTH_TRADE_EVENT_STALE_SECONDS = 180.0
HEALTH_LIGHTER_BOOK_DEGRADED_SECONDS = 10.0
HEALTH_LIGHTER_BOOK_STALE_SECONDS = 30.0
TRADE_EVENT_STARTUP_GRACE_SECONDS = 5.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def decimal_to_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


VARIATIONAL_API_AMOUNT_QUANTUM = Decimal("0.000001")
VARIATIONAL_API_AMOUNT_QUANTUM_BY_ASSET = {
    "ETH": Decimal("0.00001"),
}


def variational_api_amount_to_str(value: Decimal, *, asset: str | None = None) -> str:
    quantum = VARIATIONAL_API_AMOUNT_QUANTUM_BY_ASSET.get(str(asset or "").upper(), VARIATIONAL_API_AMOUNT_QUANTUM)
    return decimal_to_str(value.quantize(quantum, rounding=ROUND_DOWN)) or "0"


def elapsed_ms_str(start_monotonic: float | None) -> str:
    if start_monotonic is None:
        return "-"
    return f"{(time.monotonic() - start_monotonic) * 1000:.3f}"


def elapsed_ms_between_str(start_monotonic: float | None, end_monotonic: float | None) -> str:
    value = elapsed_ms(start_monotonic, end_monotonic)
    if value is None:
        return "-"
    return f"{value:.3f}"


def clean_state_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "none":
        return ""
    return text


def decimal_percent_to_bps(value: Decimal | None) -> Decimal | None:
    return percent_to_bps(value)


def find_open_paper_opportunity_ids(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []

    open_ids: set[str] = set()
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("record_kind") != "paper_opportunity":
                    continue
                opportunity_id = str(row.get("opportunity_id", "")).strip()
                if not opportunity_id:
                    continue
                status = str(row.get("status", "")).strip()
                if status == "paper_entered":
                    open_ids.add(opportunity_id)
                elif status == "paper_closed":
                    open_ids.discard(opportunity_id)
    except OSError:
        return []
    return sorted(open_ids)


def first_decimal_from_keys(payload: dict[str, Any], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        if key in payload:
            value = to_decimal(payload.get(key))
            if value is not None:
                return value
    return None


def nested_dicts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = [payload]
    for key in ("data", "payload", "quote", "indicative", "prices", "result"):
        value = payload.get(key)
        if isinstance(value, dict):
            items.append(value)
    return items


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_instance_lock(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        {
            "pid": os.getpid(),
            "created_at": utc_now(),
            "argv": list(os.sys.argv),
        },
        ensure_ascii=True,
    )

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                raw = lock_path.read_text(encoding="utf-8")
                existing = json.loads(raw)
            except Exception:
                existing = {}

            existing_pid = int(existing.get("pid", 0) or 0)
            if existing_pid and _pid_is_running(existing_pid):
                raise RuntimeError(
                    (
                        "Another main.py instance is already running "
                        f"(pid={existing_pid}, lock_file={lock_path}). Stop it before starting a new one."
                    )
                )

            with contextlib.suppress(FileNotFoundError):
                lock_path.unlink()
            continue

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
        except Exception:
            with contextlib.suppress(FileNotFoundError):
                lock_path.unlink()
            raise
        return os.getpid()


def release_instance_lock(lock_path: Path, owner_pid: int) -> None:
    try:
        raw = lock_path.read_text(encoding="utf-8")
        existing = json.loads(raw)
    except FileNotFoundError:
        return
    except Exception:
        existing = {}

    existing_pid = int(existing.get("pid", 0) or 0)
    if existing_pid not in {0, owner_pid}:
        return

    with contextlib.suppress(FileNotFoundError):
        lock_path.unlink()


def resolve_variational_ticker(ticker: str) -> str:
    return VARIATIONAL_TICKER_OVERRIDES.get(ticker.upper(), ticker.upper())


def resolve_lighter_ticker(variational_asset: str) -> str:
    asset = variational_asset.upper()
    return VARIATIONAL_ASSET_TO_LIGHTER_TICKER.get(asset, asset)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def required_int_env(name: str) -> int:
    value = required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got: {value}") from exc


def env_flag(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def spread_value(aggressive_buy_ask: Decimal | None, aggressive_sell_bid: Decimal | None) -> Decimal | None:
    if aggressive_buy_ask is None or aggressive_sell_bid is None:
        return None
    return aggressive_sell_bid - aggressive_buy_ask


def spread_percent(diff: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if diff is None or denominator is None or denominator == 0:
        return None
    return (diff / denominator) * Decimal("100")


def book_spread_percent(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / Decimal("2")
    if mid == 0:
        return None
    return ((ask - bid) / mid) * Decimal("100")


def basis_points_diff(value: Decimal | None, reference: Decimal | None) -> Decimal | None:
    if value is None or reference is None or reference == 0:
        return None
    return (abs(value - reference) / reference) * Decimal("10000")


def elapsed_ms(start: float | None, end: float | None) -> Decimal | None:
    if start is None or end is None:
        return None
    return Decimal(str(max(0.0, (end - start) * 1000)))


def elapsed_iso_ms(start_iso: str | None, end_iso: str | None) -> Decimal | None:
    if not start_iso or not end_iso:
        return None
    with contextlib.suppress(Exception):
        start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        return Decimal(str((end - start).total_seconds() * 1000))
    return None


def normalize_variational_status(status: str) -> str:
    lowered = status.strip().lower()
    if lowered in {"confirmed", "fill", "filled", "executed", "execution", "cleared"}:
        return "filled"
    return lowered


@dataclass(slots=True)
class OrderLifecycle:
    trade_key: str
    trade_id: str
    side: str
    qty: Decimal
    asset: str
    mode: str
    last_variational_status: str

    var_fill_price: Decimal | None = None
    var_fill_ts_iso: str | None = None
    synthetic_eager_fill: bool = False
    matched_variational_trade_id: str | None = None
    auto_live_cycle_id: int | None = None
    auto_live_role: str | None = None
    auto_live_merge_path: str | None = None

    lighter_side: str | None = None
    lighter_client_order_id: int | None = None
    lighter_submit_transport: str | None = None
    lighter_order_mode: str | None = None
    lighter_reduce_only: bool = False
    lighter_fill_price: Decimal | None = None
    lighter_fill_ts_iso: str | None = None
    lighter_tx_hash: str | None = None
    hedge_error: str | None = None
    lighter_reference_bid: Decimal | None = None
    lighter_reference_ask: Decimal | None = None
    dry_run_plan_side: str | None = None
    dry_run_plan_price: Decimal | None = None
    dry_run_plan_base_amount: int | None = None
    live_notional_usd: Decimal | None = None
    live_edge_bps: Decimal | None = None
    live_fill_latency_ms: Decimal | None = None
    live_var_fill_seen_at_iso: str | None = None
    live_plan_started_at_iso: str | None = None
    live_plan_ready_at_iso: str | None = None
    live_submit_started_at_iso: str | None = None
    live_submit_sent_at_iso: str | None = None
    live_var_fill_seen_monotonic: float | None = None
    live_plan_started_monotonic: float | None = None
    live_plan_ready_monotonic: float | None = None
    live_submit_started_monotonic: float | None = None
    live_submit_sent_monotonic: float | None = None
    live_lighter_fill_seen_monotonic: float | None = None
    processing_stage: str = STAGE_EVENT_RECEIVED
    stage_history: list[str] = field(default_factory=lambda: [STAGE_EVENT_RECEIVED])
    failure_stage: str | None = None
    failure_reason: str | None = None
    record_created_at: str = field(default_factory=utc_now)
    last_updated_at: str = field(default_factory=utc_now)

    def to_payload(self) -> dict[str, Any]:
        return {
            "record_kind": "execution_lifecycle",
            "trade_key": self.trade_key,
            "trade_id": self.trade_id,
            "side": self.side,
            "qty": decimal_to_str(self.qty),
            "asset": self.asset,
            "variational_filled_price": decimal_to_str(self.var_fill_price),
            "variational_filled_at": self.var_fill_ts_iso,
            "synthetic_eager_fill": self.synthetic_eager_fill,
            "matched_variational_trade_id": self.matched_variational_trade_id,
            "auto_live_cycle_id": self.auto_live_cycle_id,
            "auto_live_role": self.auto_live_role,
            "auto_live_merge_path": self.auto_live_merge_path,
            "lighter_order_side": self.lighter_side,
            "lighter_client_order_id": self.lighter_client_order_id,
            "lighter_submit_transport": getattr(
                self,
                "lighter_submit_transport",
                LIGHTER_SUBMIT_TRANSPORT_HTTP,
            ),
            "lighter_order_mode": getattr(self, "lighter_order_mode", LIGHTER_ORDER_MODE_LIMIT_GTT),
            "lighter_reduce_only": self.lighter_reduce_only,
            "lighter_filled_price": decimal_to_str(self.lighter_fill_price),
            "lighter_filled_at": self.lighter_fill_ts_iso,
            "mode": self.mode,
            "lighter_reference_bid": decimal_to_str(self.lighter_reference_bid),
            "lighter_reference_ask": decimal_to_str(self.lighter_reference_ask),
            "dry_run_plan_side": self.dry_run_plan_side,
            "dry_run_plan_price": decimal_to_str(self.dry_run_plan_price),
            "dry_run_plan_base_amount": self.dry_run_plan_base_amount,
            "live_notional_usd": decimal_to_str(self.live_notional_usd),
            "live_edge_bps": decimal_to_str(self.live_edge_bps),
            "live_fill_latency_ms": decimal_to_str(self.live_fill_latency_ms),
            "live_var_fill_seen_at": self.live_var_fill_seen_at_iso,
            "live_var_event_to_seen_ms": decimal_to_str(elapsed_iso_ms(
                self.var_fill_ts_iso,
                self.live_var_fill_seen_at_iso,
            )),
            "live_plan_started_at": self.live_plan_started_at_iso,
            "live_plan_ready_at": self.live_plan_ready_at_iso,
            "live_submit_started_at": self.live_submit_started_at_iso,
            "live_submit_sent_at": self.live_submit_sent_at_iso,
            "live_var_seen_to_plan_start_ms": decimal_to_str(elapsed_ms(
                self.live_var_fill_seen_monotonic,
                self.live_plan_started_monotonic,
            )),
            "live_plan_latency_ms": decimal_to_str(elapsed_ms(
                self.live_plan_started_monotonic,
                self.live_plan_ready_monotonic,
            )),
            "live_plan_ready_to_submit_start_ms": decimal_to_str(elapsed_ms(
                self.live_plan_ready_monotonic,
                self.live_submit_started_monotonic,
            )),
            "live_submit_call_latency_ms": decimal_to_str(elapsed_ms(
                self.live_submit_started_monotonic,
                self.live_submit_sent_monotonic,
            )),
            "live_submit_sent_to_fill_ms": decimal_to_str(elapsed_ms(
                self.live_submit_sent_monotonic,
                self.live_lighter_fill_seen_monotonic,
            )),
            "live_var_seen_to_lighter_fill_ms": decimal_to_str(elapsed_ms(
                self.live_var_fill_seen_monotonic,
                self.live_lighter_fill_seen_monotonic,
            )),
            "hedge_completion_status": self.hedge_completion_status,
            "rollback_action": self.rollback_action,
            "hedge_error": self.hedge_error,
            "processing_stage": self.processing_stage,
            "strategy_state": self.strategy_state,
            "stage_history": list(self.stage_history),
            "failure_stage": self.failure_stage,
            "failure_reason": self.failure_reason,
            "record_created_at": self.record_created_at,
            "last_updated_at": self.last_updated_at,
            "last_variational_status": self.last_variational_status,
        }

    @property
    def strategy_state(self) -> str:
        if self.failure_reason or self.failure_stage:
            return "fallback"
        if self.processing_stage == STAGE_BLOCKED_BY_MODE:
            return "closed"
        if self.processing_stage in {STAGE_EVENT_RECEIVED, STAGE_RECORD_CREATED, STAGE_EVENT_FILTERED}:
            return "idle"
        if self.processing_stage in {STAGE_VARIATIONAL_FILLED, STAGE_DRY_RUN_PENDING, STAGE_DRY_RUN_PLANNED}:
            return "entry_pending"
        if self.processing_stage == STAGE_LIVE_SUBMIT_STARTED:
            return "exit_pending"
        if self.processing_stage in {STAGE_LIVE_SUBMIT_SENT, STAGE_LIGHTER_FILLED}:
            return "in_position"
        if self.processing_stage == STAGE_LIVE_SUBMIT_FAILED:
            return "fallback"
        if self.processing_stage == STAGE_LIVE_SUBMIT_TIMED_OUT:
            return "fallback"
        return "idle"

    @property
    def hedge_completion_status(self) -> str:
        if self.var_fill_ts_iso is None:
            return "no_variational_fill"
        if self.lighter_fill_ts_iso is not None:
            return "hedged"
        if self.processing_stage in {STAGE_LIVE_SUBMIT_STARTED, STAGE_LIVE_SUBMIT_SENT}:
            return "hedge_pending"
        if self.processing_stage in {STAGE_LIVE_SUBMIT_FAILED, STAGE_LIVE_SUBMIT_TIMED_OUT}:
            return "naked_variational_leg"
        if self.failure_stage == FAILURE_STAGE_HEDGE_PLAN:
            return "hedge_blocked_before_submit"
        if self.processing_stage in {STAGE_DRY_RUN_PENDING, STAGE_DRY_RUN_PLANNED}:
            return "dry_run_only"
        if self.processing_stage == STAGE_BLOCKED_BY_MODE:
            return "not_live_mode"
        return "open"

    @property
    def rollback_action(self) -> str:
        if self.hedge_completion_status == "naked_variational_leg":
            return "manual_review_required"
        return "none"


@dataclass(slots=True)
class CrossSpreadSnapshot:
    asset: str
    var_bid: Decimal
    var_ask: Decimal
    var_mid: Decimal
    var_half_spread_bps: Decimal
    var_buy_price: Decimal | None
    var_sell_price: Decimal | None
    var_full_spread_bps: Decimal | None
    var_spread_source: str
    var_timestamp: str | None
    var_source_url: str | None
    var_source_stream: str | None
    lighter_bid: Decimal
    lighter_ask: Decimal
    lighter_mid: Decimal
    lighter_buy_price: Decimal
    lighter_sell_price: Decimal
    lighter_half_spread_bps: Decimal
    lighter_buy_fill_price: Decimal
    lighter_sell_fill_price: Decimal
    long_var_short_lighter_pct: Decimal
    short_var_long_lighter_pct: Decimal
    long_median_5m_pct: Decimal | None
    short_median_5m_pct: Decimal | None
    long_sample_count_5m: int
    short_sample_count_5m: int


@dataclass(slots=True)
class AutoLivePositionState:
    cycle_id: int
    asset: str
    direction: str
    entered_at_iso: str
    entered_at_monotonic: float
    entry_spread_pct: Decimal
    entry_median_pct: Decimal
    entry_deviation_bps: Decimal
    entry_var_mid: Decimal
    entry_lighter_mid: Decimal
    entry_var_execution_price: Decimal
    entry_lighter_execution_price: Decimal
    planned_notional_usd: Decimal
    planned_qty: Decimal
    exit_submitted: bool = False
    exit_submitted_at_iso: str | None = None
    exit_side: str | None = None
    exit_reason: str | None = None
    manual_review_required: bool = False
    manual_review_reason: str | None = None
    manual_review_logged: bool = False


@dataclass(slots=True)
class PendingAutoLiveMatch:
    record_key: str
    asset: str
    side: str
    qty: Decimal
    cycle_id: int | None
    role: str
    created_at_monotonic: float


@dataclass(slots=True)
class PendingLiveInventoryVarFillMatch:
    asset: str
    side: str
    qty: Decimal
    lot_id: int
    role: str
    created_at_monotonic: float
    context: dict[str, Any] | None = None


@dataclass(slots=True)
class StartupDiagnostics:
    passed: list[str]
    warnings: list[str]
    blocking_errors: list[str]


@dataclass(slots=True)
class HealthStatus:
    overall: str
    components: list[tuple[str, str, str]]


class VariationalRuntime:
    def __init__(
        self,
        host: str,
        ws_port: int,
        rest_port: int,
        command_port: int,
        output_dir: Path | None,
        quiet: bool,
    ) -> None:
        self.monitor = VariationalMonitor(trade_limit=500, snapshot_file=None)
        self.sink = EventSink(output_dir=output_dir, quiet=quiet, monitor=self.monitor)
        self.command_broker = CommandBroker(quiet=quiet)
        self.host = host
        self.ws_port = ws_port
        self.rest_port = rest_port
        self.command_port = command_port
        self.ws_server = None
        self.rest_server = None
        self.command_server = None

    async def start(self) -> None:
        self.ws_server = await run_receiver_server("ws", self.host, self.ws_port, self.sink)
        self.rest_server = await run_receiver_server("rest", self.host, self.rest_port, self.sink)
        self.command_server = await run_command_server(self.host, self.command_port, self.command_broker)

    async def stop(self) -> None:
        if self.ws_server is not None:
            self.ws_server.close()
            await self.ws_server.wait_closed()
        if self.rest_server is not None:
            self.rest_server.close()
            await self.rest_server.wait_closed()
        if self.command_server is not None:
            self.command_server.close()
            await self.command_server.wait_closed()


class VariationalToLighterRuntime:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.mode = args.mode
        self.risk_guard_max_base_amount = args.risk_guard_max_base_amount
        self.risk_guard_max_price_deviation_bps = Decimal(str(args.risk_guard_max_price_deviation_bps))
        self.live_max_notional_usd = Decimal(str(args.live_max_notional_usd))
        self.live_max_qty = Decimal(str(args.live_max_qty))
        self.live_require_min_edge_bps = Decimal(str(args.live_require_min_edge_bps))
        self.live_cooldown_seconds = float(args.live_cooldown_seconds)
        self.auto_live_entry = bool(args.auto_live_entry)
        self.auto_live_exit = bool(args.auto_live_exit)
        self.auto_live_eager_hedge = bool(args.auto_live_eager_hedge)
        self.auto_live_skip_entry_preview = bool(args.auto_live_skip_entry_preview)
        self.auto_live_i_confirm_flat_start = bool(args.auto_live_i_confirm_flat_start)
        self.auto_live_reset_state_after_manual_flat = bool(args.auto_live_reset_state_after_manual_flat)
        self.auto_live_command_timeout_seconds = float(args.auto_live_command_timeout_seconds)
        self.auto_live_match_window_seconds = float(args.auto_live_match_window_seconds)
        self.auto_live_min_holding_seconds = float(args.auto_live_min_holding_seconds)
        self.auto_live_entry_max_precheck_edge_bps = Decimal(str(args.auto_live_entry_max_precheck_edge_bps))
        self.auto_live_entry_min_actionable_edge_bps = Decimal(str(args.auto_live_entry_min_actionable_edge_bps))
        self.auto_live_disable_short_var_long_lighter = bool(args.auto_live_disable_short_var_long_lighter)
        self.auto_live_cooldown_seconds = float(args.auto_live_cooldown_seconds)
        self.auto_live_max_cycles = int(args.auto_live_max_cycles)
        self.live_inventory = bool(args.live_inventory)
        self.live_inventory_dry_decisions = bool(args.live_inventory_dry_decisions)
        self.live_inventory_signal_mode = args.live_inventory_signal_mode
        self.live_inventory_i_accept_basis_real_diagnostic = bool(args.live_inventory_i_accept_basis_real_diagnostic)
        self.live_inventory_i_confirm_flat_start = bool(args.live_inventory_i_confirm_flat_start)
        self.live_inventory_i_accept_open_state_resume = bool(args.live_inventory_i_accept_open_state_resume)
        self.live_inventory_reset_state_after_manual_flat = bool(args.live_inventory_reset_state_after_manual_flat)
        self.live_inventory_lot_notional_usd = Decimal(str(args.live_inventory_lot_notional_usd))
        self.live_inventory_max_lots = int(args.live_inventory_max_lots)
        self.live_inventory_max_total_lots = int(args.live_inventory_max_total_lots)
        self.live_inventory_entry_bps = Decimal(str(args.live_inventory_entry_bps))
        self.live_inventory_exit_bps = Decimal(str(args.live_inventory_exit_bps))
        self.live_inventory_max_var_spread_bps = Decimal(str(args.live_inventory_max_var_spread_bps))
        self.live_inventory_max_var_snapshot_age_seconds = float(args.live_inventory_max_var_snapshot_age_seconds)
        self.live_inventory_refresh_var_quote_before_entry = bool(args.live_inventory_refresh_var_quote_before_entry)
        self.live_inventory_dynamic_entry_buffer_bps = Decimal(str(args.live_inventory_dynamic_entry_buffer_bps))
        self.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics = bool(
            args.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics
        )
        self.live_inventory_max_lighter_slippage_bps = Decimal(str(args.live_inventory_max_lighter_slippage_bps))
        self.live_inventory_min_hold_samples = int(args.live_inventory_min_hold_samples)
        self.live_inventory_max_hold_samples = int(args.live_inventory_max_hold_samples)
        self.live_inventory_max_unrealized_loss_bps = Decimal(str(args.live_inventory_max_unrealized_loss_bps))
        self.live_inventory_max_cycles = int(args.live_inventory_max_cycles)
        self.live_inventory_basis_z_entry = Decimal(str(args.live_inventory_basis_z_entry))
        self.live_inventory_basis_z_exit = Decimal(str(args.live_inventory_basis_z_exit))
        self.live_inventory_basis_min_entry_edge_bps = Decimal(str(args.live_inventory_basis_min_entry_edge_bps))
        self.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal(
            str(args.live_inventory_basis_max_entry_roundtrip_cost_bps)
        )
        self.live_inventory_basis_min_abs_entry_bps = Decimal(str(args.live_inventory_basis_min_abs_entry_bps))
        self.live_inventory_basis_min_exit_pnl_bps = Decimal(str(args.live_inventory_basis_min_exit_pnl_bps))
        self.live_inventory_basis_exit_safety_buffer_bps = Decimal(str(args.live_inventory_basis_exit_safety_buffer_bps))
        self.live_inventory_basis_max_hold_action = args.live_inventory_basis_max_hold_action
        self.live_inventory_i_accept_basis_addon_diagnostic = bool(args.live_inventory_i_accept_basis_addon_diagnostic)
        self.live_inventory_basis_addon_min_basis_improvement_bps = Decimal(
            str(args.live_inventory_basis_addon_min_basis_improvement_bps)
        )
        self.paper_notional_usd = Decimal(str(args.paper_notional_usd))
        self.paper_entry_deviation_bps = Decimal(str(args.paper_entry_deviation_bps))
        self.paper_exit_deviation_bps = Decimal(str(args.paper_exit_deviation_bps))
        self.paper_max_var_half_spread_bps = Decimal(str(args.paper_max_var_half_spread_bps))
        self.paper_max_holding_seconds = float(args.paper_max_holding_seconds)
        self.paper_cooldown_seconds = float(args.paper_cooldown_seconds)
        self.paper_min_samples = int(args.paper_min_samples)
        self.paper_interval_seconds = float(args.paper_interval_seconds)
        self.paper_fee_bps_per_leg = Decimal(str(args.paper_fee_bps_per_leg))
        self.paper_latency_drift_bps = Decimal(str(args.paper_latency_drift_bps))
        self.paper_inventory = bool(args.paper_inventory)
        self.paper_inventory_sample_index = 0
        self.paper_inventory_engine = (
            PaperInventoryEngine(
                lot_notional_usd=Decimal(str(args.paper_inventory_lot_notional_usd)),
                max_lots=int(args.paper_inventory_max_lots),
                entry_bps=Decimal(str(args.paper_inventory_entry_bps)),
                exit_bps=Decimal(str(args.paper_inventory_exit_bps)),
                min_hold_samples=int(args.paper_inventory_min_hold_samples),
                max_total_lots=int(args.paper_inventory_max_total_lots),
                latency_samples=int(args.paper_inventory_latency_samples),
            )
            if self.paper_inventory
            else None
        )
        self.ticker: str | None = None
        self.variational_ticker: str | None = None
        self.accepted_assets: set[str] = set()
        self.live_allowed_assets = {
            asset.strip().upper() for asset in str(args.live_allowed_assets).split(",") if asset.strip()
        }
        self.live_allowed_sides = {
            side.strip().lower() for side in str(args.live_allowed_sides).split(",") if side.strip()
        }
        self.live_submit_timeout_seconds = float(args.live_submit_timeout_seconds)
        self.variational_submit_transport = args.variational_submit_transport
        self.variational_api_max_slippage = float(args.variational_api_max_slippage)
        self.lighter_submit_transport = args.lighter_submit_transport
        self.lighter_order_mode = args.lighter_order_mode
        self.lighter_prewarm_submit_ws = bool(args.lighter_prewarm_submit_ws)

        self.stop_flag = False
        self.logger = logging.getLogger("var_lighter_runtime")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.logger.propagate = False

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(APP_LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(file_handler)
        self.dashboard_console = Console()

        output_dir = OUTPUT_DIR.expanduser().resolve()
        self.runtime = VariationalRuntime(
            host=FORWARDER_HOST,
            ws_port=FORWARDER_WS_PORT,
            rest_port=FORWARDER_REST_PORT,
            command_port=FORWARDER_COMMAND_PORT,
            output_dir=output_dir,
            quiet=True,
        )

        self.orders_file = output_dir / "order_metrics.jsonl" if output_dir else None
        self.opportunities_file = output_dir / "opportunities.jsonl" if output_dir else None
        self.market_samples_file = output_dir / MARKET_SAMPLES_FILE.name if output_dir else None
        self.inventory_paper_file = output_dir / INVENTORY_PAPER_FILE.name if output_dir else None
        self.trade_records_csv_file = output_dir / TRADE_RECORDS_CSV_FILE.name if output_dir else None
        self.auto_live_state_file = output_dir / AUTO_LIVE_STATE_FILE.name if output_dir else None
        self.live_inventory_state_file = output_dir / LIVE_INVENTORY_STATE_FILE.name if output_dir else None
        self.live_inventory_sample_index = 0
        self.live_inventory_next_lot_id = 1
        self.live_inventory_open_lots: list[dict[str, Any]] = []
        self.live_inventory_realized_pnl_usd = Decimal("0")
        self.live_inventory_completed_cycles = 0
        self.pending_live_inventory_actual_pnl: dict[str, dict[str, Any]] = {}
        self.pending_live_inventory_final_pnl: dict[str, dict[str, Any]] = {}
        self.live_inventory_execution_loss_bps_samples: deque[Decimal] = deque(maxlen=20)
        self.load_recent_live_inventory_execution_loss_bps()
        self.pending_live_inventory_var_fill_matches: list[PendingLiveInventoryVarFillMatch] = []
        self.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=float(args.live_inventory_basis_half_life_seconds),
            warmup_samples=int(args.live_inventory_basis_warmup_samples),
            gap_reset_seconds=float(args.live_inventory_basis_gap_reset_seconds),
            sigma_floor_bps=float(args.live_inventory_basis_sigma_floor_bps),
        )
        self._order_write_lock = asyncio.Lock()
        self._opportunity_write_lock = asyncio.Lock()
        self._trade_csv_write_lock = asyncio.Lock()
        self._trade_records_snapshot_sig: str | None = None

        self.records: dict[str, OrderLifecycle] = {}
        self.record_order: deque[str] = deque(maxlen=500)
        self.lighter_client_order_to_trade_key: dict[int, str] = {}
        self._record_lock = asyncio.Lock()
        self.cross_spread_history: deque[tuple[float, float | None, float | None]] = deque()
        self._asset_switch_lock = asyncio.Lock()
        self._asset_switch_candidate: str | None = None
        self._asset_switch_candidate_hits = 0

        self.trade_event_cursor = 0
        self.trade_event_min_timestamp: datetime | None = None

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index: int | None = None
        self.api_key_index: int | None = None
        self.lighter_client: Any | None = None
        self._lighter_signer_lock = asyncio.Lock()
        self._lighter_submit_ws: Any | None = None
        self._lighter_submit_ws_lock = asyncio.Lock()
        self._var_command_ws: Any | None = None
        self._var_command_ws_lock = asyncio.Lock()

        self.lighter_market_index = 0
        self.base_amount_multiplier = 0
        self.price_multiplier = 0
        self.lighter_min_base_amount: Decimal | None = None
        self.lighter_min_quote_amount: Decimal | None = None

        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid: Decimal | None = None
        self.lighter_best_ask: Decimal | None = None
        self.lighter_order_book_offset = 0
        self.lighter_order_book_ready = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_sequence_gap = False
        self.lighter_order_book_lock = asyncio.Lock()

        self.last_variational_trade_event_at: str | None = None
        self.last_lighter_order_book_update_at: str | None = None
        self.last_live_submit_monotonic_by_asset: dict[str, float] = {}
        self.live_inventory_var_reject_cooldown_until: dict[tuple[str, str], float] = {}
        self.live_inventory_var_reject_cooldown_seconds = 600.0
        self.paper_position: PaperPosition | None = None
        self.auto_live_position: AutoLivePositionState | None = None
        self.pending_auto_live_matches: list[PendingAutoLiveMatch] = []
        self.auto_live_last_closed_monotonic: float | None = None
        self.auto_live_completed_cycles = 0
        self.auto_live_next_cycle_id = 1
        self.auto_live_manual_review_required = False
        self.auto_live_manual_review_reason: str | None = None
        self._last_auto_live_guard_log: tuple[str, int, int] | None = None
        self._last_auto_live_precheck_failure_log: dict[tuple[str, int, str, str, str], float] = {}
        self.paper_last_closed_monotonic: float | None = None
        self.paper_opportunity_counter = 0
        self._last_var_quote_diagnostic_at = 0.0
        self._variational_metadata_stats: dict[str, Any] | None = None
        self._variational_metadata_stats_at = 0.0
        self._last_metadata_stats_error_at = 0.0

        self.lighter_ws_task: asyncio.Task[None] | None = None
        self.trade_task: asyncio.Task[None] | None = None
        self.spread_task: asyncio.Task[None] | None = None
        self.paper_task: asyncio.Task[None] | None = None
        self.auto_live_task: asyncio.Task[None] | None = None
        self.dashboard_task: asyncio.Task[None] | None = None
        self.watchdog_task: asyncio.Task[None] | None = None

    @staticmethod
    def now_iso() -> str:
        return utc_now()

    def load_recent_live_inventory_execution_loss_bps(self) -> None:
        orders_file = getattr(self, "orders_file", None)
        if orders_file is None or not orders_file.exists():
            return
        samples: deque[Decimal] = deque(maxlen=self.live_inventory_execution_loss_bps_samples.maxlen)
        try:
            with orders_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if '"event": "live_inventory_final_pnl"' not in line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    loss_bps = to_decimal(row.get("entry_edge_capture_loss_bps"))
                    if loss_bps is not None and loss_bps > 0:
                        samples.append(loss_bps)
        except OSError as exc:
            self.logger.warning("Could not load recent live inventory execution loss samples: %s", exc)
            return
        self.live_inventory_execution_loss_bps_samples.extend(samples)

    @staticmethod
    def percentile_decimal(values: Iterable[Decimal], percentile: int) -> Decimal | None:
        ordered = sorted(value for value in values if value is not None)
        if not ordered:
            return None
        rank = ((len(ordered) * percentile) + 99) // 100
        index = max(0, min(len(ordered) - 1, rank - 1))
        return ordered[index]

    def live_inventory_recent_execution_loss_buffer_bps(self) -> Decimal:
        return self.percentile_decimal(getattr(self, "live_inventory_execution_loss_bps_samples", []), 80) or Decimal("0")

    @staticmethod
    def set_record_stage(
        record: OrderLifecycle,
        stage: str,
        *,
        failure_stage: str | None = None,
        failure_reason: str | None = None,
        clear_failure: bool = False,
    ) -> None:
        record.processing_stage = stage
        if not record.stage_history or record.stage_history[-1] != stage:
            record.stage_history.append(stage)
        record.last_updated_at = utc_now()
        if clear_failure:
            record.failure_stage = None
            record.failure_reason = None
        if failure_stage is not None:
            record.failure_stage = failure_stage
        if failure_reason is not None:
            record.failure_reason = failure_reason

    def is_observe_mode(self) -> bool:
        return self.mode == MODE_OBSERVE

    def is_dry_run_mode(self) -> bool:
        return self.mode == MODE_DRY_RUN

    def is_live_mode(self) -> bool:
        return self.mode == MODE_LIVE

    def is_paper_mode(self) -> bool:
        return self.mode == MODE_PAPER

    def is_auto_live_enabled(self) -> bool:
        return self.is_live_mode() and (self.auto_live_entry or self.auto_live_exit)

    def is_live_inventory_enabled(self) -> bool:
        return self.is_live_mode() and self.live_inventory

    def sync_live_inventory_memory_from_state(self) -> None:
        state = self.load_live_inventory_state()
        open_lots = state.get("open_lots")
        self.live_inventory_open_lots = open_lots if isinstance(open_lots, list) else []
        self.live_inventory_next_lot_id = int(state.get("next_lot_id") or 1)
        self.live_inventory_realized_pnl_usd = to_decimal(state.get("realized_pnl_usd")) or Decimal("0")
        self.live_inventory_completed_cycles = int(state.get("completed_cycles") or 0)

    def live_inventory_state_asset(self) -> str:
        for lot in self.live_inventory_open_lots:
            if isinstance(lot, dict) and lot.get("asset"):
                return str(lot.get("asset")).upper()
        if len(getattr(self, "live_allowed_assets", set()) or set()) == 1:
            return next(iter(self.live_allowed_assets)).upper()
        return "BTC"

    async def persist_live_inventory_memory(self, *, reason: str) -> None:
        await self.write_live_inventory_state_async(
            {
                "status": "open" if self.live_inventory_open_lots else "flat",
                "asset": self.live_inventory_state_asset(),
                "next_lot_id": self.live_inventory_next_lot_id,
                "open_lots": self.live_inventory_open_lots,
                "pending_actions": [],
                "realized_pnl_usd": decimal_to_str(self.live_inventory_realized_pnl_usd),
                "completed_cycles": self.live_inventory_completed_cycles,
                "reason": reason,
            }
        )

    async def require_live_inventory_manual_review(
        self,
        *,
        asset: str,
        reason: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        await self.write_live_inventory_state_async(
            {
                "status": "manual_review_required",
                "asset": asset,
                "next_lot_id": self.live_inventory_next_lot_id,
                "open_lots": self.live_inventory_open_lots,
                "pending_actions": [],
                "realized_pnl_usd": decimal_to_str(self.live_inventory_realized_pnl_usd),
                "completed_cycles": self.live_inventory_completed_cycles,
                "manual_review_reason": reason,
                "manual_review_context": context or {},
                "action": "stop_live_inventory_until_manual_flat_confirmed",
            }
        )
        await self.append_live_inventory_log(
            "live_inventory_manual_review_required",
            {
                "asset": asset,
                "reason": reason,
                "manual_review_context": context or {},
                "open_lots_total": len(self.live_inventory_open_lots),
                "completed_cycles": self.live_inventory_completed_cycles,
                "action": "stop_live_inventory_until_manual_flat_confirmed",
            },
        )
        self.logger.warning(
            "live_inventory_manual_review_required asset=%s reason=%s open_lots=%s completed_cycles=%s action=stop_live_inventory_until_manual_flat_confirmed",
            asset,
            reason,
            len(self.live_inventory_open_lots),
            self.live_inventory_completed_cycles,
        )
        self.stop_flag = True

    async def block_live_inventory_entry(self, *, asset: str, reason: str, context: dict[str, Any]) -> None:
        await self.write_live_inventory_state_async(
            {
                "status": "flat",
                "asset": asset,
                "next_lot_id": self.live_inventory_next_lot_id,
                "open_lots": self.live_inventory_open_lots,
                "pending_actions": [],
                "realized_pnl_usd": decimal_to_str(self.live_inventory_realized_pnl_usd),
                "completed_cycles": self.live_inventory_completed_cycles,
                "last_blocked_reason": reason,
                "last_blocked_context": context,
            }
        )
        await self.append_live_inventory_log(
            "live_inventory_entry_blocked",
            {
                "asset": asset,
                "reason": reason,
                "blocked_context": context,
                "open_lots_total": len(self.live_inventory_open_lots),
                "completed_cycles": self.live_inventory_completed_cycles,
            },
        )

    async def maybe_timeout_pending_live_inventory_var_entry(self, *, asset: str) -> bool:
        now_monotonic = time.monotonic()
        for item in list(getattr(self, "pending_live_inventory_var_fill_matches", [])):
            if item.asset.upper() != asset.upper() or item.role != "live_inventory_entry_pending_lighter":
                continue
            age_seconds = now_monotonic - item.created_at_monotonic
            resolved = await self.maybe_resolve_pending_live_inventory_var_entry_from_orders(
                item=item,
                age_seconds=age_seconds,
            )
            if resolved:
                return True
            if age_seconds < self.auto_live_match_window_seconds:
                continue
            self.remove_pending_live_inventory_var_fill_match(
                asset=item.asset,
                lot_id=item.lot_id,
                role=item.role,
            )
            positions_result = None
            position_qty = None
            position_check_error = None
            try:
                positions_result = await self.fetch_variational_positions()
                position_qty = self.extract_variational_position_qty(positions_result, asset=item.asset)
            except Exception as exc:
                position_check_error = str(exc)
            position_abs = abs(position_qty) if position_qty is not None else None
            reason = (
                "basis_entry_var_fill_timeout_position_detected"
                if position_abs is not None and position_abs > Decimal("0")
                else "basis_entry_var_fill_timeout"
            )
            await self.require_live_inventory_manual_review(
                asset=item.asset,
                reason=reason,
                context={
                    "lot_id": item.lot_id,
                    "side": item.side,
                    "qty": decimal_to_str(item.qty),
                    "age_seconds": age_seconds,
                    "timeout_seconds": self.auto_live_match_window_seconds,
                    "variational_position_qty": decimal_to_str(position_qty),
                    "variational_position_check_error": position_check_error,
                    "variational_positions_result": positions_result,
                    "pending_context": item.context or {},
                    "action": "confirm_variational_order_status_and_position_manually",
                },
            )
            return True
        return False

    async def maybe_resolve_pending_live_inventory_var_entry_from_orders(
        self,
        *,
        item: PendingLiveInventoryVarFillMatch,
        age_seconds: float,
    ) -> bool:
        context = item.context or {}
        rfq_id = str(context.get("rfq_id") or "").strip()
        if not rfq_id:
            return False
        now_monotonic = time.monotonic()
        last_check = context.get("orders_v2_last_check_monotonic")
        if isinstance(last_check, (int, float)) and now_monotonic - float(last_check) < 1.0:
            return False
        context["orders_v2_last_check_monotonic"] = now_monotonic
        item.context = context

        try:
            orders_result = await self.fetch_variational_orders(
                asset=item.asset,
                status="canceled,cleared,rejected",
                limit=20,
            )
        except Exception as exc:
            context["orders_v2_last_error"] = str(exc)
            return False

        order = self.find_variational_order_by_rfq_id(orders_result, rfq_id=rfq_id)
        if order is None:
            context["orders_v2_last_result"] = "rfq_not_found"
            return False

        status = str(order.get("status") or "").strip().lower()
        if status == "cleared":
            price = to_decimal(order.get("price"))
            qty = to_decimal(order.get("qty")) or item.qty
            if price is None or qty <= 0:
                self.remove_pending_live_inventory_var_fill_match(
                    asset=item.asset,
                    lot_id=item.lot_id,
                    role=item.role,
                )
                await self.require_live_inventory_manual_review(
                    asset=item.asset,
                    reason="basis_entry_var_order_cleared_missing_fill_details",
                    context={"lot_id": item.lot_id, "rfq_id": rfq_id, "order": order, "orders_result": orders_result},
                )
                return True
            item.qty = qty
            record = OrderLifecycle(
                trade_key=f"var_order:{order.get('order_id') or rfq_id}",
                trade_id=str(order.get("order_id") or rfq_id),
                side=str(order.get("side") or item.side).lower(),
                qty=qty,
                asset=item.asset.upper(),
                mode=self.mode,
                last_variational_status="filled",
                var_fill_price=price,
                var_fill_ts_iso=str(order.get("execution_timestamp") or order.get("created_at") or utc_now()),
                live_var_fill_seen_at_iso=utc_now(),
                live_var_fill_seen_monotonic=time.monotonic(),
                matched_variational_trade_id=str(order.get("order_id") or rfq_id),
                auto_live_cycle_id=item.lot_id,
                auto_live_role=item.role,
                auto_live_merge_path="orders_v2_confirmed_var_fill",
            )
            self.remove_pending_live_inventory_var_fill_match(asset=item.asset, lot_id=item.lot_id, role=item.role)
            await self.append_order_log("variational_fill", record.to_payload())
            await self.complete_live_inventory_entry_after_var_fill(
                match=item,
                record=record,
                fill_payload={**record.to_payload(), "variational_order": order},
            )
            return True

        if status in {"rejected", "canceled", "cancelled"}:
            self.remove_pending_live_inventory_var_fill_match(asset=item.asset, lot_id=item.lot_id, role=item.role)
            clearing_status = str(order.get("clearing_status") or "").strip().lower()
            if clearing_status == "rejected_failed_taker_funding":
                self.live_inventory_var_reject_cooldown_until[(item.asset.upper(), item.side.lower())] = (
                    time.monotonic() + self.live_inventory_var_reject_cooldown_seconds
                )
            await self.persist_live_inventory_memory(reason=f"basis_var_entry_order_{status}")
            await self.write_live_inventory_state_async(
                {
                    "status": "flat",
                    "asset": item.asset.upper(),
                    "next_lot_id": self.live_inventory_next_lot_id,
                    "open_lots": self.live_inventory_open_lots,
                    "pending_actions": [],
                    "realized_pnl_usd": decimal_to_str(self.live_inventory_realized_pnl_usd),
                    "completed_cycles": self.live_inventory_completed_cycles,
                    "last_rejected_reason": f"variational_order_{status}",
                    "last_rejected_context": {"lot_id": item.lot_id, "rfq_id": rfq_id, "order": order},
                }
            )
            await self.append_live_inventory_log(
                "live_inventory_var_entry_final_rejected",
                {
                    "asset": item.asset.upper(),
                    "lot_id": item.lot_id,
                    "side": item.side,
                    "qty": decimal_to_str(item.qty),
                    "age_seconds": age_seconds,
                    "rfq_id": rfq_id,
                    "status": status,
                    "clearing_status": order.get("clearing_status"),
                    "cancel_reason": order.get("cancel_reason"),
                    "failed_risk_checks": order.get("failed_risk_checks"),
                    "order": order,
                    "cooldown_seconds": self.live_inventory_var_reject_cooldown_seconds if clearing_status == "rejected_failed_taker_funding" else None,
                    "action": "removed_pending_without_lighter_hedge",
                },
            )
            return True
        context["orders_v2_last_result"] = f"unhandled_status:{status or 'missing'}"
        return False

    @staticmethod
    def live_inventory_pair_pnl(
        *,
        direction: str,
        qty: Decimal,
        entry_var_price: Decimal,
        entry_lighter_price: Decimal,
        exit_var_price: Decimal,
        exit_lighter_price: Decimal,
    ) -> tuple[Decimal, Decimal, Decimal]:
        if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
            var_leg_pnl = (exit_var_price - entry_var_price) * qty
            lighter_leg_pnl = (entry_lighter_price - exit_lighter_price) * qty
        else:
            var_leg_pnl = (entry_var_price - exit_var_price) * qty
            lighter_leg_pnl = (exit_lighter_price - entry_lighter_price) * qty
        return var_leg_pnl, lighter_leg_pnl, var_leg_pnl + lighter_leg_pnl

    @staticmethod
    def live_inventory_price_drift_bps(actual: Decimal | None, estimated: Decimal | None) -> Decimal | None:
        if actual is None or estimated is None or estimated == 0:
            return None
        return ((actual - estimated) / estimated) * Decimal("10000")

    @staticmethod
    def variational_api_order_quote_fields(side: str, result: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(result, dict):
            return {}
        payload = result.get("result") if isinstance(result.get("result"), dict) else result
        bid = to_decimal(payload.get("bid"))
        ask = to_decimal(payload.get("ask"))
        side_upper = side.strip().upper()
        execution_price = ask if side_upper == "BUY" else bid
        return {
            "quote_id": payload.get("quoteId") or payload.get("quote_id"),
            "quote_bid": decimal_to_str(bid),
            "quote_ask": decimal_to_str(ask),
            "quote_mark_price": payload.get("markPrice") or payload.get("mark_price"),
            "quote_timestamp": payload.get("quoteTimestamp") or payload.get("timestamp"),
            "quote_execution_price": decimal_to_str(execution_price),
        }

    @staticmethod
    def live_inventory_pair_edge_bps(
        *,
        direction: str,
        var_price: Decimal | None,
        lighter_price: Decimal | None,
    ) -> Decimal | None:
        if var_price is None or lighter_price is None or var_price == 0:
            return None
        if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
            return ((lighter_price - var_price) / var_price) * Decimal("10000")
        if direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
            return ((var_price - lighter_price) / var_price) * Decimal("10000")
        return None

    @staticmethod
    def live_inventory_roundtrip_pnl_bps(
        *,
        direction: str,
        var_entry_price: Decimal,
        lighter_entry_price: Decimal,
        var_exit_price: Decimal,
        lighter_exit_price: Decimal,
    ) -> Decimal:
        if var_entry_price <= 0:
            raise ValueError("var_entry_price must be > 0")
        if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
            pnl_per_unit = (var_exit_price - var_entry_price) + (lighter_entry_price - lighter_exit_price)
        elif direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
            pnl_per_unit = (var_entry_price - var_exit_price) + (lighter_exit_price - lighter_entry_price)
        else:
            raise ValueError(f"unsupported direction: {direction}")
        return pnl_per_unit / var_entry_price * Decimal("10000")

    @staticmethod
    def live_inventory_basis_direction_signal(direction: str, z: Decimal) -> Decimal:
        if direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
            return z
        if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
            return -z
        raise ValueError(f"unsupported direction: {direction}")

    def live_inventory_basis_abs_entry_ok(self, *, direction: str, basis_bps: Decimal) -> bool:
        threshold = self.live_inventory_basis_min_abs_entry_bps
        if threshold <= 0:
            return True
        if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
            return basis_bps <= -threshold
        if direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
            return basis_bps >= threshold
        raise ValueError(f"unsupported direction: {direction}")

    async def live_inventory_lighter_slippage_bps(self, *, lighter_side: str, qty: Decimal) -> tuple[Decimal | None, Decimal | None]:
        best_bid, best_ask = await self.get_lighter_best_bid_ask()
        estimated_fill = await self.estimate_lighter_fill_price(lighter_side, qty)
        if estimated_fill is None:
            return None, None
        if lighter_side.strip().upper() == "BUY":
            reference = best_ask
            slippage_bps = ((estimated_fill - reference) / reference) * Decimal("10000") if reference else None
        else:
            reference = best_bid
            slippage_bps = ((reference - estimated_fill) / reference) * Decimal("10000") if reference else None
        if slippage_bps is not None and slippage_bps < 0:
            slippage_bps = Decimal("0")
        return estimated_fill, slippage_bps

    async def fetch_live_inventory_basis_quote(self, *, asset: str) -> tuple[dict[str, Any] | None, Decimal | None]:
        result, elapsed_ms = await self._timed_submit(
            self.send_variational_place_order(
                asset=asset,
                side="BUY",
                amount=decimal_to_str(self.live_inventory_lot_notional_usd),
                expected_min_btc_qty=None,
                confirm=False,
                reduce_only=False,
            )
        )
        if not result.get("ok"):
            await self.append_live_inventory_log(
                "live_inventory_basis_quote_failed",
                {"asset": asset, "error": result.get("error") or result.get("step") or "unknown", "result": result},
            )
            return None, None
        payload = result.get("result") if isinstance(result.get("result"), dict) else result
        bid = to_decimal(payload.get("bid"))
        ask = to_decimal(payload.get("ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            await self.append_live_inventory_log(
                "live_inventory_basis_quote_failed",
                {"asset": asset, "error": "missing_or_invalid_bid_ask", "result": result},
            )
            return None, None
        return payload, Decimal(str(elapsed_ms))

    async def complete_live_inventory_entry_after_var_fill(
        self,
        *,
        match: PendingLiveInventoryVarFillMatch,
        record: OrderLifecycle,
        fill_payload: dict[str, Any],
    ) -> None:
        context = match.context or {}
        asset = match.asset.upper()
        qty = match.qty
        lot_id = match.lot_id
        direction = str(context.get("direction") or "")
        var_fill_price = record.var_fill_price or to_decimal(fill_payload.get("variational_filled_price"))
        if var_fill_price is None:
            await self.require_live_inventory_manual_review(
                asset=asset,
                reason="basis_entry_var_fill_missing_price",
                context={"lot_id": lot_id, "direction": direction, "fill_payload": fill_payload},
            )
            return
        try:
            lighter_result, lighter_submit_ms = await self._timed_submit(
                self.place_lighter_order_from_plan(
                    asset=asset,
                    side=str(context.get("var_side") or self._auto_live_direction_to_var_side(direction)),
                    qty=qty,
                    var_fill_price=var_fill_price,
                    cycle_id=lot_id,
                    role="live_inventory_entry",
                )
            )
        except Exception as exc:
            await self.require_live_inventory_manual_review(
                asset=asset,
                reason=f"basis_entry_lighter_submit_after_var_fill_exception:{exc}",
                context={"lot_id": lot_id, "direction": direction, "qty": decimal_to_str(qty)},
            )
            return
        lighter_record, lighter_payload = lighter_result
        if lighter_record is None or not self.auto_live_eager_hedge_started(lighter_record):
            await self.require_live_inventory_manual_review(
                asset=asset,
                reason="basis_entry_lighter_submit_after_var_fill_failed",
                context={"lot_id": lot_id, "direction": direction, "qty": decimal_to_str(qty), "lighter_payload": lighter_payload},
            )
            return
        lighter_fill_price = lighter_record.lighter_fill_price or to_decimal(context.get("lighter_price"))
        if lighter_fill_price is None:
            await self.require_live_inventory_manual_review(
                asset=asset,
                reason="basis_entry_lighter_fill_missing_price",
                context={"lot_id": lot_id, "direction": direction, "qty": decimal_to_str(qty), "lighter_payload": lighter_payload},
            )
            return
        lot = {
            "lot_id": lot_id,
            "signal_mode": LIVE_INVENTORY_SIGNAL_BASIS,
            "direction": direction,
            "qty": decimal_to_str(qty),
            "entry_var_fill_price": decimal_to_str(var_fill_price),
            "entry_lighter_fill_price": decimal_to_str(lighter_fill_price),
            "entry_var_price_source": "final_fill",
            "entry_lighter_price_source": "final_fill" if lighter_record.lighter_fill_price is not None else "estimated_snapshot",
            "entry_cost_status": "final_fills_confirmed" if lighter_record.lighter_fill_price is not None else "final_fills_pending",
            "entry_edge_bps": context.get("edge_bps"),
            "entry_roundtrip_pnl_bps": context.get("roundtrip_pnl_bps"),
            "entry_basis_bps": context.get("basis_bps"),
            "entry_z": context.get("z"),
            "entry_direction_signal": context.get("direction_signal"),
            "entry_var_side": context.get("var_side"),
            "entry_var_order_quote_id": context.get("quote_id"),
            "entry_var_order_quote_bid": context.get("var_bid"),
            "entry_var_order_quote_ask": context.get("var_ask"),
            "entry_var_order_quote_timestamp": context.get("quote_timestamp"),
            "entry_var_order_quote_execution_price": context.get("var_price"),
            "entry_var_submit_ms": context.get("var_submit_ms"),
            "entry_lighter_submit_ms": lighter_submit_ms,
            "entry_lighter_record_key": lighter_record.trade_key,
            "entry_lighter_payload": lighter_payload,
            "entry_kind": context.get("entry_kind") or "basis_initial",
            "entered_at": utc_now(),
            "entered_sample_index": context.get("sample_index") or self.live_inventory_sample_index,
            "status": "open",
        }
        self.live_inventory_open_lots.append(lot)
        self.remember_live_inventory_final_pnl_lot(asset=asset, lot=lot)
        self.sync_live_inventory_open_lot_entry_cost(asset=asset, lot_id=lot_id)
        await self.persist_live_inventory_memory(reason="basis_entry_lighter_submitted_after_var_fill")
        await self.append_live_inventory_log(
            "live_inventory_entered",
            {
                "asset": asset,
                "sample_index": context.get("sample_index"),
                "lot_id": lot_id,
                "direction": direction,
                "qty": decimal_to_str(qty),
                "edge_bps": context.get("edge_bps"),
                "roundtrip_pnl_bps": context.get("roundtrip_pnl_bps"),
                "entry_basis_bps": context.get("basis_bps"),
                "entry_z": context.get("z"),
                "entry_direction_signal": context.get("direction_signal"),
                "entry_kind": lot["entry_kind"],
                "var_price": decimal_to_str(var_fill_price),
                "lighter_price": decimal_to_str(lighter_fill_price),
                "var_submit_ms": context.get("var_submit_ms"),
                "lighter_submit_ms": lighter_submit_ms,
                "open_lots_total": len(self.live_inventory_open_lots),
                "realized_pnl_usd": decimal_to_str(self.live_inventory_realized_pnl_usd),
                "completed_cycles": self.live_inventory_completed_cycles,
                "entry_confirmation_mode": "var_fill_then_lighter",
            },
        )

    async def maybe_append_live_inventory_actual_pnl(self, payload: dict[str, Any]) -> None:
        trade_key = str(payload.get("trade_key") or "")
        pending = self.pending_live_inventory_actual_pnl.pop(trade_key, None)
        if not pending:
            return

        qty = to_decimal(pending.get("qty")) or Decimal("0")
        entry_var_price = to_decimal(pending.get("entry_var_price"))
        entry_lighter_price = to_decimal(pending.get("entry_lighter_price"))
        exit_var_price = to_decimal(pending.get("exit_var_price"))
        exit_lighter_price = to_decimal(payload.get("lighter_filled_price"))
        if None in {entry_var_price, entry_lighter_price, exit_var_price, exit_lighter_price}:
            await self.append_live_inventory_log(
                "live_inventory_actual_pnl_pending",
                {
                    **pending,
                    "exit_lighter_payload": payload,
                    "actual_pnl_status": "missing_final_fill_price",
                },
            )
            return

        var_leg_pnl, lighter_leg_pnl, actual_pnl = self.live_inventory_pair_pnl(
            direction=str(pending.get("direction") or ""),
            qty=qty,
            entry_var_price=entry_var_price,
            entry_lighter_price=entry_lighter_price,
            exit_var_price=exit_var_price,
            exit_lighter_price=exit_lighter_price,
        )
        notional = qty * entry_var_price
        actual_pnl_bps = actual_pnl / notional * Decimal("10000") if notional else None
        estimated_pnl = to_decimal(pending.get("estimated_pnl_usd")) or Decimal("0")
        self.live_inventory_realized_pnl_usd += actual_pnl - estimated_pnl
        await self.append_live_inventory_log(
            "live_inventory_actual_pnl",
            {
                **pending,
                "actual_pnl_status": "lighter_final_fill_confirmed",
                "exit_lighter_final_fill_price": decimal_to_str(exit_lighter_price),
                "actual_var_leg_pnl_usd": decimal_to_str(var_leg_pnl),
                "actual_lighter_leg_pnl_usd": decimal_to_str(lighter_leg_pnl),
                "actual_pnl_usd": decimal_to_str(actual_pnl),
                "actual_pnl_bps": decimal_to_str(actual_pnl_bps),
                "exit_lighter_payload": payload,
            },
        )
        await self.persist_live_inventory_memory(reason="actual_pnl_final_fill_update")

    @staticmethod
    def live_inventory_final_pnl_key(asset: str, lot_id: Any) -> str:
        return f"{str(asset).upper()}:{lot_id}"

    def remember_live_inventory_final_pnl_lot(self, *, asset: str, lot: dict[str, Any]) -> None:
        lot_id = lot.get("lot_id")
        if lot_id is None:
            return
        key = self.live_inventory_final_pnl_key(asset, lot_id)
        pending = self.pending_live_inventory_final_pnl.setdefault(key, {})
        pending.update(
            {
                "asset": asset,
                "lot_id": lot_id,
                "direction": lot.get("direction"),
                "qty": lot.get("qty"),
                "entry_estimated_var_price": lot.get("entry_var_fill_price"),
                "entry_estimated_lighter_price": lot.get("entry_lighter_fill_price"),
                "entry_signal_edge_bps": lot.get("entry_edge_bps"),
                "entry_snapshot_var_bid": lot.get("entry_snapshot_var_bid"),
                "entry_snapshot_var_ask": lot.get("entry_snapshot_var_ask"),
                "entry_snapshot_var_mid": lot.get("entry_snapshot_var_mid"),
                "entry_snapshot_var_buy_price": lot.get("entry_snapshot_var_buy_price"),
                "entry_snapshot_var_sell_price": lot.get("entry_snapshot_var_sell_price"),
                "entry_snapshot_var_full_spread_bps": lot.get("entry_snapshot_var_full_spread_bps"),
                "entry_snapshot_var_spread_source": lot.get("entry_snapshot_var_spread_source"),
                "entry_snapshot_var_timestamp": lot.get("entry_snapshot_var_timestamp"),
                "entry_snapshot_var_source_url": lot.get("entry_snapshot_var_source_url"),
                "entry_snapshot_var_source_stream": lot.get("entry_snapshot_var_source_stream"),
                "entry_initial_signal_edge_bps": lot.get("entry_initial_signal_edge_bps"),
                "entry_initial_snapshot_var_price": lot.get("entry_initial_snapshot_var_price"),
                "entry_refreshed_var_quote_ms": lot.get("entry_refreshed_var_quote_ms"),
                "entry_var_order_quote_id": lot.get("entry_var_order_quote_id"),
                "entry_var_order_quote_bid": lot.get("entry_var_order_quote_bid"),
                "entry_var_order_quote_ask": lot.get("entry_var_order_quote_ask"),
                "entry_var_order_quote_mark_price": lot.get("entry_var_order_quote_mark_price"),
                "entry_var_order_quote_timestamp": lot.get("entry_var_order_quote_timestamp"),
                "entry_var_order_quote_execution_price": lot.get("entry_var_order_quote_execution_price"),
                "entered_at": lot.get("entered_at"),
            }
        )

    def sync_live_inventory_open_lot_entry_cost(self, *, asset: str, lot_id: Any) -> bool:
        open_lots = getattr(self, "live_inventory_open_lots", [])
        if lot_id is None or not open_lots:
            return False
        key = self.live_inventory_final_pnl_key(asset, lot_id)
        pending = self.pending_live_inventory_final_pnl.get(key) or {}
        entry_var_price = pending.get("entry_var_final_fill_price")
        entry_lighter_price = pending.get("entry_lighter_final_fill_price")
        updated = False
        for lot in open_lots:
            if str(lot.get("lot_id")) != str(lot_id):
                continue
            if entry_var_price is not None and lot.get("entry_var_fill_price") != entry_var_price:
                lot["entry_var_fill_price"] = entry_var_price
                lot["entry_var_final_fill_at"] = pending.get("entry_var_final_fill_at")
                updated = True
            if entry_var_price is not None and lot.get("entry_var_price_source") != "final_fill":
                lot["entry_var_price_source"] = "final_fill"
                lot["entry_var_final_fill_at"] = pending.get("entry_var_final_fill_at")
                updated = True
            if entry_lighter_price is not None and lot.get("entry_lighter_fill_price") != entry_lighter_price:
                lot["entry_lighter_fill_price"] = entry_lighter_price
                lot["entry_lighter_final_fill_at"] = pending.get("entry_lighter_final_fill_at")
                updated = True
            if entry_lighter_price is not None and lot.get("entry_lighter_price_source") != "final_fill":
                lot["entry_lighter_price_source"] = "final_fill"
                lot["entry_lighter_final_fill_at"] = pending.get("entry_lighter_final_fill_at")
                updated = True
            cost_status = "final_fills_confirmed" if entry_var_price is not None and entry_lighter_price is not None else "final_fills_pending"
            if lot.get("entry_cost_status") != cost_status:
                lot["entry_cost_status"] = cost_status
                updated = True
            break
        return updated

    @staticmethod
    def live_inventory_entry_cost_confirmed(lot: dict[str, Any]) -> bool:
        return lot.get("entry_cost_status") == "final_fills_confirmed"

    async def maybe_append_live_inventory_final_pnl_from_fill(self, payload: dict[str, Any]) -> None:
        role = str(payload.get("auto_live_role") or "")
        if role not in {"live_inventory_entry", "live_inventory_exit"}:
            return
        lot_id = payload.get("auto_live_cycle_id")
        asset = str(payload.get("asset") or "")
        if lot_id is None or not asset:
            return

        key = self.live_inventory_final_pnl_key(asset, lot_id)
        pending = self.pending_live_inventory_final_pnl.setdefault(key, {"asset": asset, "lot_id": lot_id})
        pending.setdefault("qty", payload.get("qty"))
        if role == "live_inventory_entry":
            if payload.get("variational_filled_price") is not None and payload.get("synthetic_eager_fill") is False:
                pending["entry_var_final_fill_price"] = payload.get("variational_filled_price")
                pending["entry_var_final_fill_at"] = payload.get("variational_filled_at")
            if payload.get("lighter_filled_price") is not None:
                pending["entry_lighter_final_fill_price"] = payload.get("lighter_filled_price")
                pending["entry_lighter_final_fill_at"] = payload.get("lighter_filled_at")
            if self.sync_live_inventory_open_lot_entry_cost(asset=asset, lot_id=lot_id):
                await self.persist_live_inventory_memory(reason="entry_final_fill_cost_update")
        else:
            if payload.get("variational_filled_price") is not None and payload.get("synthetic_eager_fill") is False:
                pending["exit_var_final_fill_price"] = payload.get("variational_filled_price")
                pending["exit_var_final_fill_at"] = payload.get("variational_filled_at")
            if payload.get("lighter_filled_price") is not None:
                pending["exit_lighter_final_fill_price"] = payload.get("lighter_filled_price")
                pending["exit_lighter_final_fill_at"] = payload.get("lighter_filled_at")

        await self.maybe_append_live_inventory_final_pnl(key)

    async def maybe_append_live_inventory_final_pnl(self, key: str) -> None:
        pending = self.pending_live_inventory_final_pnl.get(key)
        if not pending or pending.get("final_pnl_emitted"):
            return

        qty = to_decimal(pending.get("qty"))
        direction = str(pending.get("direction") or "")
        entry_var_price = to_decimal(pending.get("entry_var_final_fill_price"))
        entry_lighter_price = to_decimal(pending.get("entry_lighter_final_fill_price"))
        exit_var_price = to_decimal(pending.get("exit_var_final_fill_price"))
        exit_lighter_price = to_decimal(pending.get("exit_lighter_final_fill_price"))
        if None in {qty, entry_var_price, entry_lighter_price, exit_var_price, exit_lighter_price} or not direction:
            return
        entry_estimated_var_price = to_decimal(pending.get("entry_estimated_var_price"))
        entry_estimated_lighter_price = to_decimal(pending.get("entry_estimated_lighter_price"))
        exit_estimated_var_price = to_decimal(pending.get("exit_estimated_var_price"))
        exit_estimated_lighter_price = to_decimal(pending.get("exit_estimated_lighter_price"))
        entry_snapshot_var_bid = to_decimal(pending.get("entry_snapshot_var_bid"))
        entry_snapshot_var_ask = to_decimal(pending.get("entry_snapshot_var_ask"))
        entry_snapshot_var_mid = to_decimal(pending.get("entry_snapshot_var_mid"))
        entry_snapshot_var_buy_price = to_decimal(pending.get("entry_snapshot_var_buy_price"))
        entry_snapshot_var_sell_price = to_decimal(pending.get("entry_snapshot_var_sell_price"))
        entry_var_order_quote_execution_price = to_decimal(pending.get("entry_var_order_quote_execution_price"))
        exit_var_order_quote_execution_price = to_decimal(pending.get("exit_var_order_quote_execution_price"))
        entry_signal_edge_bps = to_decimal(pending.get("entry_signal_edge_bps"))
        entry_estimated_edge_bps = self.live_inventory_pair_edge_bps(
            direction=direction,
            var_price=entry_estimated_var_price,
            lighter_price=entry_estimated_lighter_price,
        )
        entry_final_edge_bps = self.live_inventory_pair_edge_bps(
            direction=direction,
            var_price=entry_var_price,
            lighter_price=entry_lighter_price,
        )
        exit_estimated_edge_bps = self.live_inventory_pair_edge_bps(
            direction=direction,
            var_price=exit_estimated_var_price,
            lighter_price=exit_estimated_lighter_price,
        )
        exit_final_edge_bps = self.live_inventory_pair_edge_bps(
            direction=direction,
            var_price=exit_var_price,
            lighter_price=exit_lighter_price,
        )
        entry_edge_capture_loss_bps = None
        if entry_signal_edge_bps is not None and entry_final_edge_bps is not None:
            entry_edge_capture_loss_bps = entry_signal_edge_bps - entry_final_edge_bps
        final_spread_capture_bps = None
        if entry_final_edge_bps is not None and exit_final_edge_bps is not None:
            final_spread_capture_bps = entry_final_edge_bps - exit_final_edge_bps

        var_leg_pnl, lighter_leg_pnl, final_pnl = self.live_inventory_pair_pnl(
            direction=direction,
            qty=qty or Decimal("0"),
            entry_var_price=entry_var_price or Decimal("0"),
            entry_lighter_price=entry_lighter_price or Decimal("0"),
            exit_var_price=exit_var_price or Decimal("0"),
            exit_lighter_price=exit_lighter_price or Decimal("0"),
        )
        notional = (qty or Decimal("0")) * (entry_var_price or Decimal("0"))
        final_pnl_bps = final_pnl / notional * Decimal("10000") if notional else None
        if not hasattr(self, "live_inventory_execution_loss_bps_samples"):
            self.live_inventory_execution_loss_bps_samples = deque(maxlen=20)
        if entry_edge_capture_loss_bps is not None and entry_edge_capture_loss_bps > 0:
            self.live_inventory_execution_loss_bps_samples.append(entry_edge_capture_loss_bps)
        pending["final_pnl_emitted"] = True
        await self.append_live_inventory_log(
            "live_inventory_final_pnl",
            {
                **pending,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_var_leg_pnl_usd": decimal_to_str(var_leg_pnl),
                "final_lighter_leg_pnl_usd": decimal_to_str(lighter_leg_pnl),
                "final_pnl_usd": decimal_to_str(final_pnl),
                "final_pnl_bps": decimal_to_str(final_pnl_bps),
                "entry_estimated_edge_bps": decimal_to_str(entry_estimated_edge_bps),
                "entry_final_edge_bps": decimal_to_str(entry_final_edge_bps),
                "exit_estimated_edge_bps": decimal_to_str(exit_estimated_edge_bps),
                "exit_final_edge_bps": decimal_to_str(exit_final_edge_bps),
                "entry_edge_capture_loss_bps": decimal_to_str(entry_edge_capture_loss_bps),
                "final_spread_capture_bps": decimal_to_str(final_spread_capture_bps),
                "recent_execution_loss_buffer_bps": decimal_to_str(
                    self.live_inventory_recent_execution_loss_buffer_bps()
                ),
                "entry_var_final_vs_snapshot_bid_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_price, entry_snapshot_var_bid)
                ),
                "entry_var_final_vs_snapshot_ask_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_price, entry_snapshot_var_ask)
                ),
                "entry_var_final_vs_snapshot_mid_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_price, entry_snapshot_var_mid)
                ),
                "entry_var_final_vs_snapshot_buy_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_price, entry_snapshot_var_buy_price)
                ),
                "entry_var_final_vs_snapshot_sell_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_price, entry_snapshot_var_sell_price)
                ),
                "entry_var_order_quote_vs_snapshot_buy_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_order_quote_execution_price, entry_snapshot_var_buy_price)
                ),
                "entry_var_order_quote_vs_snapshot_sell_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_order_quote_execution_price, entry_snapshot_var_sell_price)
                ),
                "entry_var_final_vs_order_quote_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_price, entry_var_order_quote_execution_price)
                ),
                "entry_var_fill_drift_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_var_price, entry_estimated_var_price)
                ),
                "entry_lighter_fill_drift_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(entry_lighter_price, entry_estimated_lighter_price)
                ),
                "exit_var_fill_drift_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(exit_var_price, exit_estimated_var_price)
                ),
                "exit_var_final_vs_order_quote_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(exit_var_price, exit_var_order_quote_execution_price)
                ),
                "exit_lighter_fill_drift_bps": decimal_to_str(
                    self.live_inventory_price_drift_bps(exit_lighter_price, exit_estimated_lighter_price)
                ),
            },
        )
    async def live_inventory_entry_preflight(
        self,
        *,
        asset: str,
        direction: str,
        var_side: str,
        qty: Decimal,
        var_price: Decimal,
        lighter_price: Decimal,
        edge_bps: Decimal | None,
        var_spread_bps: Decimal | None = None,
        var_snapshot_timestamp: str | None = None,
        min_entry_bps: Decimal | None = None,
        dynamic_entry_buffer_bps: Decimal | None = None,
    ) -> tuple[bool, str, dict[str, Any]]:
        var_snapshot_age_seconds = self._age_seconds_from_iso(var_snapshot_timestamp)
        notional = qty * lighter_price
        context = {
            "action": "entry",
            "direction": direction,
            "var_side": var_side,
            "qty": decimal_to_str(qty),
            "var_price": decimal_to_str(var_price),
            "lighter_price": decimal_to_str(lighter_price),
            "lighter_notional_usd": decimal_to_str(notional),
            "entry_edge_bps": decimal_to_str(edge_bps),
            "var_spread_bps": decimal_to_str(var_spread_bps),
            "var_snapshot_timestamp": var_snapshot_timestamp,
            "var_snapshot_age_seconds": f"{var_snapshot_age_seconds:.3f}" if var_snapshot_age_seconds is not None else None,
            "live_inventory_max_var_snapshot_age_seconds": f"{self.live_inventory_max_var_snapshot_age_seconds:.3f}",
            "live_inventory_max_var_spread_bps": decimal_to_str(self.live_inventory_max_var_spread_bps),
            "live_inventory_entry_bps": decimal_to_str(self.live_inventory_entry_bps),
            "live_inventory_dynamic_entry_buffer_bps": decimal_to_str(self.live_inventory_dynamic_entry_buffer_bps),
            "live_inventory_max_lighter_slippage_bps": decimal_to_str(self.live_inventory_max_lighter_slippage_bps),
            "live_inventory_recent_execution_loss_buffer_bps": decimal_to_str(
                self.live_inventory_recent_execution_loss_buffer_bps()
            ),
            "live_inventory_required_entry_bps": None,
            "lighter_side": None,
            "lighter_estimated_fill_price": None,
            "lighter_order_book_slippage_bps": None,
            "precheck_edge_bps": None,
            "lighter_min_base_amount": decimal_to_str(self.lighter_min_base_amount),
            "lighter_min_quote_amount": decimal_to_str(self.lighter_min_quote_amount),
            "live_max_notional_usd": decimal_to_str(self.live_max_notional_usd),
            "live_cooldown_remaining_seconds": None,
        }
        if var_spread_bps is not None and var_spread_bps > self.live_inventory_max_var_spread_bps:
            return False, "var_spread_exceeds_live_inventory_limit", context
        if var_snapshot_age_seconds is None or var_snapshot_age_seconds > self.live_inventory_max_var_snapshot_age_seconds:
            return False, "variational_quote_snapshot_stale", context
        last_submit_monotonic = self.last_live_submit_monotonic_by_asset.get(asset.upper())
        if last_submit_monotonic is not None:
            cooldown_elapsed = time.monotonic() - last_submit_monotonic
            if cooldown_elapsed < self.live_cooldown_seconds:
                remaining = self.live_cooldown_seconds - cooldown_elapsed
                context["live_cooldown_remaining_seconds"] = f"{remaining:.3f}"
                return False, "live_cooldown_active", context
        ok, reason, precheck_edge_bps = await self.auto_live_lighter_precheck(
            asset=asset,
            var_side=var_side,
            qty=qty,
            var_fill_price=var_price,
        )
        context["precheck_edge_bps"] = decimal_to_str(precheck_edge_bps)
        if not ok:
            return False, reason, context

        lighter_side = "SELL" if var_side.strip().upper() == "BUY" else "BUY"
        lighter_estimated_fill_price, lighter_slippage_bps = await self.live_inventory_lighter_slippage_bps(
            lighter_side=lighter_side,
            qty=qty,
        )
        context["lighter_side"] = lighter_side
        context["lighter_estimated_fill_price"] = decimal_to_str(lighter_estimated_fill_price)
        context["lighter_order_book_slippage_bps"] = decimal_to_str(lighter_slippage_bps)
        if lighter_estimated_fill_price is None or lighter_slippage_bps is None:
            return False, "lighter_order_book_depth_insufficient", context
        if lighter_slippage_bps > self.live_inventory_max_lighter_slippage_bps:
            return False, "lighter_slippage_exceeds_live_inventory_limit", context

        required_entry_bps = min_entry_bps if min_entry_bps is not None else self.live_inventory_entry_bps
        dynamic_buffer = dynamic_entry_buffer_bps if dynamic_entry_buffer_bps is not None else self.live_inventory_dynamic_entry_buffer_bps
        recent_execution_loss_buffer_bps = Decimal("0")
        if not self.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics:
            recent_execution_loss_buffer_bps = self.live_inventory_recent_execution_loss_buffer_bps()
        dynamic_required_entry_bps = (
            (var_spread_bps or Decimal("0"))
            + lighter_slippage_bps
            + recent_execution_loss_buffer_bps
            + dynamic_buffer
        )
        context["live_inventory_recent_execution_loss_buffer_bps"] = decimal_to_str(recent_execution_loss_buffer_bps)
        context["live_inventory_ignored_recent_execution_loss_buffer_for_diagnostics"] = (
            self.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics
        )
        if dynamic_required_entry_bps > required_entry_bps:
            required_entry_bps = dynamic_required_entry_bps
        context["live_inventory_required_entry_bps"] = decimal_to_str(required_entry_bps)
        if edge_bps is None or edge_bps < required_entry_bps:
            return False, "edge_bps_below_dynamic_live_inventory_entry", context
        return True, "ok", context

    def auto_live_guard_reason(self) -> str | None:
        if self.auto_live_manual_review_required:
            return "manual_review_required"
        if self.auto_live_max_cycles > 0 and self.auto_live_completed_cycles >= self.auto_live_max_cycles:
            return "max_cycles_reached"
        if self.auto_live_last_closed_monotonic is not None:
            cooldown_elapsed = time.monotonic() - self.auto_live_last_closed_monotonic
            if cooldown_elapsed < self.auto_live_cooldown_seconds:
                return "cooldown_active"
        return None

    def maybe_log_auto_live_guard(self, reason: str) -> None:
        marker = (reason, self.auto_live_completed_cycles, self.auto_live_next_cycle_id)
        if self._last_auto_live_guard_log == marker:
            return
        self._last_auto_live_guard_log = marker
        remaining_seconds: float | None = None
        if reason == "cooldown_active" and self.auto_live_last_closed_monotonic is not None:
            elapsed = time.monotonic() - self.auto_live_last_closed_monotonic
            remaining_seconds = max(0.0, self.auto_live_cooldown_seconds - elapsed)
        if reason == "manual_review_required":
            position = self.auto_live_position
            self.logger.warning(
                "auto_live_manual_review_required cycle_id=%s asset=%s qty=%s reason=%s action=stop_auto_live_until_restart",
                position.cycle_id if position is not None else "-",
                position.asset if position is not None else "-",
                position.planned_qty if position is not None else "-",
                self.auto_live_manual_review_reason or (position.manual_review_reason if position is not None else None) or "unknown",
            )
            return
        self.logger.info(
            "auto_live_guard_blocked reason=%s next_cycle_id=%s completed_cycles=%s max_cycles=%s cooldown_remaining_seconds=%s",
            reason,
            self.auto_live_next_cycle_id,
            self.auto_live_completed_cycles,
            self.auto_live_max_cycles,
            f"{remaining_seconds:.3f}" if remaining_seconds is not None else "-",
        )

    def require_auto_live_manual_review(self, position: AutoLivePositionState | None, reason: str) -> None:
        already_required = self.auto_live_manual_review_required and self.auto_live_manual_review_reason == reason
        self.auto_live_manual_review_required = True
        self.auto_live_manual_review_reason = reason
        if position is not None:
            position.manual_review_required = True
            position.manual_review_reason = reason
            position.manual_review_logged = True
            self.write_auto_live_state(
                {
                    "status": "manual_review_required",
                    "asset": position.asset,
                    "cycle_id": position.cycle_id,
                    "direction": position.direction,
                    "qty": decimal_to_str(position.planned_qty),
                    "reason": reason,
                    "action": "stop_auto_live_until_restart",
                }
            )
        if not already_required:
            self._last_auto_live_guard_log = None
        self.maybe_log_auto_live_guard("manual_review_required")
        if already_required:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(
            self.append_order_log(
                "auto_live_manual_review_required",
                {
                    "record_kind": "auto_live_manual_review",
                    "mode": self.mode,
                    "asset": position.asset if position is not None else self.variational_ticker or self.ticker or "",
                    "auto_live_cycle_id": position.cycle_id if position is not None else None,
                    "direction": position.direction if position is not None else None,
                    "qty": decimal_to_str(position.planned_qty) if position is not None else None,
                    "reason": reason,
                    "rollback_action": "manual_review_required",
                    "action": "stop_auto_live_until_restart",
                },
            )
        )

    def should_log_auto_live_precheck_failure(
        self,
        kind: str,
        cycle_id: int,
        asset: str,
        side: str,
        reason: str | None,
        *,
        interval_seconds: float = 10.0,
    ) -> bool:
        key = (kind, cycle_id, asset.upper(), side.upper(), reason or "unknown")
        now = time.monotonic()
        last_logged_at = self._last_auto_live_precheck_failure_log.get(key)
        if last_logged_at is not None and now - last_logged_at < interval_seconds:
            return False
        self._last_auto_live_precheck_failure_log[key] = now
        return True

    def load_auto_live_state(self) -> dict[str, Any]:
        if self.auto_live_state_file is None or not self.auto_live_state_file.exists():
            return {"status": "flat"}
        try:
            with self.auto_live_state_file.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            return {"status": "unknown", "reason": f"state_load_failed:{exc}"}
        if not isinstance(state, dict):
            return {"status": "unknown", "reason": "state_file_not_object"}
        status = str(state.get("status", "")).strip() or "unknown"
        return {**state, "status": status}

    def write_auto_live_state(self, state: dict[str, Any]) -> None:
        if self.auto_live_state_file is None:
            return
        row = {"updated_at": utc_now(), **state}
        self.auto_live_state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.auto_live_state_file.with_suffix(self.auto_live_state_file.suffix + ".tmp")
        tmp_path.write_text(json.dumps(row, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp_path.replace(self.auto_live_state_file)

    async def write_auto_live_state_async(self, state: dict[str, Any]) -> None:
        await asyncio.to_thread(self.write_auto_live_state, state)

    def auto_live_state_summary(self, state: dict[str, Any]) -> str:
        status = clean_state_value(state.get("status"))
        parts = [f"status={status or 'unknown'}"]
        for key in ("asset", "cycle_id", "direction", "qty", "reason", "updated_at"):
            value = clean_state_value(state.get(key))
            if value:
                parts.append(f"{key}={value}")
        return " ".join(parts)

    def load_live_inventory_state(self) -> dict[str, Any]:
        if self.live_inventory_state_file is None or not self.live_inventory_state_file.exists():
            return {"status": "flat", "open_lots": []}
        try:
            with self.live_inventory_state_file.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            return {"status": "unknown", "reason": f"state_load_failed:{exc}"}
        if not isinstance(state, dict):
            return {"status": "unknown", "reason": "state_file_not_object"}
        status = str(state.get("status", "")).strip() or "unknown"
        return {**state, "status": status}

    def write_live_inventory_state(self, state: dict[str, Any]) -> None:
        if self.live_inventory_state_file is None:
            return
        row = {"updated_at": utc_now(), **state}
        self.live_inventory_state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.live_inventory_state_file.with_suffix(self.live_inventory_state_file.suffix + ".tmp")
        tmp_path.write_text(json.dumps(row, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp_path.replace(self.live_inventory_state_file)

    async def write_live_inventory_state_async(self, state: dict[str, Any]) -> None:
        await asyncio.to_thread(self.write_live_inventory_state, state)

    def live_inventory_state_summary(self, state: dict[str, Any]) -> str:
        status = clean_state_value(state.get("status"))
        parts = [f"status={status or 'unknown'}"]
        for key in ("asset", "next_lot_id", "manual_review_reason", "reason", "updated_at"):
            value = clean_state_value(state.get(key))
            if value:
                parts.append(f"{key}={value}")
        open_lots = state.get("open_lots")
        if isinstance(open_lots, list):
            parts.append(f"open_lots={len(open_lots)}")
        return " ".join(parts)

    @staticmethod
    def auto_live_eager_hedge_started(record: OrderLifecycle | None) -> bool:
        return record is not None and record.processing_stage in {STAGE_LIVE_SUBMIT_SENT, STAGE_LIGHTER_FILLED}

    def timing_logger(self) -> logging.Logger:
        return getattr(self, "logger", logging.getLogger(__name__))

    def log_auto_live_eager_hedge_timing(
        self,
        *,
        cycle_id: int,
        role: str,
        asset: str,
        side: str,
        signal_monotonic: float,
        task_created_monotonic: float | None,
        record: OrderLifecycle,
    ) -> None:
        self.timing_logger().info(
            "auto_live_eager_hedge_timing cycle_id=%s role=%s asset=%s side=%s record_key=%s "
            "signal_to_task_create_ms=%s task_create_to_plan_start_ms=%s plan_ms=%s "
            "plan_ready_to_submit_start_ms=%s submit_call_ms=%s signal_to_submit_sent_ms=%s",
            cycle_id,
            role,
            asset,
            side,
            record.trade_key,
            elapsed_ms_between_str(signal_monotonic, task_created_monotonic),
            elapsed_ms_between_str(task_created_monotonic, record.live_plan_started_monotonic),
            elapsed_ms_between_str(record.live_plan_started_monotonic, record.live_plan_ready_monotonic),
            elapsed_ms_between_str(record.live_plan_ready_monotonic, record.live_submit_started_monotonic),
            elapsed_ms_between_str(record.live_submit_started_monotonic, record.live_submit_sent_monotonic),
            elapsed_ms_between_str(signal_monotonic, record.live_submit_sent_monotonic),
        )

    async def auto_live_lighter_precheck(
        self,
        *,
        asset: str,
        var_side: str,
        qty: Decimal,
        var_fill_price: Decimal,
    ) -> tuple[bool, str, Decimal | None]:
        if self._lighter_order_book_is_stale():
            return False, "lighter_order_book_stale", None
        best_bid, best_ask = await self.get_lighter_best_bid_ask()
        if best_bid is None or best_ask is None:
            return False, "lighter_order_book_not_ready", None

        lighter_side = "SELL" if var_side.strip().upper() == "BUY" else "BUY"
        slippage = Decimal(str(HEDGE_SLIPPAGE_BPS)) / Decimal("10000")
        limit_price = best_ask * (Decimal("1") + slippage) if lighter_side == "BUY" else best_bid * (Decimal("1") - slippage)
        notional = qty * limit_price
        edge_bps = basis_points_diff(limit_price, var_fill_price)
        base_amount = int(qty * self.base_amount_multiplier)

        if base_amount <= 0:
            return False, "hedge_base_amount_rounds_to_zero", edge_bps
        if self.lighter_min_base_amount is not None and qty < self.lighter_min_base_amount:
            return False, "hedge_below_lighter_min_base_amount", edge_bps
        if base_amount > self.risk_guard_max_base_amount:
            return False, "hedge_base_amount_exceeds_risk_limit", edge_bps
        if edge_bps is not None and edge_bps > self.risk_guard_max_price_deviation_bps:
            return False, "hedge_price_deviation_exceeds_risk_limit", edge_bps
        if self.lighter_min_quote_amount is not None and notional < self.lighter_min_quote_amount:
            return False, "hedge_below_lighter_min_quote_amount", edge_bps
        if self.live_allowed_sides and var_side.strip().lower() not in self.live_allowed_sides:
            return False, "live_side_not_allowed", edge_bps
        if self.live_allowed_assets and asset.upper() not in self.live_allowed_assets:
            return False, "live_asset_not_allowed", edge_bps
        if self.live_max_qty > 0 and qty > self.live_max_qty:
            return False, "live_qty_exceeds_limit", edge_bps
        if self.live_max_notional_usd > 0 and notional > self.live_max_notional_usd:
            return False, "live_notional_exceeds_limit", edge_bps
        if edge_bps is not None and edge_bps < self.live_require_min_edge_bps:
            return False, "live_edge_bps_below_threshold", edge_bps
        return True, "ok", edge_bps

    def requires_lighter_market_data(self) -> bool:
        return self.is_dry_run_mode() or self.is_live_mode() or self.is_paper_mode()

    def requires_lighter_trading_credentials(self) -> bool:
        return self.is_live_mode()

    def live_config_snapshot(self) -> dict[str, Any]:
        return {
            "max_notional_usd": decimal_to_str(self.live_max_notional_usd),
            "max_qty": decimal_to_str(self.live_max_qty),
            "require_min_edge_bps": decimal_to_str(self.live_require_min_edge_bps),
            "cooldown_seconds": self.live_cooldown_seconds,
            "submit_timeout_seconds": self.live_submit_timeout_seconds,
            "variational_submit_transport": getattr(
                self,
                "variational_submit_transport",
                VARIATIONAL_SUBMIT_TRANSPORT_DOM,
            ),
            "variational_api_max_slippage": getattr(
                self,
                "variational_api_max_slippage",
                DEFAULT_VARIATIONAL_API_MAX_SLIPPAGE,
            ),
            "lighter_submit_transport": getattr(
                self,
                "lighter_submit_transport",
                LIGHTER_SUBMIT_TRANSPORT_HTTP,
            ),
            "lighter_order_mode": getattr(self, "lighter_order_mode", LIGHTER_ORDER_MODE_LIMIT_GTT),
            "allowed_assets": sorted(self.live_allowed_assets),
            "allowed_sides": sorted(self.live_allowed_sides),
            "rollback_action": "manual_review_required",
            "auto_live_flat_start_confirmed": self.auto_live_i_confirm_flat_start,
            "auto_live_entry_max_precheck_edge_bps": decimal_to_str(self.auto_live_entry_max_precheck_edge_bps),
        }

    def paper_config_snapshot(self) -> dict[str, Any]:
        return {
            "notional_usd": decimal_to_str(self.paper_notional_usd),
            "entry_deviation_bps": decimal_to_str(self.paper_entry_deviation_bps),
            "exit_deviation_bps": decimal_to_str(self.paper_exit_deviation_bps),
            "max_var_half_spread_bps": decimal_to_str(self.paper_max_var_half_spread_bps),
            "max_holding_seconds": self.paper_max_holding_seconds,
            "cooldown_seconds": self.paper_cooldown_seconds,
            "min_samples": self.paper_min_samples,
            "interval_seconds": self.paper_interval_seconds,
            "fee_bps_per_leg": decimal_to_str(self.paper_fee_bps_per_leg),
            "latency_drift_bps": decimal_to_str(self.paper_latency_drift_bps),
        }

    def print_startup_next_steps(self) -> None:
        is_zh = self.args.lang == "zh"
        mode_line = f"当前模式: {self.mode}" if is_zh else f"Current mode: {self.mode}"
        if is_zh:
            lines = [
                mode_line,
                "Python 脚本已就位，请回到 Chrome 加载并启动扩展。若 Chrome 插件已启动，请刷新网页。",
                "observe 只监听；dry-run 模拟手动成交后的对冲；paper 自动模拟机会；live 才会真实下单。",
                "Use `python main.py --lang en` for the English dashboard.",
            ]
            title = "启动指引"
        else:
            lines = [
                mode_line,
                "Python runtime is ready. Go back to Chrome and load/start the extension.",
                "observe listens only; dry-run simulates manual-fill hedges; paper auto-simulates opportunities; live sends real orders.",
                "If the Chrome extension has already started, please refresh the webpage."
            ]
            title = "Startup Guide"
        self.dashboard_console.print(Panel("\n".join(lines), title=title, border_style="yellow"))

    def track_background_task(self, task: asyncio.Task[None], name: str) -> asyncio.Task[None]:
        task.add_done_callback(lambda completed: self._handle_background_task_done(name, completed))
        return task

    def _handle_background_task_done(self, name: str, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        self.logger.exception("background_task_failed: %s", name, exc_info=exc)
        self.stop_flag = True

    def run_startup_diagnostics(self) -> StartupDiagnostics:
        passed: list[str] = []
        warnings: list[str] = []
        blocking_errors: list[str] = []

        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            passed.append(f"log_dir_ready={LOG_DIR.resolve()}")
        except Exception as exc:
            blocking_errors.append(f"log_dir_unavailable: {exc}")

        passed.append(f"mode={self.mode}")
        passed.append(f"forwarder_ws=ws://{FORWARDER_HOST}:{FORWARDER_WS_PORT}")
        passed.append(f"forwarder_rest=ws://{FORWARDER_HOST}:{FORWARDER_REST_PORT}")
        passed.append(f"forwarder_command=ws://{FORWARDER_HOST}:{FORWARDER_COMMAND_PORT}")
        passed.append(f"risk_guard_max_base_amount={self.risk_guard_max_base_amount}")
        passed.append(f"risk_guard_max_price_deviation_bps={self.risk_guard_max_price_deviation_bps}")
        passed.append(
            f"lighter_submit_transport={getattr(self, 'lighter_submit_transport', LIGHTER_SUBMIT_TRANSPORT_HTTP)}"
        )
        passed.append(
            f"variational_submit_transport={getattr(self, 'variational_submit_transport', VARIATIONAL_SUBMIT_TRANSPORT_DOM)}"
        )
        passed.append(f"lighter_order_mode={getattr(self, 'lighter_order_mode', LIGHTER_ORDER_MODE_LIMIT_GTT)}")
        passed.append(f"lighter_prewarm_submit_ws={getattr(self, 'lighter_prewarm_submit_ws', False)}")
        passed.append(f"auto_live_skip_entry_preview={getattr(self, 'auto_live_skip_entry_preview', False)}")
        passed.append(f"live_config={json.dumps(self.live_config_snapshot(), ensure_ascii=True, sort_keys=True)}")
        passed.append(f"paper_config={json.dumps(self.paper_config_snapshot(), ensure_ascii=True, sort_keys=True)}")

        if self.is_observe_mode():
            passed.append("observe_mode_skips_lighter_trading_credentials")

        if self.is_paper_mode():
            passed.append("paper_mode_auto_simulates_opportunities_without_real_orders")
            open_paper_ids = find_open_paper_opportunity_ids(self.opportunities_file)
            if open_paper_ids:
                blocking_errors.append(
                    "paper_resume_guard_open_positions_detected: "
                    + ",".join(open_paper_ids)
                    + " | close or archive old paper logs before restarting paper mode"
                )

        if self.requires_lighter_market_data():
            passed.append(f"lighter_market_data_required_for_mode={self.mode}")

        if env_flag("LIGHTER_WS_SERVER_PINGS"):
            warnings.append("lighter_ws_server_pings=true")

        account_index = os.getenv("LIGHTER_ACCOUNT_INDEX", "").strip()
        api_key_index = os.getenv("LIGHTER_API_KEY_INDEX", "").strip()
        private_key = os.getenv("LIGHTER_PRIVATE_KEY", "").strip()
        api_key_private_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip()

        if self.is_dry_run_mode():
            passed.append("dry_run_mode_uses_lighter_market_data_without_real_order_submission")
            if account_index or api_key_index or private_key or api_key_private_key:
                warnings.append("dry_run_mode_detected_live_trading_credentials")

        if self.is_live_mode():
            passed.append("live_rollback_action=manual_review_required")
            if self.auto_live_entry and self.auto_live_i_confirm_flat_start:
                passed.append("auto_live_flat_start_manually_confirmed")
                state = self.load_auto_live_state()
                state_status = clean_state_value(state.get("status")) or "unknown"
                if self.auto_live_reset_state_after_manual_flat:
                    self.write_auto_live_state({"status": "flat", "reason": "manual_flat_start_reset"})
                    passed.append("auto_live_state_reset_after_manual_flat")
                elif state_status == "flat":
                    passed.append("auto_live_state_flat")
                else:
                    blocking_errors.append(
                        "auto_live_state_not_flat: "
                        + self.auto_live_state_summary(state)
                        + " | manually confirm Var/Lighter flat, then restart with --auto-live-reset-state-after-manual-flat"
                    )
            if self.live_inventory:
                passed.append("live_inventory_enabled")
                if self.live_inventory_dry_decisions:
                    passed.append("live_inventory_dry_decisions_only_no_orders")
                else:
                    passed.append("live_inventory_real_submit_one_lot_enabled")
                if self.live_inventory_i_confirm_flat_start:
                    passed.append("live_inventory_flat_start_manually_confirmed")
                    state = self.load_live_inventory_state()
                    state_status = clean_state_value(state.get("status")) or "unknown"
                    if self.live_inventory_reset_state_after_manual_flat:
                        self.write_live_inventory_state(
                            {
                                "status": "flat",
                                "asset": "BTC",
                                "next_lot_id": 1,
                                "open_lots": [],
                                "pending_actions": [],
                                "realized_pnl_usd": "0",
                                "completed_cycles": 0,
                                "reason": "manual_flat_start_reset",
                            }
                        )
                        passed.append("live_inventory_state_reset_after_manual_flat")
                    elif state_status == "flat":
                        passed.append("live_inventory_state_flat")
                    else:
                        blocking_errors.append(
                            "live_inventory_state_not_flat: "
                            + self.live_inventory_state_summary(state)
                            + " | manually confirm Var/Lighter flat, then restart with --live-inventory-reset-state-after-manual-flat"
                        )
                elif self.live_inventory_i_accept_open_state_resume:
                    state = self.load_live_inventory_state()
                    state_status = clean_state_value(state.get("status")) or "unknown"
                    open_lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
                    if state_status == "open" and open_lots:
                        passed.append("live_inventory_open_state_resume_accepted")
                    else:
                        blocking_errors.append(
                            "live_inventory_open_state_resume_requires_open_lots: "
                            + self.live_inventory_state_summary(state)
                        )
            if not account_index:
                blocking_errors.append("LIGHTER_ACCOUNT_INDEX is not set")
            else:
                try:
                    int(account_index)
                    passed.append("LIGHTER_ACCOUNT_INDEX_ok")
                except ValueError:
                    blocking_errors.append(f"LIGHTER_ACCOUNT_INDEX must be integer: {account_index}")

            if not api_key_index:
                blocking_errors.append("LIGHTER_API_KEY_INDEX is not set")
            else:
                try:
                    int(api_key_index)
                    passed.append("LIGHTER_API_KEY_INDEX_ok")
                except ValueError:
                    blocking_errors.append(f"LIGHTER_API_KEY_INDEX must be integer: {api_key_index}")

            if api_key_private_key or private_key:
                passed.append("lighter_private_key_present")
            else:
                blocking_errors.append("LIGHTER_PRIVATE_KEY or API_KEY_PRIVATE_KEY is not set")

        return StartupDiagnostics(
            passed=passed,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )

    def print_startup_diagnostics(self, diagnostics: StartupDiagnostics) -> None:
        is_zh = self.args.lang == "zh"
        title = "启动自检" if is_zh else "Startup Diagnostics"
        lines: list[str] = []

        passed_label = "passed" if not is_zh else "通过"
        warnings_label = "warnings" if not is_zh else "警告"
        blocking_label = "blocking_errors" if not is_zh else "阻断错误"

        lines.append(f"{passed_label}: {len(diagnostics.passed)}")
        for item in diagnostics.passed:
            lines.append(f"  [ok] {item}")

        lines.append(f"{warnings_label}: {len(diagnostics.warnings)}")
        for item in diagnostics.warnings:
            lines.append(f"  [warn] {item}")

        lines.append(f"{blocking_label}: {len(diagnostics.blocking_errors)}")
        for item in diagnostics.blocking_errors:
            lines.append(f"  [error] {item}")

        border_style = "red" if diagnostics.blocking_errors else ("yellow" if diagnostics.warnings else "green")
        self.dashboard_console.print(Panel("\n".join(lines), title=title, border_style=border_style))

    def log_startup_diagnostics(self, diagnostics: StartupDiagnostics) -> None:
        for item in diagnostics.passed:
            self.logger.info("startup_diagnostics passed: %s", item)
        for item in diagnostics.warnings:
            self.logger.warning("startup_diagnostics warning: %s", item)
        for item in diagnostics.blocking_errors:
            self.logger.error("startup_diagnostics blocking_error: %s", item)

    def setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum=None, frame=None) -> None:
        self.stop_flag = True

    def initialize_lighter_client(self) -> Any:
        if not self.requires_lighter_trading_credentials():
            raise RuntimeError(f"Lighter client is only available in {MODE_LIVE} mode")
        if self.lighter_client is None:
            from lighter.signer_client import SignerClient

            if self.account_index is None or self.api_key_index is None:
                self.load_lighter_trading_credentials()
            api_key_private_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip() or required_env("LIGHTER_PRIVATE_KEY")
            last_error: Exception | None = None
            for attempt in range(1, LIGHTER_INIT_RETRY_ATTEMPTS + 1):
                try:
                    self.lighter_client = SignerClient(
                        url=self.lighter_base_url,
                        account_index=self.account_index,
                        api_private_keys={self.api_key_index: api_key_private_key},
                    )
                    err = self.lighter_client.check_client()
                    if err is not None:
                        raise RuntimeError(f"CheckClient error: {err}")
                    break
                except Exception as exc:
                    self.lighter_client = None
                    last_error = exc
                    if attempt >= LIGHTER_INIT_RETRY_ATTEMPTS:
                        raise RuntimeError(f"Failed to initialize Lighter client after {attempt} attempts: {exc}") from exc
                    time.sleep(LIGHTER_INIT_RETRY_DELAY_SECONDS)
        return self.lighter_client

    def load_lighter_trading_credentials(self) -> None:
        self.account_index = required_int_env("LIGHTER_ACCOUNT_INDEX")
        self.api_key_index = required_int_env("LIGHTER_API_KEY_INDEX")

    def get_lighter_market_config(self) -> tuple[int, int, int, Decimal | None, Decimal | None]:
        if not self.ticker:
            raise RuntimeError("Ticker is not resolved yet")
        response = requests.get(
            f"{self.lighter_base_url}/api/v1/orderBooks",
            headers={"accept": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        for market in data.get("order_books", []):
            if market.get("symbol") == self.ticker:
                price_decimals = int(market["supported_price_decimals"])
                size_decimals = int(market["supported_size_decimals"])
                return (
                    int(market["market_id"]),
                    pow(10, size_decimals),
                    pow(10, price_decimals),
                    to_decimal(market.get("min_base_amount")),
                    to_decimal(market.get("min_quote_amount")),
                )

        raise RuntimeError(f"Ticker {self.ticker} not found in Lighter order books")

    async def detect_current_variational_asset(self) -> str | None:
        async with self.runtime.monitor._lock:
            if self.runtime.monitor.current_quote_asset:
                asset = str(self.runtime.monitor.current_quote_asset).strip().upper()
                quote = self.runtime.monitor.quotes.get(asset)
                if (
                    asset
                    and asset != "UNKNOWN"
                    and isinstance(quote, dict)
                    and to_decimal(quote.get("bid")) is not None
                    and to_decimal(quote.get("ask")) is not None
                ):
                    return asset

        return None

    async def wait_for_ticker_resolution(self) -> str:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            asset = await self.detect_current_variational_asset()
            if asset:
                return asset
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        raise RuntimeError("Timed out deriving ticker from Variational quote/trade messages")

    async def _reset_state_for_asset_switch(self) -> None:
        async with self._record_lock:
            self.records.clear()
            self.record_order.clear()
            self.lighter_client_order_to_trade_key.clear()
        self.cross_spread_history.clear()
        self.paper_position = None
        self.auto_live_position = None
        self.pending_auto_live_matches.clear()
        self.auto_live_last_closed_monotonic = None
        self.auto_live_completed_cycles = 0
        self.auto_live_next_cycle_id = 1
        self._last_auto_live_guard_log = None
        self._last_auto_live_precheck_failure_log.clear()
        self.paper_last_closed_monotonic = None
        async with self._trade_csv_write_lock:
            self._trade_records_snapshot_sig = None

    async def activate_asset(self, variational_asset: str, reason: str) -> None:
        asset = variational_asset.strip().upper()
        if not asset or asset == "UNKNOWN":
            return

        async with self._asset_switch_lock:
            next_ticker = resolve_lighter_ticker(asset)
            if self.variational_ticker == asset and self.ticker == next_ticker:
                return

            self.variational_ticker = asset
            self.ticker = next_ticker
            self.accepted_assets = {
                asset,
                next_ticker,
                resolve_variational_ticker(next_ticker),
            }

            if not self.requires_lighter_market_data():
                await self._reset_state_for_asset_switch()
                self.logger.info(
                    "Switched market (%s): variational_asset=%s -> lighter_ticker=%s (observe mode skips Lighter market data)",
                    reason,
                    self.variational_ticker,
                    self.ticker,
                )
                return

            (
                self.lighter_market_index,
                self.base_amount_multiplier,
                self.price_multiplier,
                self.lighter_min_base_amount,
                self.lighter_min_quote_amount,
            ) = self.get_lighter_market_config()
            await self.reset_lighter_order_book()
            await self._reset_state_for_asset_switch()

            if self.lighter_ws_task and not self.lighter_ws_task.done():
                self.lighter_ws_task.cancel()
                await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            await self.wait_for_lighter_order_book_ready()
            self.logger.info(
                "Switched market (%s): variational_asset=%s -> lighter_ticker=%s market_id=%s min_base_amount=%s min_quote_amount=%s",
                reason,
                self.variational_ticker,
                self.ticker,
                self.lighter_market_index,
                self.lighter_min_base_amount,
                self.lighter_min_quote_amount,
            )

    async def wait_for_variational_ready(self) -> bool:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            state = await self.runtime.monitor.get_trading_state()
            hb_age = state.get("heartbeat_age")
            if hb_age is not None and hb_age <= HEARTBEAT_STALE_SECONDS:
                return True
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        return False

    async def wait_for_lighter_order_book_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            if self.lighter_order_book_ready:
                return
            await asyncio.sleep(0.2)
        raise RuntimeError("Timed out waiting for Lighter order book")

    async def reset_lighter_order_book(self) -> None:
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_ready = False
            self.lighter_snapshot_loaded = False
            self.lighter_order_book_sequence_gap = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list[Any]) -> None:
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(str(level[0]))
                size = Decimal(str(level[1]))
            elif isinstance(level, dict):
                price = Decimal(str(level.get("price", 0)))
                size = Decimal(str(level.get("size", 0)))
            else:
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                self.lighter_order_book[side].pop(price, None)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        return new_offset > self.lighter_order_book_offset

    async def request_fresh_snapshot(self, ws: Any) -> None:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

    async def handle_lighter_fill_update(self, order: dict[str, Any]) -> None:
        if order.get("status") != "filled":
            return

        client_order_id_raw = order.get("client_order_id")
        try:
            client_order_id = int(client_order_id_raw)
        except Exception:
            return

        fill_price: Decimal | None = None
        filled_quote = to_decimal(order.get("filled_quote_amount"))
        filled_base = to_decimal(order.get("filled_base_amount"))
        if filled_quote is not None and filled_base is not None and filled_base != 0:
            fill_price = filled_quote / filled_base

        now_iso = utc_now()
        now_monotonic = time.monotonic()

        async with self._record_lock:
            trade_key = self.lighter_client_order_to_trade_key.get(client_order_id)
            if not trade_key:
                return
            record = self.records.get(trade_key)
            if record is None:
                return
            if record.lighter_fill_ts_iso is not None:
                return

            record.lighter_fill_ts_iso = now_iso
            record.live_lighter_fill_seen_monotonic = now_monotonic
            record.lighter_fill_price = fill_price
            if record.var_fill_ts_iso:
                with contextlib.suppress(Exception):
                    var_fill_dt = datetime.fromisoformat(record.var_fill_ts_iso.replace("Z", "+00:00"))
                    lighter_fill_dt = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
                    record.live_fill_latency_ms = Decimal(
                        str((lighter_fill_dt - var_fill_dt).total_seconds() * 1000)
                    )
            self.set_record_stage(record, STAGE_LIGHTER_FILLED, clear_failure=True)
            payload = record.to_payload()

        await self.append_order_log("lighter_fill", payload)
        await self.maybe_append_live_inventory_actual_pnl(payload)
        await self.maybe_append_live_inventory_final_pnl_from_fill(payload)

    @staticmethod
    def _auto_live_direction_to_var_side(direction: str) -> str:
        if direction == "long_var_short_lighter":
            return "BUY"
        if direction == "short_var_long_lighter":
            return "SELL"
        raise ValueError(f"Unsupported auto-live direction: {direction}")

    @staticmethod
    def _auto_live_direction_to_lighter_side(direction: str) -> str:
        if direction == "long_var_short_lighter":
            return "SELL"
        if direction == "short_var_long_lighter":
            return "BUY"
        raise ValueError(f"Unsupported auto-live direction: {direction}")

    @staticmethod
    def auto_live_entry_actionable_edge_bps(
        direction: str,
        var_price: Decimal | None,
        lighter_bid: Decimal | None,
        lighter_ask: Decimal | None,
    ) -> Decimal | None:
        if var_price is None or var_price == 0:
            return None
        if direction == "long_var_short_lighter":
            if lighter_bid is None:
                return None
            return ((lighter_bid - var_price) / var_price) * Decimal("10000")
        if direction == "short_var_long_lighter":
            if lighter_ask is None:
                return None
            return ((var_price - lighter_ask) / var_price) * Decimal("10000")
        raise ValueError(f"Unsupported auto-live direction: {direction}")

    @staticmethod
    def variational_api_quote_execution_price(side: str, quote_result: dict[str, Any]) -> Decimal | None:
        payload = quote_result.get("result") if isinstance(quote_result.get("result"), dict) else quote_result
        bid = to_decimal(payload.get("bid"))
        ask = to_decimal(payload.get("ask"))
        if side.strip().upper() == "BUY":
            return ask
        return bid

    @staticmethod
    def _opposite_var_side(side: str) -> str:
        return "SELL" if side.strip().upper() == "BUY" else "BUY"

    def require_auto_live_manual_review_for_entry(
        self,
        *,
        cycle_id: int,
        asset: str,
        direction: str,
        qty: Decimal,
        reason: str,
    ) -> None:
        self.auto_live_manual_review_required = True
        self.auto_live_manual_review_reason = reason
        self.write_auto_live_state(
            {
                "status": "manual_review_required",
                "asset": asset,
                "cycle_id": cycle_id,
                "direction": direction,
                "qty": decimal_to_str(qty),
                "reason": reason,
                "action": "stop_auto_live_until_restart",
            }
        )
        self._last_auto_live_guard_log = None
        self.maybe_log_auto_live_guard("manual_review_required")
        try:
            self.record_order_metric(
                "auto_live_manual_review_required",
                {
                    "cycle_id": cycle_id,
                    "asset": asset,
                    "qty": decimal_to_str(qty),
                    "reason": reason,
                    "action": "stop_auto_live_until_restart",
                },
            )
        except Exception:
            self.logger.exception(
                "auto_live_manual_review_metric_failed cycle_id=%s asset=%s qty=%s reason=%s",
                cycle_id,
                asset,
                qty,
                reason,
            )

    async def send_variational_place_order(
        self,
        *,
        asset: str,
        side: str,
        amount: str,
        expected_min_btc_qty: Decimal | None,
        confirm: bool,
        reduce_only: bool = False,
        reuse_quote_id: str | None = None,
    ) -> dict[str, Any]:
        request_id = str(int(time.time() * 1000))
        use_api = self.variational_submit_transport == VARIATIONAL_SUBMIT_TRANSPORT_API
        payload = {
            "type": "VAR_API_ORDER" if use_api and confirm else "VAR_API_QUOTE" if use_api else "PLACE_ORDER",
            "requestId": request_id,
            "side": side.upper(),
            "market": asset.upper(),
            "amount": amount,
            "confirm": bool(confirm),
            "maxSlippage": self.variational_api_max_slippage,
            "reduceOnly": bool(reduce_only),
            "reuseQuoteId": reuse_quote_id,
            "expectedMinBtcQty": decimal_to_str(expected_min_btc_qty) if expected_min_btc_qty is not None else "0",
        }
        return await self.send_variational_command(payload=payload, request_id=request_id)

    async def send_variational_command(self, *, payload: dict[str, Any], request_id: str) -> dict[str, Any]:
        async with self._var_command_ws_lock:
            websocket = self._var_command_ws
            if websocket is None or getattr(websocket, "state", None) != 1:
                websocket = await websockets.connect(
                    f"ws://{FORWARDER_HOST}:{FORWARDER_COMMAND_PORT}",
                    ping_interval=20,
                    ping_timeout=20,
                )
                self._var_command_ws = websocket
            try:
                await websocket.send(json.dumps(payload, ensure_ascii=True))
                while True:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=self.auto_live_command_timeout_seconds)
                    message = json.loads(raw)
                    if message.get("requestId") == request_id:
                        return message
            except Exception:
                with contextlib.suppress(Exception):
                    await websocket.close()
                self._var_command_ws = None
                raise

    async def fetch_variational_positions(self) -> dict[str, Any]:
        request_id = str(int(time.time() * 1000))
        payload = {
            "type": "VAR_API_POSITIONS",
            "requestId": request_id,
        }
        return await self.send_variational_command(payload=payload, request_id=request_id)

    async def fetch_variational_orders(
        self,
        *,
        asset: str,
        status: str,
        limit: int = 20,
        offset: int = 0,
    ) -> dict[str, Any]:
        request_id = str(int(time.time() * 1000))
        asset = asset.upper()
        payload = {
            "type": "VAR_API_ORDERS",
            "requestId": request_id,
            "status": status,
            "instrument": f"P-{asset}-USDC-3600",
            "limit": limit,
            "offset": offset,
            "orderBy": "created_at",
            "order": "desc",
        }
        return await self.send_variational_command(payload=payload, request_id=request_id)

    @staticmethod
    def iter_variational_orders(orders_result: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not isinstance(orders_result, dict):
            return []
        payload = orders_result.get("result") if isinstance(orders_result.get("result"), dict) else orders_result
        if isinstance(payload, dict) and isinstance(payload.get("orders"), dict):
            payload = payload.get("orders")
        orders = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(orders, list):
            return []
        return [order for order in orders if isinstance(order, dict)]

    @classmethod
    def find_variational_order_by_rfq_id(
        cls,
        orders_result: dict[str, Any] | None,
        *,
        rfq_id: str,
    ) -> dict[str, Any] | None:
        rfq_id = str(rfq_id or "").strip()
        if not rfq_id:
            return None
        for order in cls.iter_variational_orders(orders_result):
            if str(order.get("rfq_id") or "").strip() == rfq_id:
                return order
        return None

    @staticmethod
    def extract_variational_position_qty(positions_result: dict[str, Any] | None, *, asset: str) -> Decimal | None:
        if not isinstance(positions_result, dict):
            return None
        payload = positions_result.get("result") if isinstance(positions_result.get("result"), dict) else positions_result
        positions = payload.get("positions") if isinstance(payload, dict) else None
        if isinstance(positions, dict):
            iterable = positions.values()
        elif isinstance(positions, list):
            iterable = positions
        else:
            return None
        asset = asset.upper()
        for position in iterable:
            if not isinstance(position, dict):
                continue
            instrument = position.get("instrument")
            candidates = [
                position.get("asset"),
                position.get("market"),
                position.get("symbol"),
                position.get("underlying"),
            ]
            if isinstance(instrument, dict):
                candidates.extend([instrument.get("underlying"), instrument.get("symbol"), instrument.get("asset")])
            if asset not in {str(item).upper() for item in candidates if item is not None}:
                continue
            for key in ("qty", "quantity", "size", "position", "position_size", "base_amount", "amount"):
                value = to_decimal(position.get(key))
                if value is not None:
                    return value
        return Decimal("0")

    @staticmethod
    def variational_error_is_no_position(error: Any) -> bool:
        text = str(error or "").lower()
        return "no position exists" in text

    async def place_lighter_order_from_plan(
        self,
        *,
        asset: str,
        side: str,
        qty: Decimal,
        var_fill_price: Decimal,
        cycle_id: int | None = None,
        role: str | None = None,
        reduce_only: bool = False,
    ) -> tuple[OrderLifecycle | None, dict[str, Any] | None]:
        synthetic_key = f"auto:{asset}:{side.lower()}:{int(time.time() * 1000)}"
        record = OrderLifecycle(
            trade_key=synthetic_key,
            trade_id=synthetic_key,
            side=side.lower(),
            qty=qty,
            asset=asset,
            mode=self.mode,
            last_variational_status="submitted",
            var_fill_price=var_fill_price,
            var_fill_ts_iso=utc_now(),
            live_var_fill_seen_at_iso=utc_now(),
            live_var_fill_seen_monotonic=time.monotonic(),
            synthetic_eager_fill=True,
            auto_live_cycle_id=cycle_id,
            auto_live_role=role,
            auto_live_merge_path="synthetic_created",
            lighter_submit_transport=self.lighter_submit_transport,
            lighter_order_mode=self.lighter_order_mode,
            lighter_reduce_only=reduce_only,
        )
        async with self._record_lock:
            self.set_record_stage(record, STAGE_RECORD_CREATED, clear_failure=True)
            self.set_record_stage(record, STAGE_VARIATIONAL_FILLED, clear_failure=True)
            self.records[synthetic_key] = record
            self.record_order.append(synthetic_key)
        await self.place_lighter_order(record)
        async with self._record_lock:
            payload = record.to_payload()
        return record, payload

    def prune_pending_auto_live_matches(self) -> None:
        now_monotonic = time.monotonic()
        self.pending_auto_live_matches = [
            item
            for item in self.pending_auto_live_matches
            if now_monotonic - item.created_at_monotonic <= self.auto_live_match_window_seconds
        ]

    def consume_pending_auto_live_match(self, *, asset: str, side: str, qty: Decimal) -> PendingAutoLiveMatch | None:
        self.prune_pending_auto_live_matches()
        side_lower = side.strip().lower()
        for idx, item in enumerate(self.pending_auto_live_matches):
            if item.asset != asset or item.side != side_lower:
                continue
            if abs(item.qty - qty) > Decimal("0.00000001"):
                continue
            match = self.pending_auto_live_matches.pop(idx)
            return match
        return None

    def prune_pending_live_inventory_var_fill_matches(self) -> None:
        now_monotonic = time.monotonic()
        pending_matches = getattr(self, "pending_live_inventory_var_fill_matches", [])
        self.pending_live_inventory_var_fill_matches = [
            item
            for item in pending_matches
            if item.role == "live_inventory_entry_pending_lighter"
            or now_monotonic - item.created_at_monotonic <= self.auto_live_match_window_seconds
        ]

    def consume_pending_live_inventory_var_fill_match(
        self,
        *,
        asset: str,
        side: str,
        qty: Decimal,
    ) -> PendingLiveInventoryVarFillMatch | None:
        self.prune_pending_live_inventory_var_fill_matches()
        side_lower = side.strip().lower()
        for idx, item in enumerate(self.pending_live_inventory_var_fill_matches):
            if item.asset != asset or item.side != side_lower:
                continue
            if abs(item.qty - qty) > Decimal("0.000002"):
                continue
            return self.pending_live_inventory_var_fill_matches.pop(idx)
        return None

    def consume_pending_live_inventory_var_status_match(
        self,
        *,
        asset: str,
        side: str,
        qty: Decimal,
        roles: set[str] | None = None,
    ) -> PendingLiveInventoryVarFillMatch | None:
        self.prune_pending_live_inventory_var_fill_matches()
        asset = asset.upper()
        side = side.lower()
        for idx, item in enumerate(list(self.pending_live_inventory_var_fill_matches)):
            if item.asset.upper() != asset or item.side.lower() != side:
                continue
            if roles is not None and item.role not in roles:
                continue
            if abs(item.qty - qty) <= max(Decimal("0.00000001"), qty * Decimal("0.0001")):
                return self.pending_live_inventory_var_fill_matches.pop(idx)
        return None

    def add_pending_live_inventory_var_fill_match(self, match: PendingLiveInventoryVarFillMatch) -> None:
        if not hasattr(self, "pending_live_inventory_var_fill_matches"):
            self.pending_live_inventory_var_fill_matches = []
        self.pending_live_inventory_var_fill_matches.append(match)

    def has_pending_live_inventory_var_fill_match(self, *, asset: str, roles: set[str] | None = None) -> bool:
        self.prune_pending_live_inventory_var_fill_matches()
        asset = asset.upper()
        for item in getattr(self, "pending_live_inventory_var_fill_matches", []):
            if item.asset.upper() != asset:
                continue
            if roles is not None and item.role not in roles:
                continue
            return True
        return False

    def remove_pending_live_inventory_var_fill_match(self, *, asset: str, lot_id: Any, role: str) -> None:
        pending_matches = getattr(self, "pending_live_inventory_var_fill_matches", [])
        self.pending_live_inventory_var_fill_matches = [
            item
            for item in pending_matches
            if not (item.asset == asset and item.lot_id == lot_id and item.role == role)
        ]

    def _rekey_record_locked(self, record: OrderLifecycle, new_trade_key: str) -> None:
        old_trade_key = record.trade_key
        if not new_trade_key or new_trade_key == old_trade_key:
            return

        existing = self.records.get(new_trade_key)
        if existing is not None and existing is not record:
            return

        self.records.pop(old_trade_key, None)
        record.trade_key = new_trade_key
        self.records[new_trade_key] = record

        updated_order = deque(
            ((new_trade_key if key == old_trade_key else key) for key in self.record_order),
            maxlen=self.record_order.maxlen,
        )
        self.record_order = updated_order

        for client_order_id, trade_key in list(self.lighter_client_order_to_trade_key.items()):
            if trade_key == old_trade_key:
                self.lighter_client_order_to_trade_key[client_order_id] = new_trade_key

    def build_lighter_ws_url(self) -> str:
        if env_flag("LIGHTER_WS_SERVER_PINGS"):
            return f"{LIGHTER_WS_URL}?server_pings=true"
        return LIGHTER_WS_URL

    async def ensure_lighter_submit_ws(self) -> Any:
        websocket = self._lighter_submit_ws
        if websocket is not None and getattr(websocket, "state", None) == 1:
            return websocket

        websocket = await websockets.connect(
            self.build_lighter_ws_url(),
            ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
            ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
        )
        try:
            while True:
                raw = await asyncio.wait_for(websocket.recv(), timeout=self.live_submit_timeout_seconds)
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                message = json.loads(raw)
                if message.get("type") == "ping":
                    await websocket.send(json.dumps({"type": "pong"}))
                    continue
                if message.get("type") == "connected":
                    self._lighter_submit_ws = websocket
                    return websocket
        except Exception:
            with contextlib.suppress(Exception):
                await websocket.close()
            if self._lighter_submit_ws is websocket:
                self._lighter_submit_ws = None
            raise

    async def prewarm_lighter_submit_ws(self) -> None:
        if self.lighter_submit_transport != LIGHTER_SUBMIT_TRANSPORT_WS:
            return
        started = time.monotonic()
        async with self._lighter_submit_ws_lock:
            await self.ensure_lighter_submit_ws()
        self.logger.info("lighter_submit_ws_prewarmed duration_ms=%s", elapsed_ms_str(started))

    async def _timed_submit(self, awaitable: Any) -> tuple[Any, str]:
        started = time.monotonic()
        result = await awaitable
        return result, elapsed_ms_str(started)

    async def preflight_variational_api_command_client(self, asset: str) -> None:
        result = await self.send_variational_place_order(
            asset=asset,
            side="BUY",
            amount="0.00000001",
            expected_min_btc_qty=None,
            confirm=False,
            reduce_only=False,
        )
        if not result.get("ok"):
            raise RuntimeError(f"Variational API command preflight failed: {result.get('error') or result}")

    @staticmethod
    def _is_lighter_ws_sendtx_response(message: dict[str, Any]) -> bool:
        message_type = str(message.get("type") or "").strip().lower()
        if "sendtx" in message_type:
            return True
        data = message.get("data")
        if isinstance(data, dict) and "code" in data:
            return True
        return "code" in message and ("tx_hash" in message or "message" in message)

    @staticmethod
    def _normalize_lighter_ws_sendtx_response(message: dict[str, Any]) -> SimpleNamespace:
        data = message.get("data")
        payload = data if isinstance(data, dict) else message
        return SimpleNamespace(
            code=int(payload.get("code", 0) or 0),
            message=payload.get("message"),
            tx_hash=payload.get("tx_hash") or payload.get("hash") or "",
            predicted_execution_time_ms=int(payload.get("predicted_execution_time_ms", 0) or 0),
            volume_quota_remaining=int(payload.get("volume_quota_remaining", 0) or 0),
            raw=message,
        )

    async def send_lighter_tx_ws(self, *, tx_type: int, tx_info: str) -> SimpleNamespace:
        total_started = time.monotonic()
        if not tx_info or tx_info[0] != "{":
            raise ValueError(f"Invalid tx_info: {tx_info}")
        parse_started = time.monotonic()
        tx_info_payload = json.loads(tx_info)
        parse_done = time.monotonic()

        payload = {
            "type": "jsonapi/sendtx",
            "data": {
                "tx_type": int(tx_type),
                "tx_info": tx_info_payload,
            },
        }
        lock_requested = time.monotonic()
        async with self._lighter_submit_ws_lock:
            lock_acquired = time.monotonic()
            ensure_started = time.monotonic()
            websocket = await self.ensure_lighter_submit_ws()
            ensure_done = time.monotonic()
            try:
                serialize_started = time.monotonic()
                payload_text = json.dumps(payload, ensure_ascii=True)
                serialize_done = time.monotonic()
                send_started = time.monotonic()
                await websocket.send(payload_text)
                send_done = time.monotonic()
                response_started = time.monotonic()
                last_message: dict[str, Any] | None = None
                while True:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=self.live_submit_timeout_seconds)
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    message = json.loads(raw)
                    last_message = message
                    if message.get("type") == "ping":
                        await websocket.send(json.dumps({"type": "pong"}))
                        continue
                    if self._is_lighter_ws_sendtx_response(message):
                        response_done = time.monotonic()
                        self.timing_logger().info(
                            "lighter_ws_sendtx_timing tx_type=%s parse_ms=%s ws_lock_wait_ms=%s ensure_ws_ms=%s "
                            "serialize_ms=%s ws_send_ms=%s ws_response_wait_ms=%s ws_total_ms=%s total_ms=%s",
                            tx_type,
                            elapsed_ms_between_str(parse_started, parse_done),
                            elapsed_ms_between_str(lock_requested, lock_acquired),
                            elapsed_ms_between_str(ensure_started, ensure_done),
                            elapsed_ms_between_str(serialize_started, serialize_done),
                            elapsed_ms_between_str(send_started, send_done),
                            elapsed_ms_between_str(response_started, response_done),
                            elapsed_ms_between_str(lock_acquired, response_done),
                            elapsed_ms_between_str(total_started, response_done),
                        )
                        return self._normalize_lighter_ws_sendtx_response(message)
            except asyncio.TimeoutError as exc:
                last_type = (last_message or {}).get("type")
                raise RuntimeError(
                    f"Lighter WS sendtx timed out after {self.live_submit_timeout_seconds}s last_message_type={last_type} tx_info_format=object"
                ) from exc
            except Exception:
                with contextlib.suppress(Exception):
                    await websocket.close()
                self._lighter_submit_ws = None
                raise

    async def create_lighter_order_ws(
        self,
        *,
        market_index: int,
        client_order_index: int,
        base_amount: int,
        price: int,
        is_ask: bool,
        order_type: int,
        time_in_force: int,
        reduce_only: bool,
        trigger_price: int,
        order_expiry: int,
    ) -> tuple[Any | None, Any | None, str | None]:
        total_started = time.monotonic()
        if not self.lighter_client:
            raise RuntimeError("Lighter client is not initialized")
        nonce_started = time.monotonic()
        api_key_index, nonce = self.lighter_client.nonce_manager.next_nonce()
        nonce_done = time.monotonic()
        sent_to_ws = False
        try:
            sign_started = time.monotonic()
            tx_type, tx_info, _tx_hash, error = self.lighter_client.sign_create_order(
                market_index=market_index,
                client_order_index=client_order_index,
                base_amount=base_amount,
                price=price,
                is_ask=int(is_ask),
                order_type=order_type,
                time_in_force=time_in_force,
                reduce_only=reduce_only,
                trigger_price=trigger_price,
                order_expiry=order_expiry,
                nonce=nonce,
                api_key_index=api_key_index,
            )
            sign_done = time.monotonic()
            if error is not None:
                self.lighter_client.nonce_manager.acknowledge_failure(api_key_index)
                return None, None, error

            sent_to_ws = True
            ws_started = time.monotonic()
            api_response = await self.send_lighter_tx_ws(tx_type=tx_type, tx_info=tx_info)
            ws_done = time.monotonic()
            self.timing_logger().info(
                "lighter_ws_create_order_timing market_index=%s client_order_index=%s side=%s base_amount=%s "
                "nonce_ms=%s sign_ms=%s ws_submit_ms=%s total_ms=%s response_code=%s",
                market_index,
                client_order_index,
                "SELL" if is_ask else "BUY",
                base_amount,
                elapsed_ms_between_str(nonce_started, nonce_done),
                elapsed_ms_between_str(sign_started, sign_done),
                elapsed_ms_between_str(ws_started, ws_done),
                elapsed_ms_between_str(total_started, ws_done),
                getattr(api_response, "code", "-"),
            )
            if api_response is None or api_response.code != 200:
                self.lighter_client.nonce_manager.acknowledge_failure(api_key_index)
            return None, api_response, None
        except Exception as exc:
            if "invalid nonce" in str(exc):
                self.lighter_client.nonce_manager.hard_refresh_nonce(api_key_index)
            elif not sent_to_ws:
                self.lighter_client.nonce_manager.acknowledge_failure(api_key_index)
            return None, None, str(exc)

    async def handle_lighter_ws(self) -> None:
        while not self.stop_flag:
            try:
                await self.reset_lighter_order_book()
                url = self.build_lighter_ws_url()
                async with websockets.connect(
                    url,
                    ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                    ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                ) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    if self.requires_lighter_trading_credentials():
                        account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"
                        try:
                            async with self._lighter_signer_lock:
                                if not self.lighter_client:
                                    self.initialize_lighter_client()
                                auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                                    api_key_index=self.api_key_index
                                )
                            if err is None:
                                await ws.send(
                                    json.dumps(
                                        {
                                            "type": "subscribe",
                                            "channel": account_orders_channel,
                                            "auth": auth_token,
                                        }
                                    )
                                )
                            else:
                                self.logger.warning("Failed to create Lighter WS auth token: %s", err)
                        except Exception as exc:
                            self.logger.warning("Error creating Lighter WS auth token: %s", exc)

                    while not self.stop_flag:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        msg_type = data.get("type")

                        if msg_type == "subscribed/order_book":
                            async with self.lighter_order_book_lock:
                                self.lighter_order_book["bids"].clear()
                                self.lighter_order_book["asks"].clear()
                                order_book = data.get("order_book", {})
                                self.lighter_order_book_offset = int(order_book.get("offset", 0) or 0)
                                self.update_lighter_order_book("bids", order_book.get("bids", []))
                                self.update_lighter_order_book("asks", order_book.get("asks", []))
                                self.lighter_snapshot_loaded = True
                                self.lighter_order_book_ready = True
                                self.last_lighter_order_book_update_at = utc_now()
                                self.lighter_best_bid = (
                                    max(self.lighter_order_book["bids"].keys())
                                    if self.lighter_order_book["bids"]
                                    else None
                                )
                                self.lighter_best_ask = (
                                    min(self.lighter_order_book["asks"].keys())
                                    if self.lighter_order_book["asks"]
                                    else None
                                )

                        elif msg_type == "update/order_book" and self.lighter_snapshot_loaded:
                            order_book = data.get("order_book", {})
                            if "offset" not in order_book:
                                continue
                            new_offset = int(order_book["offset"])
                            async with self.lighter_order_book_lock:
                                if not self.validate_order_book_offset(new_offset):
                                    self.lighter_order_book_sequence_gap = True
                                else:
                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))
                                    self.lighter_order_book_offset = new_offset
                                    self.last_lighter_order_book_update_at = utc_now()
                                    self.lighter_best_bid = (
                                        max(self.lighter_order_book["bids"].keys())
                                        if self.lighter_order_book["bids"]
                                        else None
                                    )
                                    self.lighter_best_ask = (
                                        min(self.lighter_order_book["asks"].keys())
                                        if self.lighter_order_book["asks"]
                                        else None
                                    )

                        elif msg_type == "update/account_orders":
                            orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                            for order in orders:
                                await self.handle_lighter_fill_update(order)

                        if self.lighter_order_book_sequence_gap:
                            await self.request_fresh_snapshot(ws)
                            self.lighter_order_book_sequence_gap = False

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.logger.warning(
                    "Lighter websocket reconnect after error: %s (url=%s)",
                    exc,
                    self.build_lighter_ws_url(),
                )
                await asyncio.sleep(1)

    async def get_lighter_best_bid_ask(self) -> tuple[Decimal | None, Decimal | None]:
        async with self.lighter_order_book_lock:
            return self.lighter_best_bid, self.lighter_best_ask

    def _lighter_order_book_age_seconds(self) -> float | None:
        return self._age_seconds_from_iso(self.last_lighter_order_book_update_at)

    def _lighter_order_book_is_stale(self) -> bool:
        age = self._lighter_order_book_age_seconds()
        return age is None or age >= HEALTH_LIGHTER_BOOK_STALE_SECONDS

    async def get_lighter_top_sizes(self) -> tuple[Decimal | None, Decimal | None]:
        async with self.lighter_order_book_lock:
            bid_size: Decimal | None = None
            ask_size: Decimal | None = None
            if self.lighter_best_bid is not None:
                bid_size = self.lighter_order_book["bids"].get(self.lighter_best_bid)
            if self.lighter_best_ask is not None:
                ask_size = self.lighter_order_book["asks"].get(self.lighter_best_ask)
            return bid_size, ask_size

    async def estimate_lighter_fill_price(self, side: str, quantity: Decimal) -> Decimal | None:
        if quantity <= 0:
            return None
        book_side = "asks" if side.upper() == "BUY" else "bids"
        async with self.lighter_order_book_lock:
            levels = self.lighter_order_book.get(book_side, {})
            if not levels:
                return None
            prices = sorted(levels.keys()) if book_side == "asks" else sorted(levels.keys(), reverse=True)
            remaining = quantity
            notional = Decimal("0")
            for price in prices:
                size = levels.get(price)
                if size is None or size <= 0:
                    continue
                fill_qty = min(remaining, size)
                notional += fill_qty * price
                remaining -= fill_qty
                if remaining <= 0:
                    break
        if remaining > 0:
            return None
        return notional / quantity

    async def get_variational_quote(self, preferred_asset: str | None) -> dict[str, Any] | None:
        async with self.runtime.monitor._lock:
            quote = None
            if preferred_asset:
                quote = self.runtime.monitor.quotes.get(preferred_asset)
            if quote is None and self.variational_ticker:
                quote = self.runtime.monitor.quotes.get(self.variational_ticker)
            if quote is None and self.runtime.monitor.current_quote_asset:
                quote = self.runtime.monitor.quotes.get(self.runtime.monitor.current_quote_asset)

            if quote is None:
                return None
            quote = dict(quote)

        bid = to_decimal(quote.get("bid"))
        ask = to_decimal(quote.get("ask"))
        if self.is_paper_mode() or bid is None or ask is None or ask <= bid:
            quote = await self.apply_variational_indicative_quote(quote)
        return quote

    @staticmethod
    def _fetch_variational_metadata_stats_sync() -> dict[str, Any]:
        response = requests.get(VARIATIONAL_METADATA_STATS_URL, timeout=5)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("metadata stats response is not a JSON object")
        return payload

    async def get_variational_metadata_stats(self) -> dict[str, Any] | None:
        now = time.monotonic()
        if (
            self._variational_metadata_stats is not None
            and now - self._variational_metadata_stats_at < VARIATIONAL_METADATA_STATS_TTL_SECONDS
        ):
            return self._variational_metadata_stats

        try:
            payload = await asyncio.to_thread(self._fetch_variational_metadata_stats_sync)
        except Exception as exc:
            if now - self._last_metadata_stats_error_at >= VAR_QUOTE_DIAGNOSTIC_INTERVAL_SECONDS:
                self._last_metadata_stats_error_at = now
                self.logger.warning("variational_metadata_stats_fetch_failed error=%s", exc)
            return self._variational_metadata_stats

        self._variational_metadata_stats = payload
        self._variational_metadata_stats_at = now
        return payload

    async def apply_variational_indicative_quote(self, quote: dict[str, Any]) -> dict[str, Any]:
        asset = str(quote.get("asset") or self.variational_ticker or self.ticker or "").strip().upper()
        if not asset:
            return quote

        metadata = await self.get_variational_metadata_stats()
        listings = metadata.get("listings") if isinstance(metadata, dict) else None
        if not isinstance(listings, list):
            return quote

        listing = next(
            (
                item
                for item in listings
                if isinstance(item, dict) and str(item.get("ticker") or "").strip().upper() == asset
            ),
            None,
        )
        if not isinstance(listing, dict):
            return quote

        quotes = listing.get("quotes")
        if not isinstance(quotes, dict):
            return quote

        quote_sizes = (VARIATIONAL_METADATA_QUOTE_SIZE, "base", "size_100k", "size_1m")
        quote_size = next((key for key in quote_sizes if isinstance(quotes.get(key), dict)), None)
        if quote_size is None:
            return quote

        sized_quote = quotes.get(quote_size)
        if not isinstance(sized_quote, dict):
            return quote

        bid = to_decimal(sized_quote.get("bid"))
        ask = to_decimal(sized_quote.get("ask"))
        if bid is None or ask is None:
            return quote
        if ask < bid:
            bid, ask = ask, bid

        raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
        return {
            **quote,
            "asset": asset,
            "bid": decimal_to_str(bid),
            "ask": decimal_to_str(ask),
            "mark_price": quote.get("mark_price") or listing.get("mark_price"),
            "timestamp": quotes.get("updated_at") or quote.get("timestamp"),
            "raw": {
                **raw,
                "__indicative_source_url": VARIATIONAL_METADATA_STATS_URL,
                "__indicative_source_endpoint": "/metadata/stats",
                "__indicative_quote_size": quote_size,
                "__indicative_bid": decimal_to_str(bid),
                "__indicative_ask": decimal_to_str(ask),
                "__indicative_updated_at": quotes.get("updated_at"),
            },
        }

    async def get_variational_best_bid_ask(self, preferred_asset: str | None):
        quote = await self.get_variational_quote(preferred_asset)
        if quote is None:
            return None, None, None
        return to_decimal(quote.get("bid")), to_decimal(quote.get("ask")), str(quote.get("asset", ""))

    @staticmethod
    def trade_key(event: dict[str, Any]) -> str:
        trade_id = str(event.get("trade_id", "")).strip()
        if trade_id:
            return f"id:{trade_id}"
        event_seq = str(event.get("event_seq", "")).strip()
        return f"seq:{event_seq}"

    async def append_order_log(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.orders_file is None:
            return
        stage_history = payload.get("stage_history")
        row = {
            "event": event_type,
            "logged_at": utc_now(),
            "stage_flow_text": self._fmt_stage_history(stage_history, limit=20),
            **payload,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._order_write_lock:
            await asyncio.to_thread(self.orders_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.orders_file, line)

    async def append_opportunity_log(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.opportunities_file is None:
            return
        row = {
            "event": event_type,
            "logged_at": utc_now(),
            **payload,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._opportunity_write_lock:
            await asyncio.to_thread(self.opportunities_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.opportunities_file, line)

    async def append_market_sample(self, snapshot: CrossSpreadSnapshot) -> None:
        if self.market_samples_file is None:
            return
        row = {
            "event": "market_sample",
            "logged_at": utc_now(),
            "asset": snapshot.asset,
            "var_bid": decimal_to_str(snapshot.var_bid),
            "var_ask": decimal_to_str(snapshot.var_ask),
            "var_buy_price": decimal_to_str(snapshot.var_buy_price),
            "var_sell_price": decimal_to_str(snapshot.var_sell_price),
            "var_mid": decimal_to_str(snapshot.var_mid),
            "var_full_spread_bps": decimal_to_str(snapshot.var_full_spread_bps),
            "var_half_spread_bps": decimal_to_str(snapshot.var_half_spread_bps),
            "var_spread_source": snapshot.var_spread_source,
            "lighter_bid": decimal_to_str(snapshot.lighter_bid),
            "lighter_ask": decimal_to_str(snapshot.lighter_ask),
            "lighter_buy_fill_price": decimal_to_str(snapshot.lighter_buy_fill_price),
            "lighter_sell_fill_price": decimal_to_str(snapshot.lighter_sell_fill_price),
            "lighter_mid": decimal_to_str(snapshot.lighter_mid),
            "lighter_half_spread_bps": decimal_to_str(snapshot.lighter_half_spread_bps),
            "long_var_short_lighter_bps": decimal_to_str(decimal_percent_to_bps(snapshot.long_var_short_lighter_pct)),
            "short_var_long_lighter_bps": decimal_to_str(decimal_percent_to_bps(snapshot.short_var_long_lighter_pct)),
            "long_median_5m_bps": decimal_to_str(decimal_percent_to_bps(snapshot.long_median_5m_pct)),
            "short_median_5m_bps": decimal_to_str(decimal_percent_to_bps(snapshot.short_median_5m_pct)),
            "long_sample_count_5m": snapshot.long_sample_count_5m,
            "short_sample_count_5m": snapshot.short_sample_count_5m,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._opportunity_write_lock:
            await asyncio.to_thread(self.market_samples_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.market_samples_file, line)

    async def append_live_inventory_log(self, event_type: str, payload: dict[str, Any]) -> None:
        await self.append_order_log(
            event_type,
            {
                "record_kind": "live_inventory",
                "mode": self.mode,
                "execution_mode": "dry_decision" if self.live_inventory_dry_decisions else "live",
                **payload,
            },
        )

    async def append_inventory_paper_log(self, payload: dict[str, Any]) -> None:
        if self.inventory_paper_file is None:
            return
        line = json.dumps({"logged_at": utc_now(), **payload}, ensure_ascii=True) + "\n"
        async with self._opportunity_write_lock:
            await asyncio.to_thread(self.inventory_paper_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.inventory_paper_file, line)

    @staticmethod
    def _append_line(path: Path, line: str) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    async def build_hedge_plan(self, record: OrderLifecycle) -> tuple[str, Decimal, int] | None:
        side = "SELL" if record.side == "buy" else "BUY"
        if self._lighter_order_book_is_stale():
            async with self._record_lock:
                record.hedge_error = "Lighter order book is stale"
                self.set_record_stage(
                    record,
                    record.processing_stage,
                    failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                    failure_reason="lighter_order_book_stale",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return None
        best_bid, best_ask = await self.get_lighter_best_bid_ask()
        async with self._record_lock:
            record.lighter_reference_bid = best_bid
            record.lighter_reference_ask = best_ask

        if best_bid is None or best_ask is None:
            async with self._record_lock:
                record.hedge_error = "Lighter order book not ready"
                self.set_record_stage(
                    record,
                    record.processing_stage,
                    failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                    failure_reason="lighter_order_book_not_ready",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return None

        slippage = Decimal(str(HEDGE_SLIPPAGE_BPS)) / Decimal("10000")
        if side == "BUY":
            limit_price = best_ask * (Decimal("1") + slippage)
        else:
            limit_price = best_bid * (Decimal("1") - slippage)

        notional = record.qty * limit_price
        edge_bps = basis_points_diff(limit_price, record.var_fill_price)
        async with self._record_lock:
            record.live_notional_usd = notional
            record.live_edge_bps = edge_bps

        base_amount = int(record.qty * self.base_amount_multiplier)
        if base_amount <= 0:
            async with self._record_lock:
                record.hedge_error = f"Hedge base amount rounds to zero ({record.qty})"
                self.set_record_stage(
                    record,
                    record.processing_stage,
                    failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                    failure_reason="hedge_base_amount_rounds_to_zero",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return None

        if self.lighter_min_base_amount is not None and record.qty < self.lighter_min_base_amount:
            async with self._record_lock:
                record.hedge_error = (
                    f"Hedge qty {record.qty} is below Lighter min base amount {self.lighter_min_base_amount}"
                )
                self.set_record_stage(
                    record,
                    record.processing_stage,
                    failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                    failure_reason="hedge_below_lighter_min_base_amount",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return None

        if base_amount > self.risk_guard_max_base_amount:
            async with self._record_lock:
                record.hedge_error = (
                    f"Hedge base amount {base_amount} exceeds risk limit {self.risk_guard_max_base_amount}"
                )
                self.set_record_stage(
                    record,
                    record.processing_stage,
                    failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                    failure_reason="hedge_base_amount_exceeds_risk_limit",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return None

        price_reference = record.var_fill_price
        deviation_bps = edge_bps
        if deviation_bps is not None and deviation_bps > self.risk_guard_max_price_deviation_bps:
            async with self._record_lock:
                record.hedge_error = (
                    f"Hedge price deviation {deviation_bps:.2f}bps exceeds risk limit "
                    f"{self.risk_guard_max_price_deviation_bps}bps"
                )
                self.set_record_stage(
                    record,
                    record.processing_stage,
                    failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                    failure_reason="hedge_price_deviation_exceeds_risk_limit",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return None

        if self.lighter_min_quote_amount is not None and notional < self.lighter_min_quote_amount:
            async with self._record_lock:
                record.hedge_error = (
                    f"Hedge notional {notional} is below Lighter min quote amount {self.lighter_min_quote_amount}"
                )
                self.set_record_stage(
                    record,
                    record.processing_stage,
                    failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                    failure_reason="hedge_below_lighter_min_quote_amount",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return None

        if self.is_live_mode():
            if self.live_allowed_sides and record.side not in self.live_allowed_sides:
                async with self._record_lock:
                    record.hedge_error = (
                        f"Live trading is restricted to Variational sides {sorted(self.live_allowed_sides)}, "
                        f"got {record.side}"
                    )
                    self.set_record_stage(
                        record,
                        record.processing_stage,
                        failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                        failure_reason="live_side_not_allowed",
                    )
                    payload = record.to_payload()
                await self.append_order_log("lighter_error", payload)
                return None

            if self.live_allowed_assets and record.asset.upper() not in self.live_allowed_assets:
                async with self._record_lock:
                    record.hedge_error = (
                        f"Live trading is restricted to assets {sorted(self.live_allowed_assets)}, got {record.asset}"
                    )
                    self.set_record_stage(
                        record,
                        record.processing_stage,
                        failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                        failure_reason="live_asset_not_allowed",
                    )
                    payload = record.to_payload()
                await self.append_order_log("lighter_error", payload)
                return None

            if self.live_max_qty > 0 and record.qty > self.live_max_qty:
                async with self._record_lock:
                    record.hedge_error = f"Live qty {record.qty} exceeds live max qty {self.live_max_qty}"
                    self.set_record_stage(
                        record,
                        record.processing_stage,
                        failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                        failure_reason="live_qty_exceeds_limit",
                    )
                    payload = record.to_payload()
                await self.append_order_log("lighter_error", payload)
                return None

            if self.live_max_notional_usd > 0 and notional > self.live_max_notional_usd:
                async with self._record_lock:
                    record.hedge_error = (
                        f"Live notional {notional} exceeds live max notional {self.live_max_notional_usd}"
                    )
                    self.set_record_stage(
                        record,
                        record.processing_stage,
                        failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                        failure_reason="live_notional_exceeds_limit",
                    )
                    payload = record.to_payload()
                await self.append_order_log("lighter_error", payload)
                return None

            if edge_bps is not None and edge_bps < self.live_require_min_edge_bps:
                async with self._record_lock:
                    record.hedge_error = (
                        f"Live edge {edge_bps:.2f}bps is below required minimum {self.live_require_min_edge_bps}bps"
                    )
                    self.set_record_stage(
                        record,
                        record.processing_stage,
                        failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                        failure_reason="live_edge_bps_below_threshold",
                    )
                    payload = record.to_payload()
                await self.append_order_log("lighter_error", payload)
                return None

            now_monotonic = time.monotonic()
            asset_key = record.asset.upper()
            last_submit_monotonic = self.last_live_submit_monotonic_by_asset.get(asset_key)
            if (
                not record.lighter_reduce_only
                and last_submit_monotonic is not None
                and now_monotonic - last_submit_monotonic < self.live_cooldown_seconds
            ):
                remaining = self.live_cooldown_seconds - (now_monotonic - last_submit_monotonic)
                async with self._record_lock:
                    record.hedge_error = f"Live cooldown active, wait {remaining:.2f}s"
                    self.set_record_stage(
                        record,
                        record.processing_stage,
                        failure_stage=FAILURE_STAGE_HEDGE_PLAN,
                        failure_reason="live_cooldown_active",
                    )
                    payload = record.to_payload()
                await self.append_order_log("lighter_error", payload)
                return None

        return side, limit_price, base_amount

    async def record_dry_run_plan(self, record: OrderLifecycle) -> None:
        plan = await self.build_hedge_plan(record)
        if plan is None:
            return

        side, limit_price, base_amount = plan
        async with self._record_lock:
            record.dry_run_plan_side = side
            record.dry_run_plan_price = limit_price
            record.dry_run_plan_base_amount = base_amount
            record.hedge_error = None
            self.set_record_stage(record, STAGE_DRY_RUN_PLANNED, clear_failure=True)
            payload = record.to_payload()
        await self.append_order_log("lighter_dry_run_plan", payload)

    async def place_lighter_order(self, record: OrderLifecycle) -> None:
        if not self.is_live_mode():
            async with self._record_lock:
                record.hedge_error = f"Real Lighter hedge is only allowed in {MODE_LIVE} mode"
                self.set_record_stage(
                    record,
                    STAGE_BLOCKED_BY_MODE,
                    failure_stage=FAILURE_STAGE_MODE_GUARD,
                    failure_reason="real_lighter_hedge_requires_live_mode",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_blocked", payload)
            return

        if getattr(self, "live_inventory", False) and self.live_inventory_dry_decisions:
            async with self._record_lock:
                record.hedge_error = "Live inventory dry decision mode blocks real Lighter hedges"
                self.set_record_stage(
                    record,
                    STAGE_BLOCKED_BY_MODE,
                    failure_stage=FAILURE_STAGE_MODE_GUARD,
                    failure_reason="live_inventory_dry_decisions_block_real_hedge",
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_blocked", payload)
            return

        async with self._record_lock:
            self.set_record_stage(record, STAGE_LIVE_SUBMIT_STARTED, clear_failure=True)
            record.live_plan_started_at_iso = utc_now()
            record.live_plan_started_monotonic = time.monotonic()

        plan = await self.build_hedge_plan(record)
        if plan is None:
            return

        plan_ready_iso = utc_now()
        plan_ready_monotonic = time.monotonic()
        side, limit_price, base_amount = plan
        is_ask = side == "SELL"
        asset_key = record.asset.upper()

        price_i = int(limit_price * self.price_multiplier)
        async with self._record_lock:
            record.live_plan_ready_at_iso = plan_ready_iso
            record.live_plan_ready_monotonic = plan_ready_monotonic
            record.live_submit_started_at_iso = utc_now()
            record.live_submit_started_monotonic = time.monotonic()
            client_order_id = int(time.time() * 1000)
            while client_order_id in self.lighter_client_order_to_trade_key:
                client_order_id += 1

        try:
            submit_body_started = time.monotonic()
            signer_lock_requested = time.monotonic()
            async with self._lighter_signer_lock:
                signer_lock_acquired = time.monotonic()
                init_started = time.monotonic()
                if not self.lighter_client:
                    self.initialize_lighter_client()
                init_done = time.monotonic()
                order_kwargs = {
                    "market_index": self.lighter_market_index,
                    "client_order_index": client_order_id,
                    "base_amount": base_amount,
                    "price": price_i,
                    "is_ask": is_ask,
                    "order_type": (
                        self.lighter_client.ORDER_TYPE_MARKET
                        if self.lighter_order_mode == LIGHTER_ORDER_MODE_MARKET_IOC
                        else self.lighter_client.ORDER_TYPE_LIMIT
                    ),
                    "time_in_force": (
                        self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
                        if self.lighter_order_mode == LIGHTER_ORDER_MODE_MARKET_IOC
                        else self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
                    ),
                    "reduce_only": bool(record.lighter_reduce_only),
                    "trigger_price": 0,
                    "order_expiry": (
                        self.lighter_client.DEFAULT_IOC_EXPIRY
                        if self.lighter_order_mode == LIGHTER_ORDER_MODE_MARKET_IOC
                        else self.lighter_client.DEFAULT_28_DAY_ORDER_EXPIRY
                    ),
                }
                if self.lighter_submit_transport == LIGHTER_SUBMIT_TRANSPORT_WS:
                    _, tx_hash, error = await self.create_lighter_order_ws(**order_kwargs)
                else:
                    _, tx_hash, error = await self.lighter_client.create_order(**order_kwargs)
                submit_body_done = time.monotonic()
                self.timing_logger().info(
                    "lighter_submit_body_timing asset=%s side=%s client_order_id=%s transport=%s mode=%s "
                    "signer_lock_wait_ms=%s init_ms=%s submit_body_ms=%s",
                    record.asset,
                    side,
                    client_order_id,
                    self.lighter_submit_transport,
                    self.lighter_order_mode,
                    elapsed_ms_between_str(signer_lock_requested, signer_lock_acquired),
                    elapsed_ms_between_str(init_started, init_done),
                    elapsed_ms_between_str(submit_body_started, submit_body_done),
                )

            submit_sent_iso = utc_now()
            submit_sent_monotonic = time.monotonic()
            if error is not None:
                raise RuntimeError(f"Sign error: {error}")

            async with self._record_lock:
                record.dry_run_plan_side = side
                record.dry_run_plan_price = limit_price
                record.dry_run_plan_base_amount = base_amount
                record.lighter_side = side
                record.lighter_reduce_only = bool(record.lighter_reduce_only)
                record.lighter_client_order_id = client_order_id
                record.lighter_submit_transport = self.lighter_submit_transport
                record.lighter_order_mode = self.lighter_order_mode
                record.lighter_tx_hash = tx_hash
                record.live_submit_sent_at_iso = submit_sent_iso
                record.live_submit_sent_monotonic = submit_sent_monotonic
                record.hedge_error = None
                self.set_record_stage(record, STAGE_LIVE_SUBMIT_SENT, clear_failure=True)
                self.lighter_client_order_to_trade_key[client_order_id] = record.trade_key
                self.last_live_submit_monotonic_by_asset[asset_key] = time.monotonic()
        except Exception as exc:
            async with self._record_lock:
                record.lighter_side = side
                record.hedge_error = str(exc)
                self.set_record_stage(
                    record,
                    STAGE_LIVE_SUBMIT_FAILED,
                    failure_stage=FAILURE_STAGE_LIVE_SUBMIT,
                    failure_reason=str(exc),
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)

    async def watchdog_live_submissions(self) -> None:
        while not self.stop_flag:
            timed_out: list[dict[str, Any]] = []
            async with self._record_lock:
                for record in self.records.values():
                    if record.processing_stage != STAGE_LIVE_SUBMIT_SENT:
                        continue
                    if record.lighter_fill_ts_iso is not None:
                        continue
                    if record.processing_stage == STAGE_LIVE_SUBMIT_FAILED:
                        continue
                    started_at = self._age_seconds_from_iso(record.live_submit_started_at_iso)
                    if started_at is None or started_at < self.live_submit_timeout_seconds:
                        continue
                    record.hedge_error = (
                        f"Live submit timed out after {started_at:.1f}s without lighter fill"
                    )
                    self.set_record_stage(
                        record,
                        STAGE_LIVE_SUBMIT_TIMED_OUT,
                        failure_stage=FAILURE_STAGE_LIVE_SUBMIT,
                        failure_reason="live_submit_timeout",
                    )
                    timed_out.append(record.to_payload())
            for payload in timed_out:
                await self.append_order_log("lighter_timeout", payload)
            await asyncio.sleep(1.0)

    def should_track_variational_event(self, event: dict[str, Any]) -> bool:
        side = str(event.get("side", "")).strip().lower()
        if side not in {"buy", "sell"}:
            return False

        qty = to_decimal(event.get("qty"))
        if qty is None or qty <= 0:
            return False

        asset = str(event.get("asset", "")).strip().upper()
        if not asset:
            return False
        return asset in self.accepted_assets

    def is_historical_trade_event(self, event: dict[str, Any]) -> bool:
        if self.trade_event_min_timestamp is None:
            return False

        event_ts_raw = str(event.get("timestamp", "")).strip()
        event_ts = self._parse_iso_ts(event_ts_raw)
        if event_ts is None:
            return False

        cutoff = self.trade_event_min_timestamp.timestamp() - TRADE_EVENT_STARTUP_GRACE_SECONDS
        return event_ts.timestamp() < cutoff

    async def process_variational_trade_event(self, event: dict[str, Any]) -> None:
        if not self.should_track_variational_event(event):
            return

        if self.is_historical_trade_event(event):
            return

        self.last_variational_trade_event_at = utc_now()

        key = self.trade_key(event)
        side = str(event.get("side", "")).strip().lower()
        qty = to_decimal(event.get("qty"))
        if qty is None:
            return

        status = normalize_variational_status(str(event.get("status", "")))
        asset = str(event.get("asset", "")).strip().upper() or self.variational_ticker
        trade_id = str(event.get("trade_id", "")).strip()

        now_iso = utc_now()
        fill_iso = str(event.get("timestamp") or now_iso)

        created = False
        created_record: OrderLifecycle | None = None
        matched_auto_live = None
        matched_live_inventory_var_fill = None
        if status in {"rejected", "reject", "cancelled", "canceled", "failed"}:
            rejected_live_inventory_match = self.consume_pending_live_inventory_var_status_match(
                asset=asset,
                side=side,
                qty=qty,
                roles={"live_inventory_entry_pending_lighter", "live_inventory_entry", "live_inventory_exit"},
            )
            if rejected_live_inventory_match is not None:
                await self.require_live_inventory_manual_review(
                    asset=asset,
                    reason=f"variational_{status}:pending_{rejected_live_inventory_match.role}",
                    context={
                        "trade_id": trade_id,
                        "side": side,
                        "qty": decimal_to_str(qty),
                        "lot_id": rejected_live_inventory_match.lot_id,
                        "role": rejected_live_inventory_match.role,
                        "event": event,
                    },
                )
                return
        if status == "filled":
            matched_auto_live = self.consume_pending_auto_live_match(asset=asset, side=side, qty=qty)
            matched_live_inventory_var_fill = self.consume_pending_live_inventory_var_fill_match(
                asset=asset,
                side=side,
                qty=qty,
            )
        matched_trade_key = matched_auto_live.record_key if matched_auto_live is not None else None

        async with self._record_lock:
            record = self.records.get(matched_trade_key or key)
            if record is None:
                record = OrderLifecycle(
                    trade_key=matched_trade_key or key,
                    trade_id=trade_id,
                    side=side,
                    qty=qty,
                    asset=asset if asset else "UNKNOWN",
                    mode=self.mode,
                    last_variational_status=status,
                    auto_live_cycle_id=(
                        matched_auto_live.cycle_id
                        if matched_auto_live is not None
                        else matched_live_inventory_var_fill.lot_id
                        if matched_live_inventory_var_fill is not None
                        else None
                    ),
                    auto_live_role=(
                        matched_auto_live.role
                        if matched_auto_live is not None
                        else matched_live_inventory_var_fill.role
                        if matched_live_inventory_var_fill is not None
                        else None
                    ),
                )
                self.set_record_stage(record, STAGE_RECORD_CREATED, clear_failure=True)
                self.records[record.trade_key] = record
                self.record_order.append(record.trade_key)
                created = True
                created_record = record
            else:
                record.trade_id = trade_id or record.trade_id
                previous_status = record.last_variational_status
                record.last_variational_status = status
                if matched_auto_live is not None:
                    record.auto_live_cycle_id = matched_auto_live.cycle_id
                    record.auto_live_role = matched_auto_live.role
                elif matched_live_inventory_var_fill is not None:
                    record.auto_live_cycle_id = matched_live_inventory_var_fill.lot_id
                    record.auto_live_role = matched_live_inventory_var_fill.role
                self.set_record_stage(record, STAGE_EVENT_FILTERED)

            if created:
                previous_status = ""

            should_set_fill = False
            if status == "filled":
                if record.var_fill_ts_iso is None:
                    should_set_fill = True
                elif previous_status != "filled":
                    should_set_fill = True

            if should_set_fill:
                if trade_id:
                    self._rekey_record_locked(record, f"id:{trade_id}")
                if record.synthetic_eager_fill:
                    record.auto_live_merge_path = "synthetic_matched_real_var_fill"
                record.synthetic_eager_fill = False
                record.matched_variational_trade_id = trade_id or record.matched_variational_trade_id
                record.trade_id = trade_id or record.trade_id
                record.var_fill_ts_iso = fill_iso
                record.var_fill_price = to_decimal(event.get("price"))
                if record.live_var_fill_seen_at_iso is None:
                    record.live_var_fill_seen_at_iso = now_iso
                    record.live_var_fill_seen_monotonic = time.monotonic()
                self.set_record_stage(record, STAGE_VARIATIONAL_FILLED, clear_failure=True)
                filled_payload = record.to_payload()
            else:
                filled_payload = None

        if filled_payload is not None:
            await self.append_order_log("variational_fill", filled_payload)
            await self.maybe_append_live_inventory_final_pnl_from_fill(filled_payload)
            if (
                matched_live_inventory_var_fill is not None
                and matched_live_inventory_var_fill.role == "live_inventory_entry_pending_lighter"
            ):
                await self.complete_live_inventory_entry_after_var_fill(
                    match=matched_live_inventory_var_fill,
                    record=record,
                    fill_payload=filled_payload,
                )
                return

        actionable_record = created_record if created_record is not None else record if filled_payload is not None else None
        if actionable_record is None:
            return
        if getattr(actionable_record, "auto_live_merge_path", None) == "synthetic_matched_real_var_fill":
            return

        if self.is_observe_mode():
            async with self._record_lock:
                self.set_record_stage(actionable_record, STAGE_BLOCKED_BY_MODE, clear_failure=True)
            return

        if self.is_dry_run_mode():
            async with self._record_lock:
                self.set_record_stage(actionable_record, STAGE_DRY_RUN_PENDING, clear_failure=True)
            await self.record_dry_run_plan(actionable_record)
            return

        if self.is_live_mode() and status == "filled":
            if getattr(self, "live_inventory", False) or str(getattr(actionable_record, "auto_live_role", "") or "").startswith("live_inventory_"):
                async with self._record_lock:
                    actionable_record.hedge_error = "Live inventory mode blocks trade-event auto hedges"
                    self.set_record_stage(
                        actionable_record,
                        STAGE_BLOCKED_BY_MODE,
                        failure_stage=FAILURE_STAGE_MODE_GUARD,
                        failure_reason="live_inventory_blocks_trade_event_auto_hedge",
                    )
                    payload = actionable_record.to_payload()
                await self.append_order_log("lighter_blocked", payload)
                return
            await self.place_lighter_order(actionable_record)

    async def trade_loop(self) -> None:
        while not self.stop_flag:
            current_asset = await self.detect_current_variational_asset()
            if current_asset:
                if current_asset == self.variational_ticker:
                    self._asset_switch_candidate = None
                    self._asset_switch_candidate_hits = 0
                else:
                    if current_asset == self._asset_switch_candidate:
                        self._asset_switch_candidate_hits += 1
                    else:
                        self._asset_switch_candidate = current_asset
                        self._asset_switch_candidate_hits = 1

                    if self._asset_switch_candidate_hits >= ASSET_SWITCH_CONFIRM_TICKS:
                        await self.activate_asset(current_asset, reason="quote_stream_debounced")
                        self._asset_switch_candidate = None
                        self._asset_switch_candidate_hits = 0
            else:
                self._asset_switch_candidate = None
                self._asset_switch_candidate_hits = 0

            events = await self.runtime.monitor.get_trade_events_since(self.trade_event_cursor, limit=500)
            for event in events:
                self.trade_event_cursor = max(self.trade_event_cursor, int(event.get("event_seq", 0) or 0))
                await self.process_variational_trade_event(event)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    def _fmt_price(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return format(value, "f")

    @staticmethod
    def _direction_labels(side: str) -> tuple[str, str]:
        side_n = side.strip().lower()
        if side_n == "buy":
            return "做多 Var / 做空 Lighter", "Long Var / Short Lighter"
        if side_n == "sell":
            return "做空 Var / 做多 Lighter", "Short Var / Long Lighter"
        side_u = side_n.upper() if side_n else "-"
        return side_u, side_u

    def _fmt_pct(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    @staticmethod
    def _fmt_stage(value: str | None) -> str:
        if not value:
            return "-"
        return value

    @staticmethod
    def _fmt_stage_history(history: list[str] | None, limit: int = 4) -> str:
        if not history:
            return "-"
        compact = history[-limit:]
        return " -> ".join(compact)

    @staticmethod
    def _fmt_failure(value: str | None) -> str:
        if not value:
            return "-"
        return value

    @staticmethod
    def _is_risk_guard_failure(failure_reason: str | None, failure_stage: str | None) -> bool:
        if failure_reason in RISK_GUARD_FAILURE_REASONS:
            return True
        return failure_stage == FAILURE_STAGE_HEDGE_PLAN

    @staticmethod
    def _parse_iso_ts(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _age_seconds_from_iso(value: str | None) -> float | None:
        dt = VariationalToLighterRuntime._parse_iso_ts(value)
        if dt is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())

    @staticmethod
    def _classify_age(age_seconds: float | None, degraded_after: float, stale_after: float) -> str:
        if age_seconds is None:
            return "stale"
        if age_seconds >= stale_after:
            return "stale"
        if age_seconds >= degraded_after:
            return "degraded"
        return "healthy"

    @staticmethod
    def _classify_optional_age(age_seconds: float | None, degraded_after: float, stale_after: float) -> str:
        if age_seconds is None:
            return "idle"
        return VariationalToLighterRuntime._classify_age(age_seconds, degraded_after, stale_after)

    @staticmethod
    def _status_rank(status: str) -> int:
        if status == "idle":
            return -1
        if status == "healthy":
            return 0
        if status == "degraded":
            return 1
        return 2

    @staticmethod
    def _fmt_age(age_seconds: float | None) -> str:
        if age_seconds is None:
            return "-"
        return f"{age_seconds:.1f}s"

    @staticmethod
    def _status_color(status: str) -> str:
        if status == "idle":
            return "cyan"
        if status == "healthy":
            return "green"
        if status == "degraded":
            return "yellow"
        return "red"

    def build_health_status(self) -> HealthStatus:
        components: list[tuple[str, str, str]] = []

        quote_age = self._age_seconds_from_iso(self.runtime.monitor.last_update_at)
        quote_status = self._classify_age(
            quote_age,
            HEALTH_QUOTE_DEGRADED_SECONDS,
            HEALTH_QUOTE_STALE_SECONDS,
        )
        components.append(("variational_quote", quote_status, f"last_update_age={self._fmt_age(quote_age)}"))

        heartbeat_age = self._age_seconds_from_iso(self.runtime.monitor.last_heartbeat_iso)
        heartbeat_status = self._classify_age(
            heartbeat_age,
            HEALTH_VARIATIONAL_HEARTBEAT_DEGRADED_SECONDS,
            HEALTH_VARIATIONAL_HEARTBEAT_STALE_SECONDS,
        )
        components.append(("variational_heartbeat", heartbeat_status, f"age={self._fmt_age(heartbeat_age)}"))

        trade_event_age = self._age_seconds_from_iso(self.last_variational_trade_event_at)
        trade_event_status = self._classify_optional_age(
            trade_event_age,
            HEALTH_TRADE_EVENT_DEGRADED_SECONDS,
            HEALTH_TRADE_EVENT_STALE_SECONDS,
        )
        components.append(("variational_trade_event", trade_event_status, f"age={self._fmt_age(trade_event_age)}"))

        if self.requires_lighter_market_data():
            lighter_book_age = self._age_seconds_from_iso(self.last_lighter_order_book_update_at)
            lighter_book_status = self._classify_age(
                lighter_book_age,
                HEALTH_LIGHTER_BOOK_DEGRADED_SECONDS,
                HEALTH_LIGHTER_BOOK_STALE_SECONDS,
            )
            components.append(("lighter_order_book", lighter_book_status, f"age={self._fmt_age(lighter_book_age)}"))

        overall = "healthy"
        if any(self._status_rank(status) == 2 for _, status, _ in components):
            overall = "stale"
        elif any(self._status_rank(status) == 1 for _, status, _ in components):
            overall = "degraded"

        return HealthStatus(overall=overall, components=components)

    def _fmt_signal_pct(
        self,
        current: Decimal | None,
        book_spread_baseline: Decimal | None,
        median_5m: float | None,
        median_30m: float | None,
        median_1h: float | None,
    ) -> str:
        if current is None:
            return "-"
        if book_spread_baseline is None:
            color = "red"
            return f"[{color}]{self._fmt_pct(current)}[/{color}]"

        adjusted = current - book_spread_baseline
        adjusted_f = float(adjusted)
        thresholds = [v for v in (median_5m, median_30m, median_1h) if v is not None]
        is_green = any(adjusted_f > threshold for threshold in thresholds)
        color = "green" if is_green else "red"
        return f"[{color}]{self._fmt_pct(current)}[/{color}]"

    @staticmethod
    def _fill_diff_by_direction(
        side: str,
        var_fill_price: Decimal | None,
        lighter_fill_price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal | None]:
        side_n = side.strip().lower()
        if side_n == "buy":
            # Long Var / Short Lighter: lighter_fill - var_fill
            diff = spread_value(var_fill_price, lighter_fill_price)
            pct = spread_percent(diff, var_fill_price)
            return diff, pct
        if side_n == "sell":
            # Short Var / Long Lighter: var_fill - lighter_fill
            diff = spread_value(lighter_fill_price, var_fill_price)
            pct = spread_percent(diff, lighter_fill_price)
            return diff, pct
        diff = spread_value(lighter_fill_price, var_fill_price)
        pct = spread_percent(diff, var_fill_price)
        return diff, pct

    @staticmethod
    def _notional_value(qty: Decimal | None, price: Decimal | None) -> Decimal | None:
        if qty is None or price is None:
            return None
        return qty * price

    @staticmethod
    def _price_diff(reference: Decimal | None, actual: Decimal | None) -> tuple[Decimal | None, Decimal | None]:
        if reference is None or actual is None:
            return None, None
        diff = actual - reference
        pct = spread_percent(diff, reference)
        return diff, pct

    @staticmethod
    def _decimal_as_float(value: Decimal | None) -> float | None:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _format_auto_live_order_qty(asset: str, planned_qty: Decimal) -> Decimal:
        asset_upper = asset.strip().upper()
        if asset_upper == "BTC":
            # Variational's BTC size input behaved reliably at 5 decimals in manual tests.
            normalized = max(planned_qty, Decimal("0.00021"))
            return normalized.quantize(Decimal("0.00001"), rounding=ROUND_UP)
        return planned_qty.normalize()

    @staticmethod
    def _fmt_median_pct(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    def _record_cross_spreads(
        self,
        long_var_short_lighter_pct: Decimal | None,
        short_var_long_lighter_pct: Decimal | None,
    ) -> None:
        now = time.monotonic()
        self.cross_spread_history.append(
            (
                now,
                self._decimal_as_float(long_var_short_lighter_pct),
                self._decimal_as_float(short_var_long_lighter_pct),
            )
        )
        cutoff = now - SPREAD_HISTORY_SECONDS
        while self.cross_spread_history and self.cross_spread_history[0][0] < cutoff:
            self.cross_spread_history.popleft()

    def _median_cross_spread(self, window_seconds: float, long_side: bool) -> float | None:
        now = time.monotonic()
        cutoff = now - window_seconds
        value_index = 1 if long_side else 2
        values = [
            row[value_index]
            for row in self.cross_spread_history
            if row[0] >= cutoff and row[value_index] is not None
        ]
        if not values:
            return None
        return float(median(values))

    def _cross_spread_sample_count(self, window_seconds: float, long_side: bool) -> int:
        now = time.monotonic()
        cutoff = now - window_seconds
        value_index = 1 if long_side else 2
        return sum(
            1
            for row in self.cross_spread_history
            if row[0] >= cutoff and row[value_index] is not None
        )

    async def get_raw_cross_spreads(self) -> tuple[Decimal | None, Decimal | None]:
        quote = await self.get_variational_quote(self.variational_ticker)
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        if quote is None or lighter_bid is None or lighter_ask is None:
            return None, None
        var_bid = to_decimal(quote.get("bid"))
        var_ask = to_decimal(quote.get("ask"))
        if var_bid is None or var_ask is None:
            return None, None
        var_buy_price, var_sell_price, _ = self.extract_variational_button_prices(quote)
        if var_buy_price is None or var_sell_price is None:
            var_buy_price = max(var_bid, var_ask)
            var_sell_price = min(var_bid, var_ask)
        return (
            spread_percent(spread_value(var_buy_price, lighter_bid), var_buy_price),
            spread_percent(spread_value(lighter_ask, var_sell_price), lighter_ask),
        )

    def extract_variational_button_prices(
        self,
        quote: dict[str, Any],
    ) -> tuple[Decimal | None, Decimal | None, str]:
        raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
        candidates = nested_dicts(raw) + nested_dicts(quote)
        buy_keys = (
            "buy_price",
            "buyPrice",
            "long_price",
            "longPrice",
            "ask_price",
            "askPrice",
        )
        sell_keys = (
            "sell_price",
            "sellPrice",
            "short_price",
            "shortPrice",
            "bid_price",
            "bidPrice",
        )
        for candidate in candidates:
            buy_price = first_decimal_from_keys(candidate, buy_keys)
            sell_price = first_decimal_from_keys(candidate, sell_keys)
            if buy_price is not None and sell_price is not None:
                if buy_price >= sell_price:
                    return buy_price, sell_price, "button_or_quote_fields"
                return sell_price, buy_price, "button_or_quote_fields_reordered"
        return None, None, "unavailable"

    def _log_variational_quote_diagnostic(
        self,
        quote: dict[str, Any],
        *,
        reason: str,
        var_buy_price: Decimal | None,
        var_sell_price: Decimal | None,
        var_bid: Decimal | None,
        var_ask: Decimal | None,
        var_spread_source: str,
    ) -> None:
        now = time.monotonic()
        if now - self._last_var_quote_diagnostic_at < VAR_QUOTE_DIAGNOSTIC_INTERVAL_SECONDS:
            return
        self._last_var_quote_diagnostic_at = now

        raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}

        def short_keys(payload: dict[str, Any]) -> list[str]:
            return sorted(str(key) for key in payload.keys())[:80]

        nested_summary: list[dict[str, Any]] = []
        for key, value in raw.items():
            if isinstance(value, dict):
                nested_summary.append({"path": str(key), "keys": short_keys(value)})
            elif isinstance(value, list):
                first_dict = next((item for item in value if isinstance(item, dict)), None)
                if first_dict is not None:
                    nested_summary.append({"path": f"{key}[0]", "keys": short_keys(first_dict)})
        nested_summary = nested_summary[:20]

        self.logger.warning(
            "var_quote_diagnostic reason=%s asset=%s source=%s source_url=%s source_stream=%s "
            "var_bid=%s var_ask=%s var_buy=%s var_sell=%s quote_keys=%s raw_keys=%s nested=%s",
            reason,
            quote.get("asset"),
            var_spread_source,
            raw.get("__source_url"),
            raw.get("__source_stream") or raw.get("__source_endpoint"),
            decimal_to_str(var_bid),
            decimal_to_str(var_ask),
            decimal_to_str(var_buy_price),
            decimal_to_str(var_sell_price),
            short_keys(quote),
            short_keys(raw),
            json.dumps(nested_summary, ensure_ascii=True),
        )

    async def spread_loop(self) -> None:
        while not self.stop_flag:
            long_pct, short_pct = await self.get_raw_cross_spreads()
            self._record_cross_spreads(long_pct, short_pct)
            await asyncio.sleep(DASHBOARD_REFRESH_SECONDS)

    async def get_cross_spread_snapshot(self) -> CrossSpreadSnapshot | None:
        quote = await self.get_variational_quote(self.variational_ticker)
        if quote is None:
            return None
        var_bid = to_decimal(quote.get("bid"))
        var_ask = to_decimal(quote.get("ask"))
        quote_asset = str(quote.get("asset", ""))
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        if var_bid is None or var_ask is None or lighter_bid is None or lighter_ask is None:
            return None

        var_buy_price, var_sell_price, var_spread_source = self.extract_variational_button_prices(quote)
        if var_buy_price is None or var_sell_price is None:
            var_buy_price = max(var_bid, var_ask)
            var_sell_price = min(var_bid, var_ask)
            var_spread_source = "bid_ask_fallback"

        asset = (quote_asset or self.variational_ticker or self.ticker or "UNKNOWN").upper()
        var_mid = (var_buy_price + var_sell_price) / Decimal("2")
        lighter_mid = (lighter_bid + lighter_ask) / Decimal("2")
        lighter_buy_price = lighter_ask
        lighter_sell_price = lighter_bid
        planned_qty = self.paper_notional_usd / var_mid if var_mid > 0 else Decimal("0")
        lighter_buy_fill_price = await self.estimate_lighter_fill_price("BUY", planned_qty) or lighter_buy_price
        lighter_sell_fill_price = await self.estimate_lighter_fill_price("SELL", planned_qty) or lighter_sell_price
        long_pct = spread_percent(spread_value(var_buy_price, lighter_sell_fill_price), var_buy_price)
        short_pct = spread_percent(spread_value(lighter_buy_fill_price, var_sell_price), lighter_buy_fill_price)

        long_median_5m = self._median_cross_spread(5 * 60, long_side=True)
        short_median_5m = self._median_cross_spread(5 * 60, long_side=False)
        long_median_pct = Decimal(str(long_median_5m)) if long_median_5m is not None else None
        short_median_pct = Decimal(str(short_median_5m)) if short_median_5m is not None else None
        long_count = self._cross_spread_sample_count(5 * 60, long_side=True)
        short_count = self._cross_spread_sample_count(5 * 60, long_side=False)

        var_full_spread = var_buy_price - var_sell_price
        var_full_spread_pct = spread_percent(var_full_spread, var_mid)
        var_full_spread_bps = decimal_percent_to_bps(var_full_spread_pct)
        if var_full_spread_bps is None:
            return None
        var_half_spread_bps = var_full_spread_bps / Decimal("2")
        lighter_full_spread_pct = spread_percent(lighter_buy_price - lighter_sell_price, lighter_mid)
        lighter_full_spread_bps = decimal_percent_to_bps(lighter_full_spread_pct)
        if lighter_full_spread_bps is None:
            return None
        lighter_half_spread_bps = lighter_full_spread_bps / Decimal("2")

        if var_full_spread_bps == 0 or var_buy_price == var_sell_price:
            self._log_variational_quote_diagnostic(
                quote,
                reason="zero_or_equal_var_button_spread",
                var_buy_price=var_buy_price,
                var_sell_price=var_sell_price,
                var_bid=var_bid,
                var_ask=var_ask,
                var_spread_source=var_spread_source,
            )

        raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}

        return CrossSpreadSnapshot(
            asset=asset,
            var_bid=var_bid,
            var_ask=var_ask,
            var_mid=var_mid,
            var_half_spread_bps=var_half_spread_bps,
            var_buy_price=var_buy_price,
            var_sell_price=var_sell_price,
            var_full_spread_bps=var_full_spread_bps,
            var_spread_source=var_spread_source,
            var_timestamp=str(quote.get("timestamp") or raw.get("timestamp") or "") or None,
            var_source_url=str(raw.get("__source_url") or "") or None,
            var_source_stream=str(raw.get("__source_stream") or raw.get("channel") or "") or None,
            lighter_bid=lighter_bid,
            lighter_ask=lighter_ask,
            lighter_mid=lighter_mid,
            lighter_buy_price=lighter_buy_price,
            lighter_sell_price=lighter_sell_price,
            lighter_half_spread_bps=lighter_half_spread_bps,
            lighter_buy_fill_price=lighter_buy_fill_price,
            lighter_sell_fill_price=lighter_sell_fill_price,
            long_var_short_lighter_pct=long_pct,
            short_var_long_lighter_pct=short_pct,
            long_median_5m_pct=long_median_pct,
            short_median_5m_pct=short_median_pct,
            long_sample_count_5m=long_count,
            short_sample_count_5m=short_count,
        )

    def _next_paper_opportunity_id(self) -> str:
        self.paper_opportunity_counter += 1
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"paper_{stamp}_{self.paper_opportunity_counter:04d}"

    async def _paper_depth_enough(self, direction: str, planned_qty: Decimal) -> tuple[bool, Decimal | None, Decimal | None]:
        top_bid_size, top_ask_size = await self.get_lighter_top_sizes()
        if direction == "long_var_short_lighter":
            return top_bid_size is not None and top_bid_size >= planned_qty, top_bid_size, top_ask_size
        if direction == "short_var_long_lighter":
            return top_ask_size is not None and top_ask_size >= planned_qty, top_bid_size, top_ask_size
        return False, top_bid_size, top_ask_size

    async def maybe_enter_paper_position(self, snapshot: CrossSpreadSnapshot) -> None:
        if self.paper_position is not None:
            return
        if self.paper_last_closed_monotonic is not None:
            if time.monotonic() - self.paper_last_closed_monotonic < self.paper_cooldown_seconds:
                return
        if snapshot.var_half_spread_bps > self.paper_max_var_half_spread_bps:
            return

        candidate = paper_entry_candidate(snapshot, self.paper_entry_deviation_bps, self.paper_min_samples)
        if candidate is None:
            return
        direction = candidate.direction
        current_pct = candidate.current_pct
        median_pct = candidate.median_pct
        deviation_bps = candidate.deviation_bps
        sample_count = candidate.sample_count
        if snapshot.var_mid <= 0:
            return
        planned_qty = self.paper_notional_usd / snapshot.var_mid
        depth_enough, top_bid_size, top_ask_size = await self._paper_depth_enough(direction, planned_qty)
        if not depth_enough:
            return
        entry_var_execution_price, entry_lighter_execution_price = paper_entry_execution_prices(snapshot, direction)
        entry_var_spread_cost_usd = paper_var_spread_cost_usd(snapshot, planned_qty)
        entry_lighter_taker_cost_usd = paper_lighter_taker_cost_usd(snapshot, planned_qty)
        entry_fee_usd = paper_fee_cost_usd(self.paper_notional_usd, self.paper_fee_bps_per_leg)
        entry_latency_drift_cost_usd = paper_latency_drift_cost_usd(self.paper_notional_usd, self.paper_latency_drift_bps)
        lighter_book_age_seconds = self._lighter_order_book_age_seconds()

        now_iso = utc_now()
        opportunity_id = self._next_paper_opportunity_id()
        self.paper_position = PaperPositionState(
            opportunity_id=opportunity_id,
            asset=snapshot.asset,
            direction=direction,
            entered_at_iso=now_iso,
            entered_at_monotonic=time.monotonic(),
            entry_spread_pct=current_pct,
            entry_median_pct=median_pct,
            entry_deviation_bps=deviation_bps,
            entry_var_mid=snapshot.var_mid,
            entry_lighter_mid=snapshot.lighter_mid,
            entry_var_execution_price=entry_var_execution_price,
            entry_lighter_execution_price=entry_lighter_execution_price,
            entry_var_half_spread_bps=snapshot.var_half_spread_bps,
            entry_var_spread_cost_usd=entry_var_spread_cost_usd,
            entry_lighter_half_spread_bps=snapshot.lighter_half_spread_bps,
            entry_lighter_taker_cost_usd=entry_lighter_taker_cost_usd,
            entry_fee_usd=entry_fee_usd,
            entry_latency_drift_cost_usd=entry_latency_drift_cost_usd,
            planned_notional_usd=self.paper_notional_usd,
            planned_qty=planned_qty,
        )
        await self.append_opportunity_log(
            "paper_entered",
            {
                "record_kind": "paper_opportunity",
                "opportunity_id": opportunity_id,
                "opportunity_type": "mixed",
                "execution_mode": "paper",
                "asset": snapshot.asset,
                "direction": direction,
                "status": "paper_entered",
                "entry_time": now_iso,
                "entry_var_bid": decimal_to_str(snapshot.var_bid),
                "entry_var_ask": decimal_to_str(snapshot.var_ask),
                "entry_var_buy_price": decimal_to_str(snapshot.var_buy_price),
                "entry_var_sell_price": decimal_to_str(snapshot.var_sell_price),
                "entry_var_mid": decimal_to_str(snapshot.var_mid),
                "entry_var_execution_price": decimal_to_str(entry_var_execution_price),
                "entry_var_full_spread_bps": decimal_to_str(snapshot.var_full_spread_bps),
                "entry_var_half_spread_bps": decimal_to_str(snapshot.var_half_spread_bps),
                "entry_var_spread_source": snapshot.var_spread_source,
                "entry_var_spread_cost_usd": decimal_to_str(entry_var_spread_cost_usd),
                "entry_lighter_bid": decimal_to_str(snapshot.lighter_bid),
                "entry_lighter_ask": decimal_to_str(snapshot.lighter_ask),
                "entry_lighter_mid": decimal_to_str(snapshot.lighter_mid),
                "entry_lighter_buy_fill_price": decimal_to_str(snapshot.lighter_buy_fill_price),
                "entry_lighter_sell_fill_price": decimal_to_str(snapshot.lighter_sell_fill_price),
                "entry_lighter_execution_price": decimal_to_str(entry_lighter_execution_price),
                "entry_lighter_half_spread_bps": decimal_to_str(snapshot.lighter_half_spread_bps),
                "entry_lighter_taker_cost_usd": decimal_to_str(entry_lighter_taker_cost_usd),
                "entry_fee_usd": decimal_to_str(entry_fee_usd),
                "entry_latency_drift_cost_usd": decimal_to_str(entry_latency_drift_cost_usd),
                "entry_long_var_short_lighter_bps": decimal_to_str(
                    decimal_percent_to_bps(snapshot.long_var_short_lighter_pct)
                ),
                "entry_short_var_long_lighter_bps": decimal_to_str(
                    decimal_percent_to_bps(snapshot.short_var_long_lighter_pct)
                ),
                "entry_cross_exchange_spread_bps": decimal_to_str(decimal_percent_to_bps(current_pct)),
                "entry_spread_median_bps": decimal_to_str(decimal_percent_to_bps(median_pct)),
                "entry_spread_deviation_bps": decimal_to_str(deviation_bps),
                "entry_deviation_threshold_bps": decimal_to_str(self.paper_entry_deviation_bps),
                "spread_window_seconds": 300,
                "spread_sample_count": sample_count,
                "lighter_top_bid_size": decimal_to_str(top_bid_size),
                "lighter_top_ask_size": decimal_to_str(top_ask_size),
                "lighter_depth_enough": depth_enough,
                "lighter_order_book_age_seconds": f"{lighter_book_age_seconds:.3f}"
                if lighter_book_age_seconds is not None
                else None,
                "planned_notional_usd": decimal_to_str(self.paper_notional_usd),
                "planned_qty": decimal_to_str(planned_qty),
            },
        )

    async def maybe_close_paper_position(self, snapshot: CrossSpreadSnapshot) -> None:
        position = self.paper_position
        if position is None:
            return
        holding_seconds = time.monotonic() - position.entered_at_monotonic
        exit_reason: str | None = None
        current_pct: Decimal | None = None
        median_pct: Decimal | None = None
        current_deviation_bps: Decimal | None = None

        if holding_seconds >= self.paper_max_holding_seconds:
            exit_reason = "timeout_exit"
        else:
            current_pct, median_pct, _ = paper_direction_values(snapshot, position.direction)
            if current_pct is None or median_pct is None:
                return
            current_deviation_bps = decimal_percent_to_bps(current_pct - position.entry_median_pct)
            if current_deviation_bps <= self.paper_exit_deviation_bps:
                exit_reason = "spread_reverted"
        if exit_reason is None:
            return

        exit_var_execution_price, exit_lighter_execution_price = paper_exit_execution_prices(snapshot, position.direction)
        if current_pct is not None:
            signal_spread_pnl_usd = ((position.entry_spread_pct - current_pct) / Decimal("100")) * position.planned_notional_usd
        else:
            signal_spread_pnl_usd = None
        exit_var_spread_cost_usd = paper_var_spread_cost_usd(snapshot, position.planned_qty)
        exit_lighter_taker_cost_usd = paper_lighter_taker_cost_usd(snapshot, position.planned_qty)
        exit_fee_usd = paper_fee_cost_usd(position.planned_notional_usd, self.paper_fee_bps_per_leg)
        exit_latency_drift_cost_usd = paper_latency_drift_cost_usd(position.planned_notional_usd, self.paper_latency_drift_bps)
        exit_top_bid_size, exit_top_ask_size = await self.get_lighter_top_sizes()
        if position.direction == "long_var_short_lighter":
            exit_lighter_depth_enough = exit_top_ask_size is not None and exit_top_ask_size >= position.planned_qty
        else:
            exit_lighter_depth_enough = exit_top_bid_size is not None and exit_top_bid_size >= position.planned_qty
        lighter_book_age_seconds = self._lighter_order_book_age_seconds()
        if position.direction == "long_var_short_lighter":
            var_leg_pnl_usd = (exit_var_execution_price - position.entry_var_execution_price) * position.planned_qty
            lighter_leg_pnl_usd = (position.entry_lighter_execution_price - exit_lighter_execution_price) * position.planned_qty
        else:
            var_leg_pnl_usd = (position.entry_var_execution_price - exit_var_execution_price) * position.planned_qty
            lighter_leg_pnl_usd = (exit_lighter_execution_price - position.entry_lighter_execution_price) * position.planned_qty
        gross_pair_pnl_usd = var_leg_pnl_usd + lighter_leg_pnl_usd
        fees_usd = position.entry_fee_usd + exit_fee_usd
        latency_drift_cost_usd = position.entry_latency_drift_cost_usd + exit_latency_drift_cost_usd
        net_pnl = gross_pair_pnl_usd - fees_usd - latency_drift_cost_usd
        now_iso = utc_now()
        self.paper_position = None
        self.paper_last_closed_monotonic = time.monotonic()
        await self.append_opportunity_log(
            "paper_closed",
            {
                "record_kind": "paper_opportunity",
                "opportunity_id": position.opportunity_id,
                "opportunity_type": "mixed",
                "execution_mode": "paper",
                "asset": position.asset,
                "direction": position.direction,
                "status": "paper_closed",
                "entry_time": position.entered_at_iso,
                "exit_time": now_iso,
                "exit_reason": exit_reason,
                "holding_seconds": f"{holding_seconds:.3f}",
                "exit_var_bid": decimal_to_str(snapshot.var_bid),
                "exit_var_ask": decimal_to_str(snapshot.var_ask),
                "exit_var_buy_price": decimal_to_str(snapshot.var_buy_price),
                "exit_var_sell_price": decimal_to_str(snapshot.var_sell_price),
                "exit_var_mid": decimal_to_str(snapshot.var_mid),
                "exit_var_execution_price": decimal_to_str(exit_var_execution_price),
                "exit_var_full_spread_bps": decimal_to_str(snapshot.var_full_spread_bps),
                "exit_var_half_spread_bps": decimal_to_str(snapshot.var_half_spread_bps),
                "exit_var_spread_source": snapshot.var_spread_source,
                "entry_var_spread_cost_usd": decimal_to_str(position.entry_var_spread_cost_usd),
                "exit_var_spread_cost_usd": decimal_to_str(exit_var_spread_cost_usd),
                "exit_lighter_bid": decimal_to_str(snapshot.lighter_bid),
                "exit_lighter_ask": decimal_to_str(snapshot.lighter_ask),
                "exit_lighter_mid": decimal_to_str(snapshot.lighter_mid),
                "exit_lighter_buy_fill_price": decimal_to_str(snapshot.lighter_buy_fill_price),
                "exit_lighter_sell_fill_price": decimal_to_str(snapshot.lighter_sell_fill_price),
                "exit_lighter_execution_price": decimal_to_str(exit_lighter_execution_price),
                "entry_lighter_taker_cost_usd": decimal_to_str(position.entry_lighter_taker_cost_usd),
                "exit_lighter_taker_cost_usd": decimal_to_str(exit_lighter_taker_cost_usd),
                "gross_lighter_taker_cost_usd": decimal_to_str(position.entry_lighter_taker_cost_usd + exit_lighter_taker_cost_usd),
                "entry_fee_usd": decimal_to_str(position.entry_fee_usd),
                "exit_fee_usd": decimal_to_str(exit_fee_usd),
                "entry_latency_drift_cost_usd": decimal_to_str(position.entry_latency_drift_cost_usd),
                "exit_latency_drift_cost_usd": decimal_to_str(exit_latency_drift_cost_usd),
                "gross_latency_drift_cost_usd": decimal_to_str(latency_drift_cost_usd),
                "exit_long_var_short_lighter_bps": decimal_to_str(
                    decimal_percent_to_bps(snapshot.long_var_short_lighter_pct)
                ),
                "exit_short_var_long_lighter_bps": decimal_to_str(
                    decimal_percent_to_bps(snapshot.short_var_long_lighter_pct)
                ),
                "entry_cross_exchange_spread_bps": decimal_to_str(decimal_percent_to_bps(position.entry_spread_pct)),
                "exit_cross_exchange_spread_bps": decimal_to_str(decimal_percent_to_bps(current_pct)),
                "entry_spread_median_bps": decimal_to_str(decimal_percent_to_bps(position.entry_median_pct)),
                "exit_spread_median_bps": decimal_to_str(decimal_percent_to_bps(median_pct)),
                "entry_spread_deviation_bps": decimal_to_str(position.entry_deviation_bps),
                "exit_spread_deviation_bps": decimal_to_str(current_deviation_bps),
                "exit_deviation_threshold_bps": decimal_to_str(self.paper_exit_deviation_bps),
                "exit_lighter_top_bid_size": decimal_to_str(exit_top_bid_size),
                "exit_lighter_top_ask_size": decimal_to_str(exit_top_ask_size),
                "exit_lighter_depth_enough": exit_lighter_depth_enough,
                "exit_lighter_order_book_age_seconds": f"{lighter_book_age_seconds:.3f}"
                if lighter_book_age_seconds is not None
                else None,
                "planned_notional_usd": decimal_to_str(position.planned_notional_usd),
                "planned_qty": decimal_to_str(position.planned_qty),
                "signal_spread_pnl_usd": decimal_to_str(signal_spread_pnl_usd),
                "var_leg_pnl_usd": decimal_to_str(var_leg_pnl_usd),
                "lighter_leg_pnl_usd": decimal_to_str(lighter_leg_pnl_usd),
                "gross_pair_pnl_usd": decimal_to_str(gross_pair_pnl_usd),
                "entry_spread_pnl_usd": decimal_to_str(gross_pair_pnl_usd),
                "gross_var_spread_cost_usd": decimal_to_str(position.entry_var_spread_cost_usd + exit_var_spread_cost_usd),
                "fees_usd": decimal_to_str(fees_usd),
                "latency_drift_cost_usd": decimal_to_str(latency_drift_cost_usd),
                "net_pnl_conservative_usd": decimal_to_str(net_pnl),
                "final_status": "closed",
            },
        )

    async def maybe_run_paper_inventory(self, snapshot: CrossSpreadSnapshot) -> None:
        engine = self.paper_inventory_engine
        if engine is None:
            return
        self.paper_inventory_sample_index += 1
        index = self.paper_inventory_sample_index
        directions = (
            (
                DIRECTION_LONG_VAR_SHORT_LIGHTER,
                decimal_percent_to_bps(snapshot.long_var_short_lighter_pct),
                snapshot.var_buy_price or snapshot.var_mid,
                snapshot.lighter_sell_fill_price,
                snapshot.var_sell_price or snapshot.var_mid,
                snapshot.lighter_buy_fill_price,
            ),
            (
                DIRECTION_SHORT_VAR_LONG_LIGHTER,
                decimal_percent_to_bps(snapshot.short_var_long_lighter_pct),
                snapshot.var_sell_price or snapshot.var_mid,
                snapshot.lighter_buy_fill_price,
                snapshot.var_buy_price or snapshot.var_mid,
                snapshot.lighter_sell_fill_price,
            ),
        )
        for direction, edge_bps, var_entry, lighter_entry, var_exit, lighter_exit in directions:
            if edge_bps is None:
                continue
            events = engine.on_sample(
                direction=direction,
                edge_bps=edge_bps,
                var_entry_price=var_entry,
                lighter_entry_price=lighter_entry,
                var_exit_price=var_exit,
                lighter_exit_price=lighter_exit,
                logged_at=utc_now(),
                sample_index=index,
            )
            for event in events:
                await self.append_inventory_paper_log(
                    {
                        "event": event.event,
                        "asset": snapshot.asset,
                        "direction": event.direction,
                        "lot_id": event.lot_id,
                        "qty": decimal_to_str(event.qty),
                        "edge_bps": decimal_to_str(event.edge_bps),
                        "var_price": decimal_to_str(event.var_price),
                        "lighter_price": decimal_to_str(event.lighter_price),
                        "pnl_usd": decimal_to_str(event.pnl_usd),
                        "pnl_bps": decimal_to_str(event.pnl_bps),
                        "holding_samples": event.holding_samples,
                        "open_lots_total": engine.open_lots(),
                        "open_lots_direction": engine.open_lots(event.direction),
                        "realized_pnl_usd": decimal_to_str(engine.realized_pnl_usd),
                    }
                )

    async def maybe_run_live_inventory_basis(self, snapshot: CrossSpreadSnapshot) -> None:
        asset = snapshot.asset.upper()
        if asset != "ETH":
            return
        self.live_inventory_sample_index += 1
        index = self.live_inventory_sample_index
        if self.live_inventory_completed_cycles >= self.live_inventory_max_cycles and not self.live_inventory_open_lots:
            return
        quote, quote_ms = await self.fetch_live_inventory_basis_quote(asset=asset)
        if quote is None:
            return
        var_bid = to_decimal(quote.get("bid"))
        var_ask = to_decimal(quote.get("ask"))
        if var_bid is None or var_ask is None:
            return
        lighter_buy_price = snapshot.lighter_buy_fill_price or snapshot.lighter_ask
        lighter_sell_price = snapshot.lighter_sell_fill_price or snapshot.lighter_bid
        basis_mid = (var_bid + var_ask) / Decimal("2")
        lighter_mid = (snapshot.lighter_bid + snapshot.lighter_ask) / Decimal("2")
        if basis_mid <= 0 or lighter_mid <= 0:
            return
        basis_bps = (basis_mid - lighter_mid) / lighter_mid * Decimal("10000")
        z_float, warm = self.live_inventory_basis_state.update(time.monotonic(), float(basis_bps))
        z = Decimal(str(z_float))
        long_edge_bps = self.live_inventory_pair_edge_bps(
            direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
            var_price=var_ask,
            lighter_price=lighter_sell_price,
        ) or Decimal("0")
        short_edge_bps = self.live_inventory_pair_edge_bps(
            direction=DIRECTION_SHORT_VAR_LONG_LIGHTER,
            var_price=var_bid,
            lighter_price=lighter_buy_price,
        ) or Decimal("0")
        long_roundtrip_pnl_bps = self.live_inventory_roundtrip_pnl_bps(
            direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
            var_entry_price=var_ask,
            lighter_entry_price=lighter_sell_price,
            var_exit_price=var_bid,
            lighter_exit_price=lighter_buy_price,
        )
        short_roundtrip_pnl_bps = self.live_inventory_roundtrip_pnl_bps(
            direction=DIRECTION_SHORT_VAR_LONG_LIGHTER,
            var_entry_price=var_bid,
            lighter_entry_price=lighter_buy_price,
            var_exit_price=var_ask,
            lighter_exit_price=lighter_sell_price,
        )
        state_payload = {
            "asset": asset,
            "sample_index": index,
            "quote_id": quote.get("quoteId") or quote.get("quote_id"),
            "quote_timestamp": quote.get("quoteTimestamp") or quote.get("quote_timestamp"),
            "quote_ms": decimal_to_str(quote_ms),
            "var_bid": decimal_to_str(var_bid),
            "var_ask": decimal_to_str(var_ask),
            "lighter_bid": decimal_to_str(snapshot.lighter_bid),
            "lighter_ask": decimal_to_str(snapshot.lighter_ask),
            "lighter_buy_price": decimal_to_str(lighter_buy_price),
            "lighter_sell_price": decimal_to_str(lighter_sell_price),
            "basis_bps": decimal_to_str(basis_bps),
            "basis_mean_bps": None if self.live_inventory_basis_state.signal_mean is None else str(self.live_inventory_basis_state.signal_mean),
            "basis_sigma_bps": None if self.live_inventory_basis_state.signal_sigma is None else str(self.live_inventory_basis_state.signal_sigma),
            "basis_seen": self.live_inventory_basis_state.seen,
            "z": decimal_to_str(z),
            "warm": warm,
            "long_edge_bps": decimal_to_str(long_edge_bps),
            "short_edge_bps": decimal_to_str(short_edge_bps),
            "long_roundtrip_pnl_bps": decimal_to_str(long_roundtrip_pnl_bps),
            "short_roundtrip_pnl_bps": decimal_to_str(short_roundtrip_pnl_bps),
            "open_lots_total": len(self.live_inventory_open_lots),
            "realized_pnl_usd": decimal_to_str(self.live_inventory_realized_pnl_usd),
            "completed_cycles": self.live_inventory_completed_cycles,
        }
        await self.append_live_inventory_log("live_inventory_basis_state", state_payload)
        addon_direction: str | None = None
        if (
            self.live_inventory_open_lots
            and self.live_inventory_i_accept_basis_addon_diagnostic
            and len(self.live_inventory_open_lots) < self.live_inventory_max_total_lots
            and warm
        ):
            open_directions = {str(lot.get("direction") or "") for lot in self.live_inventory_open_lots}
            if len(open_directions) == 1:
                existing_direction = next(iter(open_directions))
                entry_basis_values = [
                    value
                    for lot in self.live_inventory_open_lots
                    if (value := to_decimal(lot.get("entry_basis_bps"))) is not None
                ]
                if existing_direction == DIRECTION_LONG_VAR_SHORT_LIGHTER and entry_basis_values:
                    addon_threshold = min(entry_basis_values) - self.live_inventory_basis_addon_min_basis_improvement_bps
                    if basis_bps <= addon_threshold:
                        addon_direction = existing_direction
                elif existing_direction == DIRECTION_SHORT_VAR_LONG_LIGHTER and entry_basis_values:
                    addon_threshold = max(entry_basis_values) + self.live_inventory_basis_addon_min_basis_improvement_bps
                    if basis_bps >= addon_threshold:
                        addon_direction = existing_direction
        if not self.live_inventory_open_lots or addon_direction is not None:
            if self.has_pending_live_inventory_var_fill_match(asset=asset, roles={"live_inventory_entry_pending_lighter"}):
                if await self.maybe_timeout_pending_live_inventory_var_entry(asset=asset):
                    return
                await self.append_live_inventory_log(
                    "live_inventory_entry_blocked",
                    {**state_payload, "reason": "basis_var_entry_pending_fill"},
                )
                return
            if not warm:
                return
            candidates = (
                (DIRECTION_LONG_VAR_SHORT_LIGHTER, long_edge_bps, long_roundtrip_pnl_bps, var_ask, lighter_sell_price),
                (DIRECTION_SHORT_VAR_LONG_LIGHTER, short_edge_bps, short_roundtrip_pnl_bps, var_bid, lighter_buy_price),
            )
            for direction, edge_bps, roundtrip_bps, var_price, lighter_price in candidates:
                if addon_direction is not None and direction != addon_direction:
                    continue
                direction_signal = self.live_inventory_basis_direction_signal(direction, z)
                if direction_signal < self.live_inventory_basis_z_entry:
                    continue
                if not self.live_inventory_basis_abs_entry_ok(direction=direction, basis_bps=basis_bps):
                    await self.append_live_inventory_log(
                        "live_inventory_entry_blocked",
                        {
                            **state_payload,
                            "reason": "basis_abs_entry_threshold_not_met",
                            "direction": direction,
                            "basis_bps": decimal_to_str(basis_bps),
                            "min_abs_entry_bps": decimal_to_str(self.live_inventory_basis_min_abs_entry_bps),
                        },
                    )
                    continue
                if edge_bps < self.live_inventory_basis_min_entry_edge_bps:
                    continue
                if roundtrip_bps < -self.live_inventory_basis_max_entry_roundtrip_cost_bps:
                    continue
                qty = self.live_inventory_lot_notional_usd / var_price
                lot_id = self.live_inventory_next_lot_id
                var_side = self._auto_live_direction_to_var_side(direction)
                reject_cooldown_key = (asset.upper(), var_side.lower())
                reject_cooldown_until = self.live_inventory_var_reject_cooldown_until.get(reject_cooldown_key)
                if reject_cooldown_until is not None:
                    reject_cooldown_remaining = reject_cooldown_until - time.monotonic()
                    if reject_cooldown_remaining > 0:
                        await self.block_live_inventory_entry(
                            asset=asset,
                            reason="variational_taker_funding_reject_cooldown_active",
                            context={
                                "action": "entry",
                                "direction": direction,
                                "var_side": var_side,
                                "cooldown_remaining_seconds": reject_cooldown_remaining,
                                "cooldown_seconds": self.live_inventory_var_reject_cooldown_seconds,
                                "clearing_status": "rejected_failed_taker_funding",
                            },
                        )
                        return
                    self.live_inventory_var_reject_cooldown_until.pop(reject_cooldown_key, None)
                var_submit_ms = None
                lighter_submit_ms = None
                var_result = None
                lighter_record = None
                lighter_payload = None
                if not self.live_inventory_dry_decisions:
                    preflight_ok, preflight_reason, preflight_context = await self.live_inventory_entry_preflight(
                        asset=asset,
                        direction=direction,
                        var_side=var_side,
                        qty=qty,
                        var_price=var_price,
                        lighter_price=lighter_price,
                        edge_bps=edge_bps,
                        var_spread_bps=(var_ask - var_bid) / ((var_ask + var_bid) / Decimal("2")) * Decimal("10000"),
                        var_snapshot_timestamp=quote.get("quoteTimestamp") or quote.get("quote_timestamp"),
                        min_entry_bps=self.live_inventory_basis_min_entry_edge_bps,
                        dynamic_entry_buffer_bps=Decimal("0"),
                    )
                    if not preflight_ok:
                        await self.block_live_inventory_entry(asset=asset, reason=preflight_reason, context=preflight_context)
                        return
                    var_amount = variational_api_amount_to_str(qty, asset=asset)
                    submitted_qty = Decimal(var_amount)
                    self.add_pending_live_inventory_var_fill_match(
                        PendingLiveInventoryVarFillMatch(
                            asset=asset,
                            side=var_side.lower(),
                            qty=submitted_qty,
                            lot_id=lot_id,
                            role="live_inventory_entry_pending_lighter",
                            created_at_monotonic=time.monotonic(),
                            context={
                                "signal_mode": LIVE_INVENTORY_SIGNAL_BASIS,
                                "sample_index": index,
                                "direction": direction,
                                "qty": decimal_to_str(qty),
                                "var_side": var_side,
                                "var_price": decimal_to_str(var_price),
                                "lighter_price": decimal_to_str(lighter_price),
                                "edge_bps": decimal_to_str(edge_bps),
                                "roundtrip_pnl_bps": decimal_to_str(roundtrip_bps),
                                "basis_bps": decimal_to_str(basis_bps),
                                "z": decimal_to_str(z),
                                "direction_signal": decimal_to_str(direction_signal),
                                "quote_id": quote.get("quoteId") or quote.get("quote_id"),
                                "signal_quote_id": quote.get("quoteId") or quote.get("quote_id"),
                                "entry_order_reuses_signal_quote": False,
                                "quote_timestamp": quote.get("quoteTimestamp") or quote.get("quote_timestamp"),
                                "var_bid": decimal_to_str(var_bid),
                                "var_ask": decimal_to_str(var_ask),
                                "entry_kind": "basis_addon" if addon_direction is not None else "basis_initial",
                            },
                        )
                    )
                    try:
                        var_result, var_submit_ms = await self._timed_submit(
                            self.send_variational_place_order(
                                asset=asset,
                                side=var_side,
                                amount=var_amount,
                                expected_min_btc_qty=None,
                                confirm=True,
                                reduce_only=False,
                                reuse_quote_id=None,
                            )
                        )
                    except Exception as exc:
                        self.remove_pending_live_inventory_var_fill_match(asset=asset, lot_id=lot_id, role="live_inventory_entry_pending_lighter")
                        await self.require_live_inventory_manual_review(
                            asset=asset,
                            reason=f"basis_entry_var_submit_exception:{exc}",
                            context={"action": "entry", "direction": direction, "qty": decimal_to_str(qty), "var_amount": var_amount},
                        )
                        return
                    if not var_result.get("ok"):
                        self.remove_pending_live_inventory_var_fill_match(asset=asset, lot_id=lot_id, role="live_inventory_entry_pending_lighter")
                        await self.require_live_inventory_manual_review(
                            asset=asset,
                            reason=f"basis_entry_var_submit_failed:{var_result.get('error') or 'unknown'}",
                            context={"action": "entry", "direction": direction, "qty": decimal_to_str(qty), "var_amount": var_amount, "var_result": var_result},
                        )
                        return
                    pending_match = next(
                        (
                            item
                            for item in self.pending_live_inventory_var_fill_matches
                            if item.asset == asset and item.lot_id == lot_id and item.role == "live_inventory_entry_pending_lighter"
                        ),
                        None,
                    )
                    if pending_match is not None:
                        var_payload = var_result.get("result") if isinstance(var_result.get("result"), dict) else var_result
                        pending_match.context = {
                            **(pending_match.context or {}),
                            "var_submit_ms": var_submit_ms,
                            "var_result": var_result,
                            "rfq_id": var_payload.get("rfqId") or var_payload.get("rfq_id"),
                            "submitted_order_id": var_payload.get("orderId") or var_payload.get("order_id"),
                        }
                    self.live_inventory_next_lot_id += 1
                    await self.persist_live_inventory_memory(reason="basis_var_entry_submitted_pending_fill")
                    await self.append_live_inventory_log(
                        "live_inventory_var_entry_submitted",
                        {**state_payload, "lot_id": lot_id, "direction": direction, "qty": decimal_to_str(submitted_qty), "edge_bps": decimal_to_str(edge_bps), "roundtrip_pnl_bps": decimal_to_str(roundtrip_bps), "var_submit_ms": var_submit_ms, "entry_confirmation_mode": "wait_for_var_fill_before_lighter", "var_result": var_result},
                    )
                    return
                lot = {
                    "lot_id": lot_id,
                    "signal_mode": LIVE_INVENTORY_SIGNAL_BASIS,
                    "direction": direction,
                    "qty": decimal_to_str(qty),
                    "entry_var_fill_price": decimal_to_str(var_price),
                    "entry_lighter_fill_price": decimal_to_str(lighter_price),
                    "entry_var_price_source": "fresh_quote",
                    "entry_lighter_price_source": "estimated_snapshot",
                    "entry_cost_status": "dry_decision" if self.live_inventory_dry_decisions else "final_fills_pending",
                    "entry_edge_bps": decimal_to_str(edge_bps),
                    "entry_roundtrip_pnl_bps": decimal_to_str(roundtrip_bps),
                    "entry_basis_bps": decimal_to_str(basis_bps),
                    "entry_z": decimal_to_str(z),
                    "entry_direction_signal": decimal_to_str(direction_signal),
                    "entered_at": utc_now(),
                    "entered_sample_index": index,
                    "entry_var_side": var_side,
                    "entry_var_order_quote_id": quote.get("quoteId") or quote.get("quote_id"),
                    "entry_var_order_quote_bid": decimal_to_str(var_bid),
                    "entry_var_order_quote_ask": decimal_to_str(var_ask),
                    "entry_var_order_quote_timestamp": quote.get("quoteTimestamp") or quote.get("quote_timestamp"),
                    "entry_var_order_quote_execution_price": decimal_to_str(var_price),
                    "entry_var_submit_ms": var_submit_ms,
                    "entry_lighter_submit_ms": lighter_submit_ms,
                    "entry_var_result": var_result,
                    "entry_lighter_record_key": lighter_record.trade_key if lighter_record is not None else None,
                    "entry_lighter_payload": lighter_payload,
                    "entry_kind": "basis_addon" if addon_direction is not None else "basis_initial",
                    "status": "dry_open" if self.live_inventory_dry_decisions else "open",
                }
                self.live_inventory_next_lot_id += 1
                self.live_inventory_open_lots.append(lot)
                if not self.live_inventory_dry_decisions:
                    self.remember_live_inventory_final_pnl_lot(asset=asset, lot=lot)
                    self.sync_live_inventory_open_lot_entry_cost(asset=asset, lot_id=lot_id)
                await self.persist_live_inventory_memory(reason="basis_dry_entry_decision" if self.live_inventory_dry_decisions else "basis_entry_submitted")
                await self.append_live_inventory_log(
                    "live_inventory_dry_entered" if self.live_inventory_dry_decisions else "live_inventory_entered",
                    {**state_payload, "lot_id": lot_id, "direction": direction, "qty": lot["qty"], "edge_bps": decimal_to_str(edge_bps), "roundtrip_pnl_bps": decimal_to_str(roundtrip_bps), "var_submit_ms": var_submit_ms, "lighter_submit_ms": lighter_submit_ms, "entry_kind": lot["entry_kind"]},
                )
                return
            if not self.live_inventory_open_lots:
                return
        selected_exit: dict[str, Any] | None = None
        for lot_index, candidate_lot in enumerate(self.live_inventory_open_lots):
            direction = str(candidate_lot.get("direction") or "")
            entered_sample_index = int(candidate_lot.get("entered_sample_index") or index)
            holding_samples = index - entered_sample_index
            if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
                var_exit_price = var_bid
                lighter_exit_price = lighter_buy_price
            elif direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
                var_exit_price = var_ask
                lighter_exit_price = lighter_sell_price
            else:
                await self.require_live_inventory_manual_review(asset=asset, reason=f"unknown_direction:{direction}")
                return
            entry_var_price = to_decimal(candidate_lot.get("entry_var_fill_price")) or var_exit_price
            entry_lighter_price = to_decimal(candidate_lot.get("entry_lighter_fill_price")) or lighter_exit_price
            qty = to_decimal(candidate_lot.get("qty")) or Decimal("0")
            _, _, pnl = self.live_inventory_pair_pnl(
                direction=direction,
                qty=qty,
                entry_var_price=entry_var_price,
                entry_lighter_price=entry_lighter_price,
                exit_var_price=var_exit_price,
                exit_lighter_price=lighter_exit_price,
            )
            notional = qty * entry_var_price
            pnl_bps = pnl / notional * Decimal("10000") if notional else None
            direction_signal = self.live_inventory_basis_direction_signal(direction, z)
            can_exit_on_reversion = holding_samples >= self.live_inventory_min_hold_samples
            effective_min_exit_pnl_bps = self.live_inventory_basis_min_exit_pnl_bps + self.live_inventory_basis_exit_safety_buffer_bps
            should_exit = can_exit_on_reversion and direction_signal <= self.live_inventory_basis_z_exit and (pnl_bps is not None and pnl_bps >= effective_min_exit_pnl_bps)
            should_stop = pnl_bps is not None and pnl_bps <= -self.live_inventory_max_unrealized_loss_bps
            should_timeout = holding_samples >= self.live_inventory_max_hold_samples
            should_timeout_exit = should_timeout and self.live_inventory_basis_max_hold_action == "exit"
            if should_timeout and not should_timeout_exit:
                last_warned = int(candidate_lot.get("max_hold_warned_samples") or 0)
                if holding_samples > last_warned:
                    candidate_lot["max_hold_warned_samples"] = holding_samples
                    await self.append_live_inventory_log(
                        "live_inventory_exit_blocked",
                        {
                            **state_payload,
                            "lot_id": candidate_lot.get("lot_id"),
                            "direction": direction,
                            "reason": "basis_max_hold_reached_waiting_for_reversion",
                            "holding_samples": holding_samples,
                            "max_hold_samples": self.live_inventory_max_hold_samples,
                            "basis_max_hold_action": self.live_inventory_basis_max_hold_action,
                            "direction_signal": decimal_to_str(direction_signal),
                            "pnl_bps": decimal_to_str(pnl_bps),
                            "min_exit_pnl_bps": decimal_to_str(self.live_inventory_basis_min_exit_pnl_bps),
                            "exit_safety_buffer_bps": decimal_to_str(self.live_inventory_basis_exit_safety_buffer_bps),
                            "effective_min_exit_pnl_bps": decimal_to_str(effective_min_exit_pnl_bps),
                        },
                    )
            if should_exit or should_stop or should_timeout_exit:
                selected_exit = {
                    "lot_index": lot_index,
                    "lot": candidate_lot,
                    "direction": direction,
                    "holding_samples": holding_samples,
                    "var_exit_price": var_exit_price,
                    "lighter_exit_price": lighter_exit_price,
                    "entry_var_price": entry_var_price,
                    "entry_lighter_price": entry_lighter_price,
                    "qty": qty,
                    "pnl": pnl,
                    "pnl_bps": pnl_bps,
                    "should_exit": should_exit,
                    "should_stop": should_stop,
                    "should_timeout": should_timeout,
                    "should_timeout_exit": should_timeout_exit,
                    "effective_min_exit_pnl_bps": effective_min_exit_pnl_bps,
                }
                break
        if selected_exit is None:
            return
        lot_index = int(selected_exit["lot_index"])
        lot = selected_exit["lot"]
        direction = str(selected_exit["direction"])
        holding_samples = int(selected_exit["holding_samples"])
        var_exit_price = selected_exit["var_exit_price"]
        lighter_exit_price = selected_exit["lighter_exit_price"]
        entry_var_price = selected_exit["entry_var_price"]
        entry_lighter_price = selected_exit["entry_lighter_price"]
        qty = selected_exit["qty"]
        pnl = selected_exit["pnl"]
        pnl_bps = selected_exit["pnl_bps"]
        should_exit = bool(selected_exit["should_exit"])
        should_stop = bool(selected_exit["should_stop"])
        should_timeout = bool(selected_exit["should_timeout"])
        should_timeout_exit = bool(selected_exit["should_timeout_exit"])
        effective_min_exit_pnl_bps = selected_exit["effective_min_exit_pnl_bps"]
        exit_reason = "signal_reverted" if should_exit else "max_unrealized_loss_bps" if should_stop else "max_hold_samples"
        var_submit_ms = None
        lighter_submit_ms = None
        lighter_payload = None
        exit_var_order_quote: dict[str, Any] = {}
        if not self.live_inventory_dry_decisions:
            if should_exit and not should_timeout and not self.live_inventory_entry_cost_confirmed(lot):
                await self.append_live_inventory_log(
                    "live_inventory_exit_blocked",
                    {
                        **state_payload,
                        "lot_id": lot.get("lot_id"),
                        "direction": direction,
                        "reason": "entry_final_fill_cost_pending",
                        "entry_cost_status": lot.get("entry_cost_status"),
                        "holding_samples": holding_samples,
                        "pnl_bps": decimal_to_str(pnl_bps),
                    },
                )
                return
            exit_side = self._opposite_var_side(str(lot.get("entry_var_side") or self._auto_live_direction_to_var_side(direction)))
            var_amount = variational_api_amount_to_str(qty, asset=asset)
            self.add_pending_live_inventory_var_fill_match(
                PendingLiveInventoryVarFillMatch(
                    asset=asset,
                    side=exit_side.lower(),
                    qty=Decimal(var_amount),
                    lot_id=int(lot.get("lot_id") or 0),
                    role="live_inventory_exit",
                    created_at_monotonic=time.monotonic(),
                )
            )
            try:
                var_result, var_submit_ms = await self._timed_submit(
                    self.send_variational_place_order(
                        asset=asset,
                        side=exit_side,
                        amount=var_amount,
                        expected_min_btc_qty=None,
                        confirm=True,
                        reduce_only=True,
                    )
                )
            except Exception as exc:
                await self.require_live_inventory_manual_review(
                    asset=asset,
                    reason=f"basis_exit_submit_exception:{exc}",
                    context={"action": "exit", "lot_id": lot.get("lot_id"), "direction": direction, "qty": decimal_to_str(qty), "var_amount": var_amount},
                )
                return
            if not var_result.get("ok"):
                self.remove_pending_live_inventory_var_fill_match(asset=asset, lot_id=lot.get("lot_id"), role="live_inventory_exit")
                var_error = var_result.get("error") or "unknown"
                positions_result = None
                position_qty = None
                if self.variational_error_is_no_position(var_error):
                    with contextlib.suppress(Exception):
                        positions_result = await self.fetch_variational_positions()
                        position_qty = self.extract_variational_position_qty(positions_result, asset=asset)
                if self.variational_error_is_no_position(var_error) and position_qty == 0:
                    await self.append_live_inventory_log(
                        "live_inventory_var_exit_reconciled_flat",
                        {
                            **state_payload,
                            "lot_id": lot.get("lot_id"),
                            "direction": direction,
                            "qty": decimal_to_str(qty),
                            "var_amount": var_amount,
                            "var_result": var_result,
                            "variational_position_qty": decimal_to_str(position_qty),
                            "variational_positions_result": positions_result,
                            "action": "continue_lighter_reduce_only_exit",
                        },
                    )
                else:
                    await self.require_live_inventory_manual_review(
                        asset=asset,
                        reason=f"basis_exit_var_submit_failed:{var_error}",
                        context={
                            "action": "exit",
                            "lot_id": lot.get("lot_id"),
                            "direction": direction,
                            "qty": decimal_to_str(qty),
                            "var_amount": var_amount,
                            "var_result": var_result,
                            "variational_position_qty": decimal_to_str(position_qty) if position_qty is not None else None,
                            "variational_positions_result": positions_result,
                        },
                    )
                    return
            else:
                exit_var_order_quote = self.variational_api_order_quote_fields(exit_side, var_result)
            try:
                (lighter_record, lighter_payload), lighter_submit_ms = await self._timed_submit(
                    self.place_lighter_order_from_plan(
                        asset=asset,
                        side=exit_side,
                        qty=qty,
                        var_fill_price=var_exit_price,
                        cycle_id=int(lot.get("lot_id") or 0),
                        role="live_inventory_exit",
                        reduce_only=True,
                    )
                )
            except Exception as exc:
                await self.require_live_inventory_manual_review(
                    asset=asset,
                    reason=f"basis_exit_lighter_submit_exception:{exc}",
                    context={"action": "exit", "lot_id": lot.get("lot_id"), "direction": direction, "qty": decimal_to_str(qty), "var_amount": var_amount},
                )
                return
            if lighter_record is None or not self.auto_live_eager_hedge_started(lighter_record):
                await self.require_live_inventory_manual_review(
                    asset=asset,
                    reason="basis_exit_lighter_submit_failed",
                    context={"action": "exit", "lot_id": lot.get("lot_id"), "direction": direction, "qty": decimal_to_str(qty), "lighter_payload": lighter_payload},
                )
                return
        self.live_inventory_open_lots.pop(lot_index)
        self.live_inventory_realized_pnl_usd += pnl
        self.live_inventory_completed_cycles += 1
        await self.persist_live_inventory_memory(reason="basis_dry_exit_decision" if self.live_inventory_dry_decisions else "basis_exit_submitted")
        actual_pnl_status = "dry_decision" if self.live_inventory_dry_decisions else "pending_lighter_final_fill"
        if not self.live_inventory_dry_decisions and lighter_payload:
            self.pending_live_inventory_actual_pnl[str(lighter_payload.get("trade_key") or "")] = {
                "asset": asset,
                "lot_id": lot.get("lot_id"),
                "direction": direction,
                "qty": decimal_to_str(qty),
                "entry_var_price": decimal_to_str(entry_var_price),
                "entry_lighter_price": decimal_to_str(entry_lighter_price),
                "exit_var_price": decimal_to_str(var_exit_price),
                "estimated_pnl_usd": decimal_to_str(pnl),
                "estimated_pnl_bps": decimal_to_str(pnl_bps),
            }
        await self.append_live_inventory_log(
            "live_inventory_dry_exited" if self.live_inventory_dry_decisions else "live_inventory_exited",
            {
                **state_payload,
                "lot_id": lot.get("lot_id"),
                "direction": direction,
                "qty": decimal_to_str(qty),
                "exit_reason": exit_reason,
                "holding_samples": holding_samples,
                "pnl_usd": decimal_to_str(pnl),
                "pnl_bps": decimal_to_str(pnl_bps),
                "min_exit_pnl_bps": decimal_to_str(self.live_inventory_basis_min_exit_pnl_bps),
                "exit_safety_buffer_bps": decimal_to_str(self.live_inventory_basis_exit_safety_buffer_bps),
                "effective_min_exit_pnl_bps": decimal_to_str(effective_min_exit_pnl_bps),
                "var_price": decimal_to_str(var_exit_price),
                "lighter_price": decimal_to_str(lighter_exit_price),
                "actual_pnl_status": actual_pnl_status,
                "var_submit_ms": var_submit_ms,
                "lighter_submit_ms": lighter_submit_ms,
                "exit_var_order_quote_id": exit_var_order_quote.get("quote_id"),
                "exit_var_order_quote_bid": exit_var_order_quote.get("quote_bid"),
                "exit_var_order_quote_ask": exit_var_order_quote.get("quote_ask"),
                "exit_var_order_quote_timestamp": exit_var_order_quote.get("quote_timestamp"),
                "exit_var_order_quote_execution_price": exit_var_order_quote.get("quote_execution_price"),
            },
        )

    async def maybe_run_live_inventory(self, snapshot: CrossSpreadSnapshot) -> None:
        if not self.is_live_inventory_enabled():
            return
        if getattr(self, "live_inventory_signal_mode", LIVE_INVENTORY_SIGNAL_SNAPSHOT) == LIVE_INVENTORY_SIGNAL_BASIS:
            await self.maybe_run_live_inventory_basis(snapshot)
            return
        if snapshot.asset.upper() != "BTC":
            return
        self.live_inventory_sample_index += 1
        index = self.live_inventory_sample_index
        event_prefix = "live_inventory_dry" if self.live_inventory_dry_decisions else "live_inventory"
        if not self.live_inventory_open_lots:
            if self.live_inventory_completed_cycles >= self.live_inventory_max_cycles:
                return
            directions = (
                (
                    DIRECTION_LONG_VAR_SHORT_LIGHTER,
                    decimal_percent_to_bps(snapshot.long_var_short_lighter_pct),
                    snapshot.var_buy_price or snapshot.var_mid,
                    snapshot.lighter_sell_fill_price,
                ),
                (
                    DIRECTION_SHORT_VAR_LONG_LIGHTER,
                    decimal_percent_to_bps(snapshot.short_var_long_lighter_pct),
                    snapshot.var_sell_price or snapshot.var_mid,
                    snapshot.lighter_buy_fill_price,
                ),
            )
            for direction, edge_bps, var_price, lighter_price in directions:
                if edge_bps is None or edge_bps < self.live_inventory_entry_bps:
                    continue
                if len(self.live_inventory_open_lots) >= self.live_inventory_max_total_lots:
                    return
                notional_price = var_price * (Decimal("1") + Decimal(str(HEDGE_SLIPPAGE_BPS)) / Decimal("10000"))
                qty = self.live_inventory_lot_notional_usd / notional_price
                lot_id = self.live_inventory_next_lot_id
                var_side = self._auto_live_direction_to_var_side(direction)
                initial_signal_edge_bps = edge_bps
                initial_snapshot_var_price = var_price
                refreshed_var_quote: dict[str, Any] = {}
                refreshed_var_quote_ms: str | None = None
                if not self.live_inventory_dry_decisions:
                    if self.live_inventory_refresh_var_quote_before_entry:
                        var_amount_for_quote = variational_api_amount_to_str(qty, asset=snapshot.asset)
                        quote_result, refreshed_var_quote_ms = await self._timed_submit(
                            self.send_variational_place_order(
                                asset=snapshot.asset,
                                side=var_side,
                                amount=var_amount_for_quote,
                                expected_min_btc_qty=Decimal(var_amount_for_quote)
                                if snapshot.asset.upper() == "BTC"
                                else None,
                                confirm=False,
                                reduce_only=False,
                            )
                        )
                        if not quote_result.get("ok"):
                            await self.block_live_inventory_entry(
                                asset=snapshot.asset,
                                reason=f"variational_fresh_quote_failed:{quote_result.get('error') or 'unknown'}",
                                context={"direction": direction, "var_side": var_side, "qty": decimal_to_str(qty)},
                            )
                            return
                        refreshed_var_quote = self.variational_api_order_quote_fields(var_side, quote_result)
                        refreshed_var_price = to_decimal(refreshed_var_quote.get("quote_execution_price"))
                        if refreshed_var_price is None:
                            await self.block_live_inventory_entry(
                                asset=snapshot.asset,
                                reason="variational_fresh_quote_missing_execution_price",
                                context={"direction": direction, "var_side": var_side, "qty": decimal_to_str(qty)},
                            )
                            return
                        var_price = refreshed_var_price
                        edge_bps = self.live_inventory_pair_edge_bps(
                            direction=direction,
                            var_price=var_price,
                            lighter_price=lighter_price,
                        )
                    preflight_ok, preflight_reason, preflight_context = await self.live_inventory_entry_preflight(
                        asset=snapshot.asset,
                        direction=direction,
                        var_side=var_side,
                        qty=qty,
                        var_price=var_price,
                        lighter_price=lighter_price,
                        edge_bps=edge_bps,
                        var_spread_bps=snapshot.var_full_spread_bps or snapshot.var_half_spread_bps * Decimal("2"),
                        var_snapshot_timestamp=refreshed_var_quote.get("quote_timestamp") or snapshot.var_timestamp,
                    )
                    if not preflight_ok:
                        await self.block_live_inventory_entry(
                            asset=snapshot.asset,
                            reason=preflight_reason,
                            context=preflight_context,
                        )
                        return
                    var_amount = variational_api_amount_to_str(qty, asset=snapshot.asset)
                    self.add_pending_live_inventory_var_fill_match(
                        PendingLiveInventoryVarFillMatch(
                            asset=snapshot.asset,
                            side=var_side.lower(),
                            qty=Decimal(var_amount),
                            lot_id=lot_id,
                            role="live_inventory_entry",
                            created_at_monotonic=time.monotonic(),
                        )
                    )
                    try:
                        var_task = asyncio.create_task(
                            self._timed_submit(
                                self.send_variational_place_order(
                                    asset=snapshot.asset,
                                    side=var_side,
                                    amount=var_amount,
                                    expected_min_btc_qty=Decimal(var_amount) if snapshot.asset.upper() == "BTC" else None,
                                    confirm=True,
                                    reduce_only=False,
                                    reuse_quote_id=refreshed_var_quote.get("quote_id"),
                                )
                            )
                        )
                        lighter_task = asyncio.create_task(
                            self._timed_submit(
                                self.place_lighter_order_from_plan(
                                    asset=snapshot.asset,
                                    side=var_side,
                                    qty=qty,
                                    var_fill_price=var_price,
                                    cycle_id=self.live_inventory_next_lot_id,
                                    role="live_inventory_entry",
                                )
                            )
                        )
                        var_outcome, lighter_outcome = await asyncio.gather(
                            var_task,
                            lighter_task,
                            return_exceptions=True,
                        )
                    except Exception as exc:
                        await self.require_live_inventory_manual_review(
                            asset=snapshot.asset,
                            reason=f"entry_submit_exception:{exc}",
                            context={
                                "action": "entry",
                                "direction": direction,
                                "qty": decimal_to_str(qty),
                                "var_amount": var_amount,
                                "var_side": var_side,
                            },
                        )
                        return
                    if isinstance(var_outcome, Exception):
                        self.remove_pending_live_inventory_var_fill_match(
                            asset=snapshot.asset,
                            lot_id=lot_id,
                            role="live_inventory_entry",
                        )
                        await self.require_live_inventory_manual_review(
                            asset=snapshot.asset,
                            reason=f"entry_var_submit_exception:{var_outcome}",
                            context={
                                "action": "entry",
                                "direction": direction,
                                "qty": decimal_to_str(qty),
                                "var_amount": var_amount,
                                "var_side": var_side,
                            },
                        )
                        return
                    if isinstance(lighter_outcome, Exception):
                        await self.require_live_inventory_manual_review(
                            asset=snapshot.asset,
                            reason=f"entry_lighter_submit_exception:{lighter_outcome}",
                            context={
                                "action": "entry",
                                "direction": direction,
                                "qty": decimal_to_str(qty),
                                "var_amount": var_amount,
                                "var_side": var_side,
                            },
                        )
                        return
                    var_result, var_submit_ms = var_outcome
                    lighter_result, lighter_submit_ms = lighter_outcome
                    if not var_result.get("ok"):
                        self.remove_pending_live_inventory_var_fill_match(
                            asset=snapshot.asset,
                            lot_id=lot_id,
                            role="live_inventory_entry",
                        )
                        await self.require_live_inventory_manual_review(
                            asset=snapshot.asset,
                            reason=f"entry_var_submit_failed:{var_result.get('error') or 'unknown'}",
                            context={
                                "action": "entry",
                                "direction": direction,
                                "qty": decimal_to_str(qty),
                                "var_amount": var_amount,
                                "var_side": var_side,
                                "var_result": var_result,
                            },
                        )
                        return
                    entry_var_order_quote = refreshed_var_quote or self.variational_api_order_quote_fields(var_side, var_result)
                    lighter_record, lighter_payload = lighter_result
                    lighter_started = self.auto_live_eager_hedge_started(lighter_record)
                    if lighter_record is None or not lighter_started:
                        reason = "entry_lighter_submit_failed:unknown"
                        if lighter_record is not None:
                            reason = "entry_lighter_submit_failed:" + (
                                lighter_record.failure_reason or lighter_record.hedge_error or lighter_record.processing_stage or "unknown"
                            )
                        await self.require_live_inventory_manual_review(
                            asset=snapshot.asset,
                            reason=reason,
                            context={
                                "action": "entry",
                                "direction": direction,
                                "qty": decimal_to_str(qty),
                                "var_side": var_side,
                                "lighter_payload": lighter_payload,
                            },
                        )
                        return
                else:
                    var_result = None
                    lighter_record = None
                    lighter_payload = None
                    var_submit_ms = None
                    lighter_submit_ms = None
                    entry_var_order_quote = {}
                lot = {
                    "lot_id": lot_id,
                    "direction": direction,
                    "qty": decimal_to_str(qty),
                    "entry_var_fill_price": decimal_to_str(var_price),
                    "entry_lighter_fill_price": decimal_to_str(lighter_price),
                    "entry_var_price_source": "estimated_snapshot",
                    "entry_lighter_price_source": "estimated_snapshot",
                    "entry_cost_status": "dry_decision" if self.live_inventory_dry_decisions else "final_fills_pending",
                    "entry_snapshot_var_bid": decimal_to_str(snapshot.var_bid),
                    "entry_snapshot_var_ask": decimal_to_str(snapshot.var_ask),
                    "entry_snapshot_var_mid": decimal_to_str(snapshot.var_mid),
                    "entry_snapshot_var_buy_price": decimal_to_str(snapshot.var_buy_price),
                    "entry_snapshot_var_sell_price": decimal_to_str(snapshot.var_sell_price),
                    "entry_snapshot_var_full_spread_bps": decimal_to_str(snapshot.var_full_spread_bps),
                    "entry_snapshot_var_spread_source": snapshot.var_spread_source,
                    "entry_snapshot_var_timestamp": snapshot.var_timestamp,
                    "entry_snapshot_var_source_url": snapshot.var_source_url,
                    "entry_snapshot_var_source_stream": snapshot.var_source_stream,
                    "entry_initial_signal_edge_bps": decimal_to_str(initial_signal_edge_bps),
                    "entry_initial_snapshot_var_price": decimal_to_str(initial_snapshot_var_price),
                    "entry_refreshed_var_quote_ms": refreshed_var_quote_ms,
                    "entry_var_order_quote_id": entry_var_order_quote.get("quote_id"),
                    "entry_var_order_quote_bid": entry_var_order_quote.get("quote_bid"),
                    "entry_var_order_quote_ask": entry_var_order_quote.get("quote_ask"),
                    "entry_var_order_quote_mark_price": entry_var_order_quote.get("quote_mark_price"),
                    "entry_var_order_quote_timestamp": entry_var_order_quote.get("quote_timestamp"),
                    "entry_var_order_quote_execution_price": entry_var_order_quote.get("quote_execution_price"),
                    "entry_edge_bps": decimal_to_str(edge_bps),
                    "entered_at": utc_now(),
                    "entered_sample_index": index,
                    "status": "dry_open" if self.live_inventory_dry_decisions else "open",
                }
                if not self.live_inventory_dry_decisions:
                    lot.update(
                        {
                            "entry_var_side": var_side,
                            "entry_var_submit_ms": var_submit_ms,
                            "entry_lighter_submit_ms": lighter_submit_ms,
                            "entry_var_result": var_result,
                            "entry_lighter_record_key": lighter_record.trade_key if lighter_record is not None else None,
                            "entry_lighter_payload": lighter_payload,
                        }
                    )
                    self.remember_live_inventory_final_pnl_lot(asset=snapshot.asset, lot=lot)
                self.live_inventory_next_lot_id += 1
                self.live_inventory_open_lots.append(lot)
                if not self.live_inventory_dry_decisions:
                    self.sync_live_inventory_open_lot_entry_cost(asset=snapshot.asset, lot_id=lot_id)
                await self.persist_live_inventory_memory(
                    reason="dry_entry_decision" if self.live_inventory_dry_decisions else "entry_submitted"
                )
                await self.append_live_inventory_log(
                    f"{event_prefix}_entered",
                    {
                        "asset": snapshot.asset,
                        "lot_id": lot["lot_id"],
                        "direction": direction,
                        "qty": lot["qty"],
                        "edge_bps": decimal_to_str(edge_bps),
                        "initial_signal_edge_bps": decimal_to_str(initial_signal_edge_bps),
                        "initial_snapshot_var_price": decimal_to_str(initial_snapshot_var_price),
                        "refreshed_var_quote_ms": refreshed_var_quote_ms,
                        "var_price": decimal_to_str(var_price),
                        "lighter_price": decimal_to_str(lighter_price),
                        "var_bid": decimal_to_str(snapshot.var_bid),
                        "var_ask": decimal_to_str(snapshot.var_ask),
                        "var_mid": decimal_to_str(snapshot.var_mid),
                        "var_buy_price": decimal_to_str(snapshot.var_buy_price),
                        "var_sell_price": decimal_to_str(snapshot.var_sell_price),
                        "var_full_spread_bps": decimal_to_str(snapshot.var_full_spread_bps),
                        "var_spread_source": snapshot.var_spread_source,
                        "var_timestamp": snapshot.var_timestamp,
                        "var_source_url": snapshot.var_source_url,
                        "var_source_stream": snapshot.var_source_stream,
                        "var_order_quote_id": entry_var_order_quote.get("quote_id"),
                        "var_order_quote_bid": entry_var_order_quote.get("quote_bid"),
                        "var_order_quote_ask": entry_var_order_quote.get("quote_ask"),
                        "var_order_quote_mark_price": entry_var_order_quote.get("quote_mark_price"),
                        "var_order_quote_timestamp": entry_var_order_quote.get("quote_timestamp"),
                        "var_order_quote_execution_price": entry_var_order_quote.get("quote_execution_price"),
                        "open_lots_total": len(self.live_inventory_open_lots),
                        "realized_pnl_usd": decimal_to_str(self.live_inventory_realized_pnl_usd),
                        "completed_cycles": self.live_inventory_completed_cycles,
                        "var_submit_ms": var_submit_ms,
                        "lighter_submit_ms": lighter_submit_ms,
                    },
                )
                return

        if not self.live_inventory_open_lots:
            return
        lot = self.live_inventory_open_lots[0]
        direction = str(lot.get("direction") or "")
        if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
            edge_bps = decimal_percent_to_bps(snapshot.long_var_short_lighter_pct)
            var_exit_price = snapshot.var_sell_price or snapshot.var_mid
            lighter_exit_price = snapshot.lighter_buy_fill_price
        elif direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
            edge_bps = decimal_percent_to_bps(snapshot.short_var_long_lighter_pct)
            var_exit_price = snapshot.var_buy_price or snapshot.var_mid
            lighter_exit_price = snapshot.lighter_sell_fill_price
        else:
            await self.require_live_inventory_manual_review(
                asset=snapshot.asset,
                reason=f"unknown_direction:{direction}",
            )
            return
        entered_sample_index = int(lot.get("entered_sample_index") or index)
        holding_samples = index - entered_sample_index
        can_exit_on_reversion = holding_samples >= self.live_inventory_min_hold_samples
        should_exit = can_exit_on_reversion and edge_bps is not None and edge_bps <= self.live_inventory_exit_bps
        should_timeout = holding_samples >= self.live_inventory_max_hold_samples
        entry_cost_status = str(lot.get("entry_cost_status") or "unknown")
        if should_exit and not should_timeout and not self.live_inventory_dry_decisions and not self.live_inventory_entry_cost_confirmed(lot):
            await self.append_live_inventory_log(
                f"{event_prefix}_exit_blocked",
                {
                    "asset": snapshot.asset,
                    "lot_id": lot.get("lot_id"),
                    "direction": direction,
                    "reason": "entry_final_fill_cost_pending",
                    "entry_cost_status": entry_cost_status,
                    "edge_bps": decimal_to_str(edge_bps),
                    "holding_samples": holding_samples,
                },
            )
            return
        if not should_exit and not should_timeout:
            return
        entry_var_price = to_decimal(lot.get("entry_var_fill_price")) or var_exit_price
        entry_lighter_price = to_decimal(lot.get("entry_lighter_fill_price")) or lighter_exit_price
        qty = to_decimal(lot.get("qty")) or Decimal("0")
        exit_reason = "spread_reverted" if should_exit else "max_hold_samples"
        if not self.live_inventory_dry_decisions:
            exit_side = self._opposite_var_side(str(lot.get("entry_var_side") or self._auto_live_direction_to_var_side(direction)))
            var_amount = variational_api_amount_to_str(qty, asset=snapshot.asset)
            self.add_pending_live_inventory_var_fill_match(
                PendingLiveInventoryVarFillMatch(
                    asset=snapshot.asset,
                    side=exit_side.lower(),
                    qty=Decimal(var_amount),
                    lot_id=int(lot.get("lot_id") or 0),
                    role="live_inventory_exit",
                    created_at_monotonic=time.monotonic(),
                )
            )
            try:
                var_task = asyncio.create_task(
                    self._timed_submit(
                        self.send_variational_place_order(
                            asset=snapshot.asset,
                            side=exit_side,
                            amount=var_amount,
                            expected_min_btc_qty=Decimal(var_amount) if snapshot.asset.upper() == "BTC" else None,
                            confirm=True,
                            reduce_only=True,
                        )
                    )
                )
                lighter_task = asyncio.create_task(
                    self._timed_submit(
                        self.place_lighter_order_from_plan(
                            asset=snapshot.asset,
                            side=exit_side,
                            qty=qty,
                            var_fill_price=var_exit_price,
                            cycle_id=int(lot.get("lot_id") or 0),
                            role="live_inventory_exit",
                            reduce_only=True,
                        )
                    )
                )
                var_outcome, lighter_outcome = await asyncio.gather(
                    var_task,
                    lighter_task,
                    return_exceptions=True,
                )
            except Exception as exc:
                await self.require_live_inventory_manual_review(
                    asset=snapshot.asset,
                    reason=f"exit_submit_exception:{exc}",
                    context={
                        "action": "exit",
                        "lot_id": lot.get("lot_id"),
                        "direction": direction,
                        "qty": decimal_to_str(qty),
                        "var_amount": var_amount,
                        "exit_side": exit_side,
                    },
                )
                return
            if isinstance(var_outcome, Exception):
                self.remove_pending_live_inventory_var_fill_match(
                    asset=snapshot.asset,
                    lot_id=lot.get("lot_id"),
                    role="live_inventory_exit",
                )
                await self.require_live_inventory_manual_review(
                    asset=snapshot.asset,
                    reason=f"exit_var_submit_exception:{var_outcome}",
                    context={
                        "action": "exit",
                        "lot_id": lot.get("lot_id"),
                        "direction": direction,
                        "qty": decimal_to_str(qty),
                        "var_amount": var_amount,
                        "exit_side": exit_side,
                    },
                )
                return
            if isinstance(lighter_outcome, Exception):
                await self.require_live_inventory_manual_review(
                    asset=snapshot.asset,
                    reason=f"exit_lighter_submit_exception:{lighter_outcome}",
                    context={
                        "action": "exit",
                        "lot_id": lot.get("lot_id"),
                        "direction": direction,
                        "qty": decimal_to_str(qty),
                        "var_amount": var_amount,
                        "exit_side": exit_side,
                    },
                )
                return
            var_result, var_submit_ms = var_outcome
            lighter_result, lighter_submit_ms = lighter_outcome
            if not var_result.get("ok"):
                self.remove_pending_live_inventory_var_fill_match(
                    asset=snapshot.asset,
                    lot_id=lot.get("lot_id"),
                    role="live_inventory_exit",
                )
                await self.require_live_inventory_manual_review(
                    asset=snapshot.asset,
                    reason=f"exit_var_submit_failed:{var_result.get('error') or 'unknown'}",
                    context={
                        "action": "exit",
                        "lot_id": lot.get("lot_id"),
                        "direction": direction,
                        "qty": decimal_to_str(qty),
                        "var_amount": var_amount,
                        "exit_side": exit_side,
                        "var_result": var_result,
                    },
                )
                return
            exit_var_order_quote = self.variational_api_order_quote_fields(exit_side, var_result)
            lighter_record, lighter_payload = lighter_result
            lighter_started = self.auto_live_eager_hedge_started(lighter_record)
            if lighter_record is None or not lighter_started:
                reason = "exit_lighter_submit_failed:unknown"
                if lighter_record is not None:
                    reason = "exit_lighter_submit_failed:" + (
                        lighter_record.failure_reason or lighter_record.hedge_error or lighter_record.processing_stage or "unknown"
                    )
                await self.require_live_inventory_manual_review(
                    asset=snapshot.asset,
                    reason=reason,
                    context={
                        "action": "exit",
                        "lot_id": lot.get("lot_id"),
                        "direction": direction,
                        "qty": decimal_to_str(qty),
                        "exit_side": exit_side,
                        "lighter_payload": lighter_payload,
                    },
                )
                return
        else:
            var_submit_ms = None
            lighter_submit_ms = None
            lighter_payload = None
            exit_var_order_quote = {}
        estimated_var_leg_pnl, estimated_lighter_leg_pnl, pnl = self.live_inventory_pair_pnl(
            direction=direction,
            qty=qty,
            entry_var_price=entry_var_price,
            entry_lighter_price=entry_lighter_price,
            exit_var_price=var_exit_price,
            exit_lighter_price=lighter_exit_price,
        )
        notional = qty * entry_var_price
        pnl_bps = pnl / notional * Decimal("10000") if notional else None
        self.live_inventory_open_lots.pop(0)
        self.live_inventory_realized_pnl_usd += pnl
        self.live_inventory_completed_cycles += 1
        await self.persist_live_inventory_memory(
            reason="dry_exit_decision" if self.live_inventory_dry_decisions else "exit_submitted"
        )
        actual_pnl_status = "dry_decision" if self.live_inventory_dry_decisions else "pending_lighter_final_fill"
        if not self.live_inventory_dry_decisions and isinstance(lighter_payload, dict):
            exit_trade_key = str(lighter_payload.get("trade_key") or "")
            if exit_trade_key:
                pending_pnl = {
                    "asset": snapshot.asset,
                    "lot_id": lot.get("lot_id"),
                    "direction": direction,
                    "qty": decimal_to_str(qty),
                    "entry_var_price": decimal_to_str(entry_var_price),
                    "entry_lighter_price": decimal_to_str(entry_lighter_price),
                    "entry_cost_status": entry_cost_status,
                    "entry_var_price_source": lot.get("entry_var_price_source"),
                    "entry_lighter_price_source": lot.get("entry_lighter_price_source"),
                    "exit_var_price": decimal_to_str(var_exit_price),
                    "exit_lighter_estimated_price": decimal_to_str(lighter_exit_price),
                    "estimated_pnl_usd": decimal_to_str(pnl),
                    "estimated_pnl_bps": decimal_to_str(pnl_bps),
                    "estimated_var_leg_pnl_usd": decimal_to_str(estimated_var_leg_pnl),
                    "estimated_lighter_leg_pnl_usd": decimal_to_str(estimated_lighter_leg_pnl),
                    "holding_samples": holding_samples,
                    "exit_reason": exit_reason,
                }
                self.pending_live_inventory_actual_pnl[exit_trade_key] = pending_pnl
                final_key = self.live_inventory_final_pnl_key(snapshot.asset, lot.get("lot_id"))
                final_pending = self.pending_live_inventory_final_pnl.setdefault(final_key, {})
                final_pending.update(
                    {
                        **pending_pnl,
                        "exit_estimated_var_price": decimal_to_str(var_exit_price),
                        "exit_estimated_lighter_price": decimal_to_str(lighter_exit_price),
                        "exit_var_order_quote_id": exit_var_order_quote.get("quote_id"),
                        "exit_var_order_quote_bid": exit_var_order_quote.get("quote_bid"),
                        "exit_var_order_quote_ask": exit_var_order_quote.get("quote_ask"),
                        "exit_var_order_quote_mark_price": exit_var_order_quote.get("quote_mark_price"),
                        "exit_var_order_quote_timestamp": exit_var_order_quote.get("quote_timestamp"),
                        "exit_var_order_quote_execution_price": exit_var_order_quote.get("quote_execution_price"),
                        "exited_at": utc_now(),
                    }
                )
        await self.append_live_inventory_log(
            f"{event_prefix}_exited",
            {
                "asset": snapshot.asset,
                "lot_id": lot.get("lot_id"),
                "direction": direction,
                "qty": decimal_to_str(qty),
                "edge_bps": decimal_to_str(edge_bps),
                "var_price": decimal_to_str(var_exit_price),
                "lighter_price": decimal_to_str(lighter_exit_price),
                "pnl_usd": decimal_to_str(pnl),
                "pnl_bps": decimal_to_str(pnl_bps),
                "estimated_pnl_usd": decimal_to_str(pnl),
                "estimated_pnl_bps": decimal_to_str(pnl_bps),
                "estimated_var_leg_pnl_usd": decimal_to_str(estimated_var_leg_pnl),
                "estimated_lighter_leg_pnl_usd": decimal_to_str(estimated_lighter_leg_pnl),
                "actual_pnl_status": actual_pnl_status,
                "entry_cost_status": entry_cost_status,
                "entry_var_price_source": lot.get("entry_var_price_source"),
                "entry_lighter_price_source": lot.get("entry_lighter_price_source"),
                "holding_samples": holding_samples,
                "exit_reason": exit_reason,
                "open_lots_total": len(self.live_inventory_open_lots),
                "realized_pnl_usd": decimal_to_str(self.live_inventory_realized_pnl_usd),
                "completed_cycles": self.live_inventory_completed_cycles,
                "var_submit_ms": var_submit_ms,
                "lighter_submit_ms": lighter_submit_ms,
                "var_order_quote_id": exit_var_order_quote.get("quote_id"),
                "var_order_quote_bid": exit_var_order_quote.get("quote_bid"),
                "var_order_quote_ask": exit_var_order_quote.get("quote_ask"),
                "var_order_quote_mark_price": exit_var_order_quote.get("quote_mark_price"),
                "var_order_quote_timestamp": exit_var_order_quote.get("quote_timestamp"),
                "var_order_quote_execution_price": exit_var_order_quote.get("quote_execution_price"),
                "exit_lighter_payload": lighter_payload,
            },
        )

    async def maybe_run_auto_live(self, snapshot: CrossSpreadSnapshot) -> None:
        if not self.is_auto_live_enabled():
            return
        if snapshot.var_half_spread_bps > self.paper_max_var_half_spread_bps:
            return

        position = self.auto_live_position
        if position is None:
            if not self.auto_live_entry:
                return
            guard_reason = self.auto_live_guard_reason()
            if guard_reason is not None:
                self.maybe_log_auto_live_guard(guard_reason)
                return
            candidate = paper_entry_candidate(snapshot, self.paper_entry_deviation_bps, self.paper_min_samples)
            if candidate is None:
                return
            cycle_id = self.auto_live_next_cycle_id
            direction = candidate.direction
            if self.auto_live_disable_short_var_long_lighter and direction == "short_var_long_lighter":
                self.logger.info(
                    "auto_live_entry_direction_disabled cycle_id=%s asset=%s direction=%s action=skip_var_entry",
                    cycle_id,
                    snapshot.asset,
                    direction,
                )
                return
            current_pct = candidate.current_pct
            median_pct = candidate.median_pct
            deviation_bps = candidate.deviation_bps
            if snapshot.var_mid <= 0:
                return
            planned_qty = self.paper_notional_usd / snapshot.var_mid
            order_qty = self._format_auto_live_order_qty(snapshot.asset, planned_qty)
            depth_enough, _, _ = await self._paper_depth_enough(direction, order_qty)
            if not depth_enough:
                return
            var_side = self._auto_live_direction_to_var_side(direction)
            actionable_var_price = (snapshot.var_buy_price if var_side == "BUY" else snapshot.var_sell_price) or snapshot.var_mid
            actionable_var_price_source = "snapshot"
            if (
                self.auto_live_entry_min_actionable_edge_bps > 0
                and self.variational_submit_transport == VARIATIONAL_SUBMIT_TRANSPORT_API
            ):
                refresh_started = time.monotonic()
                try:
                    quote_result = await self.send_variational_place_order(
                        asset=snapshot.asset,
                        side=var_side,
                        amount=decimal_to_str(order_qty) or str(order_qty),
                        expected_min_btc_qty=order_qty if snapshot.asset.upper() == "BTC" else None,
                        confirm=False,
                    )
                    refreshed_price = self.variational_api_quote_execution_price(var_side, quote_result)
                    if quote_result.get("ok") and refreshed_price is not None:
                        actionable_var_price = refreshed_price
                        actionable_var_price_source = "variational_api_quote"
                    else:
                        self.logger.info(
                            "auto_live_entry_actionable_quote_refresh_failed cycle_id=%s asset=%s direction=%s var_side=%s "
                            "duration_ms=%s ok=%s error=%s action=use_snapshot_price",
                            cycle_id,
                            snapshot.asset,
                            direction,
                            var_side,
                            elapsed_ms_str(refresh_started),
                            quote_result.get("ok"),
                            quote_result.get("error") or quote_result.get("step") or "missing_price",
                        )
                except Exception as exc:
                    self.logger.info(
                        "auto_live_entry_actionable_quote_refresh_failed cycle_id=%s asset=%s direction=%s var_side=%s "
                        "duration_ms=%s error=%s action=use_snapshot_price",
                        cycle_id,
                        snapshot.asset,
                        direction,
                        var_side,
                        elapsed_ms_str(refresh_started),
                        exc,
                    )
            lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
            actionable_edge_bps = self.auto_live_entry_actionable_edge_bps(
                direction,
                actionable_var_price,
                lighter_bid,
                lighter_ask,
            )
            if self.auto_live_entry_min_actionable_edge_bps > 0:
                if actionable_edge_bps is None:
                    self.logger.info(
                        "auto_live_entry_actionable_edge_checked cycle_id=%s asset=%s direction=%s var_side=%s "
                        "var_price=%s var_price_source=%s lighter_bid=%s lighter_ask=%s edge_bps=- threshold_bps=%s action=skip_var_entry reason=edge_unavailable",
                        cycle_id,
                        snapshot.asset,
                        direction,
                        var_side,
                        decimal_to_str(actionable_var_price) or "-",
                        actionable_var_price_source,
                        decimal_to_str(lighter_bid) or "-",
                        decimal_to_str(lighter_ask) or "-",
                        decimal_to_str(self.auto_live_entry_min_actionable_edge_bps),
                    )
                    return
                if actionable_edge_bps < self.auto_live_entry_min_actionable_edge_bps:
                    self.logger.info(
                        "auto_live_entry_actionable_edge_checked cycle_id=%s asset=%s direction=%s var_side=%s "
                        "var_price=%s var_price_source=%s lighter_bid=%s lighter_ask=%s edge_bps=%s threshold_bps=%s action=skip_var_entry reason=edge_below_threshold",
                        cycle_id,
                        snapshot.asset,
                        direction,
                        var_side,
                        decimal_to_str(actionable_var_price) or "-",
                        actionable_var_price_source,
                        decimal_to_str(lighter_bid) or "-",
                        decimal_to_str(lighter_ask) or "-",
                        decimal_to_str(actionable_edge_bps) or "-",
                        decimal_to_str(self.auto_live_entry_min_actionable_edge_bps),
                    )
                    return
            self.logger.info(
                "auto_live_entry_actionable_edge_checked cycle_id=%s asset=%s direction=%s var_side=%s "
                "var_price=%s var_price_source=%s lighter_bid=%s lighter_ask=%s edge_bps=%s threshold_bps=%s action=pass",
                cycle_id,
                snapshot.asset,
                direction,
                var_side,
                decimal_to_str(actionable_var_price) or "-",
                actionable_var_price_source,
                decimal_to_str(lighter_bid) or "-",
                decimal_to_str(lighter_ask) or "-",
                decimal_to_str(actionable_edge_bps) or "-",
                decimal_to_str(self.auto_live_entry_min_actionable_edge_bps),
            )
            entry_signal_monotonic = time.monotonic()
            entry_precheck_ms = "-"
            entry_var_submit_ms = "-"
            entry_lighter_submit_ms = "-"
            if self.auto_live_eager_hedge:
                precheck_price = actionable_var_price
                entry_precheck_started = time.monotonic()
                precheck_ok, precheck_reason, precheck_edge_bps = await self.auto_live_lighter_precheck(
                    asset=snapshot.asset,
                    var_side=var_side,
                    qty=order_qty,
                    var_fill_price=precheck_price,
                )
                entry_precheck_ms = elapsed_ms_str(entry_precheck_started)
                if not precheck_ok:
                    if self.should_log_auto_live_precheck_failure(
                        "entry",
                        cycle_id,
                        snapshot.asset,
                        var_side,
                        precheck_reason,
                    ):
                        self.logger.warning(
                            "auto_live_entry_precheck_failed cycle_id=%s asset=%s side=%s qty=%s reason=%s edge_bps=%s duration_ms=%s action=skip_var_entry",
                            cycle_id,
                            snapshot.asset,
                            var_side,
                            order_qty,
                            precheck_reason,
                            decimal_to_str(precheck_edge_bps) or "-",
                            entry_precheck_ms,
                        )
                    return
                if (
                    self.auto_live_entry_max_precheck_edge_bps > 0
                    and precheck_edge_bps is not None
                    and precheck_edge_bps > self.auto_live_entry_max_precheck_edge_bps
                ):
                    precheck_reason = "entry_precheck_edge_exceeds_auto_live_limit"
                    if self.should_log_auto_live_precheck_failure(
                        "entry",
                        cycle_id,
                        snapshot.asset,
                        var_side,
                        precheck_reason,
                    ):
                        self.logger.warning(
                            "auto_live_entry_precheck_failed cycle_id=%s asset=%s side=%s qty=%s reason=%s edge_bps=%s duration_ms=%s limit_bps=%s action=skip_var_entry",
                            cycle_id,
                            snapshot.asset,
                            var_side,
                            order_qty,
                            precheck_reason,
                            decimal_to_str(precheck_edge_bps) or "-",
                            entry_precheck_ms,
                            decimal_to_str(self.auto_live_entry_max_precheck_edge_bps),
                        )
                    return
                self.logger.info(
                    "auto_live_entry_precheck_passed cycle_id=%s asset=%s side=%s qty=%s edge_bps=%s duration_ms=%s",
                    cycle_id,
                    snapshot.asset,
                    var_side,
                    order_qty,
                    decimal_to_str(precheck_edge_bps) or "-",
                    entry_precheck_ms,
                )
            if self.auto_live_skip_entry_preview or self.variational_submit_transport == VARIATIONAL_SUBMIT_TRANSPORT_API:
                entry_var_preview_ms = "skipped"
            else:
                entry_var_preview_started = time.monotonic()
                try:
                    precheck = await self.send_variational_place_order(
                        asset=snapshot.asset,
                        side=var_side,
                        amount=decimal_to_str(order_qty) or str(order_qty),
                        expected_min_btc_qty=order_qty if snapshot.asset.upper() == "BTC" else None,
                        confirm=False,
                    )
                except Exception as exc:
                    reason = f"entry_var_preview_exception:{exc}"
                    self.require_auto_live_manual_review_for_entry(
                        cycle_id=cycle_id,
                        asset=snapshot.asset,
                        direction=direction,
                        qty=order_qty,
                        reason=reason,
                    )
                    self.logger.exception(
                        "auto_live_entry_preview_exception cycle_id=%s asset=%s side=%s qty=%s",
                        cycle_id,
                        snapshot.asset,
                        var_side,
                        order_qty,
                    )
                    return
                entry_var_preview_ms = elapsed_ms_str(entry_var_preview_started)
                observed_order_qty = to_decimal((precheck.get("result") or {}).get("orderQuantityBtc"))
                if snapshot.asset.upper() == "BTC" and (observed_order_qty is None or observed_order_qty < order_qty):
                    self.logger.warning("auto_live_precheck_rejected asset=%s side=%s expected_qty=%s got=%s", snapshot.asset, var_side, order_qty, observed_order_qty)
                    return
            entry_var_submit_started = time.monotonic()
            entry_var_task = asyncio.create_task(
                self._timed_submit(
                    self.send_variational_place_order(
                        asset=snapshot.asset,
                        side=var_side,
                        amount=decimal_to_str(order_qty) or str(order_qty),
                        expected_min_btc_qty=order_qty if snapshot.asset.upper() == "BTC" else None,
                        confirm=True,
                        reduce_only=False,
                    )
                )
            )
            entry_lighter_task = None
            if self.auto_live_eager_hedge:
                entry_lighter_submit_started = time.monotonic()
                entry_lighter_task = asyncio.create_task(
                    self._timed_submit(
                        self.place_lighter_order_from_plan(
                            asset=snapshot.asset,
                            side=var_side,
                            qty=order_qty,
                            var_fill_price=actionable_var_price,
                            cycle_id=cycle_id,
                            role="entry",
                        )
                    )
                )

            try:
                result, entry_var_submit_ms = await entry_var_task
            except Exception as exc:
                if entry_lighter_task is not None:
                    with contextlib.suppress(Exception):
                        await entry_lighter_task
                reason = f"entry_var_submit_exception:{exc}"
                self.require_auto_live_manual_review_for_entry(
                    cycle_id=cycle_id,
                    asset=snapshot.asset,
                    direction=direction,
                    qty=order_qty,
                    reason=reason,
                )
                self.logger.exception(
                    "auto_live_entry_submit_exception cycle_id=%s asset=%s side=%s qty=%s",
                    cycle_id,
                    snapshot.asset,
                    var_side,
                    order_qty,
                )
                return
            if not result.get("ok"):
                if entry_lighter_task is not None:
                    with contextlib.suppress(Exception):
                        await entry_lighter_task
                reason = f"entry_var_submit_failed:{result.get('error') or 'unknown'}"
                self.require_auto_live_manual_review_for_entry(
                    cycle_id=cycle_id,
                    asset=snapshot.asset,
                    direction=direction,
                    qty=order_qty,
                    reason=reason,
                )
                self.logger.warning(
                    "auto_live_var_submit_failed cycle_id=%s asset=%s side=%s qty=%s error=%s action=manual_review_required",
                    cycle_id,
                    snapshot.asset,
                    var_side,
                    order_qty,
                    result.get("error"),
                )
                return

            entry_eager_started = not self.auto_live_eager_hedge
            if entry_lighter_task is not None:
                try:
                    lighter_result, entry_lighter_submit_ms = await entry_lighter_task
                except Exception as exc:
                    reason = f"entry_lighter_submit_exception:{exc}"
                    self.require_auto_live_manual_review_for_entry(
                        cycle_id=cycle_id,
                        asset=snapshot.asset,
                        direction=direction,
                        qty=order_qty,
                        reason=reason,
                    )
                    self.logger.exception(
                        "auto_live_entry_lighter_submit_exception cycle_id=%s asset=%s side=%s qty=%s",
                        cycle_id,
                        snapshot.asset,
                        var_side,
                        order_qty,
                    )
                    return
                lighter_record, payload = lighter_result
                entry_eager_started = self.auto_live_eager_hedge_started(lighter_record)
                if lighter_record is not None and entry_eager_started:
                    self.pending_auto_live_matches.append(
                        PendingAutoLiveMatch(
                            record_key=lighter_record.trade_key,
                            asset=snapshot.asset,
                            side=var_side.lower(),
                            qty=order_qty,
                            cycle_id=cycle_id,
                            role="entry",
                            created_at_monotonic=time.monotonic(),
                        )
                    )
                    if payload is not None:
                        self.log_auto_live_eager_hedge_timing(
                            cycle_id=cycle_id,
                            role="entry",
                            asset=snapshot.asset,
                            side=var_side,
                            signal_monotonic=entry_signal_monotonic,
                            task_created_monotonic=entry_lighter_submit_started,
                            record=lighter_record,
                        )
                        self.logger.info(
                            "auto_live_eager_hedge_started cycle_id=%s role=entry asset=%s side=%s qty=%s stage=%s record_key=%s duration_ms=%s",
                            cycle_id,
                            snapshot.asset,
                            var_side,
                            order_qty,
                            payload.get("processing_stage"),
                            lighter_record.trade_key,
                            entry_lighter_submit_ms,
                        )
                elif lighter_record is not None:
                    self.logger.warning(
                        "auto_live_entry_eager_hedge_failed cycle_id=%s asset=%s side=%s qty=%s stage=%s failure_reason=%s",
                        cycle_id,
                        snapshot.asset,
                        var_side,
                        order_qty,
                        lighter_record.processing_stage,
                        lighter_record.failure_reason or lighter_record.hedge_error or "unknown",
                    )
                    return

            if not entry_eager_started:
                self.logger.warning(
                    "auto_live_entry_eager_hedge_failed cycle_id=%s asset=%s side=%s qty=%s stage=not_started failure_reason=no_lighter_record",
                    cycle_id,
                    snapshot.asset,
                    var_side,
                    order_qty,
                )
                return

            entry_var_execution_price, entry_lighter_execution_price = paper_entry_execution_prices(snapshot, direction)
            self.auto_live_position = AutoLivePositionState(
                cycle_id=cycle_id,
                asset=snapshot.asset,
                direction=direction,
                entered_at_iso=utc_now(),
                entered_at_monotonic=time.monotonic(),
                entry_spread_pct=current_pct,
                entry_median_pct=median_pct,
                entry_deviation_bps=deviation_bps,
                entry_var_mid=snapshot.var_mid,
                entry_lighter_mid=snapshot.lighter_mid,
                entry_var_execution_price=entry_var_execution_price,
                entry_lighter_execution_price=entry_lighter_execution_price,
                planned_notional_usd=self.paper_notional_usd,
                planned_qty=order_qty,
            )
            await self.write_auto_live_state_async(
                {
                    "status": "open",
                    "asset": snapshot.asset,
                    "cycle_id": cycle_id,
                    "direction": direction,
                    "qty": decimal_to_str(order_qty),
                    "entered_at": self.auto_live_position.entered_at_iso,
                }
            )
            self.logger.info(
                "auto_live_entry_submitted cycle_id=%s asset=%s direction=%s qty=%s var_side=%s entry_total_ms=%s entry_precheck_ms=%s var_preview_ms=%s var_submit_ms=%s lighter_submit_ms=%s",
                cycle_id,
                snapshot.asset,
                direction,
                order_qty,
                var_side,
                elapsed_ms_str(entry_signal_monotonic),
                entry_precheck_ms,
                entry_var_preview_ms,
                entry_var_submit_ms,
                entry_lighter_submit_ms,
            )
            return

        if not self.auto_live_exit:
            return

        if position.manual_review_required:
            self.require_auto_live_manual_review(position, position.manual_review_reason or "unknown")
            return

        if position.exit_submitted:
            self.logger.warning(
                "auto_live_exit_already_submitted cycle_id=%s asset=%s side=%s qty=%s reason=%s submitted_at=%s action=manual_review_required",
                position.cycle_id,
                position.asset,
                position.exit_side or "-",
                position.planned_qty,
                position.exit_reason or "-",
                position.exit_submitted_at_iso or "-",
            )
            self.require_auto_live_manual_review(position, "exit_already_submitted")
            return

        holding_seconds = time.monotonic() - position.entered_at_monotonic
        exit_reason: str | None = None
        current_pct: Decimal | None = None
        median_pct: Decimal | None = None
        current_deviation_bps: Decimal | None = None

        if holding_seconds >= self.paper_max_holding_seconds:
            exit_reason = "timeout_exit"
        else:
            if holding_seconds < self.auto_live_min_holding_seconds:
                return
            current_pct, median_pct, _ = paper_direction_values(snapshot, position.direction)
            if current_pct is None or median_pct is None:
                return
            current_deviation_bps = decimal_percent_to_bps(current_pct - position.entry_median_pct)
            if current_deviation_bps <= self.paper_exit_deviation_bps:
                exit_reason = "spread_reverted"
        if exit_reason is None:
            return

        exit_side = self._opposite_var_side(self._auto_live_direction_to_var_side(position.direction))
        exit_signal_monotonic = time.monotonic()
        exit_precheck_ms = "-"
        exit_var_submit_ms = "-"
        exit_lighter_submit_ms = "-"
        if self.auto_live_eager_hedge:
            precheck_price = snapshot.var_sell_price if exit_side == "SELL" else snapshot.var_buy_price or snapshot.var_mid
            exit_precheck_started = time.monotonic()
            precheck_ok, precheck_reason, precheck_edge_bps = await self.auto_live_lighter_precheck(
                asset=snapshot.asset,
                var_side=exit_side,
                qty=position.planned_qty,
                var_fill_price=precheck_price,
            )
            exit_precheck_ms = elapsed_ms_str(exit_precheck_started)
            if not precheck_ok:
                if self.should_log_auto_live_precheck_failure(
                    "exit",
                    position.cycle_id,
                    snapshot.asset,
                    exit_side,
                    precheck_reason,
                ):
                    self.logger.warning(
                        "auto_live_exit_precheck_failed cycle_id=%s asset=%s side=%s qty=%s reason=%s edge_bps=%s duration_ms=%s action=skip_var_exit",
                        position.cycle_id,
                        snapshot.asset,
                        exit_side,
                        position.planned_qty,
                        precheck_reason,
                        decimal_to_str(precheck_edge_bps) or "-",
                        exit_precheck_ms,
                    )
                self.require_auto_live_manual_review(position, f"exit_precheck_failed:{precheck_reason}")
                return
            self.logger.info(
                "auto_live_exit_precheck_passed cycle_id=%s asset=%s side=%s qty=%s edge_bps=%s duration_ms=%s",
                position.cycle_id,
                snapshot.asset,
                exit_side,
                position.planned_qty,
                decimal_to_str(precheck_edge_bps) or "-",
                exit_precheck_ms,
            )
        exit_var_submit_started = time.monotonic()
        exit_var_task = asyncio.create_task(
            self._timed_submit(
                self.send_variational_place_order(
                    asset=snapshot.asset,
                    side=exit_side,
                    amount=decimal_to_str(position.planned_qty) or str(position.planned_qty),
                    expected_min_btc_qty=position.planned_qty if snapshot.asset.upper() == "BTC" else None,
                    confirm=True,
                    reduce_only=True,
                )
            )
        )
        exit_lighter_task = None
        if self.auto_live_eager_hedge:
            exit_lighter_submit_started = time.monotonic()
            exit_lighter_task = asyncio.create_task(
                self._timed_submit(
                    self.place_lighter_order_from_plan(
                        asset=snapshot.asset,
                        side=exit_side,
                        qty=position.planned_qty,
                        var_fill_price=snapshot.var_sell_price if exit_side == "SELL" else snapshot.var_buy_price or snapshot.var_mid,
                        cycle_id=position.cycle_id,
                        role="exit",
                    )
                )
            )

        try:
            result, exit_var_submit_ms = await exit_var_task
        except Exception as exc:
            if exit_lighter_task is not None:
                with contextlib.suppress(Exception):
                    await exit_lighter_task
            reason = f"exit_var_submit_exception:{exc}"
            self.require_auto_live_manual_review(position, reason)
            self.logger.exception(
                "auto_live_exit_submit_exception cycle_id=%s asset=%s side=%s qty=%s",
                position.cycle_id,
                snapshot.asset,
                exit_side,
                position.planned_qty,
            )
            return
        if not result.get("ok"):
            if exit_lighter_task is not None:
                with contextlib.suppress(Exception):
                    await exit_lighter_task
            reason = f"exit_var_submit_failed:{result.get('error') or 'unknown'}"
            self.require_auto_live_manual_review(position, reason)
            self.logger.warning(
                "auto_live_exit_submit_failed cycle_id=%s asset=%s side=%s qty=%s reason=%s error=%s action=manual_review_required",
                position.cycle_id,
                snapshot.asset,
                exit_side,
                position.planned_qty,
                exit_reason,
                result.get("error"),
            )
            return
        position.exit_submitted = True
        position.exit_submitted_at_iso = utc_now()
        position.exit_side = exit_side
        position.exit_reason = exit_reason
        exit_eager_started = not self.auto_live_eager_hedge
        if exit_lighter_task is not None:
            try:
                lighter_result, exit_lighter_submit_ms = await exit_lighter_task
            except Exception as exc:
                reason = f"exit_lighter_submit_exception:{exc}"
                self.require_auto_live_manual_review(position, reason)
                self.logger.exception(
                    "auto_live_exit_lighter_submit_exception cycle_id=%s asset=%s side=%s qty=%s",
                    position.cycle_id,
                    snapshot.asset,
                    exit_side,
                    position.planned_qty,
                )
                return
            lighter_record, payload = lighter_result
            exit_eager_started = self.auto_live_eager_hedge_started(lighter_record)
            if lighter_record is not None and exit_eager_started:
                self.pending_auto_live_matches.append(
                    PendingAutoLiveMatch(
                        record_key=lighter_record.trade_key,
                        asset=snapshot.asset,
                        side=exit_side.lower(),
                        qty=position.planned_qty,
                        cycle_id=position.cycle_id,
                        role="exit",
                        created_at_monotonic=time.monotonic(),
                    )
                )
                if payload is not None:
                    self.log_auto_live_eager_hedge_timing(
                        cycle_id=position.cycle_id,
                        role="exit",
                        asset=snapshot.asset,
                        side=exit_side,
                        signal_monotonic=exit_signal_monotonic,
                        task_created_monotonic=exit_lighter_submit_started,
                        record=lighter_record,
                    )
                    self.logger.info(
                        "auto_live_eager_hedge_started cycle_id=%s role=exit asset=%s side=%s qty=%s stage=%s record_key=%s duration_ms=%s",
                        position.cycle_id,
                        snapshot.asset,
                        exit_side,
                        position.planned_qty,
                        payload.get("processing_stage"),
                        lighter_record.trade_key,
                        exit_lighter_submit_ms,
                    )
            elif lighter_record is not None:
                self.logger.warning(
                    "auto_live_exit_eager_hedge_failed cycle_id=%s asset=%s side=%s qty=%s stage=%s failure_reason=%s",
                    position.cycle_id,
                    snapshot.asset,
                    exit_side,
                    position.planned_qty,
                    lighter_record.processing_stage,
                    lighter_record.failure_reason or lighter_record.hedge_error or "unknown",
                )
                self.logger.warning(
                    "auto_live_exit_manual_review_required cycle_id=%s asset=%s side=%s qty=%s reason=eager_hedge_failed var_exit_submitted_at=%s",
                    position.cycle_id,
                    snapshot.asset,
                    exit_side,
                    position.planned_qty,
                    position.exit_submitted_at_iso,
                )
                self.require_auto_live_manual_review(position, "exit_eager_hedge_failed")
                return
        if not exit_eager_started:
            self.logger.warning(
                "auto_live_exit_eager_hedge_failed cycle_id=%s asset=%s side=%s qty=%s stage=not_started failure_reason=no_lighter_record",
                position.cycle_id,
                snapshot.asset,
                exit_side,
                position.planned_qty,
            )
            self.logger.warning(
                "auto_live_exit_manual_review_required cycle_id=%s asset=%s side=%s qty=%s reason=eager_hedge_not_started var_exit_submitted_at=%s",
                position.cycle_id,
                snapshot.asset,
                exit_side,
                position.planned_qty,
                position.exit_submitted_at_iso,
            )
            self.require_auto_live_manual_review(position, "exit_eager_hedge_not_started")
            return
        self.logger.info(
            "auto_live_exit_submitted cycle_id=%s asset=%s side=%s qty=%s reason=%s exit_total_ms=%s exit_precheck_ms=%s var_submit_ms=%s lighter_submit_ms=%s",
            position.cycle_id,
            snapshot.asset,
            exit_side,
            position.planned_qty,
            exit_reason,
            elapsed_ms_str(exit_signal_monotonic),
            exit_precheck_ms,
            exit_var_submit_ms,
            exit_lighter_submit_ms,
        )
        self.auto_live_position = None
        self.auto_live_last_closed_monotonic = time.monotonic()
        self.auto_live_completed_cycles += 1
        self.auto_live_next_cycle_id += 1
        self._last_auto_live_guard_log = None
        await self.write_auto_live_state_async(
            {
                "status": "flat",
                "asset": snapshot.asset,
                "cycle_id": position.cycle_id,
                "direction": position.direction,
                "qty": decimal_to_str(position.planned_qty),
                "reason": f"exit_submitted:{exit_reason}",
            }
        )

    async def paper_loop(self) -> None:
        try:
            while not self.stop_flag:
                snapshot = await self.get_cross_spread_snapshot()
                if snapshot is not None:
                    await self.append_market_sample(snapshot)
                    await self.maybe_run_paper_inventory(snapshot)
                    await self.maybe_run_live_inventory(snapshot)
                    await self.maybe_close_paper_position(snapshot)
                    await self.maybe_enter_paper_position(snapshot)
                    await self.maybe_run_auto_live(snapshot)
                await asyncio.sleep(self.paper_interval_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("paper_loop_failed")
            raise

    async def render_dashboard(self) -> Group:
        health = self.build_health_status()
        var_bid, var_ask, quote_asset = await self.get_variational_best_bid_ask(self.variational_ticker)
        lighter_bid, lighter_ask = (None, None)
        if self.requires_lighter_market_data():
            lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        var_book_spread = spread_value(var_bid, var_ask)
        lighter_book_spread = spread_value(lighter_bid, lighter_ask)
        var_book_spread_pct = book_spread_percent(var_bid, var_ask)
        lighter_book_spread_pct = book_spread_percent(lighter_bid, lighter_ask)
        spread_color_baseline: Decimal | None = None
        if var_book_spread_pct is not None and lighter_book_spread_pct is not None:
            spread_color_baseline = (var_book_spread_pct + lighter_book_spread_pct) / Decimal("2")

        long_var_short_lighter_pct = spread_percent(spread_value(var_ask, lighter_bid), var_ask)
        short_var_long_lighter_pct = spread_percent(spread_value(lighter_ask, var_bid), lighter_ask)

        long_pct_median_5m = self._median_cross_spread(5 * 60, long_side=True)
        long_pct_median_30m = self._median_cross_spread(30 * 60, long_side=True)
        long_pct_median_1h = self._median_cross_spread(60 * 60, long_side=True)
        short_pct_median_5m = self._median_cross_spread(5 * 60, long_side=False)
        short_pct_median_30m = self._median_cross_spread(30 * 60, long_side=False)
        short_pct_median_1h = self._median_cross_spread(60 * 60, long_side=False)

        async with self._record_lock:
            recent_keys = list(self.record_order)[-DASHBOARD_ORDERS:]
            rows = [self.records[key] for key in reversed(recent_keys) if key in self.records]

        is_zh = self.args.lang == "zh"
        header_title = "Variational <-> Lighter"
        mode_label = "模式" if is_zh else "mode"
        quote_title = "最优买一 / 卖一" if is_zh else "Best Bid / Ask"
        col_exchange = "交易所" if is_zh else "Exchange"
        col_bid = "买一" if is_zh else "Bid"
        col_ask = "卖一" if is_zh else "Ask"
        col_book_spread = "买卖价差" if is_zh else "Bid/Ask Spread"
        col_book_spread_pct = "买卖价差%" if is_zh else "Bid/Ask Spread %"
        spread_title = "价差" if is_zh else "Spreads"
        col_metric = "指标" if is_zh else "Metric"
        col_formula = "公式" if is_zh else "Formula"
        col_value_pct = "当前值%" if is_zh else "Value %"
        col_median_5m_pct = "5分钟中位数%" if is_zh else "Median 5m %"
        col_median_30m_pct = "30分钟中位数%" if is_zh else "Median 30m %"
        col_median_1h_pct = "1小时中位数%" if is_zh else "Median 1h %"
        metric_long_short = "做多 Var / 做空 Lighter" if is_zh else "Long Var / Short Lighter"
        metric_short_long = "做空 Var / 做多 Lighter" if is_zh else "Short Var / Long Lighter"
        health_title = "健康状态" if is_zh else "Health"
        col_component = "组件" if is_zh else "Component"
        col_health_status = "状态" if is_zh else "Status"
        col_health_detail = "详情" if is_zh else "Detail"
        orders_title = "最近订单（最新在前）" if is_zh else "Recent Orders (latest first)"
        col_trade_id = "订单ID" if is_zh else "Trade ID"
        col_side = "方向" if is_zh else "Side"
        col_qty = "数量" if is_zh else "Qty"
        col_var_fill_px = "Var 成交价" if is_zh else "Var Fill Px"
        col_lighter_fill_px = "Lighter 成交价" if is_zh else "Lighter Fill Px"
        col_stage = "处理阶段" if is_zh else "Stage"
        col_stage_flow = "阶段轨迹" if is_zh else "Stage Flow"
        col_failure = "失败原因" if is_zh else "Failure"
        col_notional = "名义金额" if is_zh else "Notional"
        col_fill_diff = "成交价差(按方向)" if is_zh else "Fill Diff (Directional)"
        col_fill_diff_pct = "成交价差%(按方向)" if is_zh else "Fill Diff % (Directional)"
        col_plan_slippage = "计划/实价偏差" if is_zh else "Plan/Fill Diff"
        col_edge_bps = "对冲偏离bps" if is_zh else "Hedge Edge bps"
        col_latency_ms = "成交耗时ms" if is_zh else "Fill Latency ms"
        no_orders_text = "（暂无订单）" if is_zh else "(no tracked orders yet)"
        variational_label = "Variational"
        lighter_label = "Lighter"
        mode_color = "green" if self.is_live_mode() else ("yellow" if self.is_dry_run_mode() else "cyan")
        risk_guard_label = "风控" if is_zh else "risk_guard"
        risk_guard_text = (
            f"{risk_guard_label}(max_base={self.risk_guard_max_base_amount}, "
            f"max_dev_bps={self.risk_guard_max_price_deviation_bps})"
        )

        header = Panel(
            f"[bold]{header_title}[/bold] | [bold]{self.ticker}[/bold] | "
            f"[bold {mode_color}]{mode_label}={self.mode}[/] | "
            f"[bold {self._status_color(health.overall)}]health={health.overall}[/] | "
            f"{risk_guard_text} | {utc_now()}",
            border_style=self._status_color(health.overall),
        )

        health_table = Table(title=health_title, show_header=True, expand=True)
        health_table.add_column(col_component, style="bold")
        health_table.add_column(col_health_status)
        health_table.add_column(col_health_detail)
        for component, status, detail in health.components:
            color = self._status_color(status)
            health_table.add_row(component, f"[{color}]{status}[/{color}]", detail)
        health_table.add_row(
            "risk_guard",
            "configured",
            (
                f"max_base_amount={self.risk_guard_max_base_amount}, "
                f"max_price_deviation_bps={self.risk_guard_max_price_deviation_bps}"
            ),
        )

        quote_table = Table(title=quote_title, show_header=True, expand=True)
        quote_table.add_column(col_exchange, style="bold")
        quote_table.add_column(col_bid, justify="right")
        quote_table.add_column(col_ask, justify="right")
        quote_table.add_column(col_book_spread, justify="right")
        quote_table.add_column(col_book_spread_pct, justify="right")
        quote_table.add_row(
            f"{variational_label} ({quote_asset or self.variational_ticker})",
            self._fmt_price(var_bid),
            self._fmt_price(var_ask),
            self._fmt_price(var_book_spread),
            self._fmt_pct(var_book_spread_pct),
        )
        quote_table.add_row(
            lighter_label,
            self._fmt_price(lighter_bid),
            self._fmt_price(lighter_ask),
            self._fmt_price(lighter_book_spread),
            self._fmt_pct(lighter_book_spread_pct),
        )

        spread_table = Table(title=spread_title, show_header=True, expand=True)
        spread_table.add_column(col_metric, style="bold")
        spread_table.add_column(col_formula)
        spread_table.add_column(col_value_pct, justify="right")
        spread_table.add_column(col_median_5m_pct, justify="right")
        spread_table.add_column(col_median_30m_pct, justify="right")
        spread_table.add_column(col_median_1h_pct, justify="right")
        spread_table.add_row(
            metric_long_short,
            "lighter_bid - var_ask",
            self._fmt_signal_pct(
                long_var_short_lighter_pct,
                spread_color_baseline,
                long_pct_median_5m,
                long_pct_median_30m,
                long_pct_median_1h,
            ),
            self._fmt_median_pct(long_pct_median_5m),
            self._fmt_median_pct(long_pct_median_30m),
            self._fmt_median_pct(long_pct_median_1h),
        )
        spread_table.add_row(
            metric_short_long,
            "var_bid - lighter_ask",
            self._fmt_signal_pct(
                short_var_long_lighter_pct,
                spread_color_baseline,
                short_pct_median_5m,
                short_pct_median_30m,
                short_pct_median_1h,
            ),
            self._fmt_median_pct(short_pct_median_5m),
            self._fmt_median_pct(short_pct_median_30m),
            self._fmt_median_pct(short_pct_median_1h),
        )

        orders_table = Table(title=orders_title, show_header=True, expand=True)
        orders_table.add_column(col_trade_id)
        orders_table.add_column(col_side)
        orders_table.add_column(col_qty, justify="right")
        orders_table.add_column(col_var_fill_px, justify="right")
        orders_table.add_column(col_lighter_fill_px, justify="right")
        orders_table.add_column(col_stage)
        orders_table.add_column(col_stage_flow)
        orders_table.add_column(col_failure)
        orders_table.add_column(col_notional, justify="right")
        orders_table.add_column(col_fill_diff, justify="right")
        orders_table.add_column(col_fill_diff_pct, justify="right")
        orders_table.add_column(col_plan_slippage, justify="right")
        orders_table.add_column(col_edge_bps, justify="right")
        orders_table.add_column(col_latency_ms, justify="right")

        if not rows:
            orders_table.add_row(
                no_orders_text,
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
            )
        else:
            for row in rows:
                payload = row.to_payload()
                trade_display = row.trade_id[:10] if row.trade_id else row.trade_key[:10]
                fill_diff, fill_diff_pct = self._fill_diff_by_direction(
                    row.side,
                    row.var_fill_price,
                    row.lighter_fill_price,
                )
                notional = self._notional_value(row.qty, row.var_fill_price)
                plan_fill_diff, _ = self._price_diff(row.dry_run_plan_price, row.lighter_fill_price)
                side_zh, side_en = self._direction_labels(row.side)
                side_display = side_zh if is_zh else side_en
                is_risk_blocked = self._is_risk_guard_failure(
                    payload.get("failure_reason"),
                    payload.get("failure_stage"),
                )
                trade_style = "[bold red]{}[/bold red]" if is_risk_blocked else "{}"
                stage_style = "[bold yellow]{}[/bold yellow]" if is_risk_blocked else "{}"
                failure_style = "[bold red]{}[/bold red]" if is_risk_blocked else "{}"
                orders_table.add_row(
                    trade_style.format(trade_display),
                    side_display,
                    self._fmt_price(row.qty),
                    payload["variational_filled_price"] or "-",
                    payload["lighter_filled_price"] or "-",
                    stage_style.format(self._fmt_stage(payload["processing_stage"])),
                    stage_style.format(self._fmt_stage_history(payload.get("stage_history"))),
                    failure_style.format(self._fmt_failure(payload["failure_reason"])),
                    self._fmt_price(notional),
                    self._fmt_price(fill_diff),
                    self._fmt_pct(fill_diff_pct),
                    self._fmt_price(plan_fill_diff),
                    self._fmt_price(row.live_edge_bps),
                    self._fmt_price(row.live_fill_latency_ms),
                )

        return Group(header, health_table, quote_table, spread_table, orders_table)

    async def export_trade_records_csv(self) -> None:
        if self.trade_records_csv_file is None:
            return

        async with self._record_lock:
            keys = list(self.record_order)
            rows: list[dict[str, Any]] = []
            for key in keys:
                record = self.records.get(key)
                if record is None:
                    continue
                payload = record.to_payload()
                fill_diff, fill_diff_pct = self._fill_diff_by_direction(
                    record.side,
                    record.var_fill_price,
                    record.lighter_fill_price,
                )
                var_notional = self._notional_value(record.qty, record.var_fill_price)
                lighter_notional = self._notional_value(record.qty, record.lighter_fill_price)
                plan_fill_diff, plan_fill_diff_pct = self._price_diff(record.dry_run_plan_price, record.lighter_fill_price)
                ref_bid_fill_diff, ref_bid_fill_diff_pct = self._price_diff(record.lighter_reference_bid, record.lighter_fill_price)
                ref_ask_fill_diff, ref_ask_fill_diff_pct = self._price_diff(record.lighter_reference_ask, record.lighter_fill_price)
                side_zh, side_en = self._direction_labels(record.side)
                rows.append(
                    {
                        "record_kind": payload["record_kind"],
                        "trade_key": record.trade_key,
                        "trade_id": record.trade_id,
                        "synthetic_eager_fill": payload["synthetic_eager_fill"],
                        "matched_variational_trade_id": payload["matched_variational_trade_id"],
                        "auto_live_cycle_id": payload["auto_live_cycle_id"],
                        "auto_live_role": payload["auto_live_role"],
                        "auto_live_merge_path": payload["auto_live_merge_path"],
                        "asset": record.asset,
                        "side_raw": record.side,
                        "direction_zh": side_zh,
                        "direction_en": side_en,
                        "qty": decimal_to_str(record.qty),
                        "variational_filled_price": payload["variational_filled_price"],
                        "variational_filled_at": payload["variational_filled_at"],
                            "lighter_order_side": payload["lighter_order_side"],
                            "lighter_client_order_id": payload["lighter_client_order_id"],
                            "lighter_submit_transport": payload["lighter_submit_transport"],
                            "lighter_order_mode": payload["lighter_order_mode"],
                            "lighter_filled_price": payload["lighter_filled_price"],
                        "lighter_filled_at": payload["lighter_filled_at"],
                        "variational_notional": decimal_to_str(var_notional),
                        "lighter_notional": decimal_to_str(lighter_notional),
                        "live_notional_usd": payload["live_notional_usd"],
                        "live_edge_bps": payload["live_edge_bps"],
                        "live_fill_latency_ms": payload["live_fill_latency_ms"],
                        "live_var_fill_seen_at": payload["live_var_fill_seen_at"],
                        "live_var_event_to_seen_ms": payload["live_var_event_to_seen_ms"],
                        "live_plan_started_at": payload["live_plan_started_at"],
                        "live_plan_ready_at": payload["live_plan_ready_at"],
                        "live_submit_started_at": payload["live_submit_started_at"],
                        "live_submit_sent_at": payload["live_submit_sent_at"],
                        "live_var_seen_to_plan_start_ms": payload["live_var_seen_to_plan_start_ms"],
                        "live_plan_latency_ms": payload["live_plan_latency_ms"],
                        "live_plan_ready_to_submit_start_ms": payload["live_plan_ready_to_submit_start_ms"],
                        "live_submit_call_latency_ms": payload["live_submit_call_latency_ms"],
                        "live_submit_sent_to_fill_ms": payload["live_submit_sent_to_fill_ms"],
                        "live_var_seen_to_lighter_fill_ms": payload["live_var_seen_to_lighter_fill_ms"],
                        "hedge_completion_status": payload["hedge_completion_status"],
                        "rollback_action": payload["rollback_action"],
                        "fill_diff_var_minus_lighter": decimal_to_str(fill_diff),
                        "fill_diff_pct_vs_var": decimal_to_str(fill_diff_pct),
                        "mode": payload["mode"],
                        "lighter_reference_bid": payload["lighter_reference_bid"],
                        "lighter_reference_ask": payload["lighter_reference_ask"],
                        "dry_run_plan_side": payload["dry_run_plan_side"],
                        "dry_run_plan_price": payload["dry_run_plan_price"],
                        "dry_run_plan_base_amount": payload["dry_run_plan_base_amount"],
                        "plan_vs_lighter_fill_diff": decimal_to_str(plan_fill_diff),
                        "plan_vs_lighter_fill_diff_pct": decimal_to_str(plan_fill_diff_pct),
                        "ref_bid_vs_lighter_fill_diff": decimal_to_str(ref_bid_fill_diff),
                        "ref_bid_vs_lighter_fill_diff_pct": decimal_to_str(ref_bid_fill_diff_pct),
                        "ref_ask_vs_lighter_fill_diff": decimal_to_str(ref_ask_fill_diff),
                        "ref_ask_vs_lighter_fill_diff_pct": decimal_to_str(ref_ask_fill_diff_pct),
                        "processing_stage": payload["processing_stage"],
                        "stage_history": " -> ".join(payload.get("stage_history") or []),
                        "failure_stage": payload["failure_stage"],
                        "failure_reason": payload["failure_reason"],
                        "record_created_at": payload["record_created_at"],
                        "last_updated_at": payload["last_updated_at"],
                        "hedge_error": payload["hedge_error"],
                        "last_variational_status": payload["last_variational_status"],
                    }
                )

        snapshot_sig = json.dumps(rows, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        if snapshot_sig == self._trade_records_snapshot_sig:
            return

        fieldnames = [
            "record_kind",
            "trade_key",
            "trade_id",
            "synthetic_eager_fill",
            "matched_variational_trade_id",
            "auto_live_cycle_id",
            "auto_live_role",
            "auto_live_merge_path",
            "asset",
            "side_raw",
            "direction_zh",
            "direction_en",
            "qty",
            "variational_filled_price",
            "variational_filled_at",
            "lighter_order_side",
            "lighter_client_order_id",
            "lighter_submit_transport",
            "lighter_order_mode",
            "lighter_filled_price",
            "lighter_filled_at",
            "variational_notional",
            "lighter_notional",
            "live_notional_usd",
            "live_edge_bps",
            "live_fill_latency_ms",
            "live_var_fill_seen_at",
            "live_var_event_to_seen_ms",
            "live_plan_started_at",
            "live_plan_ready_at",
            "live_submit_started_at",
            "live_submit_sent_at",
            "live_var_seen_to_plan_start_ms",
            "live_plan_latency_ms",
            "live_plan_ready_to_submit_start_ms",
            "live_submit_call_latency_ms",
            "live_submit_sent_to_fill_ms",
            "live_var_seen_to_lighter_fill_ms",
            "hedge_completion_status",
            "rollback_action",
            "fill_diff_var_minus_lighter",
            "fill_diff_pct_vs_var",
            "mode",
            "lighter_reference_bid",
            "lighter_reference_ask",
            "dry_run_plan_side",
            "dry_run_plan_price",
            "dry_run_plan_base_amount",
            "plan_vs_lighter_fill_diff",
            "plan_vs_lighter_fill_diff_pct",
            "ref_bid_vs_lighter_fill_diff",
            "ref_bid_vs_lighter_fill_diff_pct",
            "ref_ask_vs_lighter_fill_diff",
            "ref_ask_vs_lighter_fill_diff_pct",
            "processing_stage",
            "stage_history",
            "failure_stage",
            "failure_reason",
            "record_created_at",
            "last_updated_at",
            "hedge_error",
            "last_variational_status",
        ]
        async with self._trade_csv_write_lock:
            if snapshot_sig == self._trade_records_snapshot_sig:
                return
            await asyncio.to_thread(self._write_csv_rows, self.trade_records_csv_file, fieldnames, rows)
            self._trade_records_snapshot_sig = snapshot_sig

    @staticmethod
    def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)

    async def dashboard_loop(self) -> None:
        refresh_interval = DASHBOARD_REFRESH_SECONDS
        refresh_per_second = max(1, int(round(1.0 / refresh_interval)))
        initial_render = await self.render_dashboard()
        await self.export_trade_records_csv()
        with Live(
            initial_render,
            console=self.dashboard_console,
            refresh_per_second=refresh_per_second,
            screen=True,
        ) as live:
            while not self.stop_flag:
                await asyncio.sleep(refresh_interval)
                live.update(await self.render_dashboard())
                await self.export_trade_records_csv()

    async def run(self) -> None:
        self.setup_signal_handlers()
        diagnostics = self.run_startup_diagnostics()
        self.print_startup_diagnostics(diagnostics)
        self.log_startup_diagnostics(diagnostics)
        if diagnostics.blocking_errors:
            raise RuntimeError("Startup diagnostics failed")
        await self.runtime.start()
        self.print_startup_next_steps()
        self.logger.info(
            "Listening for Variational forwarder events on ws://%s:%s, ws://%s:%s, command ws://%s:%s",
            FORWARDER_HOST,
            FORWARDER_WS_PORT,
            FORWARDER_HOST,
            FORWARDER_REST_PORT,
            FORWARDER_HOST,
            FORWARDER_COMMAND_PORT,
        )

        variational_ready = await self.wait_for_variational_ready()
        if variational_ready:
            self.logger.info("Variational heartbeat is live")
        else:
            self.logger.warning(
                "Variational heartbeat did not arrive within %ss; continuing in stale state until browser events appear",
                READY_TIMEOUT_SECONDS,
            )
        if self.requires_lighter_trading_credentials():
            self.load_lighter_trading_credentials()
            self.initialize_lighter_client()
        if self.requires_lighter_market_data():
            initial_asset = await self.wait_for_ticker_resolution()
            await self.activate_asset(initial_asset, reason="startup")
        if self.lighter_prewarm_submit_ws:
            try:
                await self.prewarm_lighter_submit_ws()
            except Exception:
                self.logger.exception("lighter_submit_ws_prewarm_failed")
                raise
        if self.is_live_inventory_enabled() and not self.live_inventory_dry_decisions:
            try:
                await self.preflight_variational_api_command_client(initial_asset)
                self.logger.info("variational_api_command_client_preflight_passed asset=%s", initial_asset)
            except Exception:
                self.logger.exception("variational_api_command_client_preflight_failed asset=%s", initial_asset)
                raise
        if self.is_live_inventory_enabled():
            self.sync_live_inventory_memory_from_state()

        self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()
        self.trade_event_min_timestamp = datetime.now(timezone.utc)
        self.logger.info("Tracking new Variational trade events from seq>%s", self.trade_event_cursor)

        self.trade_task = self.track_background_task(asyncio.create_task(self.trade_loop()), "trade_loop")
        if self.requires_lighter_market_data():
            self.spread_task = self.track_background_task(asyncio.create_task(self.spread_loop()), "spread_loop")
        if self.is_live_mode():
            self.watchdog_task = self.track_background_task(asyncio.create_task(self.watchdog_live_submissions()), "watchdog_live_submissions")
        if self.is_paper_mode() or self.is_auto_live_enabled() or self.is_live_inventory_enabled():
            self.paper_task = self.track_background_task(asyncio.create_task(self.paper_loop()), "paper_loop")
        self.dashboard_task = self.track_background_task(asyncio.create_task(self.dashboard_loop()), "dashboard_loop")

        while not self.stop_flag:
            await asyncio.sleep(0.25)

    async def close(self) -> None:
        self.stop_flag = True

        if self.dashboard_task and not self.dashboard_task.done():
            self.dashboard_task.cancel()
            await asyncio.gather(self.dashboard_task, return_exceptions=True)

        if self.trade_task and not self.trade_task.done():
            self.trade_task.cancel()
            await asyncio.gather(self.trade_task, return_exceptions=True)

        if self.spread_task and not self.spread_task.done():
            self.spread_task.cancel()
            await asyncio.gather(self.spread_task, return_exceptions=True)

        if self.paper_task and not self.paper_task.done():
            self.paper_task.cancel()
            await asyncio.gather(self.paper_task, return_exceptions=True)

        if self.watchdog_task and not self.watchdog_task.done():
            self.watchdog_task.cancel()
            await asyncio.gather(self.watchdog_task, return_exceptions=True)

        if self.lighter_ws_task and not self.lighter_ws_task.done():
            self.lighter_ws_task.cancel()
            await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

        if self.lighter_client is not None:
            close_method = getattr(self.lighter_client, "close", None)
            if callable(close_method):
                with contextlib.suppress(Exception):
                    close_result = close_method()
                    if asyncio.iscoroutine(close_result):
                        await close_result

        if self._lighter_submit_ws is not None:
            with contextlib.suppress(Exception):
                await self._lighter_submit_ws.close()
            self._lighter_submit_ws = None

        if self._var_command_ws is not None:
            with contextlib.suppress(Exception):
                await self._var_command_ws.close()
            self._var_command_ws = None

        await self.runtime.stop()


class LiveInventoryBasisState:
    def __init__(self, *, half_life_seconds: float, warmup_samples: int, gap_reset_seconds: float, sigma_floor_bps: float) -> None:
        if half_life_seconds <= 0:
            raise ValueError("half_life_seconds must be > 0")
        if warmup_samples <= 0:
            raise ValueError("warmup_samples must be > 0")
        if gap_reset_seconds <= 0:
            raise ValueError("gap_reset_seconds must be > 0")
        if sigma_floor_bps < 0:
            raise ValueError("sigma_floor_bps must be >= 0")
        self.half_life_seconds = half_life_seconds
        self.warmup_samples = warmup_samples
        self.gap_reset_seconds = gap_reset_seconds
        self.sigma_floor_bps = sigma_floor_bps
        self.mean: float | None = None
        self.var = 0.0
        self.seen = 0
        self.last_ts: float | None = None
        self.signal_mean: float | None = None
        self.signal_sigma: float | None = None

    def update(self, ts: float, basis_bps: float) -> tuple[float, bool]:
        if self.last_ts is not None and ts - self.last_ts > self.gap_reset_seconds:
            self.mean = None
            self.var = 0.0
            self.seen = 0
        dt = ts - self.last_ts if self.last_ts is not None else None
        self.last_ts = ts
        if self.mean is None or self.seen < self.warmup_samples:
            z = 0.0
            warm = False
        else:
            sigma = math.sqrt(self.var)
            if sigma <= self.sigma_floor_bps:
                z = 0.0
                warm = False
            else:
                z = (basis_bps - self.mean) / sigma
                warm = True
        self.signal_mean = self.mean
        self.signal_sigma = math.sqrt(self.var) if self.mean is not None else None
        if self.mean is None:
            self.mean = basis_bps
            self.var = 0.0
            self.seen = 1
        else:
            alpha = 1.0 - 0.5 ** (max(dt or 1.0, 1e-3) / self.half_life_seconds)
            diff = basis_bps - self.mean
            self.mean += alpha * diff
            self.var = (1.0 - alpha) * (self.var + alpha * diff * diff)
            self.seen += 1
        return z, warm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Track Variational order lifecycle and optionally auto-hedge on Lighter (ticker auto-detected)."
    )
    parser.add_argument(
        "--lang",
        choices=["zh", "en"],
        default="zh",
        help="Dashboard language: zh (Chinese) or en (English). Default: zh",
    )
    parser.add_argument(
        "--mode",
        choices=MODE_CHOICES,
        default=MODE_OBSERVE,
        help="Runtime mode: observe only, dry-run hedge simulation, or live hedge execution.",
    )
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        help="Required together with --mode live to allow real Lighter hedge orders.",
    )
    parser.add_argument(
        "--risk-guard-max-base-amount",
        type=int,
        default=RISK_GUARD_MAX_BASE_AMOUNT,
        help=(
            "Maximum allowed Lighter base amount for hedge planning. "
            f"Default: {RISK_GUARD_MAX_BASE_AMOUNT}"
        ),
    )
    parser.add_argument(
        "--risk-guard-max-price-deviation-bps",
        type=float,
        default=float(RISK_GUARD_MAX_PRICE_DEVIATION_BPS),
        help=(
            "Maximum allowed deviation between hedge plan price and Variational fill price, in bps. "
            f"Default: {RISK_GUARD_MAX_PRICE_DEVIATION_BPS}"
        ),
    )
    parser.add_argument(
        "--live-max-notional-usd",
        type=float,
        default=float(DEFAULT_LIVE_MAX_NOTIONAL_USD),
        help=(
            "Maximum allowed per-order live notional in quote currency before blocking the hedge. Set 0 to disable. "
            f"Default: {DEFAULT_LIVE_MAX_NOTIONAL_USD}"
        ),
    )
    parser.add_argument(
        "--live-max-qty",
        type=float,
        default=float(DEFAULT_LIVE_MAX_QTY),
        help=(
            "Maximum allowed per-order live qty before blocking the hedge. Set 0 to disable. "
            f"Default: {DEFAULT_LIVE_MAX_QTY}"
        ),
    )
    parser.add_argument(
        "--live-require-min-edge-bps",
        type=float,
        default=float(DEFAULT_LIVE_REQUIRE_MIN_EDGE_BPS),
        help=(
            "Minimum required hedge deviation in bps for live orders. "
            f"Default: {DEFAULT_LIVE_REQUIRE_MIN_EDGE_BPS}"
        ),
    )
    parser.add_argument(
        "--live-cooldown-seconds",
        type=float,
        default=DEFAULT_LIVE_COOLDOWN_SECONDS,
        help=(
            "Minimum cooldown between real live hedge submissions in seconds. "
            f"Default: {DEFAULT_LIVE_COOLDOWN_SECONDS}"
        ),
    )
    parser.add_argument(
        "--live-submit-timeout-seconds",
        type=float,
        default=DEFAULT_LIVE_SUBMIT_TIMEOUT_SECONDS,
        help=(
            "Maximum time to wait for a live hedge to reach lighter_filled before marking it timed out. "
            f"Default: {DEFAULT_LIVE_SUBMIT_TIMEOUT_SECONDS}"
        ),
    )
    parser.add_argument(
        "--variational-submit-transport",
        choices=VARIATIONAL_SUBMIT_TRANSPORT_CHOICES,
        default=VARIATIONAL_SUBMIT_TRANSPORT_DOM,
        help="Transport for auto-live Variational submits. Default: dom; use api to call the page API through the Chrome session.",
    )
    parser.add_argument(
        "--variational-api-max-slippage",
        type=float,
        default=DEFAULT_VARIATIONAL_API_MAX_SLIPPAGE,
        help=f"Max slippage sent to Variational page API market orders. Default: {DEFAULT_VARIATIONAL_API_MAX_SLIPPAGE}",
    )
    parser.add_argument(
        "--lighter-submit-transport",
        choices=LIGHTER_SUBMIT_TRANSPORT_CHOICES,
        default=LIGHTER_SUBMIT_TRANSPORT_HTTP,
        help="Transport for real Lighter order submission. Default: http; use ws to send signed tx via Lighter WebSocket.",
    )
    parser.add_argument(
        "--lighter-order-mode",
        choices=LIGHTER_ORDER_MODE_CHOICES,
        default=LIGHTER_ORDER_MODE_LIMIT_GTT,
        help="Lighter live order mode. Default: limit-gtt; use market-ioc for lower-latency taker execution.",
    )
    parser.add_argument(
        "--lighter-prewarm-submit-ws",
        action="store_true",
        help="Pre-connect the Lighter submit WebSocket at startup when using --lighter-submit-transport ws.",
    )
    parser.add_argument(
        "--live-allowed-assets",
        default="BTC,SOL,ETH",
        help="Comma-separated allowlist for live trading assets. Default: BTC,SOL,ETH",
    )
    parser.add_argument(
        "--live-allowed-sides",
        default="buy,sell",
        help="Comma-separated allowlist for Variational live fill sides. Use buy to only hedge Var buys. Default: buy,sell",
    )
    parser.add_argument(
        "--auto-live-entry",
        action="store_true",
        help="In live mode, automatically submit a Variational entry when the current paper entry signal qualifies.",
    )
    parser.add_argument(
        "--auto-live-exit",
        action="store_true",
        help="In live mode, automatically submit a Variational exit when the current paper exit signal qualifies.",
    )
    parser.add_argument(
        "--auto-live-eager-hedge",
        action="store_true",
        help="In live mode, after an auto-submitted Variational order, immediately start the matching Lighter hedge without waiting for the Variational fill event.",
    )
    parser.add_argument(
        "--auto-live-skip-entry-preview",
        action="store_true",
        help="Skip the extra confirm=false Variational entry preview call. The confirm=true click still keeps expectedMinBtcQty protection.",
    )
    parser.add_argument(
        "--auto-live-command-timeout-seconds",
        type=float,
        default=DEFAULT_AUTO_LIVE_COMMAND_TIMEOUT_SECONDS,
        help=f"Timeout for command-broker PLACE_ORDER calls used by auto-live. Default: {DEFAULT_AUTO_LIVE_COMMAND_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--auto-live-match-window-seconds",
        type=float,
        default=DEFAULT_AUTO_LIVE_MATCH_WINDOW_SECONDS,
        help=f"How long to keep an eager Lighter hedge record available for later Variational fill matching. Default: {DEFAULT_AUTO_LIVE_MATCH_WINDOW_SECONDS}",
    )
    parser.add_argument(
        "--auto-live-min-holding-seconds",
        type=float,
        default=DEFAULT_AUTO_LIVE_MIN_HOLDING_SECONDS,
        help=f"Minimum holding time before auto-live is allowed to submit an exit on spread reversion. Default: {DEFAULT_AUTO_LIVE_MIN_HOLDING_SECONDS}",
    )
    parser.add_argument(
        "--auto-live-entry-max-precheck-edge-bps",
        type=float,
        default=0.0,
        help="Maximum Lighter hedge precheck edge bps allowed for auto-live entries. Set 0 to disable. Default: 0",
    )
    parser.add_argument(
        "--auto-live-entry-min-actionable-edge-bps",
        type=float,
        default=0.0,
        help="Minimum actionable entry edge bps from current Var execution price versus Lighter top of book. Set 0 to disable. Default: 0",
    )
    parser.add_argument(
        "--auto-live-disable-short-var-long-lighter",
        action="store_true",
        help="Disable auto-live entries for short_var_long_lighter direction.",
    )
    parser.add_argument(
        "--auto-live-cooldown-seconds",
        type=float,
        default=DEFAULT_AUTO_LIVE_COOLDOWN_SECONDS,
        help=f"Cooldown after an auto-live exit before another auto-live entry is allowed. Default: {DEFAULT_AUTO_LIVE_COOLDOWN_SECONDS}",
    )
    parser.add_argument(
        "--auto-live-max-cycles",
        type=int,
        default=DEFAULT_AUTO_LIVE_MAX_CYCLES,
        help=f"Maximum completed auto-live entry/exit cycles to allow in one process. Set 0 to disable the limit. Default: {DEFAULT_AUTO_LIVE_MAX_CYCLES}",
    )
    parser.add_argument(
        "--auto-live-i-confirm-flat-start",
        action="store_true",
        help="Required with --auto-live-entry to confirm Var and Lighter positions were manually checked flat before startup.",
    )
    parser.add_argument(
        "--auto-live-reset-state-after-manual-flat",
        action="store_true",
        help="After manually confirming Var and Lighter are flat, reset log/auto_live_state.json to flat during startup.",
    )
    parser.add_argument("--live-inventory", action="store_true", help="Enable live inventory V1 guards. Real order submission is implemented in later steps.")
    parser.add_argument(
        "--live-inventory-dry-decisions",
        action="store_true",
        help="Run live inventory V1 decision logic and state/logging without submitting real orders.",
    )
    parser.add_argument(
        "--live-inventory-signal-mode",
        choices=LIVE_INVENTORY_SIGNAL_CHOICES,
        default=LIVE_INVENTORY_SIGNAL_SNAPSHOT,
        help="Signal source for live inventory. Default: snapshot; basis is dry-decision only.",
    )
    parser.add_argument(
        "--live-inventory-i-accept-basis-real-diagnostic",
        action="store_true",
        help="Allow exactly one 20u ETH basis real-submit diagnostic cycle after manually confirming both venues are flat.",
    )
    parser.add_argument(
        "--live-inventory-i-confirm-flat-start",
        action="store_true",
        help="Required with --live-inventory to confirm Var and Lighter BTC positions were manually checked flat before startup.",
    )
    parser.add_argument(
        "--live-inventory-i-accept-open-state-resume",
        action="store_true",
        help="Resume managing an existing live_inventory_state.json open position after manually confirming both venues match the state.",
    )
    parser.add_argument(
        "--live-inventory-reset-state-after-manual-flat",
        action="store_true",
        help="After manually confirming Var and Lighter are flat, reset log/live_inventory_state.json to flat during startup.",
    )
    parser.add_argument("--live-inventory-lot-notional-usd", type=float, default=20.0)
    parser.add_argument("--live-inventory-max-lots", type=int, default=1)
    parser.add_argument("--live-inventory-max-total-lots", type=int, default=1)
    parser.add_argument("--live-inventory-entry-bps", type=float, default=50.0)
    parser.add_argument(
        "--live-inventory-i-accept-diagnostic-low-entry-bps",
        action="store_true",
        help="Allow real-submit live inventory entry below 30bps for one-lot diagnostic data collection only.",
    )
    parser.add_argument("--live-inventory-exit-bps", type=float, default=10.0)
    parser.add_argument("--live-inventory-max-var-spread-bps", type=float, default=5.0)
    parser.add_argument(
        "--live-inventory-max-var-snapshot-age-seconds",
        type=float,
        default=5.0,
        help="Maximum age of the Variational quote snapshot allowed for live inventory entry. Default: 5.0",
    )
    parser.add_argument(
        "--live-inventory-refresh-var-quote-before-entry",
        action="store_true",
        help="After a live inventory entry candidate is found, fetch a fresh Variational indicative quote, recompute edge, and reuse its quoteId for the entry order.",
    )
    parser.add_argument(
        "--live-inventory-dynamic-entry-buffer-bps",
        type=float,
        default=5.0,
        help="Extra live inventory entry cushion added on top of Var spread and Lighter depth slippage. Default: 5.0",
    )
    parser.add_argument(
        "--live-inventory-ignore-recent-execution-loss-buffer-for-diagnostics",
        action="store_true",
        help="Diagnostic real-submit only: ignore historical execution-loss buffer in the dynamic entry threshold without disabling other live inventory guards.",
    )
    parser.add_argument(
        "--live-inventory-max-lighter-slippage-bps",
        type=float,
        default=3.0,
        help="Maximum estimated Lighter order-book slippage allowed for live inventory entry. Default: 3.0",
    )
    parser.add_argument("--live-inventory-min-hold-samples", type=int, default=3)
    parser.add_argument("--live-inventory-max-hold-samples", type=int, default=300)
    parser.add_argument("--live-inventory-max-unrealized-loss-bps", type=float, default=25.0)
    parser.add_argument("--live-inventory-max-cycles", type=int, default=1)
    parser.add_argument("--live-inventory-basis-z-entry", type=float, default=4.0)
    parser.add_argument("--live-inventory-basis-z-exit", type=float, default=0.0)
    parser.add_argument("--live-inventory-basis-min-entry-edge-bps", type=float, default=7.0)
    parser.add_argument("--live-inventory-basis-max-entry-roundtrip-cost-bps", type=float, default=4.0)
    parser.add_argument(
        "--live-inventory-basis-min-abs-entry-bps",
        type=float,
        default=0.0,
        help="Minimum absolute basis bps required for ETH basis entries. Long Var/short Lighter requires basis <= -value; short Var/long Lighter requires basis >= value. Set 0 to disable. Default: 0",
    )
    parser.add_argument("--live-inventory-basis-min-exit-pnl-bps", type=float, default=1.0)
    parser.add_argument(
        "--live-inventory-basis-exit-safety-buffer-bps",
        type=float,
        default=0.0,
        help="Extra estimated PnL bps required above --live-inventory-basis-min-exit-pnl-bps before basis exit. Default: 0",
    )
    parser.add_argument(
        "--live-inventory-basis-max-hold-action",
        choices=("exit", "warn"),
        default="exit",
        help="Basis live-inventory action when max hold samples is reached. Default exits; 'warn' logs and waits for z-score exit or stop-loss.",
    )
    parser.add_argument(
        "--live-inventory-i-accept-basis-addon-diagnostic",
        action="store_true",
        help="Diagnostic real-submit only: allow at most one same-direction ETH basis add-on lot when basis expands further.",
    )
    parser.add_argument("--live-inventory-basis-addon-min-basis-improvement-bps", type=float, default=1.5)
    parser.add_argument("--live-inventory-basis-half-life-seconds", type=float, default=300.0)
    parser.add_argument("--live-inventory-basis-warmup-samples", type=int, default=120)
    parser.add_argument("--live-inventory-basis-gap-reset-seconds", type=float, default=30.0)
    parser.add_argument("--live-inventory-basis-sigma-floor-bps", type=float, default=0.3)
    parser.add_argument(
        "--paper-notional-usd",
        type=float,
        default=float(DEFAULT_PAPER_NOTIONAL_USD),
        help=f"Paper-mode simulated notional per opportunity. Default: {DEFAULT_PAPER_NOTIONAL_USD}",
    )
    parser.add_argument(
        "--paper-entry-deviation-bps",
        type=float,
        default=float(DEFAULT_PAPER_ENTRY_DEVIATION_BPS),
        help=f"Paper-mode entry threshold versus 5m spread median, in bps. Default: {DEFAULT_PAPER_ENTRY_DEVIATION_BPS}",
    )
    parser.add_argument(
        "--paper-exit-deviation-bps",
        type=float,
        default=float(DEFAULT_PAPER_EXIT_DEVIATION_BPS),
        help=f"Paper-mode exit threshold versus entry median, in bps. Default: {DEFAULT_PAPER_EXIT_DEVIATION_BPS}",
    )
    parser.add_argument(
        "--paper-max-var-half-spread-bps",
        type=float,
        default=float(DEFAULT_PAPER_MAX_VAR_HALF_SPREAD_BPS),
        help=f"Maximum allowed Variational half spread for paper entries, in bps. Default: {DEFAULT_PAPER_MAX_VAR_HALF_SPREAD_BPS}",
    )
    parser.add_argument(
        "--paper-max-holding-seconds",
        type=float,
        default=DEFAULT_PAPER_MAX_HOLDING_SECONDS,
        help=f"Maximum simulated holding time before timeout exit. Default: {DEFAULT_PAPER_MAX_HOLDING_SECONDS}",
    )
    parser.add_argument(
        "--paper-cooldown-seconds",
        type=float,
        default=DEFAULT_PAPER_COOLDOWN_SECONDS,
        help=f"Cooldown after a paper close before another entry. Default: {DEFAULT_PAPER_COOLDOWN_SECONDS}",
    )
    parser.add_argument(
        "--paper-min-samples",
        type=int,
        default=DEFAULT_PAPER_MIN_SAMPLES,
        help=f"Minimum 5m spread samples before paper entry. Default: {DEFAULT_PAPER_MIN_SAMPLES}",
    )
    parser.add_argument(
        "--paper-interval-seconds",
        type=float,
        default=DEFAULT_PAPER_INTERVAL_SECONDS,
        help=f"Paper-mode evaluation interval. Default: {DEFAULT_PAPER_INTERVAL_SECONDS}",
    )
    parser.add_argument(
        "--paper-fee-bps-per-leg",
        type=float,
        default=0.5,
        help="Paper-mode fee bps per leg, applied on entry and exit. Default: 0.5",
    )
    parser.add_argument(
        "--paper-latency-drift-bps",
        type=float,
        default=float(DEFAULT_PAPER_LATENCY_DRIFT_BPS),
        help=f"Paper-mode extra latency drift penalty in bps per side. Default: {DEFAULT_PAPER_LATENCY_DRIFT_BPS}",
    )
    parser.add_argument("--paper-inventory", action="store_true", help="Enable paper-only layered inventory simulation.")
    parser.add_argument("--paper-inventory-lot-notional-usd", type=float, default=50.0)
    parser.add_argument("--paper-inventory-max-lots", type=int, default=5)
    parser.add_argument("--paper-inventory-max-total-lots", type=int, default=5)
    parser.add_argument("--paper-inventory-entry-bps", type=float, default=40.0)
    parser.add_argument("--paper-inventory-exit-bps", type=float, default=10.0)
    parser.add_argument("--paper-inventory-min-hold-samples", type=int, default=3)
    parser.add_argument("--paper-inventory-latency-samples", type=int, default=0)
    args = parser.parse_args()
    if args.mode == MODE_LIVE and not args.confirm_live:
        parser.error("--mode live requires --confirm-live")
    if args.mode == MODE_LIVE and args.live_max_notional_usd <= 0:
        parser.error("--mode live requires --live-max-notional-usd to be set to a positive small-test limit")
    if args.auto_live_exit and not args.auto_live_entry:
        parser.error("--auto-live-exit currently requires --auto-live-entry so the runtime can track the live position it opened")
    if (args.auto_live_entry or args.auto_live_exit or args.auto_live_eager_hedge) and args.mode != MODE_LIVE:
        parser.error("--auto-live-entry/exit/eager-hedge only work in --mode live")
    if args.auto_live_entry and not args.auto_live_i_confirm_flat_start:
        parser.error("--auto-live-entry requires --auto-live-i-confirm-flat-start after manually confirming Var BTC = 0 and Lighter BTC = 0")
    if args.live_inventory:
        if args.mode != MODE_LIVE:
            parser.error("--live-inventory only works in --mode live")
        if args.auto_live_entry or args.auto_live_exit or args.auto_live_eager_hedge:
            parser.error("--live-inventory cannot be combined with --auto-live-entry/exit/eager-hedge")
        if not args.live_inventory_i_confirm_flat_start and not args.live_inventory_i_accept_open_state_resume:
            parser.error(
                "--live-inventory requires --live-inventory-i-confirm-flat-start after manually confirming flat, "
                "or --live-inventory-i-accept-open-state-resume after manually confirming the saved open state matches both venues"
            )
        if args.live_inventory_i_confirm_flat_start and args.live_inventory_i_accept_open_state_resume:
            parser.error("use only one of --live-inventory-i-confirm-flat-start or --live-inventory-i-accept-open-state-resume")
        allowed_assets = {asset.strip().upper() for asset in str(args.live_allowed_assets).split(",") if asset.strip()}
        if args.live_inventory_signal_mode == LIVE_INVENTORY_SIGNAL_BASIS:
            if allowed_assets != {"ETH"}:
                parser.error("--live-inventory-signal-mode basis currently requires --live-allowed-assets ETH")
            if not args.live_inventory_dry_decisions:
                if not args.live_inventory_i_accept_basis_real_diagnostic:
                    parser.error(
                        "--live-inventory-signal-mode basis real-submit requires --live-inventory-i-accept-basis-real-diagnostic"
                    )
                if args.live_inventory_lot_notional_usd != 20:
                    parser.error("basis real-submit diagnostic requires --live-inventory-lot-notional-usd 20")
                if args.live_inventory_i_accept_basis_addon_diagnostic:
                    if args.live_inventory_max_lots != 1 or args.live_inventory_max_total_lots != 2:
                        parser.error("basis add-on diagnostic requires --live-inventory-max-lots 1 --live-inventory-max-total-lots 2")
                elif args.live_inventory_max_lots != 1 or args.live_inventory_max_total_lots != 1:
                    parser.error("basis real-submit diagnostic requires --live-inventory-max-lots 1 --live-inventory-max-total-lots 1")
                if args.live_inventory_max_cycles != 1:
                    parser.error("basis real-submit diagnostic requires --live-inventory-max-cycles 1")
            elif args.live_inventory_i_accept_basis_real_diagnostic:
                parser.error("--live-inventory-i-accept-basis-real-diagnostic is only for real-submit diagnostic runs")
            elif args.live_inventory_i_accept_basis_addon_diagnostic:
                parser.error("--live-inventory-i-accept-basis-addon-diagnostic is only for real-submit diagnostic runs")
        elif allowed_assets != {"BTC"}:
            parser.error("--live-inventory V1 snapshot mode requires --live-allowed-assets BTC")
        if args.variational_submit_transport != VARIATIONAL_SUBMIT_TRANSPORT_API:
            parser.error("--live-inventory V1 requires --variational-submit-transport api")
        if args.lighter_submit_transport != LIGHTER_SUBMIT_TRANSPORT_WS:
            parser.error("--live-inventory V1 requires --lighter-submit-transport ws")
        if args.lighter_order_mode != LIGHTER_ORDER_MODE_MARKET_IOC:
            parser.error("--live-inventory V1 requires --lighter-order-mode market-ioc")
        if not args.lighter_prewarm_submit_ws:
            parser.error("--live-inventory V1 requires --lighter-prewarm-submit-ws")
        if args.live_inventory_lot_notional_usd <= 0 or args.live_inventory_lot_notional_usd > 20:
            parser.error("--live-inventory-lot-notional-usd must be > 0 and <= 20 in V1")
        if args.live_inventory_max_lots <= 0 or args.live_inventory_max_lots > 3:
            parser.error("--live-inventory-max-lots must be > 0 and <= 3 in V1")
        if args.live_inventory_max_total_lots <= 0 or args.live_inventory_max_total_lots > 3:
            parser.error("--live-inventory-max-total-lots must be > 0 and <= 3 in V1")
        if args.live_inventory_entry_bps < 0:
            parser.error("--live-inventory-entry-bps must be >= 0")
        if (
            not args.live_inventory_dry_decisions
            and args.live_inventory_entry_bps < 30
            and not args.live_inventory_i_accept_diagnostic_low_entry_bps
        ):
            parser.error("--live-inventory-entry-bps must be >= 30 in V1 real-submit mode")
        if args.live_inventory_i_accept_diagnostic_low_entry_bps and args.live_inventory_dry_decisions:
            parser.error("--live-inventory-i-accept-diagnostic-low-entry-bps is only for real-submit diagnostic runs")
        if (
            args.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics
            and (args.live_inventory_dry_decisions or not args.live_inventory_i_accept_diagnostic_low_entry_bps)
        ):
            parser.error(
                "--live-inventory-ignore-recent-execution-loss-buffer-for-diagnostics requires real-submit diagnostic acknowledgement"
            )
        if args.live_inventory_exit_bps < 0:
            parser.error("--live-inventory-exit-bps must be >= 0")
        if args.live_inventory_max_var_spread_bps <= 0:
            parser.error("--live-inventory-max-var-spread-bps must be > 0")
        if args.live_inventory_max_var_snapshot_age_seconds <= 0:
            parser.error("--live-inventory-max-var-snapshot-age-seconds must be > 0")
        if args.live_inventory_dynamic_entry_buffer_bps < 0:
            parser.error("--live-inventory-dynamic-entry-buffer-bps must be >= 0")
        if args.live_inventory_max_lighter_slippage_bps < 0:
            parser.error("--live-inventory-max-lighter-slippage-bps must be >= 0")
        if args.live_inventory_min_hold_samples < 0:
            parser.error("--live-inventory-min-hold-samples must be >= 0")
        if args.live_inventory_max_hold_samples <= 0:
            parser.error("--live-inventory-max-hold-samples must be > 0")
        if args.live_inventory_max_unrealized_loss_bps <= 0:
            parser.error("--live-inventory-max-unrealized-loss-bps must be > 0")
        if args.live_inventory_max_cycles <= 0:
            parser.error("--live-inventory-max-cycles must be > 0 in V1")
        if args.live_inventory_basis_z_entry <= 0:
            parser.error("--live-inventory-basis-z-entry must be > 0")
        if args.live_inventory_basis_z_exit < 0:
            parser.error("--live-inventory-basis-z-exit must be >= 0")
        if args.live_inventory_basis_min_entry_edge_bps < 0:
            parser.error("--live-inventory-basis-min-entry-edge-bps must be >= 0")
        if args.live_inventory_basis_max_entry_roundtrip_cost_bps < 0:
            parser.error("--live-inventory-basis-max-entry-roundtrip-cost-bps must be >= 0")
        if args.live_inventory_basis_min_abs_entry_bps < 0:
            parser.error("--live-inventory-basis-min-abs-entry-bps must be >= 0")
        if args.live_inventory_basis_min_exit_pnl_bps < 0:
            parser.error("--live-inventory-basis-min-exit-pnl-bps must be >= 0")
        if args.live_inventory_basis_exit_safety_buffer_bps < 0:
            parser.error("--live-inventory-basis-exit-safety-buffer-bps must be >= 0")
        if args.live_inventory_basis_half_life_seconds <= 0:
            parser.error("--live-inventory-basis-half-life-seconds must be > 0")
        if args.live_inventory_basis_warmup_samples <= 0:
            parser.error("--live-inventory-basis-warmup-samples must be > 0")
        if args.live_inventory_basis_gap_reset_seconds <= 0:
            parser.error("--live-inventory-basis-gap-reset-seconds must be > 0")
        if args.live_inventory_basis_sigma_floor_bps < 0:
            parser.error("--live-inventory-basis-sigma-floor-bps must be >= 0")
    elif args.live_inventory_reset_state_after_manual_flat:
        parser.error("--live-inventory-reset-state-after-manual-flat requires --live-inventory")
    elif args.live_inventory_dry_decisions:
        parser.error("--live-inventory-dry-decisions requires --live-inventory")
    elif args.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics:
        parser.error("--live-inventory-ignore-recent-execution-loss-buffer-for-diagnostics requires --live-inventory")
    if args.auto_live_min_holding_seconds < 0:
        parser.error("--auto-live-min-holding-seconds must be >= 0")
    if args.auto_live_entry_max_precheck_edge_bps < 0:
        parser.error("--auto-live-entry-max-precheck-edge-bps must be >= 0")
    if args.auto_live_entry_min_actionable_edge_bps < 0:
        parser.error("--auto-live-entry-min-actionable-edge-bps must be >= 0")
    if args.auto_live_cooldown_seconds < 0:
        parser.error("--auto-live-cooldown-seconds must be >= 0")
    if args.auto_live_max_cycles < 0:
        parser.error("--auto-live-max-cycles must be >= 0")
    if args.paper_inventory_lot_notional_usd <= 0:
        parser.error("--paper-inventory-lot-notional-usd must be > 0")
    if args.paper_inventory_max_lots <= 0:
        parser.error("--paper-inventory-max-lots must be > 0")
    if args.paper_inventory_max_total_lots <= 0:
        parser.error("--paper-inventory-max-total-lots must be > 0")
    if args.paper_inventory_min_hold_samples < 0:
        parser.error("--paper-inventory-min-hold-samples must be >= 0")
    if args.paper_inventory_latency_samples < 0:
        parser.error("--paper-inventory-latency-samples must be >= 0")
    live_allowed_sides = {side.strip().lower() for side in str(args.live_allowed_sides).split(",") if side.strip()}
    invalid_live_allowed_sides = sorted(live_allowed_sides - {"buy", "sell"})
    if invalid_live_allowed_sides:
        parser.error(f"--live-allowed-sides only accepts buy,sell; got {invalid_live_allowed_sides}")
    args.live_allowed_sides = ",".join(sorted(live_allowed_sides))
    return args


async def _amain() -> None:
    load_dotenv()
    args = parse_args()
    owner_pid = acquire_instance_lock(INSTANCE_LOCK_FILE)
    runtime = VariationalToLighterRuntime(args)
    try:
        await runtime.run()
    finally:
        try:
            await runtime.close()
        finally:
            release_instance_lock(INSTANCE_LOCK_FILE, owner_pid)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
