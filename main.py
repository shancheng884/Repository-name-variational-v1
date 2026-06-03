import argparse
import asyncio
import contextlib
import csv
import json
import logging
import os
import signal
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_UP
from pathlib import Path
from types import SimpleNamespace
from statistics import median
from typing import Any

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
INSTANCE_LOCK_FILE = LOG_DIR / "main.instance.lock"
AUTO_LIVE_STATE_FILE = LOG_DIR / "auto_live_state.json"
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


def elapsed_ms_str(start_monotonic: float | None) -> str:
    if start_monotonic is None:
        return "-"
    return f"{(time.monotonic() - start_monotonic) * 1000:.3f}"


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
        self.auto_live_cooldown_seconds = float(args.auto_live_cooldown_seconds)
        self.auto_live_max_cycles = int(args.auto_live_max_cycles)
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
        self.trade_records_csv_file = output_dir / TRADE_RECORDS_CSV_FILE.name if output_dir else None
        self.auto_live_state_file = output_dir / AUTO_LIVE_STATE_FILE.name if output_dir else None
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

    @staticmethod
    def auto_live_eager_hedge_started(record: OrderLifecycle | None) -> bool:
        return record is not None and record.processing_stage in {STAGE_LIVE_SUBMIT_SENT, STAGE_LIGHTER_FILLED}

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
        side: str,
        amount: str,
        expected_min_btc_qty: Decimal | None,
        confirm: bool,
    ) -> dict[str, Any]:
        request_id = str(int(time.time() * 1000))
        payload = {
            "type": "PLACE_ORDER",
            "requestId": request_id,
            "side": side.upper(),
            "amount": amount,
            "confirm": bool(confirm),
            "expectedMinBtcQty": decimal_to_str(expected_min_btc_qty) if expected_min_btc_qty is not None else "0",
        }
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

    async def place_lighter_order_from_plan(
        self,
        *,
        asset: str,
        side: str,
        qty: Decimal,
        var_fill_price: Decimal,
        cycle_id: int | None = None,
        role: str | None = None,
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
        if not tx_info or tx_info[0] != "{":
            raise ValueError(f"Invalid tx_info: {tx_info}")
        tx_info_payload = json.loads(tx_info)

        payload = {
            "type": "jsonapi/sendtx",
            "data": {
                "tx_type": int(tx_type),
                "tx_info": tx_info_payload,
            },
        }
        async with self._lighter_submit_ws_lock:
            websocket = await self.ensure_lighter_submit_ws()
            try:
                await websocket.send(json.dumps(payload, ensure_ascii=True))
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
        from lighter.transactions import CreateOrder

        if not self.lighter_client:
            raise RuntimeError("Lighter client is not initialized")
        api_key_index, nonce = self.lighter_client.nonce_manager.next_nonce()
        sent_to_ws = False
        try:
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
            if error is not None:
                self.lighter_client.nonce_manager.acknowledge_failure(api_key_index)
                return None, None, error

            sent_to_ws = True
            api_response = await self.send_lighter_tx_ws(tx_type=tx_type, tx_info=tx_info)
            if api_response is None or api_response.code != 200:
                self.lighter_client.nonce_manager.acknowledge_failure(api_key_index)
            return CreateOrder.from_json(tx_info), api_response, None
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
            if last_submit_monotonic is not None and now_monotonic - last_submit_monotonic < self.live_cooldown_seconds:
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
            async with self._lighter_signer_lock:
                if not self.lighter_client:
                    self.initialize_lighter_client()
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
                    "reduce_only": False,
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

            submit_sent_iso = utc_now()
            submit_sent_monotonic = time.monotonic()
            if error is not None:
                raise RuntimeError(f"Sign error: {error}")

            async with self._record_lock:
                record.dry_run_plan_side = side
                record.dry_run_plan_price = limit_price
                record.dry_run_plan_base_amount = base_amount
                record.lighter_side = side
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
        if status == "filled":
            matched_auto_live = self.consume_pending_auto_live_match(asset=asset, side=side, qty=qty)
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
                    auto_live_cycle_id=matched_auto_live.cycle_id if matched_auto_live is not None else None,
                    auto_live_role=matched_auto_live.role if matched_auto_live is not None else None,
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

        if not created or created_record is None:
            return

        if self.is_observe_mode():
            async with self._record_lock:
                self.set_record_stage(created_record, STAGE_BLOCKED_BY_MODE, clear_failure=True)
            return

        if self.is_dry_run_mode():
            async with self._record_lock:
                self.set_record_stage(created_record, STAGE_DRY_RUN_PENDING, clear_failure=True)
            await self.record_dry_run_plan(created_record)
            return

        if self.is_live_mode() and status == "filled":
            await self.place_lighter_order(created_record)

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
            entry_signal_monotonic = time.monotonic()
            entry_precheck_ms = "-"
            entry_var_submit_ms = "-"
            entry_lighter_submit_ms = "-"
            if self.auto_live_eager_hedge:
                precheck_price = snapshot.var_buy_price if var_side == "BUY" else snapshot.var_sell_price or snapshot.var_mid
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
            if self.auto_live_skip_entry_preview:
                entry_var_preview_ms = "skipped"
            else:
                entry_var_preview_started = time.monotonic()
                try:
                    precheck = await self.send_variational_place_order(
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
                self.send_variational_place_order(
                    side=var_side,
                    amount=decimal_to_str(order_qty) or str(order_qty),
                    expected_min_btc_qty=order_qty if snapshot.asset.upper() == "BTC" else None,
                    confirm=True,
                )
            )
            entry_lighter_task = None
            if self.auto_live_eager_hedge:
                entry_lighter_submit_started = time.monotonic()
                entry_lighter_task = asyncio.create_task(
                    self.place_lighter_order_from_plan(
                        asset=snapshot.asset,
                        side=var_side,
                        qty=order_qty,
                        var_fill_price=snapshot.var_buy_price if var_side == "BUY" else snapshot.var_sell_price or snapshot.var_mid,
                        cycle_id=cycle_id,
                        role="entry",
                    )
                )

            try:
                result = await entry_var_task
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
            entry_var_submit_ms = elapsed_ms_str(entry_var_submit_started)
            if not result.get("ok"):
                if entry_lighter_task is not None:
                    with contextlib.suppress(Exception):
                        await entry_lighter_task
                self.logger.warning("auto_live_var_submit_failed side=%s error=%s", var_side, result.get("error"))
                return

            entry_eager_started = not self.auto_live_eager_hedge
            if entry_lighter_task is not None:
                try:
                    lighter_record, payload = await entry_lighter_task
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
                entry_lighter_submit_ms = elapsed_ms_str(entry_lighter_submit_started)
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
            self.send_variational_place_order(
                side=exit_side,
                amount=decimal_to_str(position.planned_qty) or str(position.planned_qty),
                expected_min_btc_qty=position.planned_qty if snapshot.asset.upper() == "BTC" else None,
                confirm=True,
            )
        )
        exit_lighter_task = None
        if self.auto_live_eager_hedge:
            exit_lighter_submit_started = time.monotonic()
            exit_lighter_task = asyncio.create_task(
                self.place_lighter_order_from_plan(
                    asset=snapshot.asset,
                    side=exit_side,
                    qty=position.planned_qty,
                    var_fill_price=snapshot.var_sell_price if exit_side == "SELL" else snapshot.var_buy_price or snapshot.var_mid,
                    cycle_id=position.cycle_id,
                    role="exit",
                )
            )

        try:
            result = await exit_var_task
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
        exit_var_submit_ms = elapsed_ms_str(exit_var_submit_started)
        if not result.get("ok"):
            if exit_lighter_task is not None:
                with contextlib.suppress(Exception):
                    await exit_lighter_task
            self.logger.warning(
                "auto_live_exit_submit_failed asset=%s side=%s reason=%s error=%s",
                snapshot.asset,
                exit_side,
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
                lighter_record, payload = await exit_lighter_task
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
            exit_lighter_submit_ms = elapsed_ms_str(exit_lighter_submit_started)
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

        self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()
        self.trade_event_min_timestamp = datetime.now(timezone.utc)
        self.logger.info("Tracking new Variational trade events from seq>%s", self.trade_event_cursor)

        self.trade_task = self.track_background_task(asyncio.create_task(self.trade_loop()), "trade_loop")
        if self.requires_lighter_market_data():
            self.spread_task = self.track_background_task(asyncio.create_task(self.spread_loop()), "spread_loop")
        if self.is_live_mode():
            self.watchdog_task = self.track_background_task(asyncio.create_task(self.watchdog_live_submissions()), "watchdog_live_submissions")
        if self.is_paper_mode() or self.is_auto_live_enabled():
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
    if args.auto_live_min_holding_seconds < 0:
        parser.error("--auto-live-min-holding-seconds must be >= 0")
    if args.auto_live_entry_max_precheck_edge_bps < 0:
        parser.error("--auto-live-entry-max-precheck-edge-bps must be >= 0")
    if args.auto_live_cooldown_seconds < 0:
        parser.error("--auto-live-cooldown-seconds must be >= 0")
    if args.auto_live_max_cycles < 0:
        parser.error("--auto-live-max-cycles must be >= 0")
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
