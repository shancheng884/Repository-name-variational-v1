#!/usr/bin/env python3
"""Standalone WTI runtime; owns the extension forwarder and never controls ETH."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from tools.basis_collector import (  # noqa: E402
    CommandBroker,
    run_command_server,
    run_receiver_server,
)
from variational.listener import EventSink as ListenerEventSink  # noqa: E402
from tools.lib.basis_store import read_basis_samples  # noqa: E402
from tools.lib.market_sessions import (  # noqa: E402
    OilSession,
    classify_usoil_session,
    session_observation_for_sample,
)
from tools.lib.oil_swap_readiness import assess_oil_readiness  # noqa: E402
from tools.lib.runtime_files import read_json, write_json_atomic  # noqa: E402
from tools.lib.wti_execution import (  # noqa: E402
    WTIPairExecutor,
    WTIExecutionError,
    WTIExecutionStore,
)
from tools.lib.wti_live_gateway import WTIProductionGateway  # noqa: E402
from tools.lib.wti_live_strategy import decide_wti_action, decide_wti_gradient_action  # noqa: E402
from tools.lib.wti_sizing import equity_gradient_slot_caps  # noqa: E402
from tools.lib.wti_replay import ReplayConfig  # noqa: E402
from tools.lib.wti_thresholds import (  # noqa: E402
    ANCHOR_WINDOW_SECONDS as WTI_THRESHOLD_WINDOW_SECONDS,
    THRESHOLD_REFRESH_SECONDS as WTI_THRESHOLD_REFRESH_SECONDS,
    WTIGradientThresholdProfile,
    build_wti_gradient_threshold_profiles,
)
from tools.oil_swap_collector import OilSwapCollector, parse_args as parse_collector_args  # noqa: E402


DEFAULT_LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"
DEFAULT_PORTS = (8766, 8767, 8768)
ETH_STATE_PATH = ROOT / "log" / "live_inventory_state.json"
WTI_STATE_PATH = ROOT / "log" / "wti_live_state.json"
WTI_HEALTH_PATH = ROOT / "log" / "wti_live_risk_health.json"
WTI_CONTROL_PATH = ROOT / "log" / "wti_live_control.json"
WTI_SAMPLE_ROOT = ROOT / "log" / "oil_swap" / "basis_samples"
MAX_WTI_NOTIONAL_USD = Decimal("25")
WTI_GRADIENT_MAX_LAYERS = 5
WTI_MAX_EQUITY_MULTIPLE = Decimal("5")
MAX_WTI_ENTRY_GAP_SECONDS = 120
MIN_ENTRY_CUTOFF_SECONDS = 1800
MAX_HOLD_SECONDS = 21600
MAX_SLIPPAGE_BPS = Decimal("8")
MAX_SINGLE_ROUND_PILOT_ENTRY_ATTEMPTS = 3
ACCOUNT_ONLY_RECONCILE_INTERVAL_SECONDS = 10
RETRYABLE_SAMPLE_VALIDATION_ERRORS = frozenset({
    "market_sample_stale_or_future",
    "variational_quote_stale_or_invalid",
    "lighter_book_stale_or_invalid",
    "exchange_session_observation_unverified",
})


def _retryable_sample_validation_error(exc: BaseException) -> bool:
    return isinstance(exc, WTIExecutionError) and str(exc) in RETRYABLE_SAMPLE_VALIDATION_ERRORS


def _decimal(value: Any, name: str, *, minimum: Decimal, maximum: Decimal | None = None) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name}_invalid") from exc
    if not parsed.is_finite() or parsed < minimum or (maximum is not None and parsed > maximum):
        raise ValueError(f"{name}_outside_safe_range")
    return parsed


def validate_eth_flat_confirmation(path: Path, *, confirmed: bool) -> dict[str, Any]:
    if not confirmed:
        raise RuntimeError("explicit_eth_flat_confirmation_required")
    state = read_json(path)
    if (
        state.get("asset") != "ETH"
        or state.get("status") != "flat"
        or state.get("open_lots")
        or state.get("pending_actions")
    ):
        raise RuntimeError("eth_local_state_not_confirmed_flat")
    return state


def single_round_pilot_owns_exposure(state: dict[str, Any]) -> bool:
    pilot = state.get("single_round_pilot") or {}
    pending = state.get("pending_action") or {}
    pilot_id = pilot.get("pilot_id")
    if not pilot_id or pilot.get("status") not in {"entry_reserved", "in_position"}:
        return False
    position = state.get("open_position") or {}
    previous_position = pending.get("previous_position") or {}
    return any(
        item.get("single_round_pilot_id") == pilot_id
        for item in (position, pending, previous_position)
        if isinstance(item, dict)
    )


def gradient_pilot_owns_exposure(state: dict[str, Any]) -> bool:
    pilot = state.get("gradient_pilot") or {}
    if not isinstance(pilot, dict) or pilot.get("status") not in {
        "entry_pending", "in_position",
    }:
        return False
    pilot_id = pilot.get("pilot_id")
    position = state.get("open_position") or {}
    pending = state.get("pending_action") or {}
    previous = pending.get("previous_position") or {}
    return any(
        isinstance(item, dict) and item.get("gradient_pilot_id") == pilot_id
        for item in (position, pending, previous)
    )


def validate_startup_preconditions(
    args: argparse.Namespace,
    *,
    eth_state_path: Path = ETH_STATE_PATH,
) -> None:
    if args.mode != "live":
        return
    validate_eth_flat_confirmation(eth_state_path, confirmed=args.confirm_eth_flat)
    if not args.i_understand_live_orders:
        raise RuntimeError("explicit_live_order_acknowledgement_required")
    if args.expected_lighter_market_id is None:
        raise RuntimeError("current_lighter_market_id_must_be_explicitly_configured")


def build_replay_config(args: argparse.Namespace) -> ReplayConfig:
    entry = _decimal(args.entry_threshold_bps, "entry_threshold_bps", minimum=Decimal("0.000001"))
    exit_target = _decimal(args.exit_target_bps, "exit_target_bps", minimum=Decimal("0.000001"))
    cost = _decimal(args.roundtrip_cost_bps, "roundtrip_cost_bps", minimum=Decimal("0.000001"))
    notional = _decimal(
        args.max_notional_usd, "max_notional_usd",
        minimum=Decimal("0.01"), maximum=MAX_WTI_NOTIONAL_USD,
    )
    slippage = _decimal(
        args.max_slippage_bps, "max_slippage_bps",
        minimum=Decimal("0.000001"), maximum=MAX_SLIPPAGE_BPS,
    )
    hold = float(args.max_hold_seconds)
    cutoff = float(args.entry_cutoff_seconds)
    if not 0 < hold <= MAX_HOLD_SECONDS:
        raise ValueError("max_hold_seconds_outside_safe_range")
    if cutoff < MIN_ENTRY_CUTOFF_SECONDS or cutoff % 60:
        raise ValueError("entry_cutoff_must_be_at_least_30_minutes_and_whole_minutes")
    config = ReplayConfig(
        entry_threshold_bps=entry,
        exit_target_bps=exit_target,
        roundtrip_cost_bps=cost,
        max_hold_seconds=hold,
        max_sample_gap_seconds=float(MAX_WTI_ENTRY_GAP_SECONDS),
        entry_cutoff_seconds=cutoff,
        max_slippage_bps=slippage,
        max_order_notional_usd=notional,
    )
    args._validated_notional = notional
    args._validated_slippage = slippage
    return config


def readiness_for_new_entries(
    rows: list[dict[str, Any]],
    *,
    market_id: int,
    multiplier: Decimal,
    now: datetime | None = None,
) -> dict[str, Any]:
    report = assess_oil_readiness(
        rows,
        now_utc=now,
        minimum_sessions=3,
        minimum_valid_samples_per_session=1200,
        minimum_coverage_seconds=6 * 3600,
        maximum_gap_seconds=MAX_WTI_ENTRY_GAP_SECONDS,
        maximum_sample_age_seconds=120,
        expected_lighter_market_id=market_id,
        expected_lighter_exposure_multiplier=multiplier,
    )
    identity_is_consistent = bool(
        report.lighter_market_id_explicitly_configured
        and report.lighter_market_id == market_id
        and report.lighter_exposure_multiplier_explicitly_configured
        and report.lighter_exposure_multiplier is not None
        and Decimal(str(report.lighter_exposure_multiplier)) == multiplier
    )
    has_clean_verified_history = any(
        session.valid_samples > 0
        and session.max_gap_seconds <= MAX_WTI_ENTRY_GAP_SECONDS
        for session in report.sessions
    )
    historical_volume_bypass_ready = (
        identity_is_consistent and has_clean_verified_history
    )
    return {
        "ready": report.ready_for_bounded_live_review,
        "market_data_ready": report.market_data_ready,
        "data_fresh": report.data_fresh,
        "production_ready": report.ready_for_production_rotation,
        "reasons": list(report.reasons),
        "session_count": report.session_count,
        "valid_sample_count": report.valid_sample_count,
        "confirmed_roundtrips": report.confirmed_roundtrips,
        "historical_volume_bypass_ready": historical_volume_bypass_ready,
        "historical_volume_bypass_evidence_sessions": sum(
            session.valid_samples > 0
            and session.max_gap_seconds <= MAX_WTI_ENTRY_GAP_SECONDS
            for session in report.sessions
        ),
    }


def _unqualified_data_pilot_enabled(args: argparse.Namespace) -> bool:
    bounded_live = bool(
        getattr(args, "confirm_bounded_live_pilot", False)
        and not getattr(args, "gradient_live", False)
    )
    bounded_gradient = bool(
        getattr(args, "gradient_live", False)
        and getattr(args, "confirm_bounded_gradient_pilot", False)
        and (
            not getattr(args, "continuous_gradient_rotation", False)
            or (
                getattr(args, "confirm_continuous_gradient_rotation", False)
                and bool(getattr(args, "rotation_test_id", None))
            )
        )
    )
    return bool(
        getattr(args, "mode", None) == "live"
        and getattr(args, "allow_unqualified_data_pilot", False)
        and (bounded_live or bounded_gradient)
        and not getattr(args, "manage_existing_wti_position", False)
    )


def _session_window(
    row: dict[str, Any],
    now: datetime,
    cutoff_seconds: float,
    *,
    feed_health: dict[str, Any] | None = None,
):
    observed = row.get("session_status_observed_at")
    closing = row.get("session_closing_at")
    try:
        observed_at = datetime.fromisoformat(str(observed).replace("Z", "+00:00"))
        closing_at = datetime.fromisoformat(str(closing).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        observed_at = closing_at = None
    health = feed_health if isinstance(feed_health, dict) else {}
    row_status = str(row.get("market_status") or "").lower()
    health_status = str(health.get("market_status") or "").lower()
    status = row_status
    if health_status and health_status != row_status:
        row_age = None
        try:
            row_at = datetime.fromisoformat(str(row.get("logged_at")).replace("Z", "+00:00"))
            if row_at.tzinfo is not None and row_at.utcoffset() is not None:
                row_age = (now - row_at.astimezone(timezone.utc)).total_seconds()
        except (TypeError, ValueError):
            pass
        heartbeat = health.get("market_status_feed_heartbeat_at")
        heartbeat_fresh = False
        try:
            heartbeat_at = datetime.fromisoformat(str(heartbeat).replace("Z", "+00:00"))
            if heartbeat_at.tzinfo is not None and heartbeat_at.utcoffset() is not None:
                heartbeat_age = (now - heartbeat_at.astimezone(timezone.utc)).total_seconds()
                heartbeat_fresh = (
                    health.get("market_status_feed_connected") is True
                    and -5 <= heartbeat_age <= 30
                )
        except (TypeError, ValueError):
            pass
        if (
            health_status in {"closed", "halted_trading"}
            and heartbeat_fresh
            and (row_age is None or row_age > 30)
        ):
            status = health_status
        else:
            status = "unknown"
    elif not row_status and health_status in {"closed", "halted_trading"}:
        status = health_status
    if status not in {"open", "closed", "halted_trading"}:
        exchange_open = None
    else:
        exchange_open = status == "open"
    if exchange_open is True:
        # State-change timestamps can remain old throughout an open session. The
        # validated subscription heartbeat proves that this state is still live.
        observation = session_observation_for_sample(row)
        if observation is None:
            exchange_open = None
        else:
            observed_at = datetime.fromisoformat(str(row["session_status_feed_heartbeat_at"]).replace("Z", "+00:00"))
    elif exchange_open is False:
        # A closed state can persist for hours. Its live feed heartbeat, rather
        # than the old state-change timestamp, proves the exchange status is current.
        connected = health.get(
            "market_status_feed_connected", row.get("session_status_feed_connected")
        )
        heartbeat = health.get(
            "market_status_feed_heartbeat_at",
            row.get("session_status_feed_heartbeat_at"),
        )
        source = row.get("session_status_source")
        if source is None and health.get("market_status_feed_connected") is True:
            source = "variational_market_status_websocket"
        if (
            connected is not True
            or source != "variational_market_status_websocket"
            or not heartbeat
        ):
            exchange_open = None
        else:
            try:
                heartbeat_at = datetime.fromisoformat(str(heartbeat).replace("Z", "+00:00"))
                if heartbeat_at.tzinfo is None or heartbeat_at.utcoffset() is None:
                    raise ValueError("timezone_required")
                heartbeat_age = (now - heartbeat_at.astimezone(timezone.utc)).total_seconds()
            except (TypeError, ValueError):
                exchange_open = None
            else:
                if heartbeat_age < -5 or heartbeat_age > 30:
                    exchange_open = None
                else:
                    observed_at = heartbeat_at
    cutoff_minutes = int(cutoff_seconds // 60)
    return classify_usoil_session(
        now,
        exchange_open=exchange_open,
        exchange_status_observed_at=observed_at,
        exchange_closing_at=closing_at,
        entry_cutoff_minutes=cutoff_minutes,
        status_max_age_seconds=30,
    )


class WTILiveRuntime:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.config = build_replay_config(args) if args.mode == "live" else None
        self.logger = logging.getLogger("wti_live")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        ROOT.joinpath("log").mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            ROOT / "log" / "wti_live.log", maxBytes=10 * 1024 * 1024,
            backupCount=5, encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(file_handler)
        self.logger.addHandler(logging.StreamHandler())
        collector_args = parse_collector_args([
            "--reuse-forwarder",
            "--forwarder-host", args.forwarder_host,
            "--forwarder-command-port", str(args.forwarder_command_port),
            "--output-dir", str(args.output_dir),
            "--notional-usd", str(args.collector_notional_usd),
            "--interval-seconds", str(args.sample_interval_seconds),
        ])
        self.collector = OilSwapCollector(collector_args)
        self.run_id = f"wti-live-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        self.stop = False
        self.servers: list[Any] = []
        self.collector_task: asyncio.Task[Any] | None = None
        self.account_client: Any = None
        self.signer_client: Any = None
        self.gateway: WTIProductionGateway | None = None
        self.executor: WTIPairExecutor | None = None
        self.market_metadata: dict[str, Any] | None = None
        self.readiness: dict[str, Any] = {"ready": False, "reasons": ["observe_only"]}
        self.entry_authorized = False
        self.manage_existing_only = False
        self.last_seen_sample_id: str | None = None
        self.last_processed_sample_id: str | None = None
        self.last_processed_sample_at: datetime | None = None
        self.last_sample_freshness_diagnostics: dict[str, Any] = {}
        self.last_execution_refresh_status = "not_attempted"
        self.last_execution_refresh_at: str | None = None
        self.last_execution_refresh_elapsed_ms: float | None = None
        self.last_execution_refresh_error: str | None = None
        self.startup_reconciled = False
        self.entry_gate_block_reason: str | None = None
        self.last_sample_gap_seconds: float | None = None
        self.pending_gap_exit = False
        self.last_risk: dict[str, Any] = {}
        self.last_action: dict[str, Any] = {"action": "none", "reason": "starting"}
        self.last_health_write_monotonic = 0.0
        self.last_runtime_error: str | None = None
        self.last_verified_cycle_at: str | None = None
        self.last_verified_cycle_kind: str | None = None
        self.verified_cycle_count = 0
        self.last_cycle_error: str | None = None
        self.pilot_store: WTIExecutionStore | None = None
        self.single_round_pilot: dict[str, Any] | None = None
        self.gradient_pilot: dict[str, Any] | None = None
        self.entry_authorization_permanently_revoked = False
        self.last_readiness_elapsed_ms: float | None = None
        self.last_reconcile_elapsed_ms: float | None = None
        self.rotation_drain_request: dict[str, Any] | None = None
        self.last_account_only_reconcile_monotonic = 0.0
        self.wti_gradient_threshold_profiles: dict[str, WTIGradientThresholdProfile] = {}
        self.wti_gradient_threshold_refreshed_monotonic = 0.0
        self.wti_gradient_threshold_source = "not_loaded"

    def request_stop(self, *_args: Any) -> None:
        self.stop = True
        self.collector.request_stop()

    async def _start_forwarder(self) -> None:
        sink = ListenerEventSink(output_dir=None, quiet=True, monitor=None)
        broker = CommandBroker(quiet=True)
        self.servers = [
            await run_receiver_server("ws", self.args.forwarder_host, self.args.forwarder_ws_port, sink),
            await run_receiver_server("rest", self.args.forwarder_host, self.args.forwarder_rest_port, sink),
            await run_command_server(self.args.forwarder_host, self.args.forwarder_command_port, broker),
        ]

    async def _initialize_execution(self) -> tuple[dict[str, Any], dict[str, Any]]:
        from lighter.signer_client import SignerClient
        from tools.wti_account_probe import (
            LighterSDKReadOnlyReader,
            _close_client,
            fetch_lighter_market_metadata,
        )
        from tools.lib.wti_readonly_adapter import (
            VariationalForwarderClient,
            WTIReadOnlyAccountAdapter,
        )

        load_dotenv(ROOT / ".env", override=False)
        try:
            account_index = int(os.environ["LIGHTER_ACCOUNT_INDEX"])
            api_key_index = int(os.environ["LIGHTER_API_KEY_INDEX"])
            private_key = (
                os.environ.get("API_KEY_PRIVATE_KEY", "").strip()
                or os.environ.get("LIGHTER_PRIVATE_KEY", "").strip()
            )
        except (KeyError, ValueError) as exc:
            raise RuntimeError("lighter_account_configuration_missing") from exc
        if not private_key:
            raise RuntimeError("lighter_signing_key_missing")
        market = await asyncio.to_thread(
            fetch_lighter_market_metadata,
            self.args.lighter_base_url,
            expected_market_id=self.args.expected_lighter_market_id,
        )
        self.market_metadata = dict(market)
        self.signer_client = SignerClient(
            url=self.args.lighter_base_url.rstrip("/"),
            account_index=account_index,
            api_private_keys={api_key_index: private_key},
        )
        if await asyncio.to_thread(self.signer_client.check_client) is not None:
            raise RuntimeError("lighter_signer_client_preflight_failed")
        if self.args.mode == "live":
            await self._load_startup_readiness()
            store = WTIExecutionStore(self.args.state_path)
            existing = store.load()
            local_exposure = (
                existing.get("open_position") is not None
                or existing.get("pending_action") is not None
                or existing.get("status") in {"pending", "manual_review_required", "open"}
            )
            predecessor = getattr(self.args, "new_pilot_after", None)
            current_pilot = existing.get("single_round_pilot") or {}
            if predecessor and local_exposure and not (
                current_pilot.get("previous_pilot_id") == predecessor
                and current_pilot.get("pilot_id") != predecessor
                and single_round_pilot_owns_exposure(existing)
            ):
                raise RuntimeError("new_single_round_pilot_requires_flat_wti_state")
            gradient_mode = bool(getattr(self.args, "gradient_live", False))
            if self.args.manage_existing_wti_position:
                # Management remains entry-disabled even if a previous run
                # already flattened the account before this restart.
                self.manage_existing_only = True
            elif gradient_mode:
                if not getattr(self.args, "confirm_bounded_gradient_pilot", False):
                    raise RuntimeError("explicit_bounded_gradient_pilot_confirmation_required")
                self.pilot_store = store
                continuous_rotation = bool(
                    getattr(self.args, "continuous_gradient_rotation", False)
                )
                existing_gradient = existing.get("gradient_pilot") or {}
                manual_after = getattr(self.args, "new_gradient_pilot_after", None)
                if manual_after:
                    if existing_gradient.get("pilot_id") == manual_after:
                        if (
                            local_exposure or existing_gradient.get("status") not in {"completed", "exhausted"}
                            or existing_gradient.get("config") != self._gradient_pilot_config()
                            or existing_gradient.get("max_layers") != WTI_GRADIENT_MAX_LAYERS
                        ):
                            raise RuntimeError("manual_gradient_requires_terminal_flat_unchanged_predecessor")
                        if (
                            not self.readiness.get("market_data_ready", self.readiness["ready"])
                            and not self._historical_volume_bypass_authorized()
                        ):
                            raise RuntimeError("manual_gradient_history_gate_not_met")
                        # Arming waits for this runtime's fresh dual-account
                        # reconciliation, not an inherited manager heartbeat.
                        self.gradient_pilot = dict(existing_gradient)
                    else:
                        if local_exposure and not gradient_pilot_owns_exposure(existing):
                            raise RuntimeError("manual_gradient_successor_exposure_identity_mismatch")
                        self.gradient_pilot = store.initialize_gradient_pilot(
                            market_id=int(market["market_id"]),
                            multiplier=Decimal(str(market["multiplier"])),
                            config=self._gradient_pilot_config(), max_layers=WTI_GRADIENT_MAX_LAYERS,
                            manual_new_pilot_after=manual_after,
                        )
                elif (
                    continuous_rotation
                    and existing_gradient.get("status") == "completed"
                    and not local_exposure
                ):
                    # A new round is armed only after this runtime proves both
                    # accounts flat with a fresh read.
                    self.gradient_pilot = dict(existing_gradient)
                elif local_exposure:
                    if not gradient_pilot_owns_exposure(existing):
                        raise RuntimeError("existing_wti_exposure_not_owned_by_gradient_pilot")
                    old_pilot = dict(existing_gradient)
                    if (
                        old_pilot.get("max_layers") != WTI_GRADIENT_MAX_LAYERS
                        or old_pilot.get("config") != self._gradient_pilot_config()
                    ):
                        # Keep legacy exposure manageable, but never layer it under
                        # a changed budget or tier map.
                        self.gradient_pilot = old_pilot
                        self.manage_existing_only = True
                        self.entry_gate_block_reason = "legacy_gradient_position_management_only"
                    else:
                        self.gradient_pilot = store.initialize_gradient_pilot(
                            market_id=int(market["market_id"]),
                            multiplier=Decimal(str(market["multiplier"])),
                            config=self._gradient_pilot_config(),
                            max_layers=WTI_GRADIENT_MAX_LAYERS,
                            rotation_test_id=getattr(self.args, "rotation_test_id", None),
                            continuous_rotation=continuous_rotation,
                        )
                else:
                    if (
                        not self.readiness.get("market_data_ready", self.readiness["ready"])
                        and not self._historical_volume_bypass_authorized()
                    ):
                        raise RuntimeError(
                            "wti_new_entries_blocked_by_data_readiness:" + ",".join(self.readiness["reasons"])
                        )
                    self.gradient_pilot = store.initialize_gradient_pilot(
                        market_id=int(market["market_id"]),
                        multiplier=Decimal(str(market["multiplier"])),
                        config=self._gradient_pilot_config(),
                        max_layers=WTI_GRADIENT_MAX_LAYERS,
                        rotation_test_id=getattr(self.args, "rotation_test_id", None),
                        continuous_rotation=continuous_rotation,
                    )
            elif local_exposure:
                pilot_owns_exposure = single_round_pilot_owns_exposure(existing)
                if not self.readiness["ready"] and not (
                    self.args.manage_existing_wti_position or pilot_owns_exposure
                ):
                    raise RuntimeError("existing_wti_exposure_requires_explicit_management_mode")
                if not self.args.manage_existing_wti_position and not pilot_owns_exposure:
                    raise RuntimeError("existing_wti_exposure_requires_explicit_management_mode")
                self.manage_existing_only = True
            else:
                if not self.args.confirm_bounded_live_pilot:
                    raise RuntimeError("explicit_bounded_live_pilot_confirmation_required")
                if (
                    not self.readiness.get("market_data_ready", self.readiness["ready"])
                    and not self._historical_volume_bypass_authorized()
                ):
                    raise RuntimeError(
                        "wti_new_entries_blocked_by_data_readiness:" + ",".join(self.readiness["reasons"])
                    )
                self.pilot_store = store
                self.single_round_pilot = store.initialize_single_round_pilot(
                    market_id=int(market["market_id"]),
                    multiplier=Decimal(str(market["multiplier"])),
                    config=self._pilot_config(),
                    readiness=self.readiness,
                    max_entry_attempts=MAX_SINGLE_ROUND_PILOT_ENTRY_ATTEMPTS,
                    new_pilot_after=getattr(self.args, "new_pilot_after", None),
                )
            if not self.args.i_understand_live_orders:
                raise RuntimeError("explicit_live_order_acknowledgement_required")

        self.account_client = VariationalForwarderClient(
            f"ws://{self.args.forwarder_host}:{self.args.forwarder_command_port}",
            timeout_seconds=self.args.command_timeout_seconds,
        )
        await self.account_client.__aenter__()
        lighter_reader = LighterSDKReadOnlyReader(
            client=self.signer_client,
            account_index=account_index,
            api_key_index=api_key_index,
        )
        account_reader = WTIReadOnlyAccountAdapter(
            variational=self.account_client,
            lighter=lighter_reader,
            market=market,
            request_timeout_ms=int(self.args.command_timeout_seconds * 1000),
        )
        loop = asyncio.get_running_loop()
        self.gateway = WTIProductionGateway(
            loop=loop,
            variational=self.account_client,
            account_reader=account_reader,
            lighter_client=self.signer_client,
            account_index=account_index,
            api_key_index=api_key_index,
            market=market,
            operation_timeout_seconds=self.args.command_timeout_seconds,
        )

        def fresh_market_reader() -> dict[str, Any]:
            future = asyncio.run_coroutine_threadsafe(
                self.collector.collect_fresh_sample(sample_kind="burst"), loop
            )
            sample = future.result(timeout=self.args.command_timeout_seconds + 3)
            if not isinstance(sample, dict):
                raise RuntimeError("wti_fresh_sample_invalid")
            return sample

        def fresh_market_reader_for_qty(
            var_qty: Decimal, lighter_qty: Decimal
        ) -> dict[str, Any]:
            future = asyncio.run_coroutine_threadsafe(
                self.collector.collect_fresh_sample(
                    var_qty=var_qty, lighter_qty=lighter_qty, sample_kind="burst"
                ),
                loop,
            )
            sample = future.result(timeout=self.args.command_timeout_seconds + 3)
            if not isinstance(sample, dict):
                raise RuntimeError("wti_fresh_sample_invalid")
            return sample

        self.executor = WTIPairExecutor(
            state_path=self.args.state_path,
            gateway=self.gateway,
            max_slippage_bps=self.args._validated_slippage,
            market_reader=fresh_market_reader,
            market_reader_for_qty=fresh_market_reader_for_qty,
            expected_lighter_market_id=market["market_id"],
            expected_lighter_multiplier=market["multiplier"],
            expected_lighter_size_decimals=market["supported_size_decimals"],
            expected_lighter_price_decimals=market["supported_price_decimals"],
            max_notional_usd=self.args._validated_notional,
        )
        return market, {"account_index": account_index, "api_key_index": api_key_index}

    def _pilot_config(self) -> dict[str, Any]:
        assert self.config is not None
        return {
            "entry_threshold_bps": str(self.config.entry_threshold_bps),
            "exit_target_bps": str(self.config.exit_target_bps),
            "roundtrip_cost_bps": str(self.config.roundtrip_cost_bps),
            "max_hold_seconds": self.config.max_hold_seconds,
            "entry_cutoff_seconds": self.config.entry_cutoff_seconds,
            "max_notional_usd": str(self.args._validated_notional),
            "max_slippage_bps": str(self.args._validated_slippage),
            "historical_readiness_mode": (
                "bounded_pilot_history_volume_bypass"
                if _unqualified_data_pilot_enabled(self.args)
                else "qualified_history_required"
            ),
        }

    def _gradient_pilot_config(self) -> dict[str, Any]:
        assert self.config is not None
        second = getattr(self.args, "_validated_gradient_second_tier", None)
        layer_notional = getattr(self.args, "_validated_gradient_layer_notional", None)
        if second is None or layer_notional is None:
            raise RuntimeError("gradient_pilot_configuration_not_validated")
        floors = [self.config.entry_threshold_bps, second]
        floors.extend(
            second + Decimal("0.25") * tier
            for tier in range(1, WTI_GRADIENT_MAX_LAYERS - 1)
        )
        return {
            **self._pilot_config(),
            "tier_thresholds_bps": [str(value) for value in floors],
            "layer_notional_usd": str(layer_notional),
            "max_layers": WTI_GRADIENT_MAX_LAYERS,
            "max_equity_multiple": str(WTI_MAX_EQUITY_MULTIPLE),
            "max_completed_roundtrips": 1,
        }

    def _sync_entry_authorization(
        self, state: dict[str, Any], row: dict[str, Any], *, now: datetime
    ) -> bool:
        gradient_mode = bool(getattr(self.args, "gradient_live", False))
        gradient_position = bool(
            gradient_mode
            and state.get("status") == "open"
            and isinstance(state.get("open_position"), dict)
            and state["open_position"].get("gradient_pilot_id")
        )
        if (
            self.args.mode != "live"
            or self.entry_authorization_permanently_revoked
            or self.manage_existing_only
            or (state.get("status") != "flat" and not gradient_position)
            or (state.get("open_position") is not None and not gradient_position)
            or state.get("pending_action") is not None
        ):
            self.entry_authorized = False
            return False
        if gradient_mode:
            pilot = state.get("gradient_pilot") or {}
            opened = pilot.get("layers_opened") or []
            if (
                not self.gradient_pilot
                or pilot.get("pilot_id") != self.gradient_pilot.get("pilot_id")
                or pilot.get("status") != ("in_position" if gradient_position else "available")
                or pilot.get("completed_roundtrips") != 0
            ):
                self.entry_authorized = False
                self.entry_gate_block_reason = "gradient_pilot_entry_budget_unavailable"
                return False
        else:
            pilot = state.get("single_round_pilot") or {}
            if (
                not isinstance(pilot, dict)
                or not self.single_round_pilot
                or pilot.get("pilot_id") != self.single_round_pilot.get("pilot_id")
                or pilot.get("status") != "available"
                or pilot.get("completed_roundtrips") != 0
                or pilot.get("entry_attempts", 0) >= pilot.get("max_entry_attempts", 0)
            ):
                self.entry_authorized = False
                self.entry_gate_block_reason = "single_round_pilot_entry_budget_unavailable"
                return False
        fresh = self._sample_health_fresh(row, now)
        qualified = bool(self.readiness.get("market_data_ready")) and fresh
        volume_bypass = self._historical_volume_bypass_authorized()
        self.entry_authorized = fresh and (qualified or volume_bypass)
        self.entry_gate_block_reason = None if self.entry_authorized else (
            "wti_current_sample_or_session_not_fresh"
            if not fresh else (
                "wti_historical_volume_bypass_evidence_not_met"
                if _unqualified_data_pilot_enabled(self.args) and not volume_bypass
                else "wti_data_readiness_not_met"
            )
        )
        return self.entry_authorized

    def _historical_volume_bypass_authorized(self) -> bool:
        return bool(
            _unqualified_data_pilot_enabled(self.args)
            and self.readiness.get("historical_volume_bypass_ready") is True
        )

    def _finish_single_round_pilot_if_done(self, state: dict[str, Any]) -> bool:
        if getattr(getattr(self, "args", None), "gradient_live", False):
            return False
        pilot = state.get("single_round_pilot") or {}
        if (
            pilot.get("status") in {"completed", "exhausted"}
            and state.get("status") == "flat"
            and state.get("open_position") is None
            and state.get("pending_action") is None
        ):
            self.single_round_pilot = dict(pilot)
            self.entry_authorized = False
            self.entry_gate_block_reason = f"single_round_pilot_{pilot['status']}"
            args = getattr(self, "args", None)
            if not (getattr(args, "manage_existing_wti_position", False)
                    or getattr(args, "keep_collecting_after_pilot", False)):
                self.request_stop()
            return True
        return False

    def _finish_gradient_pilot_if_done(self, state: dict[str, Any]) -> bool:
        pilot = state.get("gradient_pilot") or {}
        if (
            pilot.get("status") in {"completed", "exhausted"}
            and state.get("status") == "flat"
            and state.get("open_position") is None
            and state.get("pending_action") is None
        ):
            self.gradient_pilot = dict(pilot)
            self.entry_authorized = False
            self.entry_gate_block_reason = f"gradient_pilot_{pilot['status']}"
            if not (
                getattr(self.args, "keep_collecting_after_pilot", False)
                or getattr(self.args, "manage_existing_wti_position", False)
            ):
                self.request_stop()
            return True
        return False

    def _confirmed_closed_session_window(self, now: datetime):
        feed = getattr(getattr(self, "collector", None), "market_status_feed", None)
        if feed is None or getattr(feed, "connected", None) is not True:
            return None
        status = str(getattr(feed, "status", "") or "").lower()
        if status not in {"closed", "halted_trading"}:
            return None
        heartbeat = getattr(feed, "heartbeat_at", None)
        if not isinstance(heartbeat, datetime) or heartbeat.tzinfo is None or heartbeat.utcoffset() is None:
            return None
        heartbeat = heartbeat.astimezone(timezone.utc)
        heartbeat_age = (now.astimezone(timezone.utc) - heartbeat).total_seconds()
        if heartbeat_age < -5 or heartbeat_age > 30:
            return None
        row = {
            "market_status": status,
            "session_status": "closed",
            "session_status_source": "variational_market_status_websocket",
            "session_status_observed_at": heartbeat.isoformat(),
            "session_status_feed_connected": True,
            "session_status_feed_heartbeat_at": heartbeat.isoformat(),
        }
        window = _session_window(
            row,
            now,
            self.args.entry_cutoff_seconds,
            feed_health={
                "market_status": status,
                "market_status_feed_connected": True,
                "market_status_feed_heartbeat_at": heartbeat.isoformat(),
            },
        )
        return window if window.state == OilSession.EXCHANGE_CLOSED else None

    async def _wait_for_first_sample(self) -> dict[str, Any] | None:
        deadline = time.monotonic() + self.args.initial_sample_timeout_seconds
        while not self.stop and time.monotonic() < deadline:
            if self.collector.latest_sample is not None:
                return dict(self.collector.latest_sample)
            if (
                getattr(self.args, "manage_existing_wti_position", False)
                and self._confirmed_closed_session_window(datetime.now(timezone.utc)) is not None
            ):
                return None
            if self.collector_task is not None and self.collector_task.done():
                await self.collector_task
                raise RuntimeError("wti_collector_stopped_before_first_sample")
            await asyncio.sleep(0.25)
        raise RuntimeError("wti_first_sample_timeout")

    async def _read_account_only_reconcile(self) -> dict[str, Any]:
        assert self.executor is not None
        started = time.monotonic()
        now = datetime.now(timezone.utc)
        try:
            return await asyncio.to_thread(
                lambda: self.executor.reconcile_account_only(
                    now=now,
                    runtime_pid=os.getpid(),
                    runtime_run_id=self.run_id,
                )
            )
        finally:
            self.last_reconcile_elapsed_ms = round((time.monotonic() - started) * 1000, 3)

    async def _advance_continuous_gradient_round(
        self, state: dict[str, Any]
    ) -> dict[str, Any]:
        if (
            getattr(self, "manage_existing_only", False)
            or not getattr(self.args, "gradient_live", False)
            or not getattr(self.args, "continuous_gradient_rotation", False)
        ):
            return state
        self._poll_rotation_drain()
        if getattr(self, "rotation_drain_request", None) is not None:
            self.entry_authorized = False
            return state
        pilot = state.get("gradient_pilot") or {}
        if pilot.get("status") != "completed":
            return state
        if (
            state.get("status") != "flat"
            or state.get("open_position") is not None
            or state.get("pending_action") is not None
        ):
            self.entry_authorized = False
            self.entry_gate_block_reason = "gradient_next_round_requires_resolved_flat_state"
            return state
        assert self.pilot_store is not None and self.market_metadata is not None
        state = await self._read_account_only_reconcile()
        if (
            state.get("status") != "flat"
            or state.get("open_position") is not None
            or state.get("pending_action") is not None
        ):
            self.entry_authorized = False
            self.entry_gate_block_reason = "gradient_next_round_requires_verified_flat_account"
            self.last_action = {
                "action": "blocked",
                "reason": state.get("manual_review_reason") or "account_not_flat",
            }
            return state
        predecessor = str(pilot.get("pilot_id") or "")
        next_pilot = await asyncio.to_thread(
            self.pilot_store.initialize_gradient_pilot,
            market_id=int(self.market_metadata["market_id"]),
            multiplier=Decimal(str(self.market_metadata["multiplier"])),
            config=self._gradient_pilot_config(),
            max_layers=WTI_GRADIENT_MAX_LAYERS,
            rotation_test_id=getattr(self.args, "rotation_test_id", None),
            continuous_rotation=True,
            new_pilot_after=predecessor,
        )
        self.gradient_pilot = dict(next_pilot)
        self.entry_authorized = False
        self.entry_gate_block_reason = "gradient_next_round_armed_waiting_for_fresh_sample"
        self.last_action = {
            "action": "gradient_round_armed",
            "round_number": next_pilot.get("round_number"),
            "pilot_id": next_pilot.get("pilot_id"),
        }
        self.logger.info(
            "wti_gradient_round_armed session_id=%s round=%s pilot_id=%s",
            next_pilot.get("rotation_test_id"), next_pilot.get("round_number"),
            next_pilot.get("pilot_id"),
        )
        return state

    async def _arm_manual_gradient_round(
        self, state: dict[str, Any], row: dict[str, Any]
    ) -> dict[str, Any]:
        predecessor = getattr(self.args, "new_gradient_pilot_after", None)
        if not predecessor or (state.get("gradient_pilot") or {}).get("pilot_id") != predecessor:
            return state
        if (
            self.manage_existing_only or self.entry_authorization_permanently_revoked
            or getattr(self, "rotation_drain_request", None) is not None
            or state.get("status") != "flat" or state.get("open_position") is not None
            or state.get("pending_action") is not None
        ):
            self.entry_authorized = False
            return state
        candidate = await self._decision_sample(row)
        if candidate is None:
            return state
        now = datetime.now(timezone.utc)
        if (
            self.last_risk.get("risk_action") not in {"normal", "warning"}
            or self._gradient_equity_capacity_by_tier() is None
            or not _session_window(candidate, now, self.config.entry_cutoff_seconds).allow_new_entries
        ):
            self.entry_authorized = False
            self.entry_gate_block_reason = "manual_gradient_start_waiting_for_live_risk_and_session"
            return state
        assert self.pilot_store is not None and self.market_metadata is not None
        self.gradient_pilot = await asyncio.to_thread(
            self.pilot_store.initialize_gradient_pilot,
            market_id=int(self.market_metadata["market_id"]),
            multiplier=Decimal(str(self.market_metadata["multiplier"])),
            config=self._gradient_pilot_config(), max_layers=WTI_GRADIENT_MAX_LAYERS,
            manual_new_pilot_after=predecessor, runtime_pid=os.getpid(), runtime_run_id=self.run_id,
        )
        self.entry_authorized = False
        self.entry_gate_block_reason = "manual_gradient_round_armed_waiting_for_fresh_sample"
        self.logger.info(
            "manual_gradient_round_armed predecessor=%s pilot_id=%s",
            predecessor, self.gradient_pilot["pilot_id"],
        )
        return self.pilot_store.load()

    def _refresh_readiness(self) -> None:
        if self.args.mode != "live" or not self.market_metadata:
            self.readiness = {"ready": False, "reasons": ["wti_readiness_metadata_unavailable"]}
            return
        try:
            rows = read_basis_samples(
                self.args.sample_root, limit=200000, asset_filter="WTI"
            )
            self.readiness = readiness_for_new_entries(
                rows,
                market_id=int(self.market_metadata["market_id"]),
                multiplier=Decimal(str(self.market_metadata["multiplier"])),
                now=datetime.now(timezone.utc),
            )
        except Exception as exc:
            self.readiness = {
                "ready": False,
                "reasons": [f"wti_readiness_recheck_failed:{type(exc).__name__}"],
            }

    async def _gradient_thresholds_for_sample(
        self, row: dict[str, Any]
    ) -> dict[str, tuple[Decimal | None, ...]] | None:
        if (
            getattr(self, "manage_existing_only", False)
            or
            getattr(self, "gradient_pilot", None) is None
            or getattr(self, "market_metadata", None) is None
            or getattr(self, "config", None) is None
        ):
            return None
        now_monotonic = time.monotonic()
        profiles = getattr(self, "wti_gradient_threshold_profiles", {})
        refreshed_monotonic = getattr(
            self, "wti_gradient_threshold_refreshed_monotonic", 0.0
        )
        if (
            not profiles
            or now_monotonic - refreshed_monotonic
            >= WTI_THRESHOLD_REFRESH_SECONDS
        ):
            try:
                sample_at = datetime.fromisoformat(
                    str(row["logged_at"]).replace("Z", "+00:00")
                ).astimezone(timezone.utc)
                pilot_config = self.gradient_pilot.get("config") or {}
                floors = [
                    Decimal(str(value))
                    for value in pilot_config.get("tier_thresholds_bps", [])
                ]
                if len(floors) != WTI_GRADIENT_MAX_LAYERS:
                    raise ValueError("gradient_pilot_tier_floors_invalid")
                rows = await asyncio.to_thread(
                    read_basis_samples,
                    self.args.sample_root,
                    limit=200000,
                    asset_filter="WTI",
                    sample_kind_filter="baseline",
                    sample_quality_filter="valid",
                    since_utc=sample_at - timedelta(seconds=WTI_THRESHOLD_WINDOW_SECONDS),
                    until_utc=sample_at,
                )
                profiles = build_wti_gradient_threshold_profiles(
                    rows,
                    as_of=sample_at,
                    market_id=int(self.market_metadata["market_id"]),
                    multiplier=Decimal(str(self.market_metadata["multiplier"])),
                    first_floor_bps=floors[0],
                    second_floor_bps=floors[1],
                    tier_floors_bps=tuple(floors),
                    roundtrip_cost_bps=self.config.roundtrip_cost_bps,
                    exit_target_bps=self.config.exit_target_bps,
                    sample_interval_seconds=int(self.args.sample_interval_seconds),
                )
                self.wti_gradient_threshold_profiles = profiles
                self.wti_gradient_threshold_source = "directional_7d_percentiles"
            except Exception as exc:
                self.logger.warning(
                    "wti_gradient_threshold_refresh_failed reason=%s; retaining hard floor tiers",
                    type(exc).__name__,
                )
                pilot_config = self.gradient_pilot.get("config") or {}
                floors = [Decimal(str(value)) for value in pilot_config.get("tier_thresholds_bps", [])]
                if len(floors) != WTI_GRADIENT_MAX_LAYERS:
                    return None
                self.wti_gradient_threshold_profiles = build_wti_gradient_threshold_profiles(
                    [],
                    as_of=datetime.fromisoformat(
                        str(row["logged_at"]).replace("Z", "+00:00")
                    ).astimezone(timezone.utc),
                    market_id=int(self.market_metadata["market_id"]),
                    multiplier=Decimal(str(self.market_metadata["multiplier"])),
                    first_floor_bps=floors[0],
                    second_floor_bps=floors[1],
                    tier_floors_bps=tuple(floors),
                    roundtrip_cost_bps=self.config.roundtrip_cost_bps,
                    exit_target_bps=self.config.exit_target_bps,
                    sample_interval_seconds=int(self.args.sample_interval_seconds),
                )
                self.wti_gradient_threshold_source = "history_error_floor_fallback"
            self.wti_gradient_threshold_refreshed_monotonic = now_monotonic
        return {
            direction: profile.thresholds_bps
            for direction, profile in getattr(
                self, "wti_gradient_threshold_profiles", {}
            ).items()
        }

    def _gradient_equity_capacity_by_tier(self) -> tuple[Decimal, ...] | None:
        if (
            getattr(self, "manage_existing_only", False)
            or not getattr(self.args, "gradient_live", False)
        ):
            return None
        risk = self.last_risk
        if risk.get("risk_data_complete") is not True:
            return None
        try:
            observed = datetime.fromisoformat(
                str(risk["observed_at"]).replace("Z", "+00:00")
            )
            age = (datetime.now(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds()
            var_equity = Decimal(str(risk["variational_equity_usd"]))
            lighter_equity = Decimal(str(risk["lighter_equity_usd"]))
            layer = Decimal(str(getattr(self.args, "_validated_gradient_layer_notional", None)))
            order_limit = Decimal(str(getattr(self.args, "_validated_notional", None)))
            if (
                observed.tzinfo is None or observed.utcoffset() is None
                or age < -5 or age > 15
                or not var_equity.is_finite() or not lighter_equity.is_finite()
                or var_equity <= 0 or lighter_equity <= 0
                or not layer.is_finite() or not order_limit.is_finite()
                or layer <= 0 or layer > order_limit
            ):
                return None
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return None
        slots = equity_gradient_slot_caps(
            variational_equity_usd=var_equity,
            lighter_equity_usd=lighter_equity,
            child_notional_usd=layer,
            tier_count=WTI_GRADIENT_MAX_LAYERS,
            max_equity_multiple=WTI_MAX_EQUITY_MULTIPLE,
        )
        return None if slots is None else tuple(Decimal(count) * layer for count in slots)

    async def _load_startup_readiness(self) -> None:
        started = time.monotonic()
        await asyncio.to_thread(self._refresh_readiness)
        self.last_readiness_elapsed_ms = round((time.monotonic() - started) * 1000, 3)
        self.logger.info(
            "wti_startup_readiness_loaded elapsed_ms=%s",
            self.last_readiness_elapsed_ms,
        )

    @staticmethod
    def _position_notional(state: dict[str, Any], row: dict[str, Any] | None) -> Decimal:
        position = state.get("open_position") or {}
        qty = Decimal(str(position.get("var_qty") or "0"))
        if row and row.get("var_bid") and row.get("var_ask"):
            midpoint = (Decimal(str(row["var_bid"])) + Decimal(str(row["var_ask"]))) / 2
            return abs(qty * midpoint)
        return Decimal("0")

    @staticmethod
    def _sample_freshness_diagnostics(
        row: dict[str, Any] | None, now: datetime
    ) -> dict[str, Any]:
        checks: dict[str, bool] = {
            "sample_timestamp": False,
            "sample_age": False,
            "sample_quality": False,
            "both_feeds_live": False,
            "lighter_continuity": False,
            "session_evidence": False,
            "quote_age_at_capture": False,
            "quote_age_effective": False,
            "book_age_at_capture": False,
            "book_age_effective": False,
        }
        result: dict[str, Any] = {
            "sample_id": row.get("sample_id") if isinstance(row, dict) else None,
            "sample_kind": row.get("sample_kind") if isinstance(row, dict) else None,
            "sample_quality": row.get("sample_quality") if isinstance(row, dict) else None,
            "sample_quality_reasons": (
                row.get("sample_quality_reasons") if isinstance(row, dict) else None
            ),
            "market_availability": (
                row.get("market_availability") if isinstance(row, dict) else None
            ),
            "lighter_continuity_ok": (
                row.get("lighter_continuity_ok") if isinstance(row, dict) else None
            ),
            "capture_age_seconds": None,
            "quote_age_at_capture_seconds": None,
            "quote_age_effective_seconds": None,
            "book_age_at_capture_seconds": None,
            "book_age_effective_seconds": None,
            "session_heartbeat_age_seconds": None,
            "checks": checks,
        }
        if not isinstance(row, dict) or now.tzinfo is None or now.utcoffset() is None:
            result["failed_checks"] = list(checks)
            result["refreshable_cache_expiry"] = False
            result["fresh"] = False
            return result

        try:
            at = datetime.fromisoformat(str(row.get("logged_at")).replace("Z", "+00:00"))
            if at.tzinfo is None or at.utcoffset() is None:
                raise ValueError("sample_timestamp_timezone_missing")
            age = (now.astimezone(timezone.utc) - at.astimezone(timezone.utc)).total_seconds()
            result["capture_age_seconds"] = round(age, 3)
            checks["sample_timestamp"] = True
            checks["sample_age"] = -5 <= age <= 20
        except (TypeError, ValueError):
            age = None

        quote_age: Decimal | None = None
        book_age: Decimal | None = None
        try:
            quote_age = Decimal(str(row.get("var_quote_age_seconds")))
            if quote_age.is_finite():
                result["quote_age_at_capture_seconds"] = float(quote_age)
                checks["quote_age_at_capture"] = (
                    Decimal("-1") <= quote_age <= Decimal("2")
                )
        except Exception:
            pass
        try:
            book_age = Decimal(str(row.get("lighter_book_age_seconds")))
            if book_age.is_finite():
                result["book_age_at_capture_seconds"] = float(book_age)
                checks["book_age_at_capture"] = Decimal("0") <= book_age <= Decimal("2")
        except Exception:
            pass

        if age is not None and quote_age is not None and quote_age.is_finite():
            effective_quote_age = quote_age + Decimal(str(max(0.0, age)))
            result["quote_age_effective_seconds"] = float(effective_quote_age)
            checks["quote_age_effective"] = effective_quote_age <= Decimal("2")
        if age is not None and book_age is not None and book_age.is_finite():
            effective_book_age = book_age + Decimal(str(max(0.0, age)))
            result["book_age_effective_seconds"] = float(effective_book_age)
            checks["book_age_effective"] = effective_book_age <= Decimal("2")

        checks["sample_quality"] = row.get("sample_quality") == "valid"
        checks["both_feeds_live"] = row.get("market_availability") == "both_feeds_live"
        checks["lighter_continuity"] = row.get("lighter_continuity_ok") is True
        checks["session_evidence"] = session_observation_for_sample(row) is not None
        heartbeat = row.get("session_status_feed_heartbeat_at")
        try:
            heartbeat_at = datetime.fromisoformat(str(heartbeat).replace("Z", "+00:00"))
            if heartbeat_at.tzinfo is not None and heartbeat_at.utcoffset() is not None:
                result["session_heartbeat_age_seconds"] = round(
                    (now.astimezone(timezone.utc) - heartbeat_at.astimezone(timezone.utc)).total_seconds(),
                    3,
                )
        except (TypeError, ValueError):
            pass

        failed = [name for name, passed in checks.items() if not passed]
        result["failed_checks"] = failed
        # Only cached age expiry is recoverable in-cycle. Feed, continuity,
        # instrument/session evidence and source-clock failures remain hard blocks.
        cache_age_checks = {"sample_age", "quote_age_effective", "book_age_effective"}
        result["refreshable_cache_expiry"] = bool(
            failed
            and set(failed).issubset(cache_age_checks)
            and checks["sample_timestamp"]
            and age is not None and age >= -5
            and checks["quote_age_at_capture"]
            and checks["book_age_at_capture"]
            and checks["sample_quality"]
            and checks["both_feeds_live"]
            and checks["lighter_continuity"]
            and checks["session_evidence"]
        )
        result["fresh"] = not failed
        return result

    @staticmethod
    def _sample_health_fresh(row: dict[str, Any] | None, now: datetime) -> bool:
        return bool(WTILiveRuntime._sample_freshness_diagnostics(row, now).get("fresh"))

    def _revoke_entry_authorization(self, reason: str) -> None:
        self.entry_authorized = False
        self.entry_gate_block_reason = reason
        self.entry_authorization_permanently_revoked = True

    def _record_new_sample(self, row: dict[str, Any]) -> bool | None:
        sample_id = str(row.get("sample_id") or "")
        if not sample_id or sample_id in {
            getattr(self, "last_seen_sample_id", None),
            getattr(self, "last_processed_sample_id", None),
        }:
            return None
        self.last_seen_sample_id = sample_id
        if row.get("sample_quality") != "valid" or session_observation_for_sample(row) is None:
            self.last_sample_freshness_diagnostics = self._sample_freshness_diagnostics(
                row, datetime.now(timezone.utc)
            )
            self.last_action = {"action": "blocked", "reason": "wti_sample_or_session_unverified"}
            self.entry_authorized = False
            if not self.entry_authorization_permanently_revoked:
                self.entry_gate_block_reason = "wti_sample_or_session_unverified"
            return False
        diagnostics = self._sample_freshness_diagnostics(row, datetime.now(timezone.utc))
        self.last_sample_freshness_diagnostics = diagnostics
        if not diagnostics["fresh"]:
            reason = (
                "wti_cached_sample_expired_execution_refresh_required"
                if diagnostics["refreshable_cache_expiry"]
                else "wti_current_sample_or_session_not_fresh"
            )
            self.last_action = {"action": "blocked", "reason": reason}
            self.entry_authorized = False
            if not self.entry_authorization_permanently_revoked:
                self.entry_gate_block_reason = reason
            return bool(diagnostics["refreshable_cache_expiry"])
        try:
            sample_at = datetime.fromisoformat(
                str(row.get("logged_at")).replace("Z", "+00:00")
            )
            if sample_at.tzinfo is None or sample_at.utcoffset() is None:
                raise ValueError("sample_timestamp_timezone_missing")
            sample_at = sample_at.astimezone(timezone.utc)
            last_processed_at = getattr(self, "last_processed_sample_at", None)
            if last_processed_at is not None:
                self.last_sample_gap_seconds = (
                    sample_at - last_processed_at
                ).total_seconds()
                if (
                    self.last_sample_gap_seconds < 0
                    or self.last_sample_gap_seconds > MAX_WTI_ENTRY_GAP_SECONDS
                ):
                    self.pending_gap_exit = True
                    self._revoke_entry_authorization(
                        "wti_entry_authorization_revoked_after_sample_gap"
                    )
            self.last_processed_sample_at = sample_at
        except (TypeError, ValueError):
            self._revoke_entry_authorization("wti_sample_timestamp_unverified")
            return False
        self.last_processed_sample_id = sample_id
        return True

    def _write_health(
        self,
        *,
        state: dict[str, Any] | None,
        row: dict[str, Any] | None,
        error: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        state = state or read_json(self.args.state_path)
        exposure = state.get("open_position") is not None
        pending = state.get("pending_action") is not None
        sample_at = None
        age = None
        if row:
            sample_at = row.get("logged_at")
            with contextlib.suppress(TypeError, ValueError):
                at = datetime.fromisoformat(str(sample_at).replace("Z", "+00:00"))
                age = (now - at.astimezone(timezone.utc)).total_seconds()
        stale = not self._sample_health_fresh(row, now)
        freshness_diagnostics = self._sample_freshness_diagnostics(row, now)
        closed_window = self._confirmed_closed_session_window(now)
        risk = dict(self.last_risk)
        if self.args.mode == "observe":
            action, reason = "block_entry", "observe_only_mode"
        elif stale:
            action, reason = "block_entry", (
                "wti_exchange_closed" if closed_window is not None
                else "wti_market_sample_or_session_unavailable"
            )
        elif self.entry_gate_block_reason:
            action, reason = "block_entry", self.entry_gate_block_reason
        else:
            action = str(risk.get("risk_action") or "block_entry")
            reason = str(risk.get("risk_reason") or "wti_account_risk_snapshot_unavailable")
        health = {
            "schema_version": 1,
            "asset": "WTI",
            "run_id": self.run_id,
            "pid": os.getpid(),
            "updated_at": now.isoformat(),
            "status": "running" if not self.stop else "stopped",
            "execution_mode": (
                "live_bounded_gradient_pilot"
                if self.args.mode == "live" and getattr(self, "gradient_pilot", None)
                else "live_single_round_pilot"
                if self.args.mode == "live" and self.single_round_pilot
                else "live_bounded" if self.args.mode == "live" else "observe_only"
            ),
            "historical_readiness_gate_bypassed": bool(
                self.args.mode == "live"
                and _unqualified_data_pilot_enabled(self.args)
                and not self.readiness.get("market_data_ready")
                and self.readiness.get("historical_volume_bypass_ready") is True
            ),
            "historical_readiness_bypass_scope": (
                "session_count_sample_count_and_coverage_only"
                if self.args.mode == "live"
                and _unqualified_data_pilot_enabled(self.args)
                and not self.readiness.get("market_data_ready")
                and self.readiness.get("historical_volume_bypass_ready") is True
                else None
            ),
            "historical_volume_bypass_evidence_ready": bool(
                self.readiness.get("historical_volume_bypass_ready")
            ),
            "single_round_pilot": state.get("single_round_pilot"),
            "gradient_pilot": state.get("gradient_pilot"),
            "wti_entry_authorized": self.entry_authorized,
            "startup_reconciled": self.startup_reconciled,
            "entry_authorization_permanently_revoked": self.entry_authorization_permanently_revoked,
            "runtime_error": getattr(self, "last_runtime_error", None),
            "runtime_cycle_error": getattr(self, "last_cycle_error", None),
            "verified_cycle_count": getattr(self, "verified_cycle_count", 0),
            "last_verified_cycle_at": getattr(self, "last_verified_cycle_at", None),
            "last_verified_cycle_kind": getattr(self, "last_verified_cycle_kind", None),
            "readiness_elapsed_ms": getattr(self, "last_readiness_elapsed_ms", None),
            "reconcile_elapsed_ms": getattr(self, "last_reconcile_elapsed_ms", None),
            "entry_gate_block_reason": self.entry_gate_block_reason,
            "last_sample_gap_seconds": self.last_sample_gap_seconds,
            "manage_existing_only": self.manage_existing_only,
            "rotation_drain_status": (
                (read_json(getattr(self.args, "control_path", WTI_CONTROL_PATH)) or {}).get("status")
                if getattr(self, "rotation_drain_request", None) else None
            ),
            "risk_action": action,
            "risk_reason": error or reason,
            "risk_data_complete": risk.get("risk_data_complete"),
            "variational_equity_usd": risk.get("variational_equity_usd"),
            "lighter_equity_usd": risk.get("lighter_equity_usd"),
            "max_maintenance_margin_usage_pct": risk.get("max_maintenance_margin_usage_pct"),
            "max_projected_venue_leverage": risk.get("max_projected_venue_leverage"),
            "account_snapshot_observed_at": risk.get("observed_at"),
            "market_sample_at": sample_at,
            "market_sample_age_seconds": None if age is None else round(age, 3),
            "market_sample_fresh": not stale,
            "market_sample_freshness_diagnostics": freshness_diagnostics,
            "last_seen_sample_id": getattr(self, "last_seen_sample_id", None),
            "last_processed_sample_id": getattr(self, "last_processed_sample_id", None),
            "execution_refresh_status": getattr(
                self, "last_execution_refresh_status", "not_attempted"
            ),
            "execution_refresh_at": getattr(self, "last_execution_refresh_at", None),
            "execution_refresh_elapsed_ms": getattr(
                self, "last_execution_refresh_elapsed_ms", None
            ),
            "execution_refresh_error": getattr(
                self, "last_execution_refresh_error", None
            ),
            "last_sample_processing_diagnostics": getattr(
                self, "last_sample_freshness_diagnostics", {}
            ),
            "market_session": (
                row.get("session_status") if row else
                OilSession.EXCHANGE_CLOSED.value if closed_window is not None else None
            ),
            "last_action": self.last_action,
            "gradient_threshold_source": getattr(
                self, "wti_gradient_threshold_source", "not_loaded"
            ),
            "gradient_threshold_profiles": {
                direction: profile.as_dict()
                for direction, profile in getattr(
                    self, "wti_gradient_threshold_profiles", {}
                ).items()
            },
            "open_lots_total": int(exposure),
            "pending_actions_total": int(pending),
            "manual_review_reason": state.get("manual_review_reason"),
            "pending_order_diagnostics": {
                venue: {
                    "submission_started": leg.get("submission_started"),
                    "classification": (leg.get("result") or {}).get("classification"),
                    **{key: (leg.get("result") or {})[key] for key in (
                        "error_code", "error_type", "lookup_stage", "lookup_reason",
                        "submission_stage", "native_client_order_id", "transaction_hash",
                    ) if key in (leg.get("result") or {})},
                    "submission_result": leg.get("submission_result"),
                }
                for venue, leg in ((state.get("pending_action") or {}).get("legs") or {}).items()
                if isinstance(leg, dict)
            },
            "readiness": self.readiness,
            "errors": [] if error is None else [error],
        }
        write_json_atomic(self.args.health_path, health)
        self.last_health_write_monotonic = time.monotonic()

    def _poll_rotation_drain(self) -> bool:
        control_path = getattr(self.args, "control_path", WTI_CONTROL_PATH)
        control = read_json(control_path)
        if (
            control.get("action") != "drain_after_flat"
            or control.get("asset") != "WTI"
            or control.get("target_pid") != os.getpid()
            or control.get("target_run_id") != self.run_id
            or control.get("status") not in {"requested", "draining"}
            or not control.get("request_id")
        ):
            return False
        if getattr(self, "rotation_drain_request", None) is None:
            self.rotation_drain_request = dict(control)
            self.manage_existing_only = True
            self._revoke_entry_authorization("rotation_drain_requested")
            self.last_action = {"action": "draining", "reason": "rotation_drain_requested"}
        if control.get("status") == "requested":
            draining = {**control, "status": "draining", "draining_at": datetime.now(timezone.utc).isoformat()}
            write_json_atomic(control_path, draining)
            self.rotation_drain_request = draining
        return True

    def _complete_rotation_drain_if_verified_flat(self, state: dict[str, Any]) -> bool:
        request = getattr(self, "rotation_drain_request", None)
        if request is None:
            return False
        proof = state.get("last_account_reconciliation") or {}
        try:
            observed_at = datetime.fromisoformat(str(proof["observed_at"]).replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds()
            flat = (
                proof.get("verified") is True
                and proof.get("active_order_count") == 0
                and Decimal(str(proof.get("variational_qty"))) == 0
                and Decimal(str(proof.get("lighter_qty"))) == 0
                and -5 <= age <= 20
                and proof.get("market_id") == getattr(self.args, "expected_lighter_market_id", None)
            )
        except (KeyError, TypeError, ValueError, ArithmeticError):
            flat = False
        if (
            state.get("status") != "flat"
            or state.get("open_position") is not None
            or state.get("pending_action") is not None
            or not self.startup_reconciled
            or not flat
        ):
            return False
        control_path = getattr(self.args, "control_path", WTI_CONTROL_PATH)
        current = read_json(control_path)
        if current.get("request_id") != request.get("request_id"):
            self._revoke_entry_authorization("rotation_drain_request_changed")
            return False
        completed = {
            **current,
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "variational_position_qty": proof["variational_qty"],
            "lighter_position_qty": proof["lighter_qty"],
            "active_order_count": proof["active_order_count"],
            "variational_active_orders": 0,
            "lighter_active_orders": 0,
            "reconciliation_observed_at": proof["observed_at"],
        }
        write_json_atomic(control_path, completed)
        self.rotation_drain_request = completed
        self.last_action = {"action": "rotation_drain_completed", "request_id": request["request_id"]}
        self.request_stop()
        return True

    async def _read_reconcile(self, row: dict[str, Any]) -> dict[str, Any]:
        assert self.executor is not None
        started = time.monotonic()
        now = datetime.now(timezone.utc)
        session = _session_window(row, now, self.args.entry_cutoff_seconds)
        try:
            return await asyncio.to_thread(
                lambda: self.executor.reconcile(
                    sample=row, now=now, session=session,
                    runtime_pid=os.getpid(), runtime_run_id=getattr(self, "run_id", None),
                )
            )
        finally:
            self.last_reconcile_elapsed_ms = round((time.monotonic() - started) * 1000, 3)

    async def _decision_sample(self, row: dict[str, Any]) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        diagnostics = self._sample_freshness_diagnostics(row, now)
        candidate: dict[str, Any] | None = row if diagnostics["fresh"] else None
        refresh_trigger = list(diagnostics.get("failed_checks") or [])
        if candidate is None:
            latest = self.collector.latest_sample
            latest_diagnostics = self._sample_freshness_diagnostics(latest, now)
            if latest_diagnostics["fresh"] and isinstance(latest, dict):
                candidate = dict(latest)
                diagnostics = latest_diagnostics
            else:
                self.entry_authorized = False
                if not self.entry_authorization_permanently_revoked:
                    self.entry_gate_block_reason = "wti_current_sample_or_session_not_fresh"
                self.last_execution_refresh_status = "running"
                self.last_execution_refresh_at = now.isoformat()
                self.last_execution_refresh_error = None
                started = time.monotonic()
                try:
                    candidate = await self.collector.collect_fresh_sample(sample_kind="burst")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    detail = " ".join(str(exc).split())[:160] or type(exc).__name__
                    self.last_execution_refresh_status = "unavailable"
                    self.last_execution_refresh_error = f"{type(exc).__name__}:{detail}"
                    self.last_execution_refresh_elapsed_ms = round(
                        (time.monotonic() - started) * 1000, 3
                    )
                    self.last_sample_freshness_diagnostics = {
                        **diagnostics,
                        "refresh_trigger_checks": refresh_trigger,
                        "refresh_error_type": type(exc).__name__,
                        "refresh_error": detail,
                    }
                    self.logger.warning(
                        "wti_execution_refresh_unavailable trigger=%s error_type=%s reason=%s",
                        ",".join(refresh_trigger), type(exc).__name__, detail,
                    )
                    return None
                self.last_execution_refresh_elapsed_ms = round(
                    (time.monotonic() - started) * 1000, 3
                )
                self.last_execution_refresh_status = "returned"
                self.last_execution_refresh_error = None
                if not isinstance(candidate, dict):
                    self.last_execution_refresh_status = "invalid"
                    self.last_sample_freshness_diagnostics = {
                        **diagnostics,
                        "refresh_trigger_checks": refresh_trigger,
                        "refresh_error": "wti_fresh_sample_unavailable",
                    }
                    return None
                candidate = dict(candidate)

        if self._record_new_sample(candidate) is False:
            self.last_execution_refresh_status = (
                "invalid" if candidate.get("sample_kind") == "burst"
                else self.last_execution_refresh_status
            )
            self.last_sample_freshness_diagnostics = self._sample_freshness_diagnostics(
                candidate, datetime.now(timezone.utc)
            )
            return None
        diagnostics = self._sample_freshness_diagnostics(
            candidate, datetime.now(timezone.utc)
        )
        if not diagnostics["fresh"]:
            self.last_execution_refresh_status = (
                "stale" if candidate.get("sample_kind") == "burst"
                else self.last_execution_refresh_status
            )
            self.last_sample_freshness_diagnostics = {
                **diagnostics,
                "refresh_trigger_checks": refresh_trigger,
            }
            return None
        self.last_sample_freshness_diagnostics = diagnostics
        if candidate.get("sample_kind") == "burst":
            self.last_execution_refresh_status = "fresh"
        return candidate

    async def _startup_reconcile_sample(self, row: dict[str, Any]) -> dict[str, Any]:
        candidate = await self._decision_sample(row)
        if candidate is None:
            self.entry_authorized = False
            self.last_action = {
                "action": "blocked",
                "reason": (
                    "wti_execution_refresh_unavailable"
                    if self.last_execution_refresh_status == "unavailable"
                    else "wti_execution_refresh_sample_invalid"
                    if self.last_execution_refresh_status in {"invalid", "stale"}
                    else "wti_current_sample_or_session_not_fresh"
                ),
            }
            return read_json(self.args.state_path)
        try:
            state = await self._read_reconcile(candidate)
        except WTIExecutionError as exc:
            if not _retryable_sample_validation_error(exc):
                raise
            self.entry_authorized = False
            if not self.entry_authorization_permanently_revoked:
                self.entry_gate_block_reason = "wti_current_sample_or_session_not_fresh"
            self.last_runtime_error = str(exc)
            self.last_action = {"action": "deferred", "reason": str(exc)}
            self.logger.warning("wti_startup_reconcile_deferred reason=%s", exc)
            return read_json(self.args.state_path)

        self.last_risk = dict(self.gateway.latest_risk) if self.gateway else {}
        self.startup_reconciled = state.get("status") in {"flat", "open"}
        state = await self._arm_manual_gradient_round(state, candidate)
        state = await self._advance_continuous_gradient_round(state)
        self._sync_entry_authorization(
            state, candidate, now=datetime.now(timezone.utc)
        )
        self.last_action = {"action": "startup_reconcile", "status": state.get("status")}
        self.last_runtime_error = None
        return state

    async def _process_sample(self, row: dict[str, Any]) -> dict[str, Any]:
        assert self.executor is not None and self.gateway is not None and self.config is not None
        decision_sample = await self._decision_sample(row)
        if decision_sample is None:
            state = read_json(self.args.state_path)
            position = state.get("open_position")
            self.entry_authorized = False
            self.last_action = {
                "action": "blocked_close" if position else "blocked_entry",
                "reason": (
                    "wti_execution_refresh_unavailable"
                    if self.last_execution_refresh_status == "unavailable"
                    else "wti_execution_refresh_sample_invalid"
                    if self.last_execution_refresh_status in {"invalid", "stale"}
                    else "wti_current_sample_or_session_not_fresh"
                ),
            }
            return state
        row = decision_sample
        state = await self._read_reconcile(row)
        self.last_risk = dict(self.gateway.latest_risk)
        self._poll_rotation_drain()
        if getattr(self, "rotation_drain_request", None) is None:
            state = await self._arm_manual_gradient_round(state, row)
            state = await self._advance_continuous_gradient_round(state)
        if state.get("status") != "flat" and state.get("status") != "open":
            self.last_action = {"action": "reconcile", "reason": state.get("manual_review_reason") or state.get("status")}
            return state
        position = state.get("open_position")
        decision_sample = await self._decision_sample(row)
        if decision_sample is None:
            self.entry_authorized = False
            self.last_action = {
                "action": "blocked_close" if position else "blocked_entry",
                "reason": (
                    "wti_execution_refresh_unavailable"
                    if self.last_execution_refresh_status == "unavailable"
                    else "wti_execution_refresh_sample_invalid"
                    if self.last_execution_refresh_status in {"invalid", "stale"}
                    else "wti_current_sample_or_session_not_fresh"
                ),
            }
            return state
        row = decision_sample
        now = datetime.now(timezone.utc)
        if self._complete_rotation_drain_if_verified_flat(state):
            return state
        if position is None or getattr(self, "gradient_pilot", None) is not None:
            self._sync_entry_authorization(state, row, now=now)
        else:
            self.entry_authorized = False
        self.gateway.set_current_notional(self._position_notional(state, row))
        risk_action = str(self.last_risk.get("risk_action") or "block_entry")
        window = _session_window(row, now, self.config.entry_cutoff_seconds)
        gradient_entry_mode = bool(
            getattr(self.args, "gradient_live", False)
            and not self.manage_existing_only
            and getattr(self, "gradient_pilot", None) is not None
        )
        if gradient_entry_mode:
            self.gradient_pilot = dict(state.get("gradient_pilot") or self.gradient_pilot)
            thresholds_by_direction = await self._gradient_thresholds_for_sample(row)
            equity_capacity = self._gradient_equity_capacity_by_tier()
            if equity_capacity is None:
                decision = decide_wti_action(row, position, config=self.config, now=now)
                if decision.get("action") != "close":
                    decision = {"action": "hold", "reason": "gradient_equity_capacity_unavailable"}
            else:
                decision = decide_wti_gradient_action(
                    row, position, self.gradient_pilot, config=self.config, now=now,
                    thresholds_by_direction=thresholds_by_direction,
                    aggregate_capacity_by_tier_usd=equity_capacity,
                )
        elif self.manage_existing_only and position is None:
            decision = {"action": "hold", "reason": "management_only_flat_no_new_entries"}
        else:
            decision = decide_wti_action(row, position, config=self.config, now=now)
        if position is None:
            self.pending_gap_exit = False
        elif self.pending_gap_exit:
            decision = {"action": "close", "reason": "sample_gap_requires_position_drain"}
        elif risk_action in {"force_reduce", "emergency_exit"}:
            decision = {"action": "close", "reason": f"account_risk_{risk_action}"}
        if decision["action"] == "close":
            if not window.allow_position_management:
                self.last_action = {
                    "action": "blocked_close", "reason": "exchange_session_management_unavailable",
                    "trigger": decision.get("reason"),
                }
                return state
            state = await asyncio.to_thread(
                lambda: self.executor.close(
                    sample=row, session=window, decision=decision,
                    exit_target_bps=self.config.exit_target_bps,
                    roundtrip_cost_bps=self.config.roundtrip_cost_bps,
                    now=datetime.now(timezone.utc),
                )
            )
            if state.get("status") == "flat":
                self.pending_gap_exit = False
            close_check = state.get("last_close_check") or {}
            if (
                state.get("status") == "open"
                and not state.get("pending_action")
                and close_check.get("reason") == "exit_target_not_reached_after_full_size_refresh"
            ):
                self.last_action = {"action": "hold", **close_check}
            else:
                self.last_action = {
                    "action": "close", "reason": decision["reason"],
                    "status": state.get("status"),
                }
            self._finish_single_round_pilot_if_done(state)
            self._finish_gradient_pilot_if_done(state)
            return state
        if (
            decision["action"] in {"open", "add"}
            and not self.manage_existing_only
            and self.entry_authorized
            and risk_action in {"normal", "warning"}
            and window.allow_new_entries
        ):
            gradient_entry = gradient_entry_mode
            reservation_id = None
            if gradient_entry:
                pilot_id = self.gradient_pilot["pilot_id"]
                tier = int(decision["gradient_tier"])
                notional = Decimal(str(decision["layer_notional_usd"]))
                layer_id = f"{pilot_id}:tier-{tier}:{uuid.uuid4().hex}"
            else:
                assert self.pilot_store is not None and self.single_round_pilot is not None
                pilot_id = self.single_round_pilot["pilot_id"]
                tier = None
                notional = self.args._validated_notional
                layer_id = None
                reservation_id = await asyncio.to_thread(
                    self.pilot_store.reserve_single_round_pilot_entry,
                    pilot_id,
                )
            self.gateway.set_proposed_notional(
                notional,
                equity_multiple=Decimal(tier) if gradient_entry else None,
            )
            try:
                state = await asyncio.to_thread(
                    lambda: self.executor.open(
                        sample=row,
                        direction=decision["direction"],
                        notional_usd=notional,
                        session=window,
                        pilot_id=None if gradient_entry else pilot_id,
                        pilot_reservation_id=reservation_id,
                        layer_id=layer_id,
                        gradient_tier=tier,
                        gradient_pilot_id=pilot_id if gradient_entry else None,
                        aggregate_max_notional_usd=(
                            Decimal(str(decision["aggregate_max_notional_usd"]))
                            if gradient_entry and decision.get("aggregate_max_notional_usd") is not None
                            else None
                        ),
                        decision=decision,
                        now=datetime.now(timezone.utc),
                    )
                )
            except Exception:
                if not gradient_entry and reservation_id is not None:
                    state = await asyncio.to_thread(
                        self.pilot_store.release_unsubmitted_single_round_pilot_entry,
                        pilot_id, reservation_id,
                    )
                    self._finish_single_round_pilot_if_done(state)
                raise
            finally:
                self.gateway.set_proposed_notional(None)
            self.last_risk = dict(self.gateway.latest_risk)
            if state.get("status") == "open":
                self.entry_authorized = gradient_entry
                self.entry_gate_block_reason = (
                    "gradient_pilot_next_tier_pending_or_unavailable"
                    if gradient_entry else "single_round_pilot_position_open"
                )
            self._finish_single_round_pilot_if_done(state)
            self._finish_gradient_pilot_if_done(state)
            self.last_action = {
                "action": decision["action"], "direction": decision["direction"],
                "edge_bps": decision["edge_bps"], "tier": decision.get("gradient_tier"),
                "status": state.get("status"),
            }
            return state
        self.last_action = {
            "action": decision["action"],
            "reason": decision.get("reason"),
            "risk_action": risk_action,
            "edge_bps": decision.get("edge_bps"),
        }
        return state

    def _note_verified_cycle(self, state: dict[str, Any], *, kind: str) -> bool:
        if (
            self.entry_authorization_permanently_revoked
            or state.get("status") not in {"flat", "open"}
            or state.get("pending_action") is not None
            or state.get("manual_review_reason")
            or self.last_action.get("action") in {
                "blocked", "blocked_entry", "blocked_close", "deferred", "reconcile"
            }
        ):
            return False
        self.last_verified_cycle_at = datetime.now(timezone.utc).isoformat()
        self.last_verified_cycle_kind = kind
        self.verified_cycle_count = getattr(self, "verified_cycle_count", 0) + 1
        self.last_cycle_error = None
        self.last_runtime_error = None
        return True

    async def run(self) -> None:
        validate_startup_preconditions(self.args)
        await self._start_forwarder()
        self.collector_task = asyncio.create_task(self.collector.run(), name="wti-collector")
        try:
            if self.args.mode == "live":
                await self._initialize_execution()
                sample = await self._wait_for_first_sample()
                state = read_json(self.args.state_path)
                if sample is None:
                    self.entry_authorized = False
                    self.entry_gate_block_reason = "wti_exchange_closed"
                    self.last_action = {"action": "hold", "reason": "exchange_closed_no_market_sample"}
                    try:
                        state = await self._read_account_only_reconcile()
                        self.last_risk = dict(self.gateway.latest_risk) if self.gateway else {}
                        self.startup_reconciled = state.get("status") in {"flat", "open"}
                        state = await self._advance_continuous_gradient_round(state)
                        if not self.startup_reconciled:
                            self.last_action = {
                                "action": "blocked",
                                "reason": state.get("manual_review_reason") or state.get("status"),
                            }
                        else:
                            self._note_verified_cycle(state, kind="closed_account_only")
                    except WTIExecutionError as exc:
                        self.last_runtime_error = str(exc)
                        self.last_action = {"action": "blocked", "reason": str(exc)}
                        self.logger.warning("wti_closed_session_account_reconcile_failed reason=%s", exc)
                    self.last_account_only_reconcile_monotonic = time.monotonic()
                    self._write_health(state=state, row=None, error=self.last_runtime_error)
                elif self._record_new_sample(dict(sample)) is True:
                    state = await self._startup_reconcile_sample(dict(sample))
                    self._write_health(
                        state=state, row=self.collector.latest_sample or sample,
                        error=self.last_runtime_error,
                    )
                    self._finish_single_round_pilot_if_done(state)
                    self._finish_gradient_pilot_if_done(state)
                else:
                    self._write_health(
                        state=state,
                        row=sample,
                        error=self.last_action.get("reason") or "initial_wti_sample_or_session_not_valid",
                    )
                self.logger.info(
                    "wti_live_started run_id=%s entries_authorized=%s manage_existing_only=%s readiness=%s",
                    self.run_id, self.entry_authorized, self.manage_existing_only, self.readiness,
                )
            else:
                self.logger.info("wti_observe_started run_id=%s; order submission disabled", self.run_id)

            while not self.stop and not self.collector_task.done():
                self._poll_rotation_drain()
                row = self.collector.latest_sample
                state = read_json(self.args.state_path) if self.args.mode == "live" else {}
                if self.args.mode == "live" and row is not None:
                    new_valid_sample = self._record_new_sample(dict(row))
                    if new_valid_sample is True:
                        try:
                            if not self.startup_reconciled:
                                state = await self._startup_reconcile_sample(dict(row))
                            else:
                                state = await self._process_sample(dict(row))
                                self._note_verified_cycle(state, kind="market_strategy")
                            self._finish_single_round_pilot_if_done(state)
                            self._finish_gradient_pilot_if_done(state)
                            if self.last_action.get("action") not in {
                                "blocked", "blocked_entry", "blocked_close", "deferred"
                            }:
                                self.last_runtime_error = None
                            self._write_health(
                                state=state, row=self.collector.latest_sample or row
                            )
                        except Exception as exc:
                            self.last_cycle_error = f"{type(exc).__name__}:{str(exc)[:160]}"
                            if _retryable_sample_validation_error(exc):
                                self.entry_authorized = False
                                if not self.entry_authorization_permanently_revoked:
                                    self.entry_gate_block_reason = "wti_current_sample_or_session_not_fresh"
                                self.last_runtime_error = str(exc)
                                self.last_action = {"action": "deferred", "reason": str(exc)}
                                self.logger.warning(
                                    "wti_strategy_cycle_deferred sample_id=%s reason=%s",
                                    row.get("sample_id"), exc,
                                )
                                self._write_health(state=state, row=row, error=str(exc))
                            else:
                                self.logger.exception(
                                    "wti_strategy_cycle_failed error=%s reason=%s",
                                    type(exc).__name__, exc,
                                )
                                self._revoke_entry_authorization("wti_strategy_cycle_requires_review")
                                self.last_runtime_error = type(exc).__name__
                                self.last_action = {"action": "blocked", "reason": type(exc).__name__}
                                self._write_health(state=state, row=row, error=type(exc).__name__)
                    elif new_valid_sample is False:
                        self._write_health(state=state, row=row)
                    elif time.monotonic() - self.last_health_write_monotonic >= 5:
                        self._write_health(state=state, row=row, error=self.last_runtime_error)
                elif self.args.mode == "observe":
                    self._write_health(state={}, row=row)
                elif (
                    self.args.mode == "live"
                    and self.manage_existing_only
                    and self._confirmed_closed_session_window(datetime.now(timezone.utc)) is not None
                ):
                    if (
                        not self.startup_reconciled
                        or time.monotonic() - self.last_account_only_reconcile_monotonic
                        >= ACCOUNT_ONLY_RECONCILE_INTERVAL_SECONDS
                    ):
                        try:
                            state = await self._read_account_only_reconcile()
                            self.last_risk = dict(self.gateway.latest_risk) if self.gateway else {}
                            self.startup_reconciled = state.get("status") in {"flat", "open"}
                            if self.startup_reconciled:
                                self.last_runtime_error = None
                                self.last_action = {
                                    "action": "account_only_reconcile",
                                    "status": state.get("status"),
                                }
                                self._note_verified_cycle(state, kind="closed_account_only")
                            else:
                                self.last_action = {
                                    "action": "blocked",
                                    "reason": state.get("manual_review_reason") or state.get("status"),
                                }
                        except WTIExecutionError as exc:
                            self.startup_reconciled = False
                            self.last_runtime_error = str(exc)
                            self.last_action = {"action": "blocked", "reason": str(exc)}
                            self.logger.warning("wti_closed_session_account_reconcile_failed reason=%s", exc)
                        self.last_account_only_reconcile_monotonic = time.monotonic()
                        self._complete_rotation_drain_if_verified_flat(state)
                    if time.monotonic() - self.last_health_write_monotonic >= 5:
                        self._write_health(state=state, row=None, error=self.last_runtime_error)
                elif (
                    self.args.mode == "live"
                    and time.monotonic() - self.last_health_write_monotonic >= 5
                ):
                    self._write_health(state=state, row=row, error=self.last_runtime_error)
                await asyncio.sleep(1)
            if self.collector_task.done():
                await self.collector_task
        finally:
            self.stop = True
            self.collector.request_stop()
            if self.collector_task is not None and not self.collector_task.done():
                self.collector_task.cancel()
                await asyncio.gather(self.collector_task, return_exceptions=True)
            for server in self.servers:
                server.close()
                await server.wait_closed()
            if self.account_client is not None:
                await self.account_client.__aexit__(None, None, None)
            if self.signer_client is not None:
                from tools.wti_account_probe import _close_client
                await _close_client(self.signer_client)
            self._write_health(state=None, row=self.collector.latest_sample)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone WTI pair live runtime; ETH runtime is not imported or stopped.")
    parser.add_argument("--mode", choices=("observe", "live"), default="observe")
    parser.add_argument("--confirm-eth-flat", action="store_true")
    parser.add_argument("--i-understand-live-orders", action="store_true")
    parser.add_argument("--confirm-bounded-live-pilot", action="store_true")
    parser.add_argument("--gradient-live", action="store_true")
    parser.add_argument("--new-gradient-pilot-after", help="explicit manual new round after this exact terminal gradient pilot; never enables rotation")
    parser.add_argument("--confirm-bounded-gradient-pilot", action="store_true")
    parser.add_argument("--continuous-gradient-rotation", action="store_true")
    parser.add_argument("--confirm-continuous-gradient-rotation", action="store_true")
    parser.add_argument("--rotation-test-id")
    parser.add_argument("--gradient-second-tier-bps", type=Decimal)
    parser.add_argument("--gradient-layer-notional-usd", type=Decimal, default=Decimal("20"))
    parser.add_argument("--new-pilot-after", help="authorize one new round after this exact terminal pilot ID")
    parser.add_argument("--keep-collecting-after-pilot", action="store_true")
    parser.add_argument(
        "--allow-unqualified-data-pilot",
        action="store_true",
        help="bypass only historical session/sample/coverage counts for an explicitly confirmed live pilot; live gates still apply",
    )
    parser.add_argument("--manage-existing-wti-position", action="store_true")
    parser.add_argument("--entry-threshold-bps", type=Decimal)
    parser.add_argument("--exit-target-bps", type=Decimal)
    parser.add_argument("--roundtrip-cost-bps", type=Decimal)
    parser.add_argument("--max-hold-seconds", type=float, default=21600)
    parser.add_argument("--entry-cutoff-seconds", type=float, default=1800)
    parser.add_argument("--max-notional-usd", type=Decimal, default=Decimal("20"))
    parser.add_argument("--max-slippage-bps", type=Decimal, default=Decimal("8"))
    parser.add_argument("--sample-root", type=Path, default=WTI_SAMPLE_ROOT)
    parser.add_argument("--state-path", type=Path, default=WTI_STATE_PATH)
    parser.add_argument("--health-path", type=Path, default=WTI_HEALTH_PATH)
    parser.add_argument("--control-path", type=Path, default=WTI_CONTROL_PATH)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "log" / "oil_swap")
    parser.add_argument("--collector-notional-usd", type=Decimal, default=Decimal("20"))
    parser.add_argument("--sample-interval-seconds", type=float, default=10)
    parser.add_argument("--initial-sample-timeout-seconds", type=float, default=120)
    parser.add_argument("--command-timeout-seconds", type=float, default=12)
    parser.add_argument("--forwarder-host", default="127.0.0.1")
    parser.add_argument("--forwarder-ws-port", type=int, default=8766)
    parser.add_argument("--forwarder-rest-port", type=int, default=8767)
    parser.add_argument("--forwarder-command-port", type=int, default=8768)
    parser.add_argument("--lighter-base-url", default=DEFAULT_LIGHTER_URL)
    parser.add_argument("--expected-lighter-market-id", type=int)
    args = parser.parse_args(argv)
    bounded_history_mode = bool(
        args.mode == "live"
        and (
            (args.confirm_bounded_live_pilot and not args.gradient_live)
            or (args.gradient_live and args.confirm_bounded_gradient_pilot)
        )
    )
    explicitly_confirmed_continuous_mode = bool(
        args.mode == "live" and args.gradient_live
        and args.confirm_bounded_gradient_pilot
        and args.continuous_gradient_rotation
        and args.confirm_continuous_gradient_rotation
        and args.rotation_test_id
    )
    if args.allow_unqualified_data_pilot and not (
        bounded_history_mode or explicitly_confirmed_continuous_mode
    ):
        parser.error(
            "historical data bypass requires an explicitly confirmed live pilot or rotation mode"
        )
    if args.allow_unqualified_data_pilot and args.manage_existing_wti_position:
        parser.error("historical data bypass cannot be used for management-only mode")
    if args.new_pilot_after and (args.mode != "live" or not args.confirm_bounded_live_pilot):
        parser.error("new-pilot-after requires explicit bounded live pilot mode")
    if args.new_gradient_pilot_after and (
        args.mode != "live" or not args.gradient_live or not args.confirm_bounded_gradient_pilot
        or args.continuous_gradient_rotation or args.rotation_test_id
        or not re.fullmatch(r"[0-9a-f]{32}", args.new_gradient_pilot_after)
    ):
        parser.error("new-gradient-pilot-after requires an exact manual bounded gradient predecessor without rotation")
    if len({args.forwarder_ws_port, args.forwarder_rest_port, args.forwarder_command_port}) != 3:
        parser.error("forwarder ports must be distinct")
    if any(not 1 <= port <= 65535 for port in (
        args.forwarder_ws_port, args.forwarder_rest_port, args.forwarder_command_port
    )):
        parser.error("forwarder ports must be valid TCP ports")
    if args.sample_interval_seconds <= 0 or args.initial_sample_timeout_seconds <= 0 or args.command_timeout_seconds <= 0:
        parser.error("runtime timeouts and sample interval must be positive")
    if args.expected_lighter_market_id is not None and args.expected_lighter_market_id < 0:
        parser.error("expected Lighter market ID must be non-negative")
    if args.gradient_live:
        if args.mode != "live" or not args.confirm_bounded_gradient_pilot:
            parser.error("gradient-live requires live mode and explicit bounded gradient confirmation")
        if args.confirm_bounded_live_pilot or args.new_pilot_after or args.manage_existing_wti_position:
            parser.error("gradient-live is separate from single-round and management modes")
        if args.rotation_test_id and not re.fullmatch(r"[0-9a-f]{32}", args.rotation_test_id):
            parser.error("rotation-test-id must be a 32-character lowercase hex ID")
        if args.continuous_gradient_rotation:
            if not args.confirm_continuous_gradient_rotation or not args.rotation_test_id:
                parser.error("continuous gradient rotation requires explicit confirmation and a persistent session ID")
        elif args.confirm_continuous_gradient_rotation:
            parser.error("continuous rotation confirmation requires --continuous-gradient-rotation")
    elif (args.confirm_bounded_gradient_pilot or args.gradient_second_tier_bps is not None
          or args.continuous_gradient_rotation or args.confirm_continuous_gradient_rotation):
        parser.error("gradient options require --gradient-live")
    elif args.rotation_test_id:
        parser.error("rotation-test-id requires --gradient-live")
    if args.mode == "live":
        if args.entry_threshold_bps is None or args.exit_target_bps is None or args.roundtrip_cost_bps is None:
            parser.error("live mode requires explicit entry, exit, and roundtrip-cost thresholds")
        try:
            build_replay_config(args)
        except ValueError as exc:
            parser.error(str(exc))
        if args.gradient_live:
            second = args.gradient_second_tier_bps
            layer = args.gradient_layer_notional_usd
            if second is None or not second.is_finite() or second <= args.entry_threshold_bps:
                parser.error("gradient second tier must be finite and higher than the entry threshold")
            if not layer.is_finite() or layer <= 0:
                parser.error("gradient layer notional must be positive and finite")
            if layer > args._validated_notional:
                parser.error("gradient child notional exceeds the per-order limit")
            args._validated_gradient_second_tier = second
            args._validated_gradient_layer_notional = layer
        if args.manage_existing_wti_position and (
            args.confirm_bounded_live_pilot or args.confirm_bounded_gradient_pilot
            or args.allow_unqualified_data_pilot
        ):
            parser.error("choose either bounded entry pilot or existing-position management")
    elif args.gradient_live or args.confirm_bounded_gradient_pilot:
        parser.error("gradient live options require --mode live")
    return args


async def _amain(args: argparse.Namespace) -> None:
    runtime = WTILiveRuntime(args)
    loop = asyncio.get_running_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signame, runtime.request_stop)
    await runtime.run()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    try:
        asyncio.run(_amain(parse_args()))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"WTI_LIVE_STOPPED reason={type(exc).__name__}:{exc}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
