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
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
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
from tools.lib.wti_execution import WTIPairExecutor, WTIExecutionStore  # noqa: E402
from tools.lib.wti_live_gateway import WTIProductionGateway  # noqa: E402
from tools.lib.wti_live_strategy import decide_wti_action  # noqa: E402
from tools.lib.wti_replay import ReplayConfig  # noqa: E402
from tools.oil_swap_collector import OilSwapCollector, parse_args as parse_collector_args  # noqa: E402


DEFAULT_LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"
DEFAULT_PORTS = (8766, 8767, 8768)
ETH_STATE_PATH = ROOT / "log" / "live_inventory_state.json"
WTI_STATE_PATH = ROOT / "log" / "wti_live_state.json"
WTI_HEALTH_PATH = ROOT / "log" / "wti_live_risk_health.json"
WTI_SAMPLE_ROOT = ROOT / "log" / "oil_swap" / "basis_samples"
MAX_WTI_NOTIONAL_USD = Decimal("25")
MAX_WTI_ENTRY_GAP_SECONDS = 120
MIN_ENTRY_CUTOFF_SECONDS = 1800
MAX_HOLD_SECONDS = 21600
MAX_SLIPPAGE_BPS = Decimal("8")
MAX_SINGLE_ROUND_PILOT_ENTRY_ATTEMPTS = 3


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
    return {
        "ready": report.ready_for_bounded_live_review,
        "market_data_ready": report.market_data_ready,
        "data_fresh": report.data_fresh,
        "production_ready": report.ready_for_production_rotation,
        "reasons": list(report.reasons),
        "session_count": report.session_count,
        "valid_sample_count": report.valid_sample_count,
        "confirmed_roundtrips": report.confirmed_roundtrips,
    }


def _session_window(row: dict[str, Any], now: datetime, cutoff_seconds: float):
    observed = row.get("session_status_observed_at")
    closing = row.get("session_closing_at")
    try:
        observed_at = datetime.fromisoformat(str(observed).replace("Z", "+00:00"))
        closing_at = datetime.fromisoformat(str(closing).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        observed_at = closing_at = None
    status = str(row.get("market_status") or "").lower()
    if status not in {"open", "closed", "halted_trading"}:
        exchange_open = None
    else:
        exchange_open = status == "open"
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
        self.last_processed_sample_id: str | None = None
        self.last_processed_sample_at: datetime | None = None
        self.startup_reconciled = False
        self.entry_gate_block_reason: str | None = None
        self.last_sample_gap_seconds: float | None = None
        self.pending_gap_exit = False
        self.last_risk: dict[str, Any] = {}
        self.last_action: dict[str, Any] = {"action": "none", "reason": "starting"}
        self.last_health_write_monotonic = 0.0
        self.last_runtime_error: str | None = None
        self.pilot_store: WTIExecutionStore | None = None
        self.single_round_pilot: dict[str, Any] | None = None
        self.entry_authorization_permanently_revoked = False

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
        if self.signer_client.check_client() is not None:
            raise RuntimeError("lighter_signer_client_preflight_failed")
        var_rows = read_basis_samples(
            self.args.sample_root, limit=200000, asset_filter="WTI"
        )
        if self.args.mode == "live":
            self.readiness = readiness_for_new_entries(
                var_rows,
                market_id=market["market_id"],
                multiplier=market["multiplier"],
            )
            store = WTIExecutionStore(self.args.state_path)
            existing = store.load()
            local_exposure = (
                existing.get("open_position") is not None
                or existing.get("pending_action") is not None
                or existing.get("status") in {"pending", "manual_review_required", "open"}
            )
            if local_exposure:
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
                    and not getattr(self.args, "allow_unqualified_data_pilot", False)
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
                self.collector.collect_fresh_sample(), loop
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
        }

    def _sync_entry_authorization(
        self, state: dict[str, Any], row: dict[str, Any], *, now: datetime
    ) -> bool:
        if (
            self.args.mode != "live"
            or self.entry_authorization_permanently_revoked
            or self.manage_existing_only
            or state.get("status") != "flat"
            or state.get("open_position") is not None
            or state.get("pending_action") is not None
        ):
            self.entry_authorized = False
            return False
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
        unqualified_pilot = bool(
            getattr(self.args, "allow_unqualified_data_pilot", False)
        )
        self.entry_authorized = fresh and (qualified or unqualified_pilot)
        self.entry_gate_block_reason = None if self.entry_authorized else (
            "wti_current_sample_or_session_not_fresh"
            if not fresh else "wti_data_readiness_not_met"
        )
        return self.entry_authorized

    def _finish_single_round_pilot_if_done(self, state: dict[str, Any]) -> bool:
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
            self.request_stop()
            return True
        return False

    async def _wait_for_first_sample(self) -> dict[str, Any]:
        deadline = time.monotonic() + self.args.initial_sample_timeout_seconds
        while not self.stop and time.monotonic() < deadline:
            if self.collector.latest_sample is not None:
                return dict(self.collector.latest_sample)
            if self.collector_task is not None and self.collector_task.done():
                await self.collector_task
                raise RuntimeError("wti_collector_stopped_before_first_sample")
            await asyncio.sleep(0.25)
        raise RuntimeError("wti_first_sample_timeout")

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

    @staticmethod
    def _position_notional(state: dict[str, Any], row: dict[str, Any] | None) -> Decimal:
        position = state.get("open_position") or {}
        qty = Decimal(str(position.get("var_qty") or "0"))
        if row and row.get("var_bid") and row.get("var_ask"):
            midpoint = (Decimal(str(row["var_bid"])) + Decimal(str(row["var_ask"]))) / 2
            return abs(qty * midpoint)
        return Decimal("0")

    @staticmethod
    def _sample_health_fresh(row: dict[str, Any] | None, now: datetime) -> bool:
        if not isinstance(row, dict):
            return False
        try:
            at = datetime.fromisoformat(str(row.get("logged_at")).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return False
        if at.tzinfo is None or at.utcoffset() is None:
            return False
        age = (now - at.astimezone(timezone.utc)).total_seconds()
        try:
            quote_age = Decimal(str(row.get("var_quote_age_seconds")))
            book_age = Decimal(str(row.get("lighter_book_age_seconds")))
        except Exception:
            return False
        return (
            -5 <= age <= 20
            and row.get("sample_quality") == "valid"
            and row.get("market_availability") == "both_feeds_live"
            and row.get("lighter_continuity_ok") is True
            and session_observation_for_sample(row) is not None
            and quote_age.is_finite() and Decimal("-1") <= quote_age <= Decimal("2")
            and book_age.is_finite() and Decimal("0") <= book_age <= Decimal("2")
        )

    def _revoke_entry_authorization(self, reason: str) -> None:
        self.entry_authorized = False
        self.entry_gate_block_reason = reason
        self.entry_authorization_permanently_revoked = True

    def _record_new_sample(self, row: dict[str, Any]) -> bool | None:
        sample_id = str(row.get("sample_id") or "")
        if not sample_id or sample_id == self.last_processed_sample_id:
            return None
        self.last_processed_sample_id = sample_id
        if row.get("sample_quality") != "valid" or session_observation_for_sample(row) is None:
            self.last_action = {"action": "blocked", "reason": "wti_sample_or_session_unverified"}
            self.entry_authorized = False
            if not self.entry_authorization_permanently_revoked:
                self.entry_gate_block_reason = "wti_sample_or_session_unverified"
            return False
        try:
            sample_at = datetime.fromisoformat(
                str(row.get("logged_at")).replace("Z", "+00:00")
            )
            if sample_at.tzinfo is None or sample_at.utcoffset() is None:
                raise ValueError("sample_timestamp_timezone_missing")
            sample_at = sample_at.astimezone(timezone.utc)
            if self.last_processed_sample_at is not None:
                self.last_sample_gap_seconds = (
                    sample_at - self.last_processed_sample_at
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
        risk = dict(self.last_risk)
        if self.args.mode == "observe":
            action, reason = "block_entry", "observe_only_mode"
        elif stale:
            action, reason = "block_entry", "wti_market_sample_or_session_unavailable"
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
                "live_single_round_pilot"
                if self.args.mode == "live" and self.single_round_pilot
                else "live_bounded" if self.args.mode == "live" else "observe_only"
            ),
            "historical_readiness_gate_bypassed": bool(
                self.args.mode == "live"
                and getattr(self.args, "allow_unqualified_data_pilot", False)
                and not self.readiness.get("market_data_ready")
            ),
            "single_round_pilot": state.get("single_round_pilot"),
            "wti_entry_authorized": self.entry_authorized,
            "entry_gate_block_reason": self.entry_gate_block_reason,
            "last_sample_gap_seconds": self.last_sample_gap_seconds,
            "manage_existing_only": self.manage_existing_only,
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
            "market_session": None if not row else row.get("session_status"),
            "last_action": self.last_action,
            "open_lots_total": int(exposure),
            "pending_actions_total": int(pending),
            "manual_review_reason": state.get("manual_review_reason"),
            "readiness": self.readiness,
            "errors": [] if error is None else [error],
        }
        write_json_atomic(self.args.health_path, health)
        self.last_health_write_monotonic = time.monotonic()

    async def _read_reconcile(self, row: dict[str, Any]) -> dict[str, Any]:
        assert self.executor is not None
        return await asyncio.to_thread(
            lambda: self.executor.reconcile(sample=row, now=datetime.now(timezone.utc))
        )

    async def _process_sample(self, row: dict[str, Any]) -> dict[str, Any]:
        assert self.executor is not None and self.gateway is not None and self.config is not None
        now = datetime.now(timezone.utc)
        state = await self._read_reconcile(row)
        self.last_risk = dict(self.gateway.latest_risk)
        if state.get("status") != "flat" and state.get("status") != "open":
            self.last_action = {"action": "reconcile", "reason": state.get("manual_review_reason") or state.get("status")}
            return state
        position = state.get("open_position")
        if position is None:
            self._sync_entry_authorization(state, row, now=now)
        else:
            self.entry_authorized = False
        self.gateway.set_current_notional(self._position_notional(state, row))
        risk_action = str(self.last_risk.get("risk_action") or "block_entry")
        window = _session_window(row, now, self.config.entry_cutoff_seconds)
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
                lambda: self.executor.close(sample=row, session=window, now=now)
            )
            if state.get("status") == "flat":
                self.pending_gap_exit = False
            self.last_action = {"action": "close", "reason": decision["reason"], "status": state.get("status")}
            self._finish_single_round_pilot_if_done(state)
            return state
        if (
            decision["action"] == "open"
            and not self.manage_existing_only
            and self.entry_authorized
            and risk_action in {"normal", "warning"}
            and window.allow_new_entries
        ):
            assert self.pilot_store is not None and self.single_round_pilot is not None
            reservation_id = await asyncio.to_thread(
                self.pilot_store.reserve_single_round_pilot_entry,
                self.single_round_pilot["pilot_id"],
            )
            self.gateway.set_proposed_notional(self.args._validated_notional)
            try:
                state = await asyncio.to_thread(
                    lambda: self.executor.open(
                        sample=row,
                        direction=decision["direction"],
                        notional_usd=self.args._validated_notional,
                        session=window,
                        pilot_id=self.single_round_pilot["pilot_id"],
                        pilot_reservation_id=reservation_id,
                        now=now,
                    )
                )
            except Exception:
                state = await asyncio.to_thread(
                    self.pilot_store.release_unsubmitted_single_round_pilot_entry,
                    self.single_round_pilot["pilot_id"], reservation_id,
                )
                self._finish_single_round_pilot_if_done(state)
                raise
            finally:
                self.gateway.set_proposed_notional(None)
            self.last_risk = dict(self.gateway.latest_risk)
            if state.get("status") == "open":
                self.entry_authorized = False
                self.entry_gate_block_reason = "single_round_pilot_position_open"
            self._finish_single_round_pilot_if_done(state)
            self.last_action = {
                "action": "open", "direction": decision["direction"],
                "edge_bps": decision["edge_bps"], "status": state.get("status"),
            }
            return state
        self.last_action = {
            "action": decision["action"],
            "reason": decision.get("reason"),
            "risk_action": risk_action,
            "edge_bps": decision.get("edge_bps"),
        }
        return state

    async def run(self) -> None:
        validate_startup_preconditions(self.args)
        await self._start_forwarder()
        self.collector_task = asyncio.create_task(self.collector.run(), name="wti-collector")
        try:
            if self.args.mode == "live":
                await self._initialize_execution()
                sample = await self._wait_for_first_sample()
                if (
                    sample.get("sample_quality") == "valid"
                    and session_observation_for_sample(sample) is not None
                ):
                    self._refresh_readiness()
                    state = await self._read_reconcile(sample)
                    self.last_risk = dict(self.gateway.latest_risk) if self.gateway else {}
                    self.startup_reconciled = state.get("status") in {"flat", "open"}
                    self._sync_entry_authorization(
                        state, sample, now=datetime.now(timezone.utc)
                    )
                    self.last_processed_sample_id = str(sample.get("sample_id") or "") or None
                    with contextlib.suppress(TypeError, ValueError):
                        parsed_at = datetime.fromisoformat(
                            str(sample.get("logged_at")).replace("Z", "+00:00")
                        )
                        if parsed_at.tzinfo is not None:
                            self.last_processed_sample_at = parsed_at.astimezone(timezone.utc)
                    self.last_action = {"action": "startup_reconcile", "status": state.get("status")}
                    self._write_health(state=state, row=sample)
                    self._finish_single_round_pilot_if_done(state)
                else:
                    self._write_health(state=None, row=sample, error="initial_wti_sample_or_session_not_valid")
                self.logger.info(
                    "wti_live_started run_id=%s entries_authorized=%s manage_existing_only=%s readiness=%s",
                    self.run_id, self.entry_authorized, self.manage_existing_only, self.readiness,
                )
            else:
                self.logger.info("wti_observe_started run_id=%s; order submission disabled", self.run_id)

            while not self.stop and not self.collector_task.done():
                row = self.collector.latest_sample
                state = read_json(self.args.state_path) if self.args.mode == "live" else {}
                if self.args.mode == "live" and row is not None:
                    new_valid_sample = self._record_new_sample(dict(row))
                    if new_valid_sample is True:
                        try:
                            if not self.startup_reconciled:
                                self._refresh_readiness()
                                state = await self._read_reconcile(dict(row))
                                self.last_risk = dict(self.gateway.latest_risk) if self.gateway else {}
                                self.startup_reconciled = state.get("status") in {"flat", "open"}
                                self._sync_entry_authorization(
                                    state, dict(row), now=datetime.now(timezone.utc)
                                )
                                self.last_action = {
                                    "action": "startup_reconcile",
                                    "status": state.get("status"),
                                }
                            else:
                                state = await self._process_sample(dict(row))
                            self._finish_single_round_pilot_if_done(state)
                            self.last_runtime_error = None
                            self._write_health(state=state, row=row)
                        except Exception as exc:
                            self.logger.exception("wti_strategy_cycle_failed error=%s", type(exc).__name__)
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
    parser.add_argument(
        "--allow-unqualified-data-pilot",
        action="store_true",
        help="allow one durable live round with unqualified history; current market/account guards still apply",
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
    if args.mode == "live":
        if args.entry_threshold_bps is None or args.exit_target_bps is None or args.roundtrip_cost_bps is None:
            parser.error("live mode requires explicit entry, exit, and roundtrip-cost thresholds")
        try:
            build_replay_config(args)
        except ValueError as exc:
            parser.error(str(exc))
        if args.manage_existing_wti_position and (
            args.confirm_bounded_live_pilot or args.allow_unqualified_data_pilot
        ):
            parser.error("choose either bounded entry pilot or existing-position management")
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
