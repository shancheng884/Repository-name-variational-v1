"""Fail-closed target selection and flat-before-switch state machine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from tools.lib.market_sessions import OilSession, SessionWindow


class Pair(StrEnum):
    ETH = "ETH"
    WTI = "WTI"
    NONE = "NONE"


class RotationAction(StrEnum):
    RUN_TARGET = "run_target"
    MANAGE_CURRENT = "manage_current"
    DRAIN_CURRENT = "drain_current"
    HOLD_CURRENT = "hold_current"
    SWITCH_TO_TARGET = "switch_to_target"
    PAUSE_AND_RECONCILE = "pause_and_reconcile"
    WAIT_FOR_READINESS = "wait_for_readiness"


@dataclass(frozen=True, slots=True)
class RotationDecision:
    action: RotationAction
    current: Pair
    target: Pair
    allow_new_entries: bool
    reason: str


def desired_pair(
    *,
    session: SessionWindow,
    wti_ready: bool,
    eth_ready: bool,
    current: Pair,
) -> tuple[Pair, str]:
    if session.state == OilSession.OPEN and session.allow_new_entries and wti_ready:
        return Pair.WTI, "wti_exchange_open_and_live_readiness_passed"
    if session.state == OilSession.STATUS_UNKNOWN:
        return (current, "session_status_unknown_hold_current") if current != Pair.NONE else (Pair.NONE, "session_status_unknown")
    if eth_ready:
        return Pair.ETH, "wti_closed_unready_or_entry_cutoff_eth_fallback"
    if current == Pair.WTI:
        return Pair.WTI, "no_fallback_pair_ready_keep_current_for_safe_management"
    return Pair.NONE, "no_pair_passed_readiness"


def decide_rotation(
    *,
    current: Pair,
    target: Pair,
    target_ready: bool,
    session: SessionWindow,
    has_open_position: bool,
    has_pending_action: bool,
    active_orders_zero: bool | None,
    exchange_reconciled: bool | None,
) -> RotationDecision:
    if exchange_reconciled is not True:
        return RotationDecision(
            RotationAction.PAUSE_AND_RECONCILE,
            current,
            target,
            False,
            "exchange_positions_or_active_orders_not_confirmed",
        )
    if has_pending_action:
        return RotationDecision(
            RotationAction.PAUSE_AND_RECONCILE,
            current,
            target,
            False,
            "pending_execution_requires_reconciliation",
        )
    if active_orders_zero is not True:
        if current != Pair.NONE and target not in {Pair.NONE, current}:
            # Requesting a source-runtime drain is safe; the supervisor still
            # requires its PID/run-bound flat-and-zero-orders acknowledgement
            # before it can start the target runtime.
            return RotationDecision(
                RotationAction.DRAIN_CURRENT,
                current,
                target,
                False,
                "source_active_orders_must_be_verified_before_handoff",
            )
        return RotationDecision(
            RotationAction.PAUSE_AND_RECONCILE,
            current,
            target,
            False,
            "exchange_positions_or_active_orders_not_confirmed",
        )
    if current == Pair.NONE and has_open_position:
        return RotationDecision(
            RotationAction.PAUSE_AND_RECONCILE,
            current,
            target,
            False,
            "positions_exist_without_a_known_active_pair",
        )
    if target == Pair.NONE:
        if current == Pair.WTI and has_open_position and not session.allow_position_management:
            return RotationDecision(
                RotationAction.HOLD_CURRENT,
                current,
                target,
                False,
                "wti_session_unconfirmed_hold_position",
            )
        return RotationDecision(
            RotationAction.MANAGE_CURRENT if current != Pair.NONE else RotationAction.WAIT_FOR_READINESS,
            current,
            target,
            False,
            "no_target_pair_ready",
        )
    if current == Pair.NONE:
        if not target_ready or (target == Pair.WTI and not session.allow_new_entries):
            return RotationDecision(
                RotationAction.WAIT_FOR_READINESS,
                current,
                target,
                False,
                "target_readiness_or_entry_window_not_confirmed",
            )
        return RotationDecision(
            RotationAction.RUN_TARGET,
            current,
            target,
            target == Pair.ETH or session.allow_new_entries,
            "flat_verified_start_target_pair",
        )
    if current == target:
        if not target_ready:
            if has_open_position:
                management_allowed = (
                    session.allow_position_management if target == Pair.WTI else True
                )
                if not management_allowed:
                    return RotationDecision(
                        RotationAction.HOLD_CURRENT,
                        current,
                        target,
                        False,
                        "current_pair_unready_and_management_unavailable",
                    )
                return RotationDecision(
                    RotationAction.MANAGE_CURRENT,
                    current,
                    target,
                    False,
                    "current_pair_unready_manage_existing_position_only",
                )
            return RotationDecision(
                RotationAction.WAIT_FOR_READINESS,
                current,
                target,
                False,
                "current_pair_not_ready_no_position",
            )
        if target == Pair.WTI and not session.allow_new_entries:
            if has_open_position and not session.allow_position_management:
                return RotationDecision(
                    RotationAction.HOLD_CURRENT,
                    current,
                    target,
                    False,
                    "venue_closed_position_cannot_be_safely_drained",
                )
            return RotationDecision(
                RotationAction.DRAIN_CURRENT if has_open_position else RotationAction.MANAGE_CURRENT,
                current,
                target,
                False,
                "wti_entry_window_closed_drain_or_wait",
            )
        return RotationDecision(
            RotationAction.MANAGE_CURRENT,
            current,
            target,
            session.allow_new_entries if target == Pair.WTI else True,
            "current_pair_matches_target",
        )
    if not target_ready:
        if has_open_position:
            management_allowed = current != Pair.WTI or session.allow_position_management
            if not management_allowed:
                return RotationDecision(
                    RotationAction.HOLD_CURRENT,
                    current,
                    target,
                    False,
                    "target_unready_and_current_management_unavailable",
                )
            return RotationDecision(
                RotationAction.MANAGE_CURRENT,
                current,
                target,
                False,
                "target_pair_not_ready_keep_current_manage_only",
            )
        return RotationDecision(
            RotationAction.WAIT_FOR_READINESS,
            current,
            target,
            False,
            "target_pair_not_ready_do_not_switch",
        )
    if target == Pair.WTI and not session.allow_new_entries:
        if has_open_position:
            return RotationDecision(
                RotationAction.MANAGE_CURRENT,
                current,
                target,
                False,
                "wti_entry_window_closed_recheck_before_switch",
            )
        return RotationDecision(
            RotationAction.WAIT_FOR_READINESS,
            current,
            target,
            False,
            "wti_entry_window_closed_do_not_switch",
        )
    if has_open_position:
        if current == Pair.WTI and not session.allow_position_management:
            return RotationDecision(
                RotationAction.HOLD_CURRENT,
                current,
                target,
                False,
                "current_venue_closed_wait_for_safe_flattening_window",
            )
        return RotationDecision(
            RotationAction.DRAIN_CURRENT,
            current,
            target,
            False,
            "target_changed_wait_for_confirmed_flat_before_switch",
        )
    return RotationDecision(
        RotationAction.SWITCH_TO_TARGET,
        current,
        target,
        False,
        "both_venues_flat_no_orders_and_reconciled",
    )
