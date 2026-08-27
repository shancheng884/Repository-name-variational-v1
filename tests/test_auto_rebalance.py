from decimal import Decimal

import pytest

from tools.lib.auto_rebalance import (
    build_rebalance_plan,
    create_rebalance_request_if_idle,
    load_active_rebalance,
    new_rebalance_request,
    risk_recovered_after_rebalance,
    transition_rebalance_request,
    validate_rebalance_preflight,
    write_rebalance_request,
)


def test_rebalance_plan_targets_residual_imbalance() -> None:
    plan = build_rebalance_plan(
        variational_equity_usd=Decimal("130"),
        lighter_equity_usd=Decimal("70"),
    )

    assert plan["required"] is True
    assert plan["source_venue"] == "variational"
    assert Decimal(plan["amount_usd"]) == Decimal("26.75")


def test_rebalance_state_machine_is_single_path_and_persistent(tmp_path) -> None:
    request = new_rebalance_request(
        build_rebalance_plan(
            variational_equity_usd=Decimal("130"),
            lighter_equity_usd=Decimal("70"),
        )
    )
    request = transition_rebalance_request(request, "preflight")
    path = tmp_path / "rebalance.json"
    write_rebalance_request(path, request)

    assert load_active_rebalance(path)["request_id"] == request["request_id"]
    with pytest.raises(ValueError):
        transition_rebalance_request(request, "confirm_venue_credit")


def test_rebalance_state_machine_requires_wallet_intermediate_steps() -> None:
    request = new_rebalance_request(
        build_rebalance_plan(
            variational_equity_usd=Decimal("130"),
            lighter_equity_usd=Decimal("70"),
        )
    )

    for state in (
        "preflight",
        "submit_withdrawal",
        "confirm_wallet_credit",
        "submit_deposit",
        "confirm_venue_credit",
        "refresh_risk",
        "complete",
    ):
        request = transition_rebalance_request(request, state)

    assert request["state"] == "complete"


def test_rebalance_preflight_preserves_source_margin_buffer() -> None:
    request = new_rebalance_request(
        build_rebalance_plan(
            variational_equity_usd=Decimal("130"),
            lighter_equity_usd=Decimal("70"),
        )
    )
    allowed = validate_rebalance_preflight(
        request,
        source_available_usd=Decimal("100"),
        source_maintenance_margin_usd=Decimal("30"),
        source_safety_buffer_usd=Decimal("20"),
        estimated_transfer_fee_usd=Decimal("1"),
    )
    blocked = validate_rebalance_preflight(
        request,
        source_available_usd=Decimal("60"),
        source_maintenance_margin_usd=Decimal("30"),
        source_safety_buffer_usd=Decimal("20"),
        estimated_transfer_fee_usd=Decimal("1"),
    )

    assert allowed["ready"] is True
    assert blocked["ready"] is False
    assert "source_margin_buffer_would_be_insufficient" in blocked["reasons"]


def test_rebalance_request_is_idempotent_and_requires_refreshed_risk(tmp_path) -> None:
    path = tmp_path / "rebalance.json"
    plan = build_rebalance_plan(
        variational_equity_usd=Decimal("130"),
        lighter_equity_usd=Decimal("70"),
    )
    first = create_rebalance_request_if_idle(path, plan)
    second = create_rebalance_request_if_idle(path, plan)

    assert first["request_id"] == second["request_id"]
    assert risk_recovered_after_rebalance(
        risk_action="normal",
        wallet_credit_confirmed=True,
        destination_credit_confirmed=True,
        refreshed_equities=True,
    ) is True
    assert risk_recovered_after_rebalance(
        risk_action="normal",
        wallet_credit_confirmed=True,
        destination_credit_confirmed=True,
        refreshed_equities=False,
    ) is False
