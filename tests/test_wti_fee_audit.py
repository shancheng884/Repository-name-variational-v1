from __future__ import annotations

import asyncio
from copy import deepcopy

from tools.lib.wti_fee_audit import audit_latest_closed_position_fees


def _state():
    executions = []
    for phase, action_id, kind, base in (
        ("entry", "entry-1", "open", "100"),
        ("exit", "exit-1", "close", "101"),
    ):
        legs = {}
        for venue in ("variational", "lighter"):
            client_id = f"{action_id}-{venue}"
            request = {
                "venue": venue,
                "client_order_id": client_id,
                "qty": "0.2",
                "side": "buy" if (kind == "open") == (venue == "variational") else "sell",
                "native_client_order_id": f"native-{client_id}",
            }
            legs[venue] = {
                "request": request,
                "result": {
                    "classification": "filled",
                    "terminal_confirmed": True,
                    "client_order_id": client_id,
                    "filled_qty": "0.2",
                    "average_price": base,
                },
            }
        executions.append({"action_id": action_id, "kind": kind, "legs": legs})
    return {
        "last_closed_position": {
            "entry_execution_id": "entry-1",
            "exit_execution_id": "exit-1",
            "actual_net_pnl_status": "fees_unverified",
        },
        "execution_history": executions,
    }


def test_fee_audit_confirms_only_four_exact_usd_fee_proofs_without_mutating_state():
    state = _state()
    before = deepcopy(state)
    calls = []

    async def lookup(client_order_id, request):
        calls.append((client_order_id, request["venue"]))
        expected = next(
            leg["result"]
            for execution in state["execution_history"]
            for leg in execution["legs"].values()
            if leg["request"]["client_order_id"] == client_order_id
        )
        fee = "0.01" if request["venue"] == "variational" else "0.02"
        return {
            **expected,
            "status": "filled",
            "fee_usd": fee,
            "fee_evidence": {
                "status": "confirmed_usd",
                "raw_fee_unit": "USD",
                "fee_usd": fee,
            },
        }

    report = asyncio.run(audit_latest_closed_position_fees(state, lookup))

    assert report["status"] == "confirmed"
    assert report["fees_usd"] == "0.06"
    assert report["funding_status"] == "unverified"
    assert report["orders_attempted"] is False
    assert report["state_modified"] is False
    assert len(calls) == 4
    assert state == before


def test_fee_audit_keeps_unknown_currency_or_fill_mismatch_unverified():
    state = _state()

    async def lookup(client_order_id, request):
        wrong_price = request["venue"] == "lighter" and "entry" in client_order_id
        return {
            "client_order_id": client_order_id,
            "status": "filled",
            "filled_qty": "0.2",
            "average_price": "999" if wrong_price else (
                "100" if "entry" in client_order_id else "101"
            ),
            "fee_usd": None,
            "fee_evidence": {
                "status": "unverified",
                "raw_fee_unit": "unverified",
                "fee_usd": None,
            },
        }

    report = asyncio.run(audit_latest_closed_position_fees(state, lookup))

    assert report["status"] == "unverified"
    assert report["fees_usd"] is None
    assert report["funding_status"] == "unverified"
    assert all(leg["status"] == "unverified" for leg in report["legs"])


def test_fee_audit_does_not_call_exchange_when_exact_persisted_identity_missing():
    state = _state()
    state["last_closed_position"]["entry_execution_id"] = "missing"
    state["last_closed_position"]["exit_execution_id"] = "missing-too"
    calls = []

    async def lookup(*args):
        calls.append(args)
        return {}

    report = asyncio.run(audit_latest_closed_position_fees(state, lookup))

    assert report["status"] == "unverified"
    assert report["fees_usd"] is None
    assert calls == []
    assert any(leg.get("reason") == "persisted_execution_identity_missing" for leg in report["legs"])


def test_fee_audit_covers_every_gradient_entry_layer_and_exit():
    state = _state()
    first_entry = state["execution_history"][0]
    second_entry = deepcopy(first_entry)
    second_entry["action_id"] = "entry-2"
    for venue, leg in second_entry["legs"].items():
        leg["request"]["client_order_id"] = f"entry-2-{venue}"
        leg["result"]["client_order_id"] = f"entry-2-{venue}"
    state["execution_history"].insert(1, second_entry)
    state["last_closed_position"]["layers"] = [
        {"entry_execution_id": "entry-1"},
        {"entry_execution_id": "entry-2"},
    ]
    seen = []

    async def lookup(client_order_id, request):
        seen.append(client_order_id)
        expected = next(
            leg["result"]
            for execution in state["execution_history"]
            for leg in execution["legs"].values()
            if leg["request"]["client_order_id"] == client_order_id
        )
        fee = "0.01" if request["venue"] == "variational" else "0.02"
        return {
            **expected,
            "status": "filled",
            "fee_usd": fee,
            "fee_evidence": {
                "status": "confirmed_usd", "raw_fee_unit": "USD", "fee_usd": fee,
            },
        }

    report = asyncio.run(audit_latest_closed_position_fees(state, lookup))
    assert report["status"] == "confirmed"
    assert report["expected_order_leg_count"] == report["audited_order_leg_count"] == 6
    assert report["entry_execution_ids"] == ["entry-1", "entry-2"]
    assert report["fees_usd"] == "0.09"
    assert len(seen) == 6


def test_fee_audit_keeps_multilayer_position_unverified_when_one_fee_is_missing():
    state = _state()
    second_entry = deepcopy(state["execution_history"][0])
    second_entry["action_id"] = "entry-2"
    for venue, leg in second_entry["legs"].items():
        leg["request"]["client_order_id"] = f"entry-2-{venue}"
        leg["result"]["client_order_id"] = f"entry-2-{venue}"
    state["execution_history"].insert(1, second_entry)
    state["last_closed_position"]["layers"] = [
        {"entry_execution_id": "entry-1"},
        {"entry_execution_id": "entry-2"},
    ]

    async def lookup(client_order_id, request):
        expected = next(
            leg["result"]
            for execution in state["execution_history"]
            for leg in execution["legs"].values()
            if leg["request"]["client_order_id"] == client_order_id
        )
        if client_order_id == "entry-2-lighter":
            return {**expected, "status": "filled", "fee_usd": None}
        fee = "0.01" if request["venue"] == "variational" else "0.02"
        return {
            **expected, "status": "filled", "fee_usd": fee,
            "fee_evidence": {
                "status": "confirmed_usd", "raw_fee_unit": "USD", "fee_usd": fee,
            },
        }

    report = asyncio.run(audit_latest_closed_position_fees(state, lookup))
    assert report["status"] == "unverified"
    assert report["fees_usd"] is None
    assert report["audited_order_leg_count"] == 6
