"""Crash-safe, bounded manual WTI test rounds; never asset rotation or orders."""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal


def _flat(state):
    return state.get("status") == "flat" and not state.get("open_position") and not state.get("pending_action")


def ensure_trial(store, *, trial_id, root_pilot_id, max_rounds, model_sha256, config):
    """Bind a fixed budget once; finalize interrupted successor linkage once."""
    if type(max_rounds) is not int or not 1 <= max_rounds <= 3:
        raise ValueError("manual_wti_trial_round_limit_invalid")
    with store.exclusive():
        state = store.load()
        pilot = state.get("gradient_pilot") or {}
        if (pilot.get("schema_version") != 1 or not pilot.get("pilot_id")
                or pilot.get("market_id") != 145 or str(pilot.get("multiplier")) not in ("1", "1.000000000000000000")
                or pilot.get("config") != config or pilot.get("max_layers") != 5
                or pilot.get("continuous_rotation") or pilot.get("rotation_test_id")
                or state.get("gradient_rotation_session") is not None):
            raise ValueError("manual_wti_trial_pilot_identity_mismatch")
        session = state.get("manual_wti_trial")
        if session is None:
            if (not _flat(state) or pilot.get("pilot_id") != root_pilot_id
                    or pilot.get("status") != "available" or pilot.get("layers_opened")
                    or pilot.get("completed_roundtrips") != 0):
                raise ValueError("manual_wti_trial_requires_untouched_flat_root")
            session = {
                "schema_version": 1, "trial_id": trial_id, "root_pilot_id": root_pilot_id,
                "max_rounds": max_rounds, "model_sha256": model_sha256,
                "config": deepcopy(config), "aggregate_cap_usd": "20",
                "status": "active", "round_pilot_ids": [root_pilot_id],
                "completed_pilot_ids": [], "advance_from": None,
            }
            state["manual_wti_trial"] = session
            store.save(state)
        if (session.get("schema_version") != 1 or session.get("trial_id") != trial_id
                or session.get("root_pilot_id") != root_pilot_id
                or session.get("max_rounds") != max_rounds
                or session.get("model_sha256") != model_sha256
                or session.get("config") != config or session.get("aggregate_cap_usd") != "20"
                or session.get("status") not in ("active", "completed")):
            raise ValueError("manual_wti_trial_budget_or_identity_changed")
        ids, completed = session.get("round_pilot_ids"), session.get("completed_pilot_ids")
        if (not isinstance(ids, list) or not ids or ids[0] != root_pilot_id
                or len(ids) != len(set(ids)) or len(ids) > max_rounds
                or not isinstance(completed, list) or completed != ids[:len(completed)]):
            raise ValueError("manual_wti_trial_round_chain_corrupt")
        current = pilot["pilot_id"]
        if current != ids[-1]:
            predecessor = session.get("advance_from")
            if (session.get("status") != "active" or predecessor != ids[-1]
                    or completed != ids or len(ids) >= max_rounds
                    or pilot.get("previous_pilot_id") != predecessor
                    or pilot.get("authorization_kind") != "manual_new_round"):
                raise ValueError("manual_wti_trial_successor_unverifiable")
            ids.append(current)
            session["advance_from"] = None
            store.save(state)
        elif session.get("advance_from") not in (None, current):
            raise ValueError("manual_wti_trial_transition_corrupt")
        return deepcopy(session)


def advance_trial(store, *, trial_id, root_pilot_id, max_rounds, model_sha256,
                  config, runtime_pid, runtime_run_id):
    """Persist intent before the store's verified-flat, idempotent round transition."""
    ensure_trial(store, trial_id=trial_id, root_pilot_id=root_pilot_id,
                 max_rounds=max_rounds, model_sha256=model_sha256, config=config)
    with store.exclusive():
        state = store.load()
        pilot = state.get("gradient_pilot") or {}
        session = state["manual_wti_trial"]
        if session["status"] == "completed":
            return state
        if not _flat(state) or pilot.get("status") != "completed" or pilot.get("completed_roundtrips") != 1:
            return state
        current = pilot["pilot_id"]
        completed = session["completed_pilot_ids"]
        if current not in completed:
            if session["round_pilot_ids"][-1] != current:
                raise ValueError("manual_wti_trial_completed_identity_mismatch")
            completed.append(current)
        if len(completed) >= max_rounds:
            session["status"] = "completed"
            session["advance_from"] = None
            store.save(state)
            return state
        session["advance_from"] = current
        store.save(state)
    store.initialize_gradient_pilot(
        market_id=145, multiplier=Decimal("1"),
        config=config, max_layers=5, manual_new_pilot_after=current,
        runtime_pid=runtime_pid, runtime_run_id=runtime_run_id,
    )
    ensure_trial(store, trial_id=trial_id, root_pilot_id=root_pilot_id,
                 max_rounds=max_rounds, model_sha256=model_sha256, config=config)
    return store.load()
