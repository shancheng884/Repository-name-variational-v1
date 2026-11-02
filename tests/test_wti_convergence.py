import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import hashlib
import json
import time
from types import SimpleNamespace

import pytest

from test_wti_research import sample as research_sample, CONFIG, AS_OF
from tools import wti_live
from tools.lib.wti_convergence import (
    MODEL_KIND, convergence_thresholds, features_for_row, fit_exit_model,
    load_model, policy_config, validate_model,
)
from tools.lib.wti_research import DIRECTIONS, stamp, audit_rows
from tools.lib.wti_live_strategy import decide_wti_gradient_action
from tools.lib.wti_replay import ReplayConfig
from tools.wti_takeover import management_command

CFG = CONFIG | {"max_slippage_bps": "8"}
CONTROL = {d: tuple(D(x) for x in (90, 91, 92, 93, 94)) for d in DIRECTIONS}


def sample(*args, **kwargs):
    return research_sample(*args, **({"lighter_pair_drift_bps": "0"} | kwargs))


def model_for(row, delta="-15"):
    at = stamp(row["logged_at"])
    return {
        "schema_version": 1, "kind": MODEL_KIND, "experimental_only": True,
        "market_id": 145, "multiplier": "1", "horizon_seconds": 3600,
        "aggregate_cap_usd": "20", "constraints": policy_config(CFG),
        "created_at": (at - timedelta(minutes=10)).isoformat(),
        "expires_at": (at + timedelta(hours=23)).isoformat(),
        "train_before": (at - timedelta(days=1)).isoformat(),
        "latest_label_end_at": (at - timedelta(days=1, seconds=1)).isoformat(),
        "training_sessions": ["a", "b", "c"],
        "cells": {d + "|*|*": {"exit_delta_bps": delta,
                  "sessions": 3, "labels": 100, "nonoverlapping_labels": 6}
                  for d in DIRECTIONS},
    }


FEATURES = {d: {"phase": "early", "state": "expanded"} for d in DIRECTIONS}


def test_convergence_gate_uses_predicted_return_once_not_historical_extreme():
    row = sample(day=5, index=40)
    model = model_for(row)
    validate_model(model, config=CFG)
    values, diagnostic = convergence_thresholds(model, row, FEATURES, CONTROL, now=stamp(row["logged_at"]))
    first = values[DIRECTIONS[0]][0]
    assert D("30") <= first < D(row["long_edge_bps"]) < CONTROL[DIRECTIONS[0]][0]
    assert diagnostic["directions"][DIRECTIONS[0]]["predicted_net_bps"] == "5"
    assert all(a < b for a, b in zip(values[DIRECTIONS[0]], values[DIRECTIONS[0]][1:]))
    assert all(a >= D(b) for a, b in zip(values[DIRECTIONS[0]], CFG["tier_thresholds_bps"]))
    insufficient = model_for(row, delta="-5")
    blocked, _ = convergence_thresholds(insufficient, row, FEATURES, CONTROL, now=stamp(row["logged_at"]))
    assert blocked[DIRECTIONS[0]][0] > D(row["long_edge_bps"])


@pytest.mark.parametrize("change", [
    {"var_quote_age_seconds": "2.001"}, {"lighter_book_age_seconds": "2.001"},
    {"lighter_continuity_ok": False}, {"lighter_market_id": 146},
    {"lighter_qty": "0.18"}, {"session_status_feed_connected": False},
])
def test_new_model_keeps_capture_session_market_and_quantity_checks(change):
    row = sample(day=5, index=40, **change)
    values, diagnostic = convergence_thresholds(model_for(row), row, FEATURES, CONTROL,
                                                now=stamp(row["logged_at"]))
    assert all(v == (None,) * 5 for v in values.values())
    assert diagnostic["reason"] == "experimental_exit_model_sample_unverified_or_training_overlap"


def test_expiry_missing_features_and_unsupported_cells_never_fall_back_to_floor():
    row = sample(day=5, index=40)
    model = model_for(row)
    for features, now in (({}, stamp(row["logged_at"])), (FEATURES, stamp(model["expires_at"]))):
        values, _ = convergence_thresholds(model, row, features, CONTROL, now=now)
        assert all(v == (None,) * 5 for v in values.values())
    model["cells"] = {}
    values, _ = convergence_thresholds(model, row, FEATURES, CONTROL, now=stamp(row["logged_at"]))
    assert all(v == (None,) * 5 for v in values.values())


def test_feature_history_is_causal_and_resets_after_existing_120_second_gap():
    rows = [sample(day=5, index=i) for i in range(100)]
    current = rows[70]
    past = features_for_row(rows[:70], current)
    assert past and past == features_for_row(rows, current)
    assert features_for_row(rows[:40], rows[60]) == {}


def test_fit_is_prior_only_with_mature_exact_quantity_outcomes():
    rows = []
    for day in range(3):
        for index in range(900):
            row = sample(day=day, index=index)
            opened = stamp(row["session_id"].split("|")[0])
            close = opened + timedelta(hours=8)
            row.update(session_id=f"{opened.isoformat()}|{close.isoformat()}",
                       session_closing_at=close.isoformat())
            rows.append(row)
    cutoff = AS_OF - timedelta(days=2)
    clean, _ = audit_rows(rows, as_of=AS_OF, market_id=145, multiplier=D(1))
    first = fit_exit_model(clean, config=CFG, train_before=cutoff, created_at=AS_OF)
    second = fit_exit_model([*clean, sample(day=5, index=30)], config=CFG,
                            train_before=cutoff, created_at=AS_OF)
    assert first == second
    assert first["cells"]
    assert stamp(first["latest_label_end_at"]) < cutoff
    validate_model(first, config=CFG)


def test_hash_constraint_and_training_leakage_validation(tmp_path):
    model = model_for(sample(day=5, index=40))
    p = tmp_path / "model.json"
    p.write_text(json.dumps(model))
    sha = hashlib.sha256(p.read_bytes()).hexdigest()
    assert load_model(p, sha, config=CFG) == model
    with pytest.raises(ValueError, match="sha256"):
        load_model(p, "0" * 64, config=CFG)
    with pytest.raises(ValueError, match="constraints"):
        load_model(p, sha, config=CFG | {"roundtrip_cost_bps": "0"})
    model["latest_label_end_at"] = model["train_before"]
    with pytest.raises(ValueError, match="leakage"):
        validate_model(model, config=CFG)


def test_live_pilot_caps_all_five_tiers_at_twenty_dollars_without_changing_default():
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.manage_existing_only = False
    runtime.args = SimpleNamespace(gradient_live=True, _validated_gradient_layer_notional=D(20),
                                   _validated_notional=D(20))
    runtime.last_risk = {"risk_data_complete": True, "observed_at": datetime.now(timezone.utc).isoformat(),
                         "variational_equity_usd": "144", "lighter_equity_usd": "129"}
    original = runtime._gradient_equity_capacity_by_tier()
    assert original[0] > 20 and original[-1] > original[0]
    runtime.experimental_exit_model = {"kind": MODEL_KIND}
    assert runtime._gradient_equity_capacity_by_tier() == (D(20),) * 5


def test_explicit_cli_confirmation_hash_and_no_rotation_are_required(tmp_path):
    base = management_command(tmp_path, new_gradient_after="a" * 32)[2:]
    extras = ["--experimental-exit-model", str(tmp_path / "model.json"),
              "--experimental-exit-model-sha256", "b" * 64]
    with pytest.raises(SystemExit):
        wti_live.parse_args([*base, *extras])
    args = wti_live.parse_args([*base, *extras, "--confirm-experimental-exit-model"])
    assert args.roundtrip_cost_bps == D(10) and args._validated_slippage == D(8)
    assert not args.continuous_gradient_rotation
    with pytest.raises(SystemExit):
        wti_live.parse_args([*base, *extras, "--confirm-experimental-exit-model", "--roundtrip-cost-bps", "0"])


def test_runtime_and_replay_use_the_identical_policy_function():
    rows = [sample(day=5, index=i) for i in range(80)]
    row = rows[-1]
    model = model_for(row)
    now = datetime.now(timezone.utc)
    model.update(created_at=(now-timedelta(minutes=1)).isoformat(),
                 expires_at=(now+timedelta(hours=1)).isoformat())
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.manage_existing_only = False
    runtime.gradient_pilot = {"config": CFG}
    runtime.market_metadata = {"market_id": 145, "multiplier": "1"}
    runtime.config = ReplayConfig(D(30), D(1), D(10), 21600, 120)
    runtime.wti_gradient_threshold_profiles = {
        d: SimpleNamespace(thresholds_bps=CONTROL[d]) for d in DIRECTIONS
    }
    runtime.wti_gradient_threshold_refreshed_monotonic = time.monotonic()
    runtime.experimental_exit_model = model
    runtime.args = SimpleNamespace(confirm_exploratory_entry=False)
    runtime.experimental_exit_history = rows[:-1]
    expected, _ = convergence_thresholds(model, row, features_for_row(rows[:-1], row), CONTROL, now=now)
    assert asyncio.run(runtime._gradient_thresholds_for_sample(row)) == expected
    assert runtime.wti_gradient_threshold_source == MODEL_KIND
    retained = len(runtime.experimental_exit_history)
    assert retained < 80
    assert asyncio.run(runtime._gradient_thresholds_for_sample(row)) == expected
    assert len(runtime.experimental_exit_history) == retained


def test_session_balanced_p75_is_not_the_worst_session_or_density_weighted(monkeypatch):
    import tools.lib.wti_convergence as policy
    labels = []
    for day, delta in enumerate((-20, -10, 100)):
        for i in range(100):
            at = AS_OF - timedelta(days=4-day) + timedelta(minutes=i*2)
            labels.append({"session_id": str(day), "at": at.isoformat(),
                           "end_at": (at+timedelta(hours=1)).isoformat(),
                           "direction": DIRECTIONS[0], "phase": "early", "state": "expanded",
                           "exit_delta_bps": str(delta)})
    monkeypatch.setattr(policy, "labels_for", lambda *a, **k: (labels, {}))
    first = fit_exit_model([], config=CFG, train_before=AS_OF, created_at=AS_OF)
    assert first["cells"][DIRECTIONS[0]+"|*|*"]["exit_delta_bps"] == "-10"
    labels.extend(deepcopy(labels[:100])*5)
    second = fit_exit_model([], config=CFG, train_before=AS_OF, created_at=AS_OF)
    assert second["cells"][DIRECTIONS[0]+"|*|*"]["exit_delta_bps"] == "-10"


def test_explicit_exploration_is_labelled_not_model_qualified_and_keeps_hard_checks():
    row = sample(day=5, index=40)
    model = model_for(row, delta="2")
    at = stamp(row["logged_at"])
    ordinary, _ = convergence_thresholds(model, row, FEATURES, CONTROL, now=at)
    assert ordinary[DIRECTIONS[0]][0] > D(row["long_edge_bps"])
    trial, diagnostic = convergence_thresholds(model, row, FEATURES, CONTROL, now=at, exploratory=True)
    assert trial[DIRECTIONS[0]][0] == 30
    assert diagnostic["model_would_qualify"][DIRECTIONS[0]] is False
    assert diagnostic["not_a_profitability_qualification"] is True
    for invalid in (dict(row, var_quote_age_seconds="2.001"), dict(row, lighter_qty="0.18")):
        result, _ = convergence_thresholds(model, invalid, FEATURES, CONTROL, now=at, exploratory=True)
        assert all(v == (None,)*5 for v in result.values())
    for drift in (None, "NaN", "-0.1", "1.5001"):
        result, _ = convergence_thresholds(model, dict(row, lighter_pair_drift_bps=drift),
                                           FEATURES, CONTROL, now=at, exploratory=True)
        assert all(v == (None,)*5 for v in result.values())
    expired, _ = convergence_thresholds(model, row, FEATURES, CONTROL,
                                       now=stamp(model["expires_at"]), exploratory=True)
    assert all(v == (None,)*5 for v in expired.values())


def test_restart_loads_same_pinned_model_without_mutating_or_rearming_ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(wti_live, "ROOT", tmp_path)
    row = sample(day=5, index=40)
    model = model_for(row)
    path = tmp_path / "model.json"
    path.write_text(json.dumps(model))
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"status": "flat", "pending_action": None,
                                 "gradient_pilot": {"pilot_id": "same-round"}}))
    before = state.read_bytes()
    argv = management_command(tmp_path, new_gradient_after="a"*32)[2:] + [
        "--state-path", str(state), "--health-path", str(tmp_path/"health.json"),
        "--control-path", str(tmp_path/"control.json"), "--output-dir", str(tmp_path/"samples"),
        "--experimental-exit-model", str(path), "--experimental-exit-model-sha256", sha,
        "--confirm-experimental-exit-model",
    ]
    for _ in range(2):
        runtime = wti_live.WTILiveRuntime(wti_live.parse_args(argv))
        try:
            assert runtime.experimental_exit_model == model
            assert state.read_bytes() == before
            assert runtime.executor is None and runtime.servers == []
        finally:
            for handler in runtime.logger.handlers:
                handler.close()
            runtime.logger.handlers.clear()


def test_expired_entry_model_does_not_block_existing_position_exit():
    row = sample(day=5, index=40, var_bid="102", var_ask="102.01")
    at = datetime.now(timezone.utc)
    close = at + timedelta(hours=1)
    row.update(logged_at=at.isoformat(), session_status_observed_at=at.isoformat(),
               session_status_feed_heartbeat_at=at.isoformat(),
               session_id=f"{(at-timedelta(hours=1)).isoformat()}|{close.isoformat()}",
               session_closing_at=close.isoformat())
    position = {"direction": DIRECTIONS[0], "opened_at": (at-timedelta(minutes=10)).isoformat(),
                "session_id": row["session_id"], "var_qty": ".19", "lighter_qty": ".19",
                "multiplier": "1", "var_entry_price": "100", "lighter_entry_price": "100.5"}
    config = ReplayConfig(D(30), D(1), D(10), 21600, 120)
    result = decide_wti_gradient_action(row, position,
        {"pilot_id": "test", "max_layers": 5, "config": CFG}, config=config, now=at,
        thresholds_by_direction={d: (None,) * 5 for d in DIRECTIONS},
        aggregate_capacity_by_tier_usd=(D(20),)*5)
    assert result["action"] == "close", result
