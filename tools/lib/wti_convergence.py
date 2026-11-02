"""Explicit, frozen exit-convergence pilot policy; no order submission."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from tools.lib.wti_exit_shadow import causal_features, labels_for, nonoverlapping_count
from tools.lib.wti_research import DIRECTIONS, audit_rows, digest, stamp, _entry_gap
from tools.lib.wti_thresholds import _percentile

MODEL_KIND = "exit_convergence_session_balanced_p75_1h_v1"
PILOT_CAP_USD = Decimal("20")
HORIZON_SECONDS = 3600
MIN_SESSIONS = 3
MIN_LABELS = 100
MIN_INDEPENDENT_LABELS = 6
CONSTRAINT_KEYS = (
    "entry_threshold_bps", "tier_thresholds_bps", "roundtrip_cost_bps",
    "exit_target_bps", "max_hold_seconds", "entry_cutoff_seconds",
    "max_slippage_bps", "max_notional_usd", "layer_notional_usd",
)


def policy_config(config):
    return {k: config[k] for k in CONSTRAINT_KEYS}


def fit_exit_model(clean, *, config, train_before, created_at):
    """Only labels whose outcomes precede the purged training cutoff are fitted."""
    training = [r for r in clean if stamp(r["logged_at"]) < train_before]
    features = causal_features(training)
    labels, rejected = labels_for(
        training, features, horizon=HORIZON_SECONDS, end_before=train_before,
    )
    groups = defaultdict(list)
    for label in labels:
        d = label["direction"]
        for key in ((d, label["phase"], label["state"]),
                    (d, "*", label["state"]), (d, "*", "*")):
            groups[key].append(label)
    cells = {}
    for key, rows in groups.items():
        sessions = defaultdict(list)
        for label in rows:
            sessions[label["session_id"]].append(Decimal(label["exit_delta_bps"]))
        independent = nonoverlapping_count(rows)
        if len(sessions) < MIN_SESSIONS or len(rows) < MIN_LABELS or independent < MIN_INDEPENDENT_LABELS:
            continue
        per_session = [_percentile(v, Decimal("75")) for v in sessions.values()]
        cells["|".join(key)] = {
            "exit_delta_bps": str(_percentile(per_session, Decimal("50"))),
            "sessions": len(sessions), "labels": len(rows),
            "nonoverlapping_labels": independent,
        }
    return {
        "schema_version": 1, "kind": MODEL_KIND,
        "created_at": created_at.isoformat(),
        "expires_at": (created_at + timedelta(hours=24)).isoformat(),
        "train_before": train_before.isoformat(),
        "latest_label_end_at": max((r["end_at"] for r in labels), default=None),
        "training_sha256": digest(training), "training_labels_sha256": digest(labels),
        "training_sessions": sorted({r["session_id"] for r in labels}),
        "market_id": 145, "multiplier": "1", "horizon_seconds": HORIZON_SECONDS,
        "aggregate_cap_usd": str(PILOT_CAP_USD), "constraints": policy_config(config),
        "cells": cells, "rejected_labels": rejected,
        "profitability_claim": False, "experimental_only": True,
    }


def validate_model(model, *, config):
    if (
        model.get("schema_version") != 1 or model.get("kind") != MODEL_KIND
        or model.get("experimental_only") is not True
        or model.get("market_id") != 145 or Decimal(str(model.get("multiplier"))) != 1
        or model.get("horizon_seconds") != HORIZON_SECONDS
        or Decimal(str(model.get("aggregate_cap_usd"))) != PILOT_CAP_USD
        or model.get("constraints") != policy_config(config)
        or Decimal(str(config["roundtrip_cost_bps"])) != 10
        or Decimal(str(config["exit_target_bps"])) != 1
    ):
        raise ValueError("experimental_exit_model_constraints_mismatch")
    created, cutoff, expires = (stamp(model[k]) for k in ("created_at", "train_before", "expires_at"))
    if cutoff > created or not created < expires <= created + timedelta(hours=24):
        raise ValueError("experimental_exit_model_time_invalid")
    if not model.get("latest_label_end_at") or stamp(model["latest_label_end_at"]) >= cutoff:
        raise ValueError("experimental_exit_model_training_leakage")
    if len(set(model.get("training_sessions") or [])) < MIN_SESSIONS or not model.get("cells"):
        raise ValueError("experimental_exit_model_support_missing")
    for key, cell in model["cells"].items():
        d, phase, state = key.split("|")
        value = Decimal(str(cell["exit_delta_bps"]))
        if (d not in DIRECTIONS or phase not in ("*", "early", "middle", "late")
                or state not in ("*", "below", "normal", "expanded", "extreme")
                or not value.is_finite() or abs(value) > 10000
                or cell["sessions"] < MIN_SESSIONS or cell["labels"] < MIN_LABELS
                or cell["nonoverlapping_labels"] < MIN_INDEPENDENT_LABELS):
            raise ValueError("experimental_exit_model_cell_invalid")
    return model


def load_model(path: Path, expected_sha256: str, *, config):
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("experimental_exit_model_sha256_mismatch")
    return validate_model(json.loads(raw), config=config)


def features_for_row(history, row):
    at = stamp(row["logged_at"])
    recent = [r for r in history if r.get("session_id") == row.get("session_id")
              and at - timedelta(minutes=30) <= stamp(r["logged_at"]) < at]
    recent.sort(key=lambda r: (stamp(r["logged_at"]), r["sample_id"]))
    return causal_features([*recent, row]).get(row["sample_id"], {})


def convergence_thresholds(model, row, features, control, *, now, exploratory=False):
    """Same threshold function in live and replay; unknown support never relaxes."""
    floors = tuple(Decimal(x) for x in model["constraints"]["tier_thresholds_bps"])
    result = {d: (None,) * 5 for d in DIRECTIONS}
    diagnostic = {"kind": MODEL_KIND, "reason": None, "directions": {},
                  "entry_mode": "exploratory_floor_pilot" if exploratory else "model_qualified"}
    if not stamp(model["created_at"]) <= now < stamp(model["expires_at"]):
        diagnostic["reason"] = "experimental_exit_model_expired_or_not_yet_valid"
        return result, diagnostic
    at = stamp(row["logged_at"])
    checked, _ = audit_rows([row], as_of=at + timedelta(microseconds=1),
                            market_id=145, multiplier=Decimal("1"))
    if not checked or at < stamp(model["train_before"]):
        diagnostic["reason"] = "experimental_exit_model_sample_unverified_or_training_overlap"
        return result, diagnostic
    try:
        drift = Decimal(str(row.get("lighter_pair_drift_bps")))
        pair_ok = drift.is_finite() and 0 <= drift <= Decimal("1.5")
    except ArithmeticError:
        pair_ok = False
    if not pair_ok:
        diagnostic["reason"] = "experimental_exit_model_pair_drift_unverified_or_exceeded"
        return result, diagnostic
    for direction in DIRECTIONS:
        feature = features.get(direction)
        values = control.get(direction)
        if not feature or not values or values[0] is None:
            diagnostic["directions"][direction] = {"reason": "feature_or_control_unavailable"}
            continue
        keys = ((direction, feature["phase"], feature["state"]),
                (direction, "*", feature["state"]), (direction, "*", "*"))
        cell_key = next(("|".join(k) for k in keys if "|".join(k) in model["cells"]), None)
        if cell_key is None:
            diagnostic["directions"][direction] = {"reason": "exit_model_cell_unsupported"}
            continue
        delta = Decimal(model["cells"][cell_key]["exit_delta_bps"])
        entry_gap = _entry_gap(row, direction)
        reserve = Decimal(model["constraints"]["roundtrip_cost_bps"])
        target = Decimal(model["constraints"]["exit_target_bps"])
        v = Decimal(row["var_ask"] if direction == DIRECTIONS[0] else row["var_bid"])
        l = Decimal(row["lighter_sell_price"] if direction == DIRECTIONS[0] else row["lighter_buy_price"])
        right = v if direction == DIRECTIONS[0] else l
        required = (entry_gap + delta + reserve + target) * (v + l) / (2 * right)
        first = max(floors[0], required)
        thresholds = []
        for i in range(5):
            if values[i] is None:
                thresholds.append(None)
                continue
            thresholds.append(max(floors[i], first + values[i] - values[0]))
        result[direction] = tuple(thresholds)
        diagnostic["directions"][direction] = {
            "cell": cell_key, "feature": feature, "entry_gap_bps": str(entry_gap),
            "hybrid_context": {
                "historical_median_bps": feature.get("prior_median_bps"),
                "excess_over_median_bps": feature.get("excess_bps"),
                "legacy_thresholds_bps": [None if x is None else str(x) for x in values],
                "legacy_tier_spacing_and_maturity_preserved": True,
                "absolute_thresholds_not_added_twice": True,
            },
            "predicted_exit_gap_bps": str(entry_gap + delta),
            "predicted_net_bps": str(-delta - reserve),
            "thresholds_bps": [None if x is None else str(x) for x in thresholds],
        }
    if exploratory:
        diagnostic["model_would_qualify"] = {
            d: result[d][0] is not None and Decimal(row[field]) >= result[d][0]
            for d, field in zip(DIRECTIONS, ("long_edge_bps", "short_edge_bps"))
        }
        diagnostic["not_a_profitability_qualification"] = True
        for direction in DIRECTIONS:
            values = control.get(direction)
            if not values or values[0] is None:
                continue
            actual = []
            for i in range(5):
                if values[i] is None:
                    actual.append(None)
                else:
                    actual.append(max(floors[i], actual[i-1] + values[i]-values[i-1]) if i else floors[0])
            result[direction] = tuple(actual)
        diagnostic["actual_thresholds_bps"] = {
            d: [None if v is None else str(v) for v in values] for d, values in result.items()
        }
    return result, diagnostic


def clean_feature_history(rows, *, as_of):
    clean, _ = audit_rows(rows, as_of=as_of, market_id=145, multiplier=Decimal("1"))
    return [r for r in clean if stamp(r["logged_at"]) >= as_of - timedelta(minutes=31)]
