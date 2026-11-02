#!/usr/bin/env python3
"""Freeze and replay an explicitly experimental exit-convergence policy. No orders."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import read_basis_samples
from tools.lib.wti_convergence import (
    PILOT_CAP_USD, convergence_thresholds, fit_exit_model, validate_model,
)
from tools.lib.wti_exit_shadow import causal_features
from tools.lib.wti_research import DIRECTIONS, audit_rows, digest, profiles, stamp
from tools.lib.wti_replay import ReplayConfig, replay_wti_gradient
from tools.wti_strategy_replay import _input_manifest, _write_json_atomic


def build_experiment(rows, *, config, created_at):
    as_of = max(stamp(r["logged_at"]) for r in rows) + timedelta(microseconds=1)
    clean, quality = audit_rows(rows, as_of=as_of, market_id=145, multiplier=Decimal("1"))
    sessions = sorted({r["session_id"] for r in clean
                       if stamp(r["session_closing_at"]) <= as_of})
    if len(sessions) < 5:
        raise ValueError("exit_model_requires_three_training_and_two_holdout_sessions")
    holdouts = sessions[-2:]
    split = min(stamp(r["logged_at"]) for r in clean if r["session_id"] in holdouts)
    cutoff = split - timedelta(seconds=120)
    model = fit_exit_model(clean, config=config, train_before=cutoff, created_at=created_at)
    validate_model(model, config=config)
    features = causal_features(clean)
    clean_ids = {r["sample_id"] for r in clean}
    schedules = {"control": {}, "candidate": {}}
    history, cached, refreshed = [], None, None
    crossings = {arm: {d: 0 for d in DIRECTIONS} for arm in schedules}
    candidate_reasons = {}
    for row in sorted(rows, key=lambda r: stamp(r["logged_at"])):
        if row.get("sample_kind", "baseline") != "baseline":
            continue
        at = stamp(row["logged_at"])
        if row.get("session_id") in holdouts and row.get("sample_id") in clean_ids:
            if cached is None or (at - refreshed).total_seconds() >= 300:
                cached, refreshed = profiles(history, at, config), at
            control = {d: cached[d].thresholds_bps for d in DIRECTIONS}
            candidate, diagnostic = convergence_thresholds(
                model, row, features.get(row["sample_id"], {}), control, now=created_at,
            )
            schedules["control"][row["sample_id"]] = control
            schedules["candidate"][row["sample_id"]] = candidate
            for arm, thresholds in (("control", control), ("candidate", candidate)):
                for direction, field in zip(DIRECTIONS, ("long_edge_bps", "short_edge_bps")):
                    crossings[arm][direction] += int(
                        thresholds[direction][0] is not None
                        and Decimal(row[field]) >= thresholds[direction][0]
                    )
            if diagnostic.get("reason"):
                reason = diagnostic["reason"]
                candidate_reasons[reason] = candidate_reasons.get(reason, 0) + 1
        history.append(row)

    cfg = ReplayConfig(
        Decimal(config["entry_threshold_bps"]), Decimal(config["exit_target_bps"]),
        Decimal(config["roundtrip_cost_bps"]), float(config["max_hold_seconds"]), 120,
        float(config["entry_cutoff_seconds"]), Decimal(config["max_slippage_bps"]),
        Decimal(config["max_notional_usd"]),
    )
    arms = {}
    for arm, schedule in schedules.items():
        reports = []
        for session in holdouts:
            subset = [r for r in rows if r.get("session_id") == session]
            start = min(stamp(r["logged_at"]) for r in subset)
            warm = [r for r in rows if stamp(r["logged_at"]) < start
                    and r.get("sample_kind", "baseline") == "baseline"]
            report = replay_wti_gradient(
                subset, config=cfg,
                second_tier_threshold_bps=Decimal(config["tier_thresholds_bps"][1]),
                layer_notional_usd=Decimal(config["layer_notional_usd"]),
                aggregate_max_notional_usd=PILOT_CAP_USD, equity_usd=PILOT_CAP_USD,
                max_layers=5, expected_market_id=145, expected_multiplier=Decimal("1"),
                max_completed_roundtrips=1, initial_threshold_history=warm,
                research_thresholds_by_sample=schedule,
            )
            reports.append({"session_id": session, **asdict(report)})
        arms[arm] = {
            "crossings": crossings[arm],
            "completed_trades": sum(r["completed_trades"] for r in reports),
            "net_pnl_usd": str(sum((Decimal(r["net_pnl_usd"]) for r in reports), Decimal(0))),
            "unresolved_sessions": sum(r["unresolved_position"] for r in reports),
            "sessions": reports,
        }
    return model, {
        "schema_version": 1, "mode": "frozen_experimental_quote_path_comparison",
        "orders_attempted": False, "state_modified": False,
        "input_sha256": digest(rows), "as_of": as_of.isoformat(), "quality": quality,
        "training_cutoff": cutoff.isoformat(), "holdout_sessions": holdouts,
        "fixed_policy_registered_before_results": True,
        "aggregate_cap_usd": str(PILOT_CAP_USD), "roundtrip_cost_bps": "10",
        "max_sample_gap_seconds": 120, "arms": arms,
        "candidate_rejections": candidate_reasons,
        "profitability_claim": False,
        "limitations": ["quote_path_not_final_fills", "account_risk_history_not_reconstructed",
                        "independent_simulated_flat_session_starts", "no_forced_entry_permission"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-root", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    rows = read_basis_samples(args.sample_root, limit=200000, asset_filter="WTI")
    state = json.loads(args.state.read_text())
    config = dict(state["gradient_pilot"]["config"])
    config.update(market_id=state["gradient_pilot"]["market_id"],
                  multiplier=state["gradient_pilot"]["multiplier"])
    model, report = build_experiment(rows, config=config, created_at=datetime.now(timezone.utc))
    report["files"], report["input_files_sha256"] = _input_manifest(args.sample_root)
    _write_json_atomic(args.output_dir / "model.json", model)
    report["model_sha256"] = hashlib.sha256((args.output_dir / "model.json").read_bytes()).hexdigest()
    _write_json_atomic(args.output_dir / "report.json", report)
    print(json.dumps({"model_sha256": report["model_sha256"], "model_cells": len(model["cells"]),
                      "training_sessions": model["training_sessions"], "holdouts": report["holdout_sessions"],
                      "arms": {k: {x: v[x] for x in ("crossings", "completed_trades", "net_pnl_usd", "unresolved_sessions")}
                               for k, v in report["arms"].items()}}, indent=2))


if __name__ == "__main__":
    main()
