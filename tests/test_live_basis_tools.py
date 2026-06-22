from __future__ import annotations

import argparse
import json
from decimal import Decimal

from tools.inspect_live_basis_state import inspect
from tools.analyze_live_basis_slippage import summarize as summarize_slippage
from tools.analyze_live_basis_execution_quality import summarize as summarize_execution_quality
from tools.grid_replay_live_basis_params import parse_decimals, run_grid
from tools.replay_live_basis_params import replay
from tools.recommend_live_basis_params import recommend
from tools.summarize_latest_live_basis_round import latest_events, load_state, suggest_action
from tools.preflight_live_basis import check as preflight_check
from tools.archive_live_basis_round import archive


def write_jsonl(path, rows):
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_inspect_live_basis_state_estimates_open_pnl(tmp_path) -> None:
    state = tmp_path / "state.json"
    metrics = tmp_path / "metrics.jsonl"
    state.write_text(
        json.dumps(
            {
                "status": "open",
                "completed_cycles": 0,
                "realized_pnl_usd": "0",
                "open_lots": [
                    {
                        "lot_id": 1,
                        "entry_kind": "basis_initial",
                        "direction": "long_var_short_lighter",
                        "qty": "1",
                        "entry_basis_bps": "-10",
                        "entry_var_fill_price": "100",
                        "entry_lighter_fill_price": "110",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    write_jsonl(
        metrics,
        [
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "basis_bps": "-5",
                "z": "0",
                "var_bid": "105",
                "var_ask": "106",
                "lighter_buy_price": "107",
                "lighter_sell_price": "106",
            }
        ],
    )

    report = inspect(state, metrics, asset="ETH")

    assert report["open_lots"] == 1
    assert report["weighted_entry_basis_bps"] == "-10"
    assert report["estimated_open_pnl_usd"] == "8"


def test_replay_live_basis_params_respects_abs_entry_gate(tmp_path) -> None:
    metrics = tmp_path / "metrics.jsonl"
    write_jsonl(
        metrics,
        [
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "sample_index": 1,
                "basis_bps": "-8",
                "z": "-5",
                "var_bid": "99",
                "var_ask": "100",
                "lighter_buy_price": "101",
                "lighter_sell_price": "100",
                "long_edge_bps": "10",
                "long_roundtrip_pnl_bps": "-2",
            },
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "sample_index": 2,
                "basis_bps": "-13",
                "z": "-5",
                "var_bid": "99",
                "var_ask": "100",
                "lighter_buy_price": "101",
                "lighter_sell_price": "100",
                "long_edge_bps": "10",
                "long_roundtrip_pnl_bps": "-2",
            },
        ],
    )
    args = argparse.Namespace(
        asset="ETH",
        lot_notional_usd=Decimal("20"),
        max_total_lots=1,
        max_cycles=1,
        z_entry=Decimal("3"),
        z_exit=Decimal("999"),
        min_entry_edge_bps=Decimal("7"),
        min_abs_entry_bps=Decimal("12"),
        max_entry_roundtrip_cost_bps=Decimal("3"),
        addon_min_basis_improvement_bps=Decimal("4"),
        min_exit_pnl_bps=Decimal("1"),
        min_hold_samples=0,
    )

    result = replay(metrics, args)

    assert result["rows_seen"] == 2
    assert result["entered"] == 1


def test_analyze_live_basis_slippage_reports_shortfall(tmp_path) -> None:
    metrics = tmp_path / "metrics.jsonl"
    write_jsonl(
        metrics,
        [
            {"event": "live_inventory_exited", "asset": "ETH", "lot_id": 1, "direction": "long_var_short_lighter", "pnl_bps": "1.5", "pnl_usd": "0.003"},
            {"event": "live_inventory_actual_pnl", "asset": "ETH", "lot_id": 1, "direction": "long_var_short_lighter", "actual_pnl_bps": "0.5", "actual_pnl_usd": "0.001"},
        ],
    )

    summary = summarize_slippage(metrics, asset="ETH")

    assert summary["matched_exits"] == 1
    assert summary["positive_shortfalls"] == 1
    assert summary["avg_positive_shortfall_bps"] == Decimal("1.0")


def test_grid_replay_live_basis_params_runs_combinations(tmp_path) -> None:
    metrics = tmp_path / "metrics.jsonl"
    write_jsonl(
        metrics,
        [
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "sample_index": 1,
                "basis_bps": "-13",
                "z": "-5",
                "var_bid": "99",
                "var_ask": "100",
                "lighter_buy_price": "101",
                "lighter_sell_price": "100",
                "long_edge_bps": "10",
                "long_roundtrip_pnl_bps": "-2",
            }
        ],
    )
    args = argparse.Namespace(
        input=metrics,
        asset="ETH",
        lot_notional_usd=Decimal("20"),
        max_cycles=1,
        z_entry=parse_decimals("3,4"),
        z_exit=Decimal("999"),
        min_entry_edge_bps=parse_decimals("7"),
        min_abs_entry_bps=parse_decimals("12"),
        max_entry_roundtrip_cost_bps=parse_decimals("3"),
        addon_min_basis_improvement_bps=parse_decimals("4"),
        min_exit_pnl_bps=parse_decimals("1"),
        max_total_lots=[1],
        min_hold_samples=0,
        adjust_shortfall_bps=Decimal("0"),
    )

    rows = run_grid(args)

    assert len(rows) == 2
    assert {row["entered"] for row in rows} == {1}
    assert "adjusted_pnl_usd" in rows[0]


def test_recommend_live_basis_params_returns_candidate(tmp_path) -> None:
    metrics = tmp_path / "metrics.jsonl"
    write_jsonl(
        metrics,
        [
            {"event": "live_inventory_exited", "asset": "ETH", "lot_id": 1, "direction": "long_var_short_lighter", "pnl_bps": "1.5", "pnl_usd": "0.003"},
            {"event": "live_inventory_actual_pnl", "asset": "ETH", "lot_id": 1, "direction": "long_var_short_lighter", "actual_pnl_bps": "0.5", "actual_pnl_usd": "0.001"},
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "sample_index": 1,
                "basis_bps": "-13",
                "z": "-5",
                "var_bid": "99",
                "var_ask": "100",
                "lighter_buy_price": "101",
                "lighter_sell_price": "100",
                "long_edge_bps": "10",
                "long_roundtrip_pnl_bps": "-2",
            },
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "sample_index": 2,
                "basis_bps": "-5",
                "z": "0",
                "var_bid": "110",
                "var_ask": "111",
                "lighter_buy_price": "100",
                "lighter_sell_price": "99",
                "long_edge_bps": "0",
                "long_roundtrip_pnl_bps": "0",
            },
        ],
    )
    args = argparse.Namespace(
        input=metrics,
        asset="ETH",
        lot_notional_usd=Decimal("20"),
        max_cycles=1,
        z_entry=parse_decimals("3"),
        z_exit=Decimal("999"),
        min_entry_edge_bps=parse_decimals("7"),
        min_abs_entry_bps=parse_decimals("12"),
        max_entry_roundtrip_cost_bps=parse_decimals("3"),
        addon_min_basis_improvement_bps=parse_decimals("4"),
        min_exit_pnl_bps=parse_decimals("1"),
        max_total_lots=[1],
        min_hold_samples=0,
        adjust_shortfall_bps=None,
    )

    result = recommend(args)

    assert result["candidate_count"] == 1
    assert result["suggested_exit_safety_buffer_bps"] == Decimal("1.0")
    assert result["warnings"] == ["sample_too_small", "p80_shortfall_unreliable"]
    assert "--live-inventory-basis-z-entry 3" in result["suggested_flags_one_line"]


def test_recommend_live_basis_params_warns_when_no_candidates(tmp_path) -> None:
    metrics = tmp_path / "metrics.jsonl"
    write_jsonl(metrics, [{"event": "live_inventory_actual_pnl", "asset": "ETH", "lot_id": 1, "actual_pnl_bps": "0"}])
    args = argparse.Namespace(
        input=metrics,
        asset="ETH",
        lot_notional_usd=Decimal("20"),
        max_cycles=1,
        z_entry=parse_decimals("3"),
        z_exit=Decimal("999"),
        min_entry_edge_bps=parse_decimals("7"),
        min_abs_entry_bps=parse_decimals("12"),
        max_entry_roundtrip_cost_bps=parse_decimals("3"),
        addon_min_basis_improvement_bps=parse_decimals("4"),
        min_exit_pnl_bps=parse_decimals("1"),
        max_total_lots=[1],
        min_hold_samples=0,
        adjust_shortfall_bps=None,
        min_actual_exits=3,
    )

    result = recommend(args)

    assert result["candidate_count"] == 0
    assert "do_not_trade" in result["warnings"]
    assert result["suggested_action"] == "do_not_trade"


def test_summarize_latest_live_basis_round_suggests_flat_action(tmp_path) -> None:
    state = tmp_path / "state.json"
    metrics = tmp_path / "metrics.jsonl"
    state.write_text(json.dumps({"status": "flat", "open_lots": [], "completed_cycles": 1}), encoding="utf-8")
    write_jsonl(metrics, [{"event": "live_inventory_actual_pnl", "asset": "ETH", "lot_id": 1, "actual_pnl_bps": "0"}])

    loaded = load_state(state)
    events = latest_events(metrics, asset="ETH", limit=5)

    assert loaded["status"] == "flat"
    assert suggest_action(loaded, events).startswith("flat:")


def test_analyze_live_basis_execution_quality_summarizes_lighter_drift(tmp_path) -> None:
    metrics = tmp_path / "metrics.jsonl"
    write_jsonl(
        metrics,
        [
            {
                "event": "live_inventory_actual_pnl",
                "asset": "ETH",
                "estimated_pnl_bps": "1",
                "actual_pnl_bps": "0",
                "entry_var_price": "100",
                "exit_lighter_price": "101",
                "exit_lighter_final_fill_price": "102",
                "actual_var_leg_pnl_usd": "0.1",
                "actual_lighter_leg_pnl_usd": "-0.1",
            }
        ],
    )

    summary = summarize_execution_quality(metrics, asset="ETH")

    assert summary["actual_rows"] == 1
    assert summary["avg_estimated_minus_actual_bps"] == Decimal("1")
    assert summary["avg_lighter_exit_fill_drift_bps"] == Decimal("100")


def test_preflight_live_basis_recommends_open_state_resume(tmp_path) -> None:
    state = tmp_path / "state.json"
    metrics = tmp_path / "metrics.jsonl"
    state.write_text(json.dumps({"status": "open", "open_lots": [{"lot_id": 1}]}), encoding="utf-8")
    metrics.write_text("", encoding="utf-8")

    result = preflight_check(state, metrics, asset="ETH", scan=100)

    assert result["recommended_command_type"] == "open_state_resume"


def test_archive_live_basis_round_writes_summary(tmp_path) -> None:
    state = tmp_path / "live_inventory_state.json"
    metrics = tmp_path / "order_metrics.jsonl"
    archive_dir = tmp_path / "archive"
    state.write_text(json.dumps({"status": "flat", "open_lots": [], "completed_cycles": 1}), encoding="utf-8")
    write_jsonl(
        metrics,
        [
            {"event": "live_inventory_run_config", "asset": "ETH", "run_id": "old-run", "config": {"mode": "live"}},
            {"event": "live_inventory_exited", "asset": "ETH", "run_id": "old-run", "lot_id": 99, "direction": "long_var_short_lighter", "pnl_bps": "9", "pnl_usd": "9"},
            {"event": "live_inventory_run_config", "asset": "ETH", "run_id": "run-1", "config": {"mode": "live"}},
            {"event": "live_inventory_exited", "asset": "ETH", "run_id": "run-1", "lot_id": 1, "direction": "long_var_short_lighter", "pnl_bps": "1.5", "pnl_usd": "0.003"},
            {"event": "live_inventory_actual_pnl", "asset": "ETH", "run_id": "run-1", "lot_id": 1, "direction": "long_var_short_lighter", "actual_pnl_bps": "0.5", "actual_pnl_usd": "0.001"},
            {"event": "live_inventory_exit_blocked", "asset": "ETH", "reason": "test_without_run_id"},
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "run_id": "run-1",
                "sample_index": 1,
                "basis_bps": "-13",
                "z": "-5",
                "var_bid": "99",
                "var_ask": "100",
                "lighter_buy_price": "101",
                "lighter_sell_price": "100",
                "long_edge_bps": "10",
                "long_roundtrip_pnl_bps": "-2",
            },
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "run_id": "run-1",
                "sample_index": 2,
                "basis_bps": "-5",
                "z": "0",
                "var_bid": "110",
                "var_ask": "111",
                "lighter_buy_price": "100",
                "lighter_sell_price": "99",
                "long_edge_bps": "0",
                "long_roundtrip_pnl_bps": "0",
            },
        ],
    )
    args = argparse.Namespace(
        state=state,
        metrics=metrics,
        input=metrics,
        archive_dir=archive_dir,
        asset="ETH",
        limit=10,
        lot_notional_usd=Decimal("20"),
        max_cycles=1,
        z_entry=parse_decimals("3"),
        z_exit=Decimal("999"),
        min_entry_edge_bps=parse_decimals("7"),
        min_abs_entry_bps=parse_decimals("12"),
        max_entry_roundtrip_cost_bps=parse_decimals("3"),
        addon_min_basis_improvement_bps=parse_decimals("4"),
        min_exit_pnl_bps=parse_decimals("1"),
        max_total_lots=[1],
        min_hold_samples=0,
        adjust_shortfall_bps=None,
        min_actual_exits=3,
    )

    target = archive(args)
    summary = json.loads((target / "summary.json").read_text(encoding="utf-8"))
    summary_text = (target / "summary.txt").read_text(encoding="utf-8")
    round_rows = [json.loads(line) for line in (target / "round_order_metrics.jsonl").read_text(encoding="utf-8").splitlines()]

    assert (target / "live_inventory_state.json").exists()
    assert (target / "order_metrics.jsonl").exists()
    assert (target / "summary.txt").exists()
    assert summary["run_id"] == "run-1"
    assert summary["run_id_filter_applied"] is True
    assert summary["since_run_start_applied"] is True
    assert any(row.get("reason") == "test_without_run_id" for row in round_rows)
    assert {row.get("run_id") for row in round_rows if row.get("run_id")} == {"run-1"}
    assert summary["state"]["status"] == "flat"
    assert summary["actual_pnl"]["actual_exit_count"] == 1
    assert summary["actual_pnl"]["actual_pnl_total_usd"] == "0.001"
    assert summary["actual_pnl"]["actual_pnl_avg_bps"] == "0.5"
    assert summary["next_step_commands"] == [
        "python3 tools/preflight_live_basis.py",
        "python3 tools/recommend_live_basis_params.py --input log/order_metrics.jsonl --asset ETH",
    ]
    assert summary["slippage"]["matched_exits"] == 1
    assert summary["recommendation"]["candidate_count"] == 1
    assert "run_id: run-1" in summary_text
    assert "since_run_start_applied: True" in summary_text
    assert "actual_pnl_total_usd: 0.001" in summary_text
    assert "next_step_commands:" in summary_text
    assert "suggested_flags_one_line:" in summary_text
    assert "recommendation_warnings: sample_too_small,p80_shortfall_unreliable" in summary_text
