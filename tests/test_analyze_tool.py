from decimal import Decimal

from tools.analyze import (
    basis_state_rows,
    build_basis_regimes,
    build_basis_v2_replay,
    build_entry_semantics,
    dynamic_cost_summary,
    summarize_basis_v2_sweep_events,
    print_execution_calibration,
)


def test_basis_state_metrics_do_not_double_count_matching_block_event() -> None:
    state = {
        "event": "live_inventory_basis_state",
        "asset": "SOL",
        "basis_bps": "-8",
        "normalized_basis_bps": "-2",
        "long_edge_bps": "8",
        "short_edge_bps": "-9",
        "normalized_long_edge_bps": "2",
        "normalized_short_edge_bps": "-3",
        "basis_sample_move_bps": "0.5",
        "var_bid": "100",
        "var_ask": "100.01",
        "lighter_bid": "100",
        "lighter_ask": "100.01",
        "z": "2",
    }
    blocked = {**state, "event": "live_inventory_entry_blocked", "reason": "basis_sample_move_too_large"}
    rows = [state, blocked]

    assert basis_state_rows(rows) == [state]
    regime = build_basis_regimes(rows)["SOL"]
    assert regime["rows"] == 1
    assert regime["blocked_count"] == 1
    assert regime["raw_edge_p95"] == Decimal("8")
    assert dynamic_cost_summary(rows, asset_filter="SOL")["rows"] == 1


def test_entry_semantics_distinguishes_normalized_primary_from_raw_primary_filter() -> None:
    rows = [
        {
            "event": "live_inventory_basis_state",
            "run_id": "run-1",
            "logged_at": "2026-07-10T00:00:00+00:00",
            "asset": "SOL",
            "basis_bps": "-8",
            "long_edge_bps": "8",
            "short_edge_bps": "-9",
            "normalized_long_edge_bps": "2",
            "normalized_short_edge_bps": "-3",
            "var_bid": "99.9",
            "var_ask": "100",
            "lighter_buy_price": "100.1",
            "lighter_sell_price": "100.2",
            "long_stablecoin_filter_ok": True,
            "short_stablecoin_filter_ok": False,
            "basis_sample_move_ok": True,
        },
        {
            "event": "live_inventory_basis_state",
            "run_id": "run-1",
            "logged_at": "2026-07-10T00:00:01+00:00",
            "asset": "SOL",
            "basis_bps": "-8.1",
            "long_edge_bps": "8.5",
            "short_edge_bps": "-9",
            "normalized_long_edge_bps": "2.1",
            "normalized_short_edge_bps": "-3",
            "var_bid": "100",
            "var_ask": "100.1",
            "lighter_buy_price": "100",
            "lighter_sell_price": "100.2",
            "long_stablecoin_filter_ok": True,
            "short_stablecoin_filter_ok": False,
            "basis_sample_move_ok": True,
        },
    ]

    result = build_entry_semantics(
        rows,
        primary_threshold=Decimal("7"),
        min_abs_entry=Decimal("7"),
        min_normalized_filter=Decimal("1"),
    )["SOL"]

    assert result["normalized_primary_candidates"] == 0
    assert result["raw_primary_candidates"] == 2
    assert result["forward"][1]["attempts"] == 1
    assert result["forward"][1]["retained"] == 1
    assert result["forward"][1]["pnl_bps"] == [Decimal("20")]


def test_basis_v2_uses_real_time_and_does_not_require_normalized_edge() -> None:
    rows = []
    for timestamp, edge, age in (
        ("2026-07-10T00:00:00+00:00", "8", "0.1"),
        ("2026-07-10T00:00:00.100000+00:00", "8.5", "0.1"),
        ("2026-07-10T00:00:01.200000+00:00", "9", "0.1"),
    ):
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "run_id": "run-v2",
                "logged_at": timestamp,
                "asset": "SOL",
                "long_edge_bps": edge,
                "short_edge_bps": "-2",
                "var_bid": "99.9",
                "var_ask": "100",
                "lighter_bid": "100.08",
                "lighter_ask": "100.1",
                "lighter_sell_price": "100.08",
                "lighter_buy_price": "100.1",
                "var_quote_age_seconds": age,
            }
        )

    result = build_basis_v2_replay(rows, min_raw_edge_bps=Decimal("7"))["SOL"]

    assert result["candidate_count"] == 3
    assert result["direction_counts"]["long_var_short_lighter"] == 3
    assert result["horizons"][1]["attempts"] == 2
    assert result["horizons"][1]["pnl_bps"]
    assert not result["contexts"]["short_vs_long"]["insufficient_history"][1]["pnl_bps"] == []


def test_basis_v2_filter_sweep_inputs_filter_normalized_edge_and_stablecoin_share() -> None:
    rows = []
    for index, (edge, normalized, share) in enumerate(
        (("8", "1", "0.5"), ("8.5", "2.5", "0.5"), ("9", "3", "0.8"))
    ):
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "run_id": "run-filter",
                "logged_at": f"2026-07-10T00:00:0{index}+00:00",
                "asset": "SOL",
                "long_edge_bps": edge,
                "short_edge_bps": "-2",
                "normalized_long_edge_bps": normalized,
                "normalized_short_edge_bps": "-3",
                "long_stablecoin_edge_share": share,
                "short_stablecoin_edge_share": "0.2",
            }
        )

    unfiltered = build_basis_v2_replay(rows, min_raw_edge_bps=Decimal("7"))["SOL"]
    filtered = build_basis_v2_replay(
        rows,
        min_raw_edge_bps=Decimal("7"),
        min_normalized_edge_bps=Decimal("2.5"),
        max_stablecoin_edge_share=Decimal("0.6"),
    )["SOL"]

    assert unfiltered["candidate_count"] == 3
    assert filtered["candidate_count"] == 1


def test_execution_calibration_summary_only_uses_versioned_final_actual_rows(capsys) -> None:
    rows = [
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "execution-calibration-v1",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "asset": "SOL",
            "direction": "long_var_short_lighter",
            "actual_pnl_bps": "-2",
            "estimated_vs_actual_pnl_shortfall_bps": "1.5",
            "entry_lighter_slippage_bps": "0.2",
            "exit_lighter_slippage_bps": "0.3",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "legacy",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "asset": "SOL",
            "direction": "long_var_short_lighter",
            "actual_pnl_bps": "99",
        },
    ]

    print_execution_calibration(rows)

    output = capsys.readouterr().out
    assert "completed_actual_cycles=1" in output
    assert "actual_pnl_p50=-2.00" in output
    assert "shortfall_p80=1.50" in output
    assert "99" not in output


def test_basis_v2_event_cooldown_collapses_repeated_same_direction_candidates() -> None:
    rows = []
    for index in range(3):
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "run_id": "run-cooldown",
                "logged_at": f"2026-07-10T00:00:0{index}+00:00",
                "asset": "SOL",
                "long_edge_bps": "8",
                "short_edge_bps": "-2",
                "var_bid": "99.9",
                "var_ask": "100",
                "lighter_bid": "100.08",
                "lighter_ask": "100.1",
                "lighter_sell_price": "100.08",
                "lighter_buy_price": "100.1",
            }
        )

    result = build_basis_v2_replay(
        rows,
        min_raw_edge_bps=Decimal("7"),
        horizons=(1,),
        event_cooldown_seconds=300,
    )["SOL"]

    assert result["raw_candidate_count"] == 3
    assert result["candidate_count"] == 1
    assert len(result["candidate_events"]) == 1


def test_basis_v2_sweep_requires_train_and_holdout_positive_p20() -> None:
    events = [
        {"timestamp": float(index), "direction": "long_var_short_lighter", "pnl_bps": {5: Decimal(value)}}
        for index, value in enumerate(("3", "4", "5", "4", "5", "6"))
    ]

    summary = summarize_basis_v2_sweep_events(
        events,
        horizons=(5,),
        holdout_fraction=Decimal("0.5"),
        min_independent_samples=3,
        execution_reserve_bps=Decimal("1"),
    )[5]

    assert summary["train"]["n"] == 3
    assert summary["holdout"]["n"] == 3
    assert summary["train"]["p20"] == Decimal("2")
    assert summary["holdout"]["p20"] == Decimal("3")
    assert summary["verdict"] == "manual_review_candidate"
