from pathlib import Path

from tools.analyze_auto_live_cycles import enrich_cycles_with_order_metrics, filter_cycles, parse_runtime_log, print_summary


def test_parse_auto_live_success_and_manual_review_cycles(tmp_path: Path) -> None:
    log_path = tmp_path / "runtime.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-06-01 16:08:17,452 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=long_var_short_lighter qty=0.00022 var_side=BUY entry_total_ms=900.1 entry_precheck_ms=12.3 var_preview_ms=200.4 var_submit_ms=300.5 lighter_submit_ms=386.9",
                "2026-06-01 16:08:33,900 | INFO | auto_live_exit_precheck_passed cycle_id=1 asset=BTC side=SELL qty=0.00022 edge_bps=12.3456 duration_ms=10.5",
                "2026-06-01 16:08:34,075 | INFO | auto_live_exit_submitted cycle_id=1 asset=BTC side=SELL qty=0.00022 reason=spread_reverted exit_total_ms=800.2 exit_precheck_ms=10.5 var_submit_ms=300.6 lighter_submit_ms=489.1",
                "2026-06-01 16:17:08,100 | WARNING | auto_live_entry_precheck_failed cycle_id=1 asset=BTC side=BUY qty=0.00022 reason=hedge_price_deviation_exceeds_risk_limit edge_bps=101.5 action=skip_var_entry",
                "2026-06-01 16:17:08,300 | INFO | auto_live_entry_precheck_passed cycle_id=1 asset=BTC side=BUY qty=0.00022 edge_bps=9.8765 duration_ms=11.1",
                "2026-06-01 16:17:08,463 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=long_var_short_lighter qty=0.00022 var_side=BUY entry_total_ms=700.2 entry_precheck_ms=11.1 var_preview_ms=180.2 var_submit_ms=250.3 lighter_submit_ms=258.6",
                "2026-06-01 16:17:23,613 | WARNING | auto_live_exit_precheck_failed cycle_id=1 asset=BTC side=SELL qty=0.00022 reason=hedge_price_deviation_exceeds_risk_limit edge_bps=110.1499309277765240303833049 duration_ms=9.9 action=skip_var_exit",
                "2026-06-01 16:17:23,614 | WARNING | auto_live_manual_review_required cycle_id=1 asset=BTC qty=0.00022 reason=exit_precheck_failed:hedge_price_deviation_exceeds_risk_limit action=stop_auto_live_until_restart",
                "2026-06-01 16:58:31,100 | INFO | auto_live_entry_precheck_passed cycle_id=1 asset=BTC side=BUY qty=0.00022 edge_bps=85.713",
                "2026-06-01 16:58:31,473 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=long_var_short_lighter qty=0.00022 var_side=BUY",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cycles = parse_runtime_log(log_path, {"BTC"})

    assert len(cycles) == 3
    assert cycles[0].status == "flat"
    assert cycles[0].occurrence == 1
    assert str(cycles[0].holding_seconds) == "16.623"
    assert cycles[0].exit_precheck_status == "passed"
    assert cycles[0].last_exit_precheck_edge_bps is not None
    assert f"{cycles[0].last_exit_precheck_edge_bps:.3f}" == "12.346"
    assert f"{cycles[0].entry_total_ms:.3f}" == "900.100"
    assert f"{cycles[0].entry_precheck_ms:.3f}" == "12.300"
    assert f"{cycles[0].entry_var_preview_ms:.3f}" == "200.400"
    assert f"{cycles[0].entry_var_submit_ms:.3f}" == "300.500"
    assert f"{cycles[0].entry_lighter_submit_ms:.3f}" == "386.900"
    assert f"{cycles[0].exit_total_ms:.3f}" == "800.200"
    assert f"{cycles[0].exit_precheck_ms:.3f}" == "10.500"
    assert f"{cycles[0].exit_var_submit_ms:.3f}" == "300.600"
    assert f"{cycles[0].exit_lighter_submit_ms:.3f}" == "489.100"
    assert cycles[1].status == "manual_review_required"
    assert cycles[1].occurrence == 2
    assert cycles[1].entry_precheck_status == "passed"
    assert cycles[1].entry_precheck_failures == 1
    assert cycles[1].last_entry_precheck_edge_bps is not None
    assert f"{cycles[1].last_entry_precheck_edge_bps:.3f}" == "9.876"
    assert cycles[1].exit_precheck_status == "failed"
    assert cycles[1].manual_review_reason == "exit_precheck_failed:hedge_price_deviation_exceeds_risk_limit"
    assert cycles[1].last_exit_precheck_edge_bps is not None
    assert f"{cycles[1].last_exit_precheck_edge_bps:.3f}" == "110.150"
    assert f"{cycles[1].entry_total_ms:.3f}" == "700.200"
    assert f"{cycles[1].exit_precheck_ms:.3f}" == "9.900"
    assert cycles[2].status == "open"
    assert cycles[2].occurrence == 3
    assert cycles[2].entry_precheck_status == "passed"
    assert cycles[2].last_entry_precheck_edge_bps is not None
    assert f"{cycles[2].last_entry_precheck_edge_bps:.3f}" == "85.713"


def test_print_summary_includes_latency_percentiles(tmp_path: Path, capsys) -> None:
    log_path = tmp_path / "runtime.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-06-01 16:08:17,452 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=long_var_short_lighter qty=0.00022 var_side=BUY entry_total_ms=900.1 entry_precheck_ms=12.3 var_preview_ms=200.4 var_submit_ms=300.5 lighter_submit_ms=386.9",
                "2026-06-01 16:08:34,075 | INFO | auto_live_exit_submitted cycle_id=1 asset=BTC side=SELL qty=0.00022 reason=spread_reverted exit_total_ms=800.2 exit_precheck_ms=10.5 var_submit_ms=300.6 lighter_submit_ms=489.1",
                "2026-06-01 16:17:08,463 | INFO | auto_live_entry_submitted cycle_id=2 asset=BTC direction=long_var_short_lighter qty=0.00022 var_side=BUY entry_total_ms=700.2 entry_precheck_ms=11.1 var_preview_ms=180.2 var_submit_ms=250.3 lighter_submit_ms=258.6",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cycles = parse_runtime_log(log_path, {"BTC"})

    print_summary(cycles, log_path, limit=30)
    captured = capsys.readouterr().out

    assert "latency percentiles" in captured
    assert "entry_total_ms 2 700.200 900.100" in captured
    assert "entry_var_submit_ms 2 250.300 300.500" in captured
    assert "exit_total_ms 1 800.200 800.200" in captured


def test_order_metrics_enrich_fill_result_latency(tmp_path: Path, capsys) -> None:
    log_path = tmp_path / "runtime.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-06-05 06:42:58,276 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=short_var_long_lighter qty=0.00049 var_side=SELL entry_total_ms=162.366 entry_precheck_ms=0.061 var_preview_ms=skipped var_submit_ms=161.124 lighter_submit_ms=19.205",
                "2026-06-05 06:43:16,448 | INFO | auto_live_exit_submitted cycle_id=1 asset=BTC side=BUY qty=0.00049 reason=spread_reverted exit_total_ms=134.559 exit_precheck_ms=0.072 var_submit_ms=134.036 lighter_submit_ms=16.639",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    metrics_path = tmp_path / "order_metrics.jsonl"
    metrics_path.write_text(
        "\n".join(
            [
                '{"event":"lighter_fill","logged_at":"2026-06-05T06:42:58.600000+00:00","asset":"BTC","auto_live_cycle_id":1,"auto_live_role":"entry","synthetic_eager_fill":true,"variational_filled_at":"2026-06-05T06:42:58.420000Z","lighter_filled_at":"2026-06-05T06:42:58.600000+00:00","live_submit_sent_to_fill_ms":"305.1","variational_filled_price":"104000","lighter_filled_price":"103900"}',
                '{"event":"lighter_fill","logged_at":"2026-06-05T06:43:16.760000+00:00","asset":"BTC","auto_live_cycle_id":1,"auto_live_role":"exit","synthetic_eager_fill":true,"variational_filled_at":"2026-06-05T06:43:16.590000Z","lighter_filled_at":"2026-06-05T06:43:16.760000+00:00","live_submit_sent_to_fill_ms":"302.2","variational_filled_price":"103800","lighter_filled_price":"103850"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cycles = parse_runtime_log(log_path, {"BTC"})
    enrich_cycles_with_order_metrics(cycles, metrics_path, {"BTC"})

    assert len(cycles) == 1
    assert f"{cycles[0].entry_lighter_fill_ms:.3f}" == "305.100"
    assert f"{cycles[0].exit_lighter_fill_ms:.3f}" == "302.200"
    assert cycles[0].entry_signal_to_both_filled_ms is not None
    assert cycles[0].exit_signal_to_both_filled_ms is not None
    assert f"{cycles[0].entry_spread_usd:.2f}" == "100.00"
    assert f"{cycles[0].exit_spread_usd:.2f}" == "-50.00"
    assert f"{cycles[0].spread_capture_usd:.2f}" == "150.00"
    assert f"{cycles[0].spread_capture_bps:.3f}" == "14.423"
    assert f"{cycles[0].gross_pnl_usd:.6f}" == "0.073500"
    assert f"{cycles[0].gross_pnl_bps:.3f}" == "14.423"

    print_summary(cycles, log_path, limit=30)
    captured = capsys.readouterr().out
    assert "entry_signal_to_both_filled_ms" in captured
    assert "exit_signal_to_both_filled_ms" in captured
    assert "gross pnl summary (fees assumed zero)" in captured
    assert "spread_capture_usd" in captured
    assert "gross_pnl_usd" in captured
    assert "0.073500" in captured


def test_filter_cycles_can_keep_recent_completed_pnl_cycles(tmp_path: Path) -> None:
    log_path = tmp_path / "runtime.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-06-05 06:42:58,276 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=short_var_long_lighter qty=0.00049 var_side=SELL entry_total_ms=162.366",
                "2026-06-05 06:43:16,448 | INFO | auto_live_exit_submitted cycle_id=1 asset=BTC side=BUY qty=0.00049 reason=spread_reverted exit_total_ms=134.559",
                "2026-06-05 07:33:45,259 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=long_var_short_lighter qty=0.00049 var_side=BUY entry_total_ms=147.260",
                "2026-06-05 07:34:00,427 | INFO | auto_live_exit_submitted cycle_id=1 asset=BTC side=SELL qty=0.00049 reason=spread_reverted exit_total_ms=143.062",
                "2026-06-05 07:42:42,305 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=long_var_short_lighter qty=0.00048 var_side=BUY entry_total_ms=134.843",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    metrics_path = tmp_path / "order_metrics.jsonl"
    metrics_path.write_text(
        "\n".join(
            [
                '{"event":"lighter_fill","logged_at":"2026-06-05T06:42:58.600000+00:00","asset":"BTC","auto_live_cycle_id":1,"auto_live_role":"entry","variational_filled_at":"2026-06-05T06:42:58.420000Z","lighter_filled_at":"2026-06-05T06:42:58.600000+00:00","variational_filled_price":"104000","lighter_filled_price":"103900"}',
                '{"event":"lighter_fill","logged_at":"2026-06-05T06:43:16.760000+00:00","asset":"BTC","auto_live_cycle_id":1,"auto_live_role":"exit","variational_filled_at":"2026-06-05T06:43:16.590000Z","lighter_filled_at":"2026-06-05T06:43:16.760000+00:00","variational_filled_price":"103800","lighter_filled_price":"103850"}',
                '{"event":"lighter_fill","logged_at":"2026-06-05T07:33:45.580000+00:00","asset":"BTC","auto_live_cycle_id":1,"auto_live_role":"entry","variational_filled_at":"2026-06-05T07:33:45.420000Z","lighter_filled_at":"2026-06-05T07:33:45.580000+00:00","variational_filled_price":"62410.70","lighter_filled_price":"62445.00"}',
                '{"event":"lighter_fill","logged_at":"2026-06-05T07:34:00.780000+00:00","asset":"BTC","auto_live_cycle_id":1,"auto_live_role":"exit","variational_filled_at":"2026-06-05T07:34:00.590000Z","lighter_filled_at":"2026-06-05T07:34:00.780000+00:00","variational_filled_price":"62416.11","lighter_filled_price":"62457.28"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cycles = parse_runtime_log(log_path, {"BTC"})
    enrich_cycles_with_order_metrics(cycles, metrics_path, {"BTC"})
    filtered = filter_cycles(cycles, min_occurrence=2, completed_only=True, pnl_only=True)

    assert len(filtered) == 1
    assert filtered[0].occurrence == 2
    assert filtered[0].status == "flat"
    assert filtered[0].gross_pnl_usd is not None
