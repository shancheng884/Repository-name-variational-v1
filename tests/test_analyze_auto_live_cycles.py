from pathlib import Path

from tools.analyze_auto_live_cycles import parse_runtime_log


def test_parse_auto_live_success_and_manual_review_cycles(tmp_path: Path) -> None:
    log_path = tmp_path / "runtime.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-06-01 16:08:17,452 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=long_var_short_lighter qty=0.00022 var_side=BUY",
                "2026-06-01 16:08:33,900 | INFO | auto_live_exit_precheck_passed cycle_id=1 asset=BTC side=SELL qty=0.00022 edge_bps=12.3456",
                "2026-06-01 16:08:34,075 | INFO | auto_live_exit_submitted cycle_id=1 asset=BTC side=SELL qty=0.00022 reason=spread_reverted",
                "2026-06-01 16:17:08,100 | WARNING | auto_live_entry_precheck_failed cycle_id=1 asset=BTC side=BUY qty=0.00022 reason=hedge_price_deviation_exceeds_risk_limit edge_bps=101.5 action=skip_var_entry",
                "2026-06-01 16:17:08,300 | INFO | auto_live_entry_precheck_passed cycle_id=1 asset=BTC side=BUY qty=0.00022 edge_bps=9.8765",
                "2026-06-01 16:17:08,463 | INFO | auto_live_entry_submitted cycle_id=1 asset=BTC direction=long_var_short_lighter qty=0.00022 var_side=BUY",
                "2026-06-01 16:17:23,613 | WARNING | auto_live_exit_precheck_failed cycle_id=1 asset=BTC side=SELL qty=0.00022 reason=hedge_price_deviation_exceeds_risk_limit edge_bps=110.1499309277765240303833049 action=skip_var_exit",
                "2026-06-01 16:17:23,614 | WARNING | auto_live_manual_review_required cycle_id=1 asset=BTC qty=0.00022 reason=exit_precheck_failed:hedge_price_deviation_exceeds_risk_limit action=stop_auto_live_until_restart",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cycles = parse_runtime_log(log_path, {"BTC"})

    assert len(cycles) == 2
    assert cycles[0].status == "flat"
    assert cycles[0].occurrence == 1
    assert str(cycles[0].holding_seconds) == "16.623"
    assert cycles[0].exit_precheck_status == "passed"
    assert cycles[0].last_exit_precheck_edge_bps is not None
    assert f"{cycles[0].last_exit_precheck_edge_bps:.3f}" == "12.346"
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
