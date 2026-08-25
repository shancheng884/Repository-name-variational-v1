import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from main import (
    VariationalToLighterRuntime,
    extract_lighter_account_metrics,
    extract_variational_account_metrics,
)
from tools.pnl_report import (
    build_report,
    deduplicated_actual_pnl,
    latest_pnl_telegram_payload,
    previous_flat_snapshot,
)


def test_extract_variational_account_metrics_adds_upnl() -> None:
    metrics = extract_variational_account_metrics(
        {
            "pool_portfolio_result": {
                "balance": "14.80",
                "upnl": "0.05",
            }
        }
    )

    assert metrics["balance_usd"] == Decimal("14.80")
    assert metrics["upnl_usd"] == Decimal("0.05")
    assert metrics["equity_usd"] == Decimal("14.85")
    assert metrics["equity_formula"] == "balance_plus_upnl"


def test_extract_lighter_account_metrics_uses_collateral() -> None:
    metrics = extract_lighter_account_metrics(
        {
            "accounts": [
                {
                    "collateral": "20.25",
                    "available_balance": "19.00",
                    "total_asset_value": "20.50",
                }
            ]
        }
    )

    assert metrics["collateral_usd"] == Decimal("20.25")
    assert metrics["available_balance_usd"] == Decimal("19.00")
    assert metrics["total_asset_value_usd"] == Decimal("20.50")
    assert metrics["equity_usd"] == Decimal("20.25")


def test_deduplicated_actual_pnl_requires_confirmed_fill() -> None:
    base = {
        "event": "live_inventory_actual_pnl",
        "run_id": "run-1",
        "asset": "ETH",
        "lot_id": 1,
        "logged_at": "2026-08-25T00:00:00+00:00",
    }
    rows = [
        {
            **base,
            "actual_pnl_status": "pending_lighter_final_fill",
            "actual_pnl_usd": "1.00",
        },
        {
            **base,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.10",
        },
        {
            **base,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.20",
        },
    ]

    selected = deduplicated_actual_pnl(rows)

    assert len(selected) == 1
    assert selected[0]["actual_pnl_usd"] == "0.20"


def test_build_report_falls_back_to_first_complete_snapshot_capital() -> None:
    rows = [
        {
            "event": "live_inventory_run_config",
            "run_id": "run-1",
            "asset": "ETH",
            "logged_at": "2026-08-25T00:00:00+00:00",
        },
        {
            "event": "live_inventory_account_snapshot",
            "run_id": "run-1",
            "asset": "ETH",
            "snapshot_status": "complete",
            "snapshot_stage": "startup_flat",
            "combined_equity_usd": "35.00",
            "snapshot_captured_at": "2026-08-25T00:01:00+00:00",
            "logged_at": "2026-08-25T00:01:00+00:00",
        },
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.05",
            "logged_at": "2026-08-26T00:00:00+00:00",
        },
    ]

    report = build_report(rows, since=None, capital_usd=None)

    assert report.capital_usd == Decimal("35.00")
    assert report.capital_source == "first_complete_account_snapshot"
    assert report.period_start == datetime(
        2026, 8, 25, tzinfo=timezone.utc
    )
    assert len(report.cycles) == 1


def test_previous_flat_snapshot_ignores_entry_snapshot() -> None:
    snapshots = [
        {
            "snapshot_stage": "startup_flat",
            "snapshot_captured_at": "2026-08-25T00:00:00+00:00",
        },
        {
            "snapshot_stage": "entry_confirmed",
            "snapshot_captured_at": "2026-08-25T01:00:00+00:00",
        },
        {
            "snapshot_stage": "exit_confirmed_flat",
            "snapshot_captured_at": "2026-08-25T02:00:00+00:00",
        },
    ]

    baseline = previous_flat_snapshot(snapshots, snapshots[-1])

    assert baseline is snapshots[0]


def test_latest_pnl_telegram_payload_uses_latest_confirmed_cycle() -> None:
    rows = [
        {
            "event": "live_inventory_run_config",
            "run_id": "run-1",
            "asset": "ETH",
            "logged_at": "2026-08-25T00:00:00+00:00",
        },
        {
            "event": "live_inventory_account_snapshot",
            "run_id": "run-1",
            "asset": "ETH",
            "snapshot_stage": "startup_flat",
            "snapshot_status": "complete",
            "combined_equity_usd": "35.00",
            "snapshot_captured_at": "2026-08-25T00:00:01+00:00",
            "logged_at": "2026-08-25T00:00:01+00:00",
        },
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.04",
            "logged_at": "2026-08-25T01:00:00+00:00",
        },
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 2,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.06",
            "logged_at": "2026-08-25T02:00:00+00:00",
        },
        {
            "event": "live_inventory_account_snapshot",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 2,
            "snapshot_stage": "exit_confirmed_flat",
            "snapshot_status": "complete",
            "account_snapshot_flat": True,
            "variational_equity_usd": "14.90",
            "lighter_equity_usd": "20.20",
            "combined_equity_usd": "35.10",
            "snapshot_captured_at": "2026-08-25T02:00:01+00:00",
            "logged_at": "2026-08-25T02:00:01+00:00",
        },
    ]
    report = build_report(rows, since=None, capital_usd=None)

    payload = latest_pnl_telegram_payload(rows, report, asset="ETH")

    assert payload is not None
    assert payload["lot_id"] == 2
    assert payload["summary_status"] == "complete"
    assert payload["cycle_actual_pnl_usd"] == "0.06"
    assert payload["run_actual_pnl_usd"] == "0.10"
    assert payload["account_net_change_usd"] == "0.10"
    assert payload["combined_equity_usd"] == "35.10"
    assert payload["completed_cycles"] == 2
    assert payload["return_pnl_source"] == "account_equity_delta"


def test_account_snapshot_logs_normalized_equity_without_raw_account(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(
            VariationalToLighterRuntime
        )
        runtime.mode = "live"
        runtime.live_inventory_dry_decisions = False
        runtime.live_inventory_run_id = "run-1"
        runtime.live_inventory_schema_version = "2"
        runtime.live_inventory_strategy_version = "basis-v4-live-test"
        runtime.live_inventory_strategy_variant = "test"
        runtime.live_inventory_config_hash = "hash"
        runtime.live_inventory_open_lots = []
        runtime.orders_file = tmp_path / "order_metrics.jsonl"
        runtime._order_write_lock = asyncio.Lock()
        runtime.telegram_notifier = None
        runtime.runtime = SimpleNamespace(
            monitor=SimpleNamespace(
                _lock=asyncio.Lock(),
                portfolio_summary={
                    "balance": "14.80",
                    "upnl": "0.05",
                    "published_at": "2026-08-25T00:00:00+00:00",
                },
            )
        )

        await runtime.capture_live_inventory_account_snapshot(
            stage="startup_flat",
            asset="ETH",
            lighter_account_result={
                "accounts": [
                    {
                        "collateral": "20.25",
                        "available_balance": "19.00",
                        "l1_address": "must-not-be-logged",
                    }
                ]
            },
        )

        row = json.loads(runtime.orders_file.read_text(encoding="utf-8"))
        assert row["snapshot_status"] == "complete"
        assert row["variational_equity_usd"] == "14.85"
        assert row["lighter_equity_usd"] == "20.25"
        assert row["combined_equity_usd"] == "35.10"
        assert "must-not-be-logged" not in json.dumps(row)

    asyncio.run(run())


def test_live_pnl_summary_uses_account_change_for_annualized_return(
    monkeypatch,
) -> None:
    monkeypatch.delenv("PNL_REPORT_CAPITAL_USD", raising=False)
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_last_actual_pnl_payload = {
        "lot_id": 3,
        "actual_pnl_status": "lighter_final_fill_confirmed",
        "actual_pnl_usd": "0.05",
    }
    runtime.live_inventory_realized_pnl_usd = Decimal("1.05")
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("1.00")
    runtime.live_inventory_completed_cycles = 1
    runtime.live_inventory_pnl_account_baseline_equity_usd = Decimal("35")
    runtime.live_inventory_pnl_account_baseline_at = (
        "2026-08-25T00:00:00+00:00"
    )

    payload = runtime.live_inventory_pnl_summary_payload(
        asset="ETH",
        lot_id=3,
        variational_equity_usd=Decimal("14.90"),
        lighter_equity_usd=Decimal("20.15"),
        combined_equity_usd=Decimal("35.05"),
        captured_at="2026-08-26T00:00:00+00:00",
    )

    assert payload["summary_status"] == "complete"
    assert payload["cycle_actual_pnl_usd"] == "0.05"
    assert payload["run_actual_pnl_usd"] == "0.05"
    assert payload["account_net_change_usd"] == "0.05"
    assert payload["capital_usd"] == "35"
    assert payload["return_pnl_source"] == "account_equity_delta"
    assert Decimal(payload["return_pct"]) > 0
    assert Decimal(payload["annualized_simple_pct"]) > 0
    assert payload["annualized_reliability"] == "sample_under_30_days"

    non_flat_payload = runtime.live_inventory_pnl_summary_payload(
        asset="ETH",
        lot_id=3,
        variational_equity_usd=Decimal("14.90"),
        lighter_equity_usd=Decimal("20.15"),
        combined_equity_usd=Decimal("35.05"),
        captured_at="2026-08-26T00:00:00+00:00",
        account_snapshot_flat=False,
    )

    assert non_flat_payload["summary_status"] == "partial"
    assert non_flat_payload["account_net_change_usd"] is None
    assert non_flat_payload["return_pnl_source"] == "confirmed_pair_fills"
