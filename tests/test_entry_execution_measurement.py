import asyncio
import json
from decimal import Decimal

import pytest

from main import VariationalToLighterRuntime, utc_now


class _Logger:
    def __init__(self):
        self.warnings = []

    def warning(self, msg, *args, **kwargs):
        self.warnings.append((msg, kwargs.get("exc_info")))

    def info(self, *args, **kwargs):
        pass


def _runtime(tmp_path):
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.logger = _Logger()
    runtime.execution_measurements_file = (
        (tmp_path / "execution_measurements.jsonl") if tmp_path is not None else None
    )
    runtime.live_inventory_asset = "ETH"
    return runtime


def test_measurement_records_adverse_slippage_per_leg(tmp_path):
    runtime = _runtime(tmp_path)
    runtime.record_entry_execution_measurement(
        lot={
            "lot_id": "L1",
            "direction": "long_var_short_lighter",
            "qty": "0.2",
            "entry_basis_bps": "-18.5",
            "entry_v4_threshold_bps": "16.0",
            "entry_var_submit_ms": 120,
            "entry_lighter_submit_ms": 95,
            "entry_gradient_tier": 1,
        },
        context={
            "var_price": "2130.00",
            "lighter_price": "2133.00",
            "var_side": "buy",
            "quote_timestamp": utc_now(),
        },
        var_fill_price=Decimal("2130.50"),
        lighter_fill_price=Decimal("2132.90"),
    )
    rows = [
        json.loads(line)
        for line in runtime.execution_measurements_file.read_text().splitlines()
    ]
    assert len(rows) == 1
    row = rows[0]
    # Var 买贵 0.5/2130 = +2.35 bps，不利
    assert float(row["var_slippage_bps"]) == pytest.approx(2.35, abs=0.05)
    # 卖出腿成交价低于计划价 = 不利，按方向记号应为正
    assert float(row["lighter_slippage_bps"]) == pytest.approx(0.47, abs=0.05)
    assert row["record_kind"] == "entry_execution_measurement"
    assert row["quote_age_ms"] is not None


def test_measurement_never_raises_on_bad_input(tmp_path):
    runtime = _runtime(tmp_path)
    runtime.record_entry_execution_measurement(
        lot={},
        context={"var_price": "not-a-number"},
        var_fill_price=None,
        lighter_fill_price=None,
    )
    rows = [
        json.loads(line)
        for line in runtime.execution_measurements_file.read_text().splitlines()
    ]
    assert rows and rows[0]["var_slippage_bps"] is None


def test_measurement_skips_when_no_output_dir():
    runtime = _runtime(None)
    runtime.record_entry_execution_measurement(
        lot={}, context={}, var_fill_price=None, lighter_fill_price=None
    )
