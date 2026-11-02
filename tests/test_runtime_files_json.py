import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from tools.lib.runtime_files import read_json, write_json_atomic


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_decimal_keeps_full_precision(tmp_path: Path) -> None:
    target = tmp_path / "health.json"
    write_json_atomic(target, {"equity": Decimal("273.1624539999999736")})

    assert _read(target)["equity"] == "273.1624539999999736"


def test_decimal_exponent_form_renders_plain_number(tmp_path: Path) -> None:
    target = tmp_path / "health.json"
    write_json_atomic(target, {"amount": Decimal("1E+2")})

    assert _read(target)["amount"] == "100"


def test_non_finite_decimal_renders_text(tmp_path: Path) -> None:
    target = tmp_path / "health.json"
    write_json_atomic(target, {"value": Decimal("NaN")})

    assert _read(target)["value"] == "NaN"


def test_timestamps_render_isoformat(tmp_path: Path) -> None:
    target = tmp_path / "health.json"
    write_json_atomic(
        target,
        {
            "observed_at": datetime(2026, 11, 2, 3, 0, tzinfo=timezone.utc),
            "trading_day": date(2026, 11, 2),
        },
    )
    data = _read(target)

    assert data["observed_at"] == "2026-11-02T03:00:00+00:00"
    assert data["trading_day"] == "2026-11-02"


def test_path_tuple_and_set_are_json_safe(tmp_path: Path) -> None:
    target = tmp_path / "health.json"
    write_json_atomic(
        target,
        {
            "log_file": tmp_path / "runtime.log",
            "ordered": (1, 2),
            "directions": {"short_var_long_lighter", "long_var_short_lighter"},
        },
    )
    data = _read(target)

    assert data["log_file"].endswith("runtime.log")
    assert data["ordered"] == [1, 2]
    assert data["directions"] == ["long_var_short_lighter", "short_var_long_lighter"]


def test_nested_payload_with_decimal_is_written(tmp_path: Path) -> None:
    target = tmp_path / "health.json"
    write_json_atomic(
        target,
        {"legs": [{"fill": Decimal("2290.17")}], "summary": {"pnl": Decimal("-0.023226")}},
    )
    data = _read(target)

    assert data["legs"][0]["fill"] == "2290.17"
    assert data["summary"]["pnl"] == "-0.023226"


def test_risk_health_payload_accepts_decimal_portfolio_refresh_equity(
    tmp_path: Path,
) -> None:
    target = tmp_path / "live_inventory_risk_health.json"
    write_json_atomic(
        target,
        {
            "schema_version": 1,
            "updated_at": "2026-11-02T03:00:00+00:00",
            "risk_action": "normal",
            "risk_reason": "account_risk_normal",
            "variational_portfolio_refresh_ok": True,
            "variational_portfolio_refresh_equity_usd": Decimal("273.1624539999999736"),
        },
    )
    data = _read(target)

    assert data["variational_portfolio_refresh_ok"] is True
    assert data["variational_portfolio_refresh_equity_usd"] == "273.1624539999999736"


def test_unsupported_object_still_raises(tmp_path: Path) -> None:
    target = tmp_path / "health.json"
    with pytest.raises(TypeError):
        write_json_atomic(target, {"bad": object()})


def test_failed_write_keeps_previous_content_and_leaves_no_temp_file(
    tmp_path: Path,
) -> None:
    target = tmp_path / "health.json"
    write_json_atomic(target, {"status": "running"})

    with pytest.raises(TypeError):
        write_json_atomic(target, {"status": object()})

    assert read_json(target) == {"status": "running"}
    assert not (tmp_path / "health.json.tmp").exists()