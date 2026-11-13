from argparse import Namespace
from pathlib import Path

import pytest

from tools.market_capture.config import CaptureConfig, ConfigError, DEFAULT_ASSETS, load_config
from tools.market_capture.safety import assert_no_order_submission_source


def args(**overrides):
    base = dict(
        enable_capture=True, assets="OPENAI,ANTHROPIC,NVDA,SNDK,CRCL,AAPL,QQQ,DRAM,COIN", output_dir="",
        notional_usd=20, book_snapshot_seconds=5, health_seconds=5,
        latency_seconds=5, system_seconds=60, fill_poll_seconds=2,
        edge_seconds=1, var_quote_seconds=0, retention_days=14,
        disk_floor_gb=2, max_book_levels=10, duration_seconds=0,
        no_variational=False, no_lighter_mainnet=False, no_lighter_rh=False,
        no_entropy=False,
    )
    base.update(overrides)
    return Namespace(**base)


def test_default_assets_cover_five_probe_instruments():
    assert [a.name for a in DEFAULT_ASSETS[:5]] == [
        "NVDA", "SNDK", "CRCL", "AAPL", "QQQ",
    ]
    assert DEFAULT_ASSETS[0].symbol("entropy") == "io:NVDA"
    assert DEFAULT_ASSETS[1].symbol("entropy") == "io:SNDK"
    assert DEFAULT_ASSETS[2].symbol("entropy") == "io:CRCL"
    assert DEFAULT_ASSETS[3].symbol("entropy") == "io:AAPL"
    assert DEFAULT_ASSETS[4].supports("entropy") is False
    assert all(DEFAULT_ASSETS[i].supports("variational") for i in range(5))
    assert all(DEFAULT_ASSETS[i].supports("lighter_mainnet") for i in range(5))


def test_config_refuses_order_submission_flag():
    with pytest.raises(ConfigError, match="order_submission"):
        load_config(args(), env={"MARKET_CAPTURE_ENABLED": "true", "ALLOW_ORDER_SUBMISSION": "true"})


def test_capture_requires_explicit_enable():
    with pytest.raises(ConfigError, match="disabled"):
        load_config(args(enable_capture=False), env={})


def test_public_config_never_contains_private_keys():
    config = load_config(args(), env={
        "MARKET_CAPTURE_ENABLED": "true",
        "LIGHTER_ACCOUNT_INDEX": "7",
        "LIGHTER_API_KEY_INDEX": "3",
        "LIGHTER_PRIVATE_KEY": "super-secret-mainnet",
        "LIGHTER_RH_ACCOUNT_INDEX": "33584",
        "LIGHTER_RH_API_KEY_INDEX": "4",
        "LIGHTER_RH_API_PRIVATE_KEY": "super-secret-rh",
        "ENTROPY_ACCOUNT_ADDRESS": "0x" + "1" * 40,
    })
    public = config.public_dict()
    text = repr(public)
    assert "super-secret" not in text
    assert public["credentials_present"] == {
        "lighter_mainnet": True, "lighter_rh": True, "entropy": True,
    }
    assert public["orders_attempted"] is False
    assert public["order_submission_enabled"] is False


def test_capture_source_has_no_order_calls():
    assert assert_no_order_submission_source() == []
