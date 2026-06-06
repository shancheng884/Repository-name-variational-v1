import json
from decimal import Decimal
from pathlib import Path

from tools.analyze_live_inventory_edges import latest_watch_signal, load_samples, summarize_edges


def _sample(ts: str, var_buy: str, var_sell: str, lighter_bid: str, lighter_ask: str) -> dict:
    long_edge = (Decimal(lighter_bid) - Decimal(var_buy)) / Decimal(var_buy) * Decimal("10000")
    short_edge = (Decimal(var_sell) - Decimal(lighter_ask)) / Decimal(var_sell) * Decimal("10000")
    return {
        "event": "market_sample",
        "logged_at": ts,
        "asset": "BTC",
        "var_buy_price": var_buy,
        "var_sell_price": var_sell,
        "lighter_bid": lighter_bid,
        "lighter_ask": lighter_ask,
        "long_var_short_lighter_bps": str(long_edge),
        "short_var_long_lighter_bps": str(short_edge),
    }


def test_live_inventory_edge_summary_counts_thresholds(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    rows = [
        _sample("t1", "60000", "59990", "60180", "60200"),
        _sample("t2", "60000", "59990", "60250", "60270"),
        _sample("t3", "60000", "59990", "60100", "60120"),
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    samples = load_samples(path, asset="BTC")
    summary = summarize_edges(
        samples,
        file=path,
        asset="BTC",
        lot_notional_usd=Decimal("15"),
        lighter_min_base_amount=Decimal("0.00020"),
        lighter_min_quote_amount=Decimal("10"),
        thresholds=(Decimal("30"), Decimal("40")),
    )

    long_row = summary.by_direction["long_var_short_lighter"]

    assert summary.samples == 3
    assert long_row.threshold_counts[Decimal("30")] == 2
    assert long_row.threshold_counts[Decimal("40")] == 1
    assert long_row.executable_count == 3
    assert long_row.executable_threshold_counts[Decimal("30")] == 2
    assert long_row.latest == Decimal("16.66666666666666666666666667")
    assert long_row.max_edge == Decimal("41.66666666666666666666666667")


def test_live_inventory_edge_summary_applies_min_base(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    path.write_text(json.dumps(_sample("t1", "60000", "59990", "60350", "60370")) + "\n", encoding="utf-8")
    samples = load_samples(path, asset="BTC")

    too_small = summarize_edges(
        samples,
        file=path,
        asset="BTC",
        lot_notional_usd=Decimal("10"),
        lighter_min_base_amount=Decimal("0.00020"),
        lighter_min_quote_amount=Decimal("10"),
        thresholds=(Decimal("50"),),
    )
    large_enough = summarize_edges(
        samples,
        file=path,
        asset="BTC",
        lot_notional_usd=Decimal("15"),
        lighter_min_base_amount=Decimal("0.00020"),
        lighter_min_quote_amount=Decimal("10"),
        thresholds=(Decimal("50"),),
    )

    assert too_small.by_direction["long_var_short_lighter"].threshold_counts[Decimal("50")] == 1
    assert too_small.by_direction["long_var_short_lighter"].executable_count == 0
    assert too_small.by_direction["long_var_short_lighter"].executable_threshold_counts[Decimal("50")] == 0
    assert large_enough.by_direction["long_var_short_lighter"].executable_count == 1
    assert large_enough.by_direction["long_var_short_lighter"].executable_threshold_counts[Decimal("50")] == 1


def test_live_inventory_watch_signal_alerts_on_latest_executable_edge(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    rows = [
        _sample("t1", "60000", "59990", "60100", "60120"),
        _sample("t2", "60000", "59990", "60350", "60370"),
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    samples = load_samples(path, asset="BTC")

    signal = latest_watch_signal(
        samples,
        threshold_bps=Decimal("40"),
        lot_notional_usd=Decimal("15"),
        lighter_min_base_amount=Decimal("0.00020"),
        lighter_min_quote_amount=Decimal("10"),
    )

    assert signal.triggered is True
    assert signal.direction == "long_var_short_lighter"
    assert signal.edge_bps == Decimal("58.33333333333333333333333333")
    assert signal.executable is True
    assert signal.logged_at == "t2"


def test_live_inventory_watch_signal_waits_when_latest_edge_is_not_executable(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    path.write_text(json.dumps(_sample("t1", "60000", "59990", "60350", "60370")) + "\n", encoding="utf-8")
    samples = load_samples(path, asset="BTC")

    signal = latest_watch_signal(
        samples,
        threshold_bps=Decimal("40"),
        lot_notional_usd=Decimal("10"),
        lighter_min_base_amount=Decimal("0.00020"),
        lighter_min_quote_amount=Decimal("10"),
    )

    assert signal.triggered is False
    assert signal.direction == "long_var_short_lighter"
    assert signal.edge_bps == Decimal("58.33333333333333333333333333")
    assert signal.executable is False
