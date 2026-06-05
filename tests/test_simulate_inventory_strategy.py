import json
from decimal import Decimal
from pathlib import Path

from tools.simulate_inventory_strategy import read_samples, simulate_inventory


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


def test_inventory_simulator_layers_and_exits(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    rows = [
        _sample("t1", "100000", "99990", "100100", "100110"),
        _sample("t2", "100010", "100000", "100120", "100130"),
        _sample("t3", "100050", "100040", "100070", "100080"),
        _sample("t4", "100060", "100050", "100080", "100090"),
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    samples = read_samples(path, "BTC")
    result = simulate_inventory(
        samples,
        direction="long_var_short_lighter",
        lot_notional_usd=Decimal("100"),
        max_lots=2,
        entry_bps=Decimal("8"),
        exit_bps=Decimal("4"),
    )

    assert result.samples == 4
    assert result.entries == 2
    assert result.exits == 2
    assert result.forced_exits == 0
    assert result.max_open_lots == 2
    assert result.realized_pnl_usd > 0


def test_inventory_simulator_supports_short_var_long_lighter(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    rows = [
        _sample("t1", "100010", "100000", "99880", "99900"),
        _sample("t2", "100020", "100010", "99890", "99910"),
        _sample("t3", "99960", "99950", "99930", "99940"),
        _sample("t4", "99950", "99940", "99920", "99930"),
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    samples = read_samples(path, "BTC")
    result = simulate_inventory(
        samples,
        direction="short_var_long_lighter",
        lot_notional_usd=Decimal("100"),
        max_lots=2,
        entry_bps=Decimal("8"),
        exit_bps=Decimal("4"),
    )

    assert result.samples == 4
    assert result.entries == 2
    assert result.exits == 2
    assert result.forced_exits == 0
    assert result.max_open_lots == 2
    assert result.realized_pnl_usd > 0
