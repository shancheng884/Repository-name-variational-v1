import json
from decimal import Decimal
from pathlib import Path

from tools.analyze_fresh_quote_edges import build_result, latest_lighter_snapshot, percentile


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_latest_lighter_snapshot_uses_latest_valid_asset_row(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    _write_jsonl(
        path,
        [
            {"event": "market_sample", "logged_at": "old", "asset": "BTC", "lighter_bid": "99", "lighter_ask": "100"},
            {"event": "market_sample", "logged_at": "eth", "asset": "ETH", "lighter_bid": "1", "lighter_ask": "2"},
            {
                "event": "market_sample",
                "logged_at": "new",
                "asset": "BTC",
                "lighter_bid": "101",
                "lighter_ask": "102",
                "lighter_buy_fill_price": "102.5",
                "lighter_sell_fill_price": "100.5",
            },
        ],
    )

    snapshot = latest_lighter_snapshot(path, asset="BTC", latest=10)

    assert snapshot is not None
    assert snapshot.logged_at == "new"
    assert snapshot.bid == Decimal("101")
    assert snapshot.ask == Decimal("102")
    assert snapshot.buy_fill_price == Decimal("102.5")
    assert snapshot.sell_fill_price == Decimal("100.5")


def test_build_result_computes_fresh_edges_and_stats(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    _write_jsonl(
        path,
        [
            {
                "event": "market_sample",
                "logged_at": "now",
                "asset": "BTC",
                "lighter_bid": "101",
                "lighter_ask": "102",
                "lighter_buy_fill_price": "102",
                "lighter_sell_fill_price": "101",
            },
        ],
    )
    snapshot = latest_lighter_snapshot(path, asset="BTC", latest=10)
    samples: list[dict[str, Decimal]] = []

    result = build_result(
        snapshot,
        {"ok": True, "result": {"quoteId": "q1", "bid": "100", "ask": "100.5", "quoteTimestamp": "ts"}},
        quote_ms=Decimal("12.5"),
        samples=samples,
    )

    assert result["fresh_quote_ok"] is True
    assert result["fresh_quote_id"] == "q1"
    assert result["best_direction"] == "long_var_short_lighter"
    assert Decimal(result["long_var_short_lighter_fresh_bps"]) > Decimal("0")
    assert Decimal(result["short_var_long_lighter_fresh_bps"]) < Decimal("0")
    assert result["sample_count"] == 1
    assert result["best_fresh_edge_median_bps"] == result["best_fresh_edge_bps"]


def test_percentile_returns_nearest_rank() -> None:
    assert percentile([Decimal("1"), Decimal("2"), Decimal("10")], 0.9) == Decimal("10")
