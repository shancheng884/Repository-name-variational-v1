import json
from decimal import Decimal
from pathlib import Path

from tools.analyze_var_quote_sources import analyze_candidate, latest_candidate


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_latest_candidate_picks_highest_threshold_direction(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    _write_jsonl(
        path,
        [
            {
                "event": "market_sample",
                "logged_at": "2026-06-16T00:00:00Z",
                "asset": "BTC",
                "var_buy_price": "100",
                "var_sell_price": "99",
                "lighter_bid": "100.05",
                "lighter_ask": "100.2",
                "long_var_short_lighter_bps": "5",
                "short_var_long_lighter_bps": "-120",
            },
            {
                "event": "market_sample",
                "logged_at": "2026-06-16T00:00:01Z",
                "asset": "BTC",
                "var_buy_price": "100",
                "var_sell_price": "99",
                "lighter_bid": "100.2",
                "lighter_ask": "98.7",
                "long_var_short_lighter_bps": "20",
                "short_var_long_lighter_bps": "30",
                "var_timestamp": "2026-06-16T00:00:01Z",
                "var_source_url": "wss://example/prices",
                "var_source_stream": "/prices",
            },
        ],
    )

    candidate = latest_candidate(path, asset="BTC", threshold_bps=Decimal("10"), lot_notional_usd=Decimal("20"), latest=10)

    assert candidate is not None
    assert candidate.direction == "short_var_long_lighter"
    assert candidate.snapshot_edge_bps == Decimal("30")
    assert candidate.snapshot_var_price == Decimal("99")
    assert candidate.lighter_price == Decimal("98.7")
    assert candidate.snapshot_var_timestamp == "2026-06-16T00:00:01Z"


def test_analyze_candidate_computes_fresh_edge_loss(tmp_path: Path) -> None:
    path = tmp_path / "market_samples.jsonl"
    _write_jsonl(
        path,
        [
            {
                "event": "market_sample",
                "logged_at": "2026-06-16T00:00:01Z",
                "asset": "BTC",
                "var_buy_price": "100",
                "var_sell_price": "99",
                "lighter_bid": "101",
                "lighter_ask": "102",
                "long_var_short_lighter_bps": "100",
                "short_var_long_lighter_bps": "-303.030303",
            },
        ],
    )
    candidate = latest_candidate(path, asset="BTC", threshold_bps=Decimal("10"), lot_notional_usd=Decimal("20"), latest=10)

    result = analyze_candidate(
        candidate,
        {"ok": True, "result": {"quoteId": "q1", "bid": "100.5", "ask": "100.8", "quoteTimestamp": "now"}},
        quote_ms=Decimal("12.3"),
    )

    assert result["fresh_quote_id"] == "q1"
    assert result["fresh_var_price"] == "100.8"
    assert Decimal(result["fresh_edge_bps"]) < Decimal("100")
    assert Decimal(result["edge_loss_bps"]) > Decimal("0")
    assert result["fresh_quote_ms"] == "12.3"
