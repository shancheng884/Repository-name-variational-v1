import asyncio
import json
from collections import deque
from decimal import Decimal

import pytest

from tools.basis_collector import MultiAssetCollector, MultiLighterBooks, parse_args
from tools.archive_legacy_logs import archive_file
from tools.lib.basis_store import BasisSampleStore, read_basis_samples


def test_basis_store_rotates_closed_days_without_losing_rows(tmp_path) -> None:
    store = BasisSampleStore(tmp_path, config_hash="config", commit="commit")
    store.append(
        {
            "event": "live_inventory_basis_state",
            "logged_at": "2026-07-15T23:59:59+00:00",
            "asset": "SOL",
            "sample_kind": "baseline",
        }
    )
    store.write_manifests()

    compressed = store.rotate_closed_days(current_day="2026-07-16")

    assert compressed == [tmp_path / "SOL" / "2026-07-15.jsonl.gz"]
    assert not (tmp_path / "SOL" / "2026-07-15.jsonl").exists()
    rows = read_basis_samples(tmp_path, limit=10, asset_filter="SOL")
    assert len(rows) == 1
    assert rows[0]["sample_kind"] == "baseline"
    manifest = json.loads((tmp_path / "SOL" / "2026-07-15.manifest.json").read_text(encoding="utf-8"))
    assert manifest["rows_this_process"] == 1


def test_basis_store_can_read_only_valid_baseline_rows(tmp_path) -> None:
    store = BasisSampleStore(tmp_path, config_hash="config", commit="commit")
    for sample_kind, sample_quality in (
        ("baseline", "valid"),
        ("burst", "valid"),
        ("baseline", "degraded"),
        (None, None),
    ):
        row = {
            "event": "live_inventory_basis_state",
            "logged_at": "2026-07-24T00:00:00+00:00",
            "asset": "ETH",
        }
        if sample_kind is not None:
            row["sample_kind"] = sample_kind
        if sample_quality is not None:
            row["sample_quality"] = sample_quality
        store.append(row)

    rows = read_basis_samples(
        tmp_path,
        limit=10,
        asset_filter="ETH",
        sample_kind_filter="baseline",
        sample_quality_filter="valid",
    )

    assert len(rows) == 1
    assert rows[0]["sample_kind"] == "baseline"
    assert rows[0]["sample_quality"] == "valid"


def test_legacy_archive_preserves_content_and_reopens_active_file(tmp_path) -> None:
    source = tmp_path / "order_metrics.jsonl"
    source.write_text('{"event":"old"}\n', encoding="utf-8")

    target = archive_file(source, "20260716T000000Z")

    assert source.exists() and source.stat().st_size == 0
    assert read_basis_samples(tmp_path, limit=10) == []
    import gzip

    with gzip.open(target, "rt", encoding="utf-8") as handle:
        assert handle.read() == '{"event":"old"}\n'


def test_multi_asset_parser_is_hard_isolated_to_multiple_supported_assets() -> None:
    args = parse_args(["--assets", "SOL,BTC,ETH"])
    assert args.assets == ("SOL", "BTC", "ETH")

    with pytest.raises(SystemExit):
        parse_args(["--assets", "SOL"])


def test_lighter_depth_fill_uses_executable_levels() -> None:
    levels = {Decimal("100"): Decimal("0.1"), Decimal("99"): Decimal("1")}
    fill = MultiLighterBooks._fill(levels, side="SELL", quote_notional=Decimal("20"))
    assert fill is not None
    assert Decimal("99") < fill < Decimal("100")


def test_burst_rows_do_not_enter_baseline_history(tmp_path) -> None:
    args = parse_args(["--assets", "SOL,BTC", "--output-dir", str(tmp_path)])
    collector = MultiAssetCollector(args)
    collector.histories["SOL"]["long"] = deque([Decimal("1")] * 60, maxlen=8640)
    collector.histories["SOL"]["short"] = deque([Decimal("1")] * 60, maxlen=8640)
    collector.prebuffers["SOL"].append(
        {
            "event": "live_inventory_basis_state",
            "logged_at": "2026-07-16T00:00:00+00:00",
            "sample_id": "pre",
            "sample_quality": "valid",
            "asset": "SOL",
            "long_edge_bps": "1",
            "short_edge_bps": "1",
        }
    )
    row = {
        "event": "live_inventory_basis_state",
        "logged_at": "2026-07-16T00:00:01+00:00",
        "sample_id": "trigger",
        "sample_quality": "valid",
        "asset": "SOL",
        "long_edge_bps": "2",
        "short_edge_bps": "1",
        "basis_sample_move_bps": "0",
    }

    collector._process_row("SOL", row, 100.0)

    assert len(collector.histories["SOL"]["long"]) == 61
    rows = read_basis_samples(tmp_path / "basis_samples", limit=10, asset_filter="SOL")
    assert [item["sample_kind"] for item in rows] == ["burst", "baseline"]


def test_collector_builds_valid_zero_fee_executable_row(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(["--assets", "SOL,BTC", "--output-dir", str(tmp_path)])
        collector = MultiAssetCollector(args)
        snapshots = [
            {
                "bid": Decimal("100.10"),
                "ask": Decimal("100.20"),
                "sell_price": Decimal("100.10"),
                "buy_price": Decimal("100.20"),
                "nonce": 10,
                "server_timestamp_ms": int(__import__("time").time() * 1000),
                "book_age_seconds": 0.1,
                "continuity_ok": True,
                "cold": False,
                "sequence_gaps": 0,
            },
            {
                "bid": Decimal("100.10"),
                "ask": Decimal("100.20"),
                "sell_price": Decimal("100.10"),
                "buy_price": Decimal("100.20"),
                "nonce": 11,
                "server_timestamp_ms": int(__import__("time").time() * 1000),
                "book_age_seconds": 0.1,
                "continuity_ok": True,
                "cold": False,
                "sequence_gaps": 0,
            },
        ]

        async def fake_snapshot(_asset, _notional):
            return snapshots.pop(0)

        async def fake_quote(_asset, _amount):
            return {
                "ok": True,
                "result": {
                    "bid": "100.00",
                    "ask": "100.05",
                    "quoteId": "quote",
                    "quoteTimestamp": __import__("datetime").datetime.now(
                        __import__("datetime").timezone.utc
                    ).isoformat(),
                },
            }, 25.0

        async def fake_stablecoin():
            return {"stablecoin_rate_status": "ok", "usdcusdt_price": "1"}

        collector.books.snapshot = fake_snapshot
        collector.quote_client.quote = fake_quote
        collector._stablecoin = fake_stablecoin
        row, error = await collector._build_row("SOL")

        assert error is None
        assert row is not None
        assert row["sample_quality"] == "valid"
        assert row["fee_bps_per_leg"] == "0"
        assert row["lighter_sell_price"] == "100.10"
        assert row["lighter_buy_price"] == "100.20"
        assert row["asset"] == "SOL"

    asyncio.run(run())


def test_collector_normalizes_unbounded_error_responses() -> None:
    html = "<!doctype html><html><style>" + ("x" * 10000) + "</style></html>"

    assert MultiAssetCollector._normalize_error(html) == "variational_html_response"
    assert MultiAssetCollector._normalize_error('{"error":"' + ("x" * 10000)) == (
        "variational_structured_error_response"
    )
    assert MultiAssetCollector._normalize_error("HTTP 503") == "HTTP 503"
    assert len(MultiAssetCollector._normalize_error("error " + ("x" * 10000))) == 160
