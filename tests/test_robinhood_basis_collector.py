import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from tools.robinhood_basis_collector import (
    CurrentDayJsonlFollower,
    RobinhoodBasisCollector,
    parse_args,
)


def test_follower_waits_for_complete_json_lines(tmp_path) -> None:
    follower = CurrentDayJsonlFollower(tmp_path, "ETH")
    path = follower.current_path()
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"event":"first"}\n{"event":"second"')

    assert follower.read_new() == [{"event": "first"}]

    with path.open("ab") as handle:
        handle.write(b"}\n")

    assert follower.read_new() == [{"event": "second"}]


def test_follower_seek_to_end_only_emits_new_rows(tmp_path) -> None:
    follower = CurrentDayJsonlFollower(tmp_path, "ETH")
    path = follower.current_path()
    path.parent.mkdir(parents=True)
    path.write_text('{"event":"old"}\n', encoding="utf-8")
    follower.seek_to_end()

    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"event":"new"}\n')

    assert follower.read_new() == [{"event": "new"}]


def test_collector_builds_depth_ladder_without_credentials(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(
            [
                "--source-root",
                str(tmp_path / "source"),
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )
        collector = RobinhoodBasisCollector(args)
        collector.books.by_asset["ETH"] = SimpleNamespace(market_id=0)

        async def fake_snapshot(_asset, notional):
            price_offset = notional / Decimal("10000")
            return {
                "bid": Decimal("1999.9"),
                "ask": Decimal("2000.1"),
                "sell_price": Decimal("1999.9") - price_offset,
                "buy_price": Decimal("2000.1") + price_offset,
                "nonce": 10,
                "book_age_seconds": 0.1,
                "continuity_ok": True,
                "cold": False,
                "sequence_gaps": 0,
            }

        collector.books.snapshot = fake_snapshot
        now = datetime(2026, 8, 25, 0, 0, 1, tzinfo=timezone.utc)
        source = {
            "event": "live_inventory_basis_state",
            "sample_id": "source-1",
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "logged_at": "2026-08-25T00:00:00+00:00",
            "asset": "ETH",
            "run_id": "live-run",
            "sample_index": 1,
            "var_bid": "2001",
            "var_ask": "2002",
            "normalized_var_bid": "2001.2",
            "normalized_var_ask": "2002.2",
        }

        row, error = await collector.build_row(source, now=now)

        assert error is None
        assert row is not None
        assert row["event"] == "robinhood_lighter_basis_state"
        assert row["robinhood_lighter_market_id"] == 0
        assert row["source_sample_id"] == "source-1"
        assert row["private_credentials_loaded"] is False
        assert [item["notional_usd"] for item in row["depth_ladder"]] == [
            "20",
            "40",
            "60",
        ]
        assert Decimal(row["short_edge_bps"]) > 0

    asyncio.run(run())


def test_collector_rejects_stale_source_sample(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(
            [
                "--source-root",
                str(tmp_path / "source"),
                "--output-dir",
                str(tmp_path / "output"),
                "--max-source-age-seconds",
                "30",
            ]
        )
        collector = RobinhoodBasisCollector(args)
        source = {
            "logged_at": "2026-08-25T00:00:00+00:00",
            "var_bid": "2000",
            "var_ask": "2001",
        }

        row, error = await collector.build_row(
            source,
            now=datetime(2026, 8, 25, 0, 1, 0, tzinfo=timezone.utc),
        )

        assert row is None
        assert error == "source_sample_stale"

    asyncio.run(run())


def test_health_file_is_collect_only_and_separate(tmp_path) -> None:
    args = parse_args(
        [
            "--source-root",
            str(tmp_path / "source"),
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )
    collector = RobinhoodBasisCollector(args)
    collector._write_health()

    health = json.loads(collector.health_path.read_text(encoding="utf-8"))
    assert health["execution_mode"] == "collect_only"
    assert health["venue"] == "robinhood_chain_lighter"
    assert collector.health_path.name == "robinhood_basis_health.json"
