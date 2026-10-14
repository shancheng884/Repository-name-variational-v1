from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tools.lib.mainnet_anchor_follow import SourceFollower, paired_row


def source(now):
    return {"asset": "ETH", "logged_at": now.isoformat(), "sample_kind": "baseline",
            "sample_quality": "valid", "sample_pair_valid": True,
            "quote_size_mode": "exact_base_qty_v1", "quote_request_qty": "0.008",
            "quote_source": "passive_browser_stream", "var_quote_age_seconds": "0.1",
            "var_bid": "2500", "var_ask": "2500", "quote_received_at": now.isoformat(),
            "reference_price": "2500", "long_edge_bps": "999", "lighter_buy_price": "1"}


def test_pair_uses_mainnet_depth_and_source_exact_base_qty():
    now = datetime.now(timezone.utc)
    row, reason = paired_row(source(now),
        bids={Decimal("2502"): Decimal("0.004"), Decimal("2501"): Decimal("1")},
        asks={Decimal("2503"): Decimal("1")}, book_age=0.1, now=now)
    assert reason is None
    assert row["lighter_sell_price"] == "2501.5"
    assert row["long_edge_bps"] == "6.0000"
    assert row["venue"] == "mainnet_lighter"
    assert row["quote_request_qty"] == "0.008"


def test_pair_rejects_old_source_missing_quantity_and_depth():
    now = datetime.now(timezone.utc)
    bids, asks = {Decimal("2500"): Decimal("1")}, {Decimal("2501"): Decimal("1")}
    for changes, expected in (({"logged_at": (now-timedelta(seconds=2)).isoformat()}, "source_stale"),
                              ({"quote_request_qty": None}, "source_numbers"),
                              ({"sample_pair_valid": False}, "source_quality")):
        row, reason = paired_row({**source(now), **changes}, bids=bids, asks=asks, book_age=0.1, now=now)
        assert row is None and reason == expected
    assert paired_row(source(now), bids=bids, asks={}, book_age=0.1, now=now)[1] == "mainnet_depth_insufficient"


def test_follower_waits_for_new_complete_lines_and_follows_new_day(tmp_path):
    root = tmp_path / "ETH"
    root.mkdir()
    old = root / "2026-10-14.jsonl"
    old.write_text('{"old": true}\n', encoding="utf-8")
    follower = SourceFollower(tmp_path)
    assert follower.read_new() == []
    with old.open("a", encoding="utf-8") as f:
        f.write('{"new":')
    assert follower.read_new() == []
    with old.open("a", encoding="utf-8") as f:
        f.write('true}\n')
    assert follower.read_new() == [{"new": True}]
    (root / "2026-10-15.jsonl").write_text('{"next": true}\n', encoding="utf-8")
    assert follower.read_new() == [{"next": True}]
    follower.close()
