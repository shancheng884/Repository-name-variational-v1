from datetime import date

from tools.replay_capture import (
    date_range, local_dir, partition_specs, remote_dir, rclone_command,
    summary_sql,
)


def test_partition_specs_for_quotes_and_edges():
    assert partition_specs("quotes", "NVDA", None)[0] == ("variational", "quotes/variational")
    assert ("entropy", "quotes/entropy") in partition_specs("quotes", "NVDA", None)
    assert partition_specs("edges", "NVDA", None) == [("cross", "edges/cross")]
    assert partition_specs("quotes", "NVDA", "lighter_mainnet") == [
        ("lighter_mainnet", "quotes/lighter_mainnet")
    ]


def test_remote_and_local_paths(tmp_path):
    env = {"R2_REMOTE": "r2", "R2_BUCKET": "bucket", "R2_PREFIX": "market_capture"}
    assert remote_dir(env, "quotes/lighter_mainnet", "NVDA") == (
        "r2:bucket/market_capture/quotes/lighter_mainnet/NVDA"
    )
    assert local_dir(tmp_path, date(2026, 11, 13), "quotes/lighter_mainnet", "NVDA") == (
        tmp_path / "2026-11-13" / "quotes/lighter_mainnet" / "NVDA"
    )


def test_rclone_command_is_date_scoped_and_low_impact(tmp_path):
    command = rclone_command(
        "r2:bucket/market_capture/quotes/lighter_mainnet/NVDA",
        tmp_path,
        date(2026, 11, 13),
        low_priority=False,
        bwlimit="20M",
    )
    assert command[0:2] == ["rclone", "copy"]
    assert "--include" in command
    assert command[command.index("--include") + 1] == "20261113-*.jsonl.gz"
    assert command[command.index("--bwlimit") + 1] == "20M"


def test_date_range_and_summary_sql():
    assert [d.isoformat() for d in date_range("2026-11-12", "2026-11-13")] == [
        "2026-11-12", "2026-11-13",
    ]
    sql = summary_sql("edges", "/tmp/x/*.jsonl.gz")
    assert "union_by_name=true" in sql
    assert "quantile_cont" in sql
