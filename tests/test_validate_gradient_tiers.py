from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tools.validate_gradient_tiers import (
    DIRECTION_LONG_VAR_SHORT_LIGHTER,
    DIRECTION_SHORT_VAR_LONG_LIGHTER,
    confirmed_crossing_indexes,
    evaluate_tier_holdout,
    parse_sample,
)


def test_parse_sample_selects_requested_direction() -> None:
    row = {
        "logged_at": "2026-09-01T00:00:00+00:00",
        "long_edge_bps": "4.5",
        "short_edge_bps": "-5.0",
    }

    assert parse_sample(
        row,
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
    )[1] == Decimal("4.5")
    assert parse_sample(
        row,
        direction=DIRECTION_SHORT_VAR_LONG_LIGHTER,
    )[1] == Decimal("-5.0")


def test_confirmed_crossings_require_latest_and_two_of_three() -> None:
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    edges = ["0", "2", "0", "2", "2", "2", "0", "2", "2"]
    samples = [
        (start + timedelta(seconds=index), Decimal(edge))
        for index, edge in enumerate(edges)
    ]

    assert confirmed_crossing_indexes(
        samples,
        threshold=Decimal("1"),
    ) == [3, 7]


def test_directional_holdout_reports_confirmed_opportunities() -> None:
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    samples = [
        (start + timedelta(seconds=index * 30), Decimal(index % 20))
        for index in range(200)
    ]

    result = evaluate_tier_holdout(
        samples,
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
    )

    assert result["direction"] == DIRECTION_LONG_VAR_SHORT_LIGHTER
    assert result["train_samples"] == 140
    assert result["holdout_samples"] == 60
    assert all("holdout_crossings" in tier for tier in result["tiers"])
