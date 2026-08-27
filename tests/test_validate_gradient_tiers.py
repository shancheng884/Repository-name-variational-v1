from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tools.validate_gradient_tiers import evaluate_tier_holdout


def test_gradient_tier_validation_uses_chronological_holdout() -> None:
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    samples = []
    for index in range(200):
        edge = Decimal(index % 20)
        samples.append((started + timedelta(minutes=index), edge))

    result = evaluate_tier_holdout(samples)

    assert result["train_samples"] == 140
    assert result["holdout_samples"] == 60
    assert len(result["tiers"]) == 5
    assert all("holdout_crossings" in tier for tier in result["tiers"])
