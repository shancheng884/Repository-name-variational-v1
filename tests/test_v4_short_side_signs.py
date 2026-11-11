from decimal import Decimal

import main as app

LONG = "long_var_short_lighter"
SHORT = "short_var_long_lighter"


def test_short_side_tier_counts_more_negative_edges() -> None:
    thresholds = [Decimal("-10.0"), Decimal("-12.0")]
    # -13 is past both short-side bars, -9 past neither.
    assert app.v4_real_gradient_eligible_tier(Decimal("-13"), thresholds, direction=SHORT) == 2
    assert app.v4_real_gradient_eligible_tier(Decimal("-9"), thresholds, direction=SHORT) == 0
    # The long side keeps the plain "greater is better" reading.
    assert app.v4_real_gradient_eligible_tier(Decimal("11"), [Decimal("10")], direction=LONG) == 1


def test_short_side_probe_allowed_below_the_threshold() -> None:
    common = dict(
        v4_enabled=True,
        real_gradient_enabled=True,
        account_flat=True,
        refresh_enabled=True,
        candidate_kind=None,
    )
    assert app.v4_single_lot_rfq_probe_allowed(
        passive_edge_bps=Decimal("-12.0"),
        passive_threshold_bps=Decimal("-7.2"),
        direction=SHORT,
        **common,
    )
    assert not app.v4_single_lot_rfq_probe_allowed(
        passive_edge_bps=Decimal("-6.0"),
        passive_threshold_bps=Decimal("-7.2"),
        direction=SHORT,
        **common,
    )


def test_short_side_probe_candidate_kind_uses_direction() -> None:
    assert (
        app.v4_entry_rfq_probe_candidate_kind(
            enabled=True,
            refresh_enabled=True,
            account_flat=True,
            predicted_exact_edge_bps=Decimal("-15.4"),
            exact_entry_threshold_bps=Decimal("-10.2"),
            bias_ready=True,
            exploration_due=False,
            direction=SHORT,
        )
        == "predicted_exact_threshold"
    )
    # Just inside the exploration band on the short side.
    assert (
        app.v4_entry_rfq_probe_candidate_kind(
            enabled=True,
            refresh_enabled=True,
            account_flat=True,
            predicted_exact_edge_bps=Decimal("-9.5"),
            exact_entry_threshold_bps=Decimal("-10.2"),
            bias_ready=True,
            exploration_due=True,
            direction=SHORT,
        )
        == "near_threshold_exploration"
    )
    assert (
        app.v4_entry_rfq_probe_candidate_kind(
            enabled=True,
            refresh_enabled=True,
            account_flat=True,
            predicted_exact_edge_bps=Decimal("-6.0"),
            exact_entry_threshold_bps=Decimal("-10.2"),
            bias_ready=True,
            exploration_due=True,
            direction=SHORT,
        )
        is None
    )


def test_exact_edge_passes_is_direction_aware() -> None:
    assert app.v4_entry_rfq_exact_edge_passes(
        exact_edge_bps=Decimal("-12.0"),
        required_edge_bps=Decimal("-10.0"),
        direction=SHORT,
    )
    assert not app.v4_entry_rfq_exact_edge_passes(
        exact_edge_bps=Decimal("-9.0"),
        required_edge_bps=Decimal("-10.0"),
        direction=SHORT,
    )
    assert app.v4_entry_rfq_exact_edge_passes(
        exact_edge_bps=Decimal("11.0"),
        required_edge_bps=Decimal("10.0"),
        direction=LONG,
    )


def test_edge_margin_is_positive_only_on_the_profitable_side() -> None:
    assert app.v4_edge_margin_bps(Decimal("-12"), Decimal("-10"), direction=SHORT) == Decimal("2")
    assert app.v4_edge_margin_bps(Decimal("-9"), Decimal("-10"), direction=SHORT) == Decimal("-1")
    assert app.v4_edge_margin_bps(Decimal("11"), Decimal("10"), direction=LONG) == Decimal("1")


def test_mirrored_percentile_flips_only_the_short_side() -> None:
    assert app.v4_mirrored_percentile(Decimal("95"), SHORT) == Decimal("5")
    assert app.v4_mirrored_percentile(Decimal("95"), LONG) == Decimal("95")
    assert app.v4_mirrored_percentile(Decimal("95"), None) == Decimal("95")


def test_gradient_thresholds_descend_on_the_short_side() -> None:
    history = [Decimal(str(-10 - index)) for index in range(100)]
    thresholds = app.v4_real_gradient_thresholds(
        history_values=history,
        base_threshold_bps=Decimal("-14.5"),
        entry_execution_reserve_bps=Decimal("0.5"),
        direction=SHORT,
    )
    assert len(thresholds) == len(app.LIVE_INVENTORY_BASIS_V4_REAL_GRADIENT_PERCENTILES)
    assert all(
        later < earlier
        for earlier, later in zip(thresholds, thresholds[1:])
    )
