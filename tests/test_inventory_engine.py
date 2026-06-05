from decimal import Decimal

from inventory_engine import DIRECTION_LONG_VAR_SHORT_LIGHTER, DIRECTION_SHORT_VAR_LONG_LIGHTER, PaperInventoryEngine


def test_paper_inventory_enters_and_exits_long_direction() -> None:
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal("100"),
        max_lots=2,
        entry_bps=Decimal("8"),
        exit_bps=Decimal("4"),
        min_hold_samples=2,
    )

    entered = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("10"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("100100"),
        var_exit_price=Decimal("99990"),
        lighter_exit_price=Decimal("100110"),
        logged_at="t1",
        sample_index=1,
    )
    early_exit = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("3"),
        var_entry_price=Decimal("100010"),
        lighter_entry_price=Decimal("100090"),
        var_exit_price=Decimal("100000"),
        lighter_exit_price=Decimal("100095"),
        logged_at="t2",
        sample_index=2,
    )
    exited = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("3"),
        var_entry_price=Decimal("100010"),
        lighter_entry_price=Decimal("100090"),
        var_exit_price=Decimal("100050"),
        lighter_exit_price=Decimal("100060"),
        logged_at="t3",
        sample_index=3,
    )

    assert entered[0].event == "inventory_paper_entered"
    assert early_exit == []
    assert exited[0].event == "inventory_paper_exited"
    assert exited[0].pnl_usd is not None and exited[0].pnl_usd > 0
    assert engine.open_lots() == 0


def test_paper_inventory_enters_and_exits_short_direction() -> None:
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal("100"),
        max_lots=1,
        entry_bps=Decimal("8"),
        exit_bps=Decimal("4"),
        min_hold_samples=0,
    )

    engine.on_sample(
        direction=DIRECTION_SHORT_VAR_LONG_LIGHTER,
        edge_bps=Decimal("10"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("99900"),
        var_exit_price=Decimal("100010"),
        lighter_exit_price=Decimal("99890"),
        logged_at="t1",
        sample_index=1,
    )
    exited = engine.on_sample(
        direction=DIRECTION_SHORT_VAR_LONG_LIGHTER,
        edge_bps=Decimal("3"),
        var_entry_price=Decimal("100010"),
        lighter_entry_price=Decimal("99980"),
        var_exit_price=Decimal("99950"),
        lighter_exit_price=Decimal("99960"),
        logged_at="t2",
        sample_index=2,
    )

    assert exited[0].event == "inventory_paper_exited"
    assert exited[0].pnl_usd is not None and exited[0].pnl_usd > 0


def test_paper_inventory_latency_enters_at_later_sample_price() -> None:
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal("100"),
        max_lots=2,
        entry_bps=Decimal("8"),
        exit_bps=Decimal("4"),
        min_hold_samples=0,
        latency_samples=1,
    )

    scheduled = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("10"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("100100"),
        var_exit_price=Decimal("99990"),
        lighter_exit_price=Decimal("100110"),
        logged_at="t1",
        sample_index=1,
    )
    entered = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("1"),
        var_entry_price=Decimal("100200"),
        lighter_entry_price=Decimal("100250"),
        var_exit_price=Decimal("100190"),
        lighter_exit_price=Decimal("100260"),
        logged_at="t2",
        sample_index=2,
    )

    assert scheduled == []
    assert entered[0].event == "inventory_paper_entered"
    assert entered[0].var_price == Decimal("100200")
    assert entered[0].lighter_price == Decimal("100250")


def test_paper_inventory_latency_exit_keeps_lot_open_until_fill() -> None:
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal("100"),
        max_lots=2,
        entry_bps=Decimal("8"),
        exit_bps=Decimal("4"),
        min_hold_samples=0,
        latency_samples=1,
    )

    engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("10"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("100100"),
        var_exit_price=Decimal("99990"),
        lighter_exit_price=Decimal("100110"),
        logged_at="t1",
        sample_index=1,
    )
    engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("1"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("100100"),
        var_exit_price=Decimal("99990"),
        lighter_exit_price=Decimal("100110"),
        logged_at="t2",
        sample_index=2,
    )
    scheduled_exit = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("3"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("100100"),
        var_exit_price=Decimal("100050"),
        lighter_exit_price=Decimal("100060"),
        logged_at="t3",
        sample_index=3,
    )
    assert scheduled_exit == []
    assert engine.open_lots() == 1

    exited = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("3"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("100100"),
        var_exit_price=Decimal("100040"),
        lighter_exit_price=Decimal("100070"),
        logged_at="t4",
        sample_index=4,
    )

    assert exited[0].event == "inventory_paper_exited"
    assert exited[0].var_price == Decimal("100040")
    assert exited[0].lighter_price == Decimal("100070")
    assert engine.open_lots() == 0


def test_paper_inventory_max_total_lots_limits_both_directions() -> None:
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal("100"),
        max_lots=5,
        entry_bps=Decimal("8"),
        exit_bps=Decimal("4"),
        min_hold_samples=0,
        max_total_lots=1,
    )

    first = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=Decimal("10"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("100100"),
        var_exit_price=Decimal("99990"),
        lighter_exit_price=Decimal("100110"),
        logged_at="t1",
        sample_index=1,
    )
    second = engine.on_sample(
        direction=DIRECTION_SHORT_VAR_LONG_LIGHTER,
        edge_bps=Decimal("10"),
        var_entry_price=Decimal("100000"),
        lighter_entry_price=Decimal("99900"),
        var_exit_price=Decimal("100010"),
        lighter_exit_price=Decimal("99890"),
        logged_at="t1",
        sample_index=1,
    )

    assert first[0].event == "inventory_paper_entered"
    assert second == []
    assert engine.open_lots() == 1
