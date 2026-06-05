import pytest

from main import parse_args


def test_auto_live_entry_requires_flat_start_confirmation(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--auto-live-entry",
        ],
    )

    with pytest.raises(SystemExit):
        parse_args()


def test_auto_live_entry_accepts_flat_start_confirmation(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--auto-live-entry",
            "--auto-live-i-confirm-flat-start",
        ],
    )

    args = parse_args()

    assert args.auto_live_entry is True
    assert args.auto_live_i_confirm_flat_start is True


def test_auto_live_reset_state_requires_flat_start_confirmation(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--auto-live-entry",
            "--auto-live-reset-state-after-manual-flat",
        ],
    )

    with pytest.raises(SystemExit):
        parse_args()


def test_auto_live_entry_max_precheck_edge_must_be_non_negative(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--auto-live-entry",
            "--auto-live-i-confirm-flat-start",
            "--auto-live-entry-max-precheck-edge-bps",
            "-1",
        ],
    )

    with pytest.raises(SystemExit):
        parse_args()


def test_auto_live_entry_max_precheck_edge_accepts_positive_value(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--auto-live-entry",
            "--auto-live-i-confirm-flat-start",
            "--auto-live-entry-max-precheck-edge-bps",
            "80",
        ],
    )

    args = parse_args()

    assert args.auto_live_entry_max_precheck_edge_bps == 80


def test_auto_live_entry_min_actionable_edge_must_be_non_negative(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--auto-live-entry",
            "--auto-live-i-confirm-flat-start",
            "--auto-live-entry-min-actionable-edge-bps",
            "-1",
        ],
    )

    with pytest.raises(SystemExit):
        parse_args()


def test_auto_live_entry_actionable_edge_flags_parse(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--auto-live-entry",
            "--auto-live-i-confirm-flat-start",
            "--auto-live-entry-min-actionable-edge-bps",
            "8",
            "--auto-live-disable-short-var-long-lighter",
        ],
    )

    args = parse_args()

    assert args.auto_live_entry_min_actionable_edge_bps == 8
    assert args.auto_live_disable_short_var_long_lighter is True


def test_lighter_submit_transport_defaults_to_http(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["main.py"])

    args = parse_args()

    assert args.lighter_submit_transport == "http"
    assert args.lighter_order_mode == "limit-gtt"


def test_lighter_submit_transport_accepts_ws(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--lighter-submit-transport",
            "ws",
            "--lighter-order-mode",
            "market-ioc",
        ],
    )

    args = parse_args()

    assert args.lighter_submit_transport == "ws"
    assert args.lighter_order_mode == "market-ioc"


def test_low_latency_flags_are_explicit(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["main.py"])

    args = parse_args()

    assert args.lighter_prewarm_submit_ws is False
    assert args.auto_live_skip_entry_preview is False


def test_low_latency_flags_accept_opt_in(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--mode",
            "live",
            "--confirm-live",
            "--live-max-notional-usd",
            "25",
            "--auto-live-entry",
            "--auto-live-i-confirm-flat-start",
            "--lighter-submit-transport",
            "ws",
            "--lighter-order-mode",
            "market-ioc",
            "--lighter-prewarm-submit-ws",
            "--auto-live-skip-entry-preview",
        ],
    )

    args = parse_args()

    assert args.lighter_prewarm_submit_ws is True
    assert args.auto_live_skip_entry_preview is True


def live_inventory_safe_argv() -> list[str]:
    return [
        "main.py",
        "--mode",
        "live",
        "--confirm-live",
        "--live-max-notional-usd",
        "10",
        "--live-allowed-assets",
        "BTC",
        "--variational-submit-transport",
        "api",
        "--lighter-submit-transport",
        "ws",
        "--lighter-order-mode",
        "market-ioc",
        "--lighter-prewarm-submit-ws",
        "--live-inventory",
        "--live-inventory-i-confirm-flat-start",
        "--live-inventory-dry-decisions",
    ]


def test_live_inventory_accepts_v1_safe_flags(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv())

    args = parse_args()

    assert args.live_inventory is True
    assert args.live_inventory_dry_decisions is True
    assert args.live_inventory_i_confirm_flat_start is True
    assert args.live_inventory_lot_notional_usd == 10.0
    assert args.live_inventory_max_total_lots == 1
    assert args.live_inventory_entry_bps == 50.0


def test_live_inventory_requires_flat_start_confirmation(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-i-confirm-flat-start")
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_rejects_large_lot_notional(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv() + ["--live-inventory-lot-notional-usd", "11"])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_rejects_auto_live_combo(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv() + ["--auto-live-entry", "--auto-live-i-confirm-flat-start"])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_requires_low_latency_transports(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("api")
    argv.insert(argv.index("--lighter-submit-transport"), "dom")
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_dry_decisions_allow_low_entry_threshold(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv() + ["--live-inventory-entry-bps", "5"])

    args = parse_args()

    assert args.live_inventory_dry_decisions is True
    assert args.live_inventory_entry_bps == 5.0


def test_live_inventory_real_submit_rejects_low_entry_threshold(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr("sys.argv", argv + ["--live-inventory-entry-bps", "5"])

    with pytest.raises(SystemExit):
        parse_args()
