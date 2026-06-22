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
    assert args.live_inventory_signal_mode == "snapshot"
    assert args.live_inventory_i_confirm_flat_start is True
    assert args.live_inventory_lot_notional_usd == 20.0
    assert args.live_inventory_max_total_lots == 1
    assert args.live_inventory_entry_bps == 50.0
    assert args.live_inventory_max_var_spread_bps == 5.0
    assert args.live_inventory_dynamic_entry_buffer_bps == 5.0
    assert args.live_inventory_max_lighter_slippage_bps == 3.0
    assert args.live_inventory_max_lighter_book_age_seconds == 0.0
    assert args.live_inventory_exit_blocked_log_throttle_seconds == 0.0


def test_live_inventory_accepts_open_state_resume_instead_of_flat_start(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-i-confirm-flat-start")
    argv.append("--live-inventory-i-accept-open-state-resume")
    monkeypatch.setattr("sys.argv", argv)

    args = parse_args()

    assert args.live_inventory_i_confirm_flat_start is False
    assert args.live_inventory_i_accept_open_state_resume is True


def test_live_inventory_rejects_flat_start_and_open_state_resume_together(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.append("--live-inventory-i-accept-open-state-resume")
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit):
        parse_args()


def live_inventory_basis_safe_argv() -> list[str]:
    argv = live_inventory_safe_argv()
    argv[argv.index("BTC")] = "ETH"
    return argv + ["--live-inventory-signal-mode", "basis"]


def test_live_inventory_basis_dry_accepts_eth(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_basis_safe_argv())

    args = parse_args()

    assert args.live_inventory_signal_mode == "basis"
    assert args.live_inventory_dry_decisions is True
    assert args.live_allowed_assets == "ETH"
    assert args.live_inventory_basis_z_entry == 4.0
    assert args.live_inventory_basis_min_entry_edge_bps == 7.0
    assert args.live_inventory_basis_min_abs_entry_bps == 0.0
    assert args.live_inventory_basis_exit_safety_buffer_bps == 0.0
    assert args.live_inventory_basis_dynamic_exit_buffer is False
    assert args.live_inventory_basis_refresh_exit_quote_before_submit is False
    assert args.live_inventory_basis_max_var_quote_age_ms == 0.0


def test_live_inventory_basis_accepts_abs_entry_and_exit_buffer(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        live_inventory_basis_safe_argv()
        + [
            "--live-inventory-basis-min-abs-entry-bps",
            "12",
            "--live-inventory-basis-exit-safety-buffer-bps",
            "1.5",
            "--live-inventory-basis-dynamic-exit-buffer",
            "--live-inventory-basis-refresh-exit-quote-before-submit",
        ],
    )

    args = parse_args()

    assert args.live_inventory_basis_min_abs_entry_bps == 12.0
    assert args.live_inventory_basis_exit_safety_buffer_bps == 1.5
    assert args.live_inventory_basis_dynamic_exit_buffer is True
    assert args.live_inventory_basis_refresh_exit_quote_before_submit is True


def test_live_inventory_basis_rejects_real_submit(monkeypatch) -> None:
    argv = live_inventory_basis_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_basis_real_submit_accepts_one_cycle_diagnostic(monkeypatch) -> None:
    argv = live_inventory_basis_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr(
        "sys.argv",
        argv
        + [
            "--live-inventory-max-cycles",
            "1",
            "--live-inventory-i-accept-basis-real-diagnostic",
        ],
    )

    args = parse_args()

    assert args.live_inventory_signal_mode == "basis"
    assert args.live_inventory_dry_decisions is False
    assert args.live_inventory_i_accept_basis_real_diagnostic is True
    assert args.live_inventory_lot_notional_usd == 20.0
    assert args.live_inventory_max_lots == 1
    assert args.live_inventory_max_total_lots == 1
    assert args.live_inventory_max_cycles == 1


def test_live_inventory_basis_real_submit_accepts_addon_diagnostic(monkeypatch) -> None:
    argv = live_inventory_basis_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr(
        "sys.argv",
        argv
        + [
            "--live-inventory-max-total-lots",
            "2",
            "--live-inventory-max-cycles",
            "1",
            "--live-inventory-i-accept-basis-real-diagnostic",
            "--live-inventory-i-accept-basis-addon-diagnostic",
        ],
    )

    args = parse_args()

    assert args.live_inventory_max_lots == 1
    assert args.live_inventory_max_total_lots == 2
    assert args.live_inventory_i_accept_basis_addon_diagnostic is True


def test_live_inventory_basis_real_submit_rejects_addon_without_opt_in(monkeypatch) -> None:
    argv = live_inventory_basis_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr(
        "sys.argv",
        argv
        + [
            "--live-inventory-max-total-lots",
            "2",
            "--live-inventory-max-cycles",
            "1",
            "--live-inventory-i-accept-basis-real-diagnostic",
        ],
    )

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_basis_real_submit_rejects_multiple_cycles(monkeypatch) -> None:
    argv = live_inventory_basis_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr(
        "sys.argv",
        argv
        + [
            "--live-inventory-max-cycles",
            "2",
            "--live-inventory-i-accept-basis-real-diagnostic",
        ],
    )

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_basis_rejects_non_eth(monkeypatch) -> None:
    argv = live_inventory_basis_safe_argv()
    argv[argv.index("ETH")] = "BTC"
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_real_submit_accepts_v1_safe_flags(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr("sys.argv", argv)

    args = parse_args()

    assert args.live_inventory is True
    assert args.live_inventory_dry_decisions is False
    assert args.live_inventory_entry_bps == 50.0


def test_live_inventory_real_submit_accepts_20u_lot_notional(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr(
        "sys.argv",
        argv
        + [
            "--live-max-notional-usd",
            "20",
            "--live-inventory-lot-notional-usd",
            "20",
        ],
    )

    args = parse_args()

    assert args.live_inventory is True
    assert args.live_inventory_dry_decisions is False
    assert args.live_inventory_lot_notional_usd == 20.0


def test_live_inventory_real_submit_accepts_30bps_entry_threshold(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr("sys.argv", argv + ["--live-inventory-entry-bps", "30"])

    args = parse_args()

    assert args.live_inventory_dry_decisions is False
    assert args.live_inventory_entry_bps == 30.0


def test_live_inventory_requires_flat_start_confirmation(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-i-confirm-flat-start")
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_rejects_large_lot_notional(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv() + ["--live-inventory-lot-notional-usd", "21"])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_rejects_non_positive_var_spread_limit(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv() + ["--live-inventory-max-var-spread-bps", "0"])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_rejects_negative_dynamic_entry_buffer(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv() + ["--live-inventory-dynamic-entry-buffer-bps", "-1"])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_rejects_negative_lighter_slippage_limit(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv() + ["--live-inventory-max-lighter-slippage-bps", "-1"])

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


def test_live_inventory_rejects_non_positive_var_snapshot_age(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        live_inventory_safe_argv() + ["--live-inventory-max-var-snapshot-age-seconds", "0"],
    )

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_refresh_var_quote_before_entry_flag_parses(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        live_inventory_safe_argv() + ["--live-inventory-refresh-var-quote-before-entry"],
    )

    args = parse_args()

    assert args.live_inventory_refresh_var_quote_before_entry is True


def test_live_inventory_dry_decisions_allow_low_entry_threshold(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", live_inventory_safe_argv() + ["--live-inventory-entry-bps", "5"])

    args = parse_args()

    assert args.live_inventory_dry_decisions is True
    assert args.live_inventory_entry_bps == 5.0


def test_live_inventory_real_submit_rejects_below_30bps_entry_threshold(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr("sys.argv", argv + ["--live-inventory-entry-bps", "29.99"])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_real_submit_rejects_5bps_entry_threshold(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr("sys.argv", argv + ["--live-inventory-entry-bps", "5"])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_real_submit_accepts_low_entry_threshold_with_diagnostic_ack(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr(
        "sys.argv",
        argv
        + [
            "--live-inventory-entry-bps",
            "15",
            "--live-inventory-i-accept-diagnostic-low-entry-bps",
        ],
    )

    args = parse_args()

    assert args.live_inventory_dry_decisions is False
    assert args.live_inventory_entry_bps == 15.0
    assert args.live_inventory_i_accept_diagnostic_low_entry_bps is True


def test_live_inventory_diagnostic_can_ignore_recent_execution_loss_buffer(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr(
        "sys.argv",
        argv
        + [
            "--live-inventory-entry-bps",
            "15",
            "--live-inventory-i-accept-diagnostic-low-entry-bps",
            "--live-inventory-ignore-recent-execution-loss-buffer-for-diagnostics",
        ],
    )

    args = parse_args()

    assert args.live_inventory_dry_decisions is False
    assert args.live_inventory_i_accept_diagnostic_low_entry_bps is True
    assert args.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics is True


def test_live_inventory_ignore_recent_execution_loss_buffer_requires_diagnostic_ack(monkeypatch) -> None:
    argv = live_inventory_safe_argv()
    argv.remove("--live-inventory-dry-decisions")
    monkeypatch.setattr(
        "sys.argv",
        argv + ["--live-inventory-ignore-recent-execution-loss-buffer-for-diagnostics"],
    )

    with pytest.raises(SystemExit):
        parse_args()


def test_live_inventory_dry_decisions_rejects_diagnostic_low_entry_ack(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        live_inventory_safe_argv() + ["--live-inventory-i-accept-diagnostic-low-entry-bps"],
    )

    with pytest.raises(SystemExit):
        parse_args()
