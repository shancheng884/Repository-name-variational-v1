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
