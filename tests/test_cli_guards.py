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
