from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_chrome_runs_on_a_virtual_display() -> None:
    script = (ROOT / "deploy" / "systemd" / "run-var-chrome-xvfb.sh").read_text(
        encoding="utf-8"
    )
    unit = (ROOT / "deploy" / "systemd" / "var-chrome.service").read_text(
        encoding="utf-8"
    )

    # RDP 断开时 Chrome 会冻结页面，报价停更；虚拟显示不依赖会话。
    assert "export DISPLAY=:99" in script
    assert "Xvfb :99" in script
    assert "--user-data-dir=/home/ubuntu/chrome-var-profile" in script
    assert "--disable-background-timer-throttling" in script
    assert "Restart=always" in unit
    assert "run-var-chrome-xvfb.sh" in unit


def test_chrome_unit_is_user_scoped_and_restartable() -> None:
    unit = (ROOT / "deploy" / "systemd" / "var-chrome.service").read_text(
        encoding="utf-8"
    )
    assert "User=ubuntu" in unit
    assert "WantedBy=multi-user.target" in unit
