import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_extension_has_persistent_keepalive_and_recovery() -> None:
    manifest = json.loads(
        (ROOT / "chrome_extension" / "manifest.json").read_text(encoding="utf-8")
    )
    background = (ROOT / "chrome_extension" / "background.js").read_text(
        encoding="utf-8"
    )

    assert "alarms" in manifest["permissions"]
    assert manifest["version"] == "1.1.0"
    assert "variationalForwarderKeepalive" in background
    assert "saveForwarderSession(true" in background
    assert "restoreForwarding" in background
    assert "Page.setWebLifecycleState" in background
