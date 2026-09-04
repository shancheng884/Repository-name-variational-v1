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
    assert 'type === "VAR_API_PORTFOLIO"' in background
    assert 'type === "VAR_API_READY"' in background


def test_extension_can_fetch_portfolio_through_authenticated_page() -> None:
    api_script = (ROOT / "chrome_extension" / "var_api.js").read_text(
        encoding="utf-8"
    )

    assert 'action === "PORTFOLIO"' in api_script
    assert 'request("GET", "/api/portfolio"' in api_script
    assert "AbortController" in api_script
    assert "requestTimeoutMs" in api_script
    assert "request_timeout" in api_script
    assert "operationTimeoutMs" in api_script
    assert "timedOut: Boolean(response.timedOut)" in api_script


def test_extension_supports_explicit_variational_instrument_types() -> None:
    api_script = (ROOT / "chrome_extension" / "var_api.js").read_text(
        encoding="utf-8"
    )

    assert 'o.instrumentType || o.instrument_type || "perpetual_future"' in api_script
    assert '["perpetual_future", "swap"]' in api_script
    assert 'instrument_type: instrumentType' in api_script
    assert 'instrumentType: instrument.instrument_type' in api_script
