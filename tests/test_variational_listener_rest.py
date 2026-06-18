import base64
import asyncio
import json

from variational.listener import VariationalMonitor


def test_listener_remembers_unknown_variational_rest_response() -> None:
    monitor = VariationalMonitor()
    body = json.dumps({"result": [{"id": "order-1", "status": "rejected"}]})
    payload = {
        "kind": "rest_response",
        "timestamp": "2026-06-18T00:00:00+00:00",
        "url": "https://omni.variational.io/api/orders/history?limit=20",
        "status": 200,
        "type": "Fetch",
        "matchedPattern": "auto:variational_rest",
        "body": base64.b64encode(body.encode()).decode(),
        "base64Encoded": True,
    }

    lines = asyncio.run(monitor.process_rest_event(payload))

    assert lines == []
    assert monitor.recent_rest_responses[0]["path"] == "/api/orders/history"
    assert monitor.recent_rest_responses[0]["json_keys"] == ["result"]
    assert monitor.recent_rest_responses[0]["result_len"] == 1
    assert "status" in monitor.recent_rest_responses[0]["first_result_keys"]
    assert monitor.snapshot()["recent_rest_responses"] == monitor.recent_rest_responses
