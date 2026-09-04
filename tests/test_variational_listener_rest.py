import base64
import asyncio
import json
import time

from variational.listener import EventSink, VariationalMonitor


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


def test_trading_raw_event_policy_skips_market_noise_but_keeps_orders(
    tmp_path,
) -> None:
    sink = EventSink(
        output_dir=tmp_path,
        raw_event_policy="trading",
    )
    quote = {
        "kind": "rest_response",
        "url": "https://omni.variational.io/api/quotes/indicative",
        "body": "large quote body",
    }
    order = {
        "kind": "rest_response",
        "url": "https://omni.variational.io/api/orders/v2",
        "body": '{"status":"filled"}',
    }

    asyncio.run(sink.handle("rest", json.dumps(quote)))
    assert not (tmp_path / "rest_events.jsonl").exists()

    asyncio.run(sink.handle("rest", json.dumps(order)))
    rows = [
        json.loads(line)
        for line in (tmp_path / "rest_events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert len(rows) == 1
    assert rows[0]["payload"]["url"].endswith("/api/orders/v2")


def test_quote_records_local_receive_time_when_exchange_timestamp_is_missing() -> None:
    monitor = VariationalMonitor()

    updated = monitor._update_quote(
        {
            "asset": "ETH",
            "bid": "2400.10",
            "ask": "2400.20",
        }
    )

    assert updated is True
    assert monitor.quotes["ETH"]["timestamp"] is None
    assert monitor.quotes["ETH"]["received_at"]
    assert monitor.quotes["ETH"]["received_monotonic"] <= time.monotonic()
