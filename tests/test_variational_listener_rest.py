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


def test_prices_stream_stays_in_reference_cache_without_fake_bid_ask() -> None:
    monitor = VariationalMonitor()
    payload = {
        "kind": "ws_frame",
        "direction": "received",
        "url": "wss://omni-ws-server.prod.ap-northeast-1.variational.io/prices",
        "timestamp": "2026-09-04T00:00:01.000Z",
        "payloadData": json.dumps(
            {
                "channel": "instrument_price:P-ETH-USDC-3600",
                "pricing": {
                    "price": "2500.25",
                    "underlying_price": "2500.40",
                    "timestamp": "2026-09-04T00:00:00.900Z",
                },
            }
        ),
    }

    asyncio.run(monitor.process_ws_event(payload))

    assert "ETH" not in monitor.quotes
    assert monitor.reference_quotes["ETH"]["reference_price"] == "2500.25"
    assert "bid" not in monitor.reference_quotes["ETH"]
    assert "ask" not in monitor.reference_quotes["ETH"]


def test_reference_stream_rejects_duplicate_source_timestamp() -> None:
    monitor = VariationalMonitor()

    assert monitor._update_quote(
        {
            "asset": "ETH",
            "price": "2500.25",
            "timestamp": "2026-09-04T00:00:00Z",
            "channel": "instrument_price:P-ETH-USDC-3600",
        }
    )
    assert not monitor._update_quote(
        {
            "asset": "ETH",
            "price": "2500.25",
            "timestamp": "2026-09-04T00:00:00Z",
            "channel": "instrument_price:P-ETH-USDC-3600",
        }
    )

    assert monitor.reference_quotes["ETH"]["reference_price"] == "2500.25"


def test_reference_stream_accepts_changed_price_at_same_source_timestamp() -> None:
    monitor = VariationalMonitor()

    assert monitor._update_quote(
        {
            "asset": "ETH",
            "price": "2500.25",
            "timestamp": "2026-09-04T00:00:00Z",
            "channel": "instrument_price:P-ETH-USDC-3600",
        }
    )
    assert monitor._update_quote(
        {
            "asset": "ETH",
            "price": "2600.25",
            "timestamp": "2026-09-04T00:00:00Z",
            "channel": "instrument_price:P-ETH-USDC-3600",
        }
    )

    assert monitor.reference_quotes["ETH"]["reference_price"] == "2600.25"


def test_rest_and_ws_quote_caches_do_not_overwrite_each_other() -> None:
    monitor = VariationalMonitor()

    assert monitor._update_quote(
        {
            "asset": "ETH",
            "price": "2500.25",
            "timestamp": "2026-09-04T00:00:00Z",
            "channel": "instrument_price:P-ETH-USDC-3600",
        }
    )
    assert monitor._update_quote(
        {
            "asset": "ETH",
            "bid": "2499.90",
            "ask": "2500.10",
            "timestamp": "2026-09-04T00:00:01Z",
            "__source_endpoint": "/api/quotes/indicative",
        }
    )

    assert monitor.reference_quotes["ETH"]["reference_price"] == "2500.25"
    assert monitor.quotes["ETH"]["bid"] == "2499.90"
    assert monitor.quotes["ETH"]["ask"] == "2500.10"
