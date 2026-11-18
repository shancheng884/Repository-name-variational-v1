from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
import requests
from urllib3.connection import HTTPSConnection

from tools.lib.entropy_order_timing import _OrderConnection, instrumented_exchange, timed_order
from tools.lib.venue_probe import EntropyProbeExecutor


class StubExchange:
    def __init__(self):
        self.base_url = "https://exchange.invalid"
        self.session = requests.Session()

    def order(self, path="/exchange"):
        return self._post_action(path)

    def _post_action(self, path):
        adapter = self.session.get_adapter(self.base_url + path)
        pool = adapter.poolmanager.connection_from_url(self.base_url)
        conn = pool.ConnectionCls("exchange.invalid")
        conn.request("POST", path, body=b"opaque")
        return "ack"


def test_signed_request_uses_a_private_transport_and_stamps_before_ack(monkeypatch):
    outbound = []
    monkeypatch.setattr(HTTPSConnection, "request", lambda self, method, url, **kw: outbound.append(url))
    exchange = instrumented_exchange(StubExchange)
    trace = {}
    assert timed_order(trace, exchange.order) == "ack"
    assert outbound == ["/exchange"]
    assert type(exchange.session.get_adapter(exchange.base_url + "/info")) is requests.adapters.HTTPAdapter
    assert sorted(trace) == sorted(
        k + suffix for k in ("worker_started_at", "signature_completed_at", "request_write_started_at",
                             "request_written_at", "exchange_ack_received_at")
        for suffix in ("", "_mono_ns")
    )
    ordered = [trace[k + "_mono_ns"] for k in (
        "worker_started_at", "signature_completed_at", "request_write_started_at",
        "request_written_at", "exchange_ack_received_at")]
    assert ordered == sorted(ordered)


def test_failed_write_and_after_scope_do_not_create_a_successful_write_stamp(monkeypatch):
    def failed(*args, **kwargs):
        raise OSError("connection lost")
    monkeypatch.setattr(HTTPSConnection, "request", failed)
    exchange = instrumented_exchange(StubExchange)
    trace = {}
    with pytest.raises(OSError):
        timed_order(trace, exchange.order)
    assert "signature_completed_at" in trace and "request_write_started_at" in trace
    assert "request_written_at" not in trace and "exchange_ack_received_at" not in trace
    old = dict(trace)
    with pytest.raises(OSError):
        exchange.order()
    assert trace == old


def test_write_completion_follows_the_actual_request_body():
    trace, chunks = {}, []

    class FakeSocket:
        def settimeout(self, timeout):
            pass

        def sendall(self, data):
            assert "request_written_at" not in trace
            chunks.append(bytes(data))

    conn = _OrderConnection("exchange.invalid")
    conn.sock = FakeSocket()
    body = b'{"action":"synthetic"}'
    timed_order(trace, conn.request, "POST", "/exchange", body=body)
    sent = b"".join(chunks)
    assert sent.startswith(b"POST /exchange HTTP/1.1") and sent.endswith(body)
    assert "request_written_at" in trace


def test_traces_do_not_leak_into_other_workers_or_info_requests(monkeypatch):
    from threading import Barrier
    barrier = Barrier(2, timeout=3)
    monkeypatch.setattr(HTTPSConnection, "request", lambda *a, **kw: barrier.wait())
    order, info = {}, {}
    with ThreadPoolExecutor(2) as workers:
        sent = workers.submit(timed_order, order, _OrderConnection("example.org").request, "POST", "/exchange")
        other = workers.submit(timed_order, info, _OrderConnection("example.org").request, "POST", "/info")
        sent.result()
        other.result()
    assert "request_written_at" in order and "request_written_at" not in info


def test_split_fill_uses_first_and_last_exchange_times_not_local_ack():
    rows = [{"time": 1794950413161}, {"time": 1794950413100}]
    expected = dict(zip(("exchange_first_fill_at", "exchange_last_fill_at"), (
        datetime.fromtimestamp(r["time"]/1000, timezone.utc).isoformat() for r in reversed(rows))))
    assert EntropyProbeExecutor._exchange_fill_times(rows) == expected
    assert all(t is None for t in EntropyProbeExecutor._exchange_fill_times([*rows, {}]).values())
