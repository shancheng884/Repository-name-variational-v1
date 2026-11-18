"""Local write-side timestamps for the Entropy SDK's HTTPS order path."""
from contextvars import ContextVar
from datetime import datetime, timezone
import time

from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool


_active_trace = ContextVar("entropy_order_trace", default=None)


def _stamp(key):
    trace = _active_trace.get()
    if trace is not None:
        trace[key] = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        trace[key + "_mono_ns"] = time.monotonic_ns()


def timed_order(trace, order, *args, **kwargs):
    scope = _active_trace.set(trace)
    try:
        _stamp("worker_started_at")
        result = order(*args, **kwargs)
        _stamp("exchange_ack_received_at")
        return result
    finally:
        _active_trace.reset(scope)


class _OrderConnection(HTTPSConnection):
    def request(self, method, url, *args, **kwargs):
        track = _active_trace.get() is not None and method.upper() == "POST" and url == "/exchange"
        if track:
            _stamp("request_write_started_at")
        super().request(method, url, *args, **kwargs)
        if track:
            # urllib3 has handed the complete headers/body to the local TLS
            # connection. This does not prove ingress at the exchange.
            _stamp("request_written_at")


class _OrderPool(HTTPSConnectionPool):
    ConnectionCls = _OrderConnection


class _OrderAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        super().init_poolmanager(*args, **kwargs)
        self.poolmanager.pool_classes_by_scheme = {
            **self.poolmanager.pool_classes_by_scheme, "https": _OrderPool,
        }


def instrumented_exchange(exchange_class, *args, **kwargs):
    class TracedExchange(exchange_class):
        def _post_action(self, *args, **kwargs):
            _stamp("signature_completed_at")
            return super()._post_action(*args, **kwargs)

    exchange = TracedExchange(*args, **kwargs)
    # Only the SDK's direct write session is instrumented; leave market data
    # sessions and any proxy path untouched (with no fabricated write stamp).
    exchange.session.mount(exchange.base_url.rstrip("/") + "/exchange", _OrderAdapter())
    return exchange
