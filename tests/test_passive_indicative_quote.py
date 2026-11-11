import asyncio
import logging
import time
from types import SimpleNamespace

import main as app


def _runtime() -> app.VariationalToLighterRuntime:
    runtime = app.VariationalToLighterRuntime.__new__(app.VariationalToLighterRuntime)
    runtime.logger = logging.getLogger("test_passive_indicative_quote")
    return runtime


def _reference_quote(*, monotonic: float = 1000.0, price: str = "100.00") -> dict:
    return {
        "asset": "ETH",
        "reference_price": price,
        "mark_price": price,
        "received_monotonic": monotonic,
        "received_at": "2026-11-11T03:55:59.000000+00:00",
    }


def test_fresh_indicative_spread_is_attached_to_the_reference_quote(monkeypatch) -> None:
    runtime = _runtime()
    now = 5000.0
    monkeypatch.setattr(app.time, "monotonic", lambda: now)
    reference = _reference_quote(monotonic=now - 0.2)
    indicative = {
        "bid": "99.90",
        "ask": "100.10",
        "received_monotonic": now - 0.6,
        "received_at": "2026-11-11T03:55:58.900000+00:00",
    }

    merged = runtime.merge_passive_indicative_into_reference(
        reference,
        indicative,
        asset="ETH",
    )

    assert merged["passive_indicative_state"] == "fresh"
    assert app.to_decimal(merged["bid"]) == app.Decimal("99.90")
    assert app.to_decimal(merged["ask"]) == app.Decimal("100.10")
    # Freshness must follow the older of the two passive sources.
    assert merged["received_monotonic"] == now - 0.6
    assert merged["reference_quote_received_monotonic"] == now - 0.2


def test_stale_indicative_never_upgrades_the_reference_quote(monkeypatch) -> None:
    runtime = _runtime()
    now = 5000.0
    monkeypatch.setattr(app.time, "monotonic", lambda: now)
    reference = _reference_quote(monotonic=now - 0.1)
    indicative = {
        "bid": "99.90",
        "ask": "100.10",
        "received_monotonic": now - 30.0,
    }

    merged = runtime.merge_passive_indicative_into_reference(
        reference,
        indicative,
        asset="ETH",
    )

    assert merged["passive_indicative_state"] == "stale"
    assert "bid" not in merged
    assert merged["received_monotonic"] == now - 0.1


def test_zero_width_indicative_stays_rejected(monkeypatch) -> None:
    runtime = _runtime()
    now = 5000.0
    monkeypatch.setattr(app.time, "monotonic", lambda: now)
    reference = _reference_quote(monotonic=now)
    indicative = {
        "bid": "100.00",
        "ask": "100.00",
        "received_monotonic": now,
    }

    merged = runtime.merge_passive_indicative_into_reference(
        reference,
        indicative,
        asset="ETH",
    )

    assert merged["passive_indicative_state"] == "unusable"
    assert "bid" not in merged


def test_missing_indicative_keeps_reference_only_quote(monkeypatch) -> None:
    runtime = _runtime()
    merged = runtime.merge_passive_indicative_into_reference(
        _reference_quote(),
        None,
        asset="ETH",
    )
    assert merged["passive_indicative_state"] == "missing"
    assert "bid" not in merged


def test_passive_basis_quote_keeps_the_indicative_spread(monkeypatch) -> None:
    runtime = _runtime()

    async def fake_reference(_asset):
        return {
            "asset": "ETH",
            "reference_price": "100.00",
            "bid": "99.90",
            "ask": "100.10",
            "received_at": "2026-11-11T03:55:59.000000+00:00",
            "received_monotonic": time.monotonic(),
        }

    monkeypatch.setattr(runtime, "get_variational_reference_quote", fake_reference)

    quote, rfq_cost = asyncio.run(
        runtime.get_live_inventory_basis_quote(
            asset="ETH",
            qty=app.Decimal("0.098"),
            priority="background",
        )
    )

    assert rfq_cost == app.Decimal("0")
    assert quote["quote_semantics"] == "passive_indicative_bid_ask"
    assert app.to_decimal(quote["bid"]) == app.Decimal("99.90")
    assert app.to_decimal(quote["ask"]) == app.Decimal("100.10")
    assert app.to_decimal(quote["reference_price"]) == app.Decimal("100.00")


def test_passive_basis_quote_flattens_only_a_reference_stream(monkeypatch) -> None:
    runtime = _runtime()

    async def fake_reference(_asset):
        return {
            "asset": "ETH",
            "reference_price": "100.00",
            "received_at": "2026-11-11T03:55:59.000000+00:00",
            "received_monotonic": time.monotonic(),
        }

    monkeypatch.setattr(runtime, "get_variational_reference_quote", fake_reference)

    quote, _ = asyncio.run(
        runtime.get_live_inventory_basis_quote(
            asset="ETH",
            qty=app.Decimal("0.098"),
            priority="background",
        )
    )

    assert quote["quote_semantics"] == "reference_price_only"
    assert app.to_decimal(quote["bid"]) == app.to_decimal(quote["ask"]) == app.Decimal("100.00")

