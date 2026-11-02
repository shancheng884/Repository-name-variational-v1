"""Read-only high-frequency WTI signal observations; never an execution input."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
from typing import Any
from urllib.parse import parse_qs, urlparse


PAIR_DRIFT_LIMIT_BPS = Decimal("1.5")
BOOK_AGE_LIMIT_SECONDS = Decimal("2")
QUOTE_AGE_LIMIT_SECONDS = Decimal("2")


def _decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _crossed(edge: Decimal, thresholds: Any) -> list[int]:
    if not isinstance(thresholds, (list, tuple)):
        return []
    return [
        index + 1
        for index, value in enumerate(thresholds)
        if (threshold := _decimal(value)) is not None and edge >= threshold
    ]


def passive_wti_quote_sample(
    quote: dict[str, Any] | None,
    baseline: dict[str, Any] | None,
    *,
    now: datetime,
) -> tuple[dict[str, Any] | None, str | None]:
    """Build an exact-quantity shadow row from an already observed REST quote."""
    if not isinstance(quote, dict) or not isinstance(baseline, dict):
        return None, "passive_quote_or_baseline_unavailable"
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    url = str(raw.get("__source_url") or "")
    parsed_url = urlparse(url)
    if parsed_url.path != "/api/quotes/indicative":
        return None, "passive_quote_source_unverified"
    query = parse_qs(parsed_url.query)
    instrument = raw.get("instrument") if isinstance(raw.get("instrument"), dict) else {}

    def evidence(*keys: str) -> str | None:
        for source in (instrument, raw):
            for key in keys:
                value = source.get(key)
                if value is not None and str(value).strip():
                    return str(value).strip()
        for key in keys:
            values = query.get(key) or query.get(key.lower()) or query.get(key.upper())
            if values and str(values[0]).strip():
                return str(values[0]).strip()
        return None

    market = evidence("market", "underlying", "symbol", "asset")
    instrument_type = evidence("instrumentType", "instrument_type")
    kind = evidence("kind", "instrumentKind", "instrument_kind")
    amount = evidence("amount", "qty", "quantity")
    if market is None or market.upper() != "USOILP":
        return None, "passive_quote_market_unverified"
    if instrument_type is None or instrument_type.lower() != "swap":
        return None, "passive_quote_instrument_type_unverified"
    if kind is None or kind.lower() != "commodity":
        return None, "passive_quote_kind_unverified"
    expected_qty = _decimal(baseline.get("variational_qty"))
    observed_qty = _decimal(amount)
    if expected_qty is None or observed_qty is None or observed_qty != expected_qty:
        return None, "passive_quote_quantity_mismatch_or_unknown"

    timestamp = quote.get("timestamp")
    try:
        source_at = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if source_at.tzinfo is None or source_at.utcoffset() is None:
            raise ValueError("passive_quote_timestamp_timezone_missing")
        source_at = source_at.astimezone(timezone.utc)
        age_at_receive = (now.astimezone(timezone.utc) - source_at).total_seconds()
    except (TypeError, ValueError):
        return None, "passive_quote_timestamp_unverified"
    try:
        baseline_at = datetime.fromisoformat(
            str(baseline.get("quote_timestamp")).replace("Z", "+00:00")
        )
        if baseline_at.tzinfo is None or baseline_at.utcoffset() is None:
            raise ValueError("baseline_quote_timestamp_timezone_missing")
        if source_at <= baseline_at.astimezone(timezone.utc):
            return None, "passive_quote_not_newer_than_baseline"
    except (TypeError, ValueError):
        return None, "baseline_quote_timestamp_unverified"
    if not -1 <= age_at_receive <= float(QUOTE_AGE_LIMIT_SECONDS):
        return None, "passive_quote_stale"

    bid = _decimal(quote.get("bid"))
    ask = _decimal(quote.get("ask"))
    if bid is None or ask is None or bid <= 0 or ask < bid:
        return None, "passive_quote_bid_ask_invalid"
    reasons = set(baseline.get("sample_quality_reasons") or [])
    if not reasons.issubset({"variational_quote_age_invalid", "lighter_book_stale"}):
        return None, "baseline_has_nonrefreshable_quality_failure"
    received_at = quote.get("received_at")
    try:
        received = datetime.fromisoformat(str(received_at).replace("Z", "+00:00"))
        if received.tzinfo is None or received.utcoffset() is None:
            raise ValueError("passive_quote_receive_timestamp_timezone_missing")
        received = received.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None, "passive_quote_receive_timestamp_unverified"
    age_at_capture = (received - source_at).total_seconds()
    if not -1 <= age_at_capture <= float(QUOTE_AGE_LIMIT_SECONDS):
        return None, "passive_quote_stale_at_capture"

    sample_id = hashlib.sha256(
        f"{url}|{timestamp}|{bid}|{ask}|{observed_qty}".encode()
    ).hexdigest()[:32]
    row = dict(baseline)
    row.update({
        "sample_id": f"passive-{sample_id}",
        "sample_kind": "passive_shadow",
        "sample_quality": "valid",
        "sample_quality_reasons": [],
        "logged_at": received.isoformat(),
        "quote_timestamp": str(timestamp),
        "quote_id": raw.get("quoteId") or raw.get("quote_id"),
        "var_bid": str(bid),
        "var_ask": str(ask),
        "var_quote_age_seconds": str(Decimal(str(age_at_capture))),
        "quote_source": "passive_browser_rest",
        "passive_quote_source_url": url,
    })
    return row, None


def observe_shadow_book(
    sample: dict[str, Any] | None,
    book: dict[str, Any] | None,
    thresholds: dict[str, Any] | None,
    freshness: dict[str, Any] | None,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Score a read-only book snapshot only while the paired quote is still safe."""
    result: dict[str, Any] = {
        "observed_at": now.astimezone(timezone.utc).isoformat(),
        "sample_id": sample.get("sample_id") if isinstance(sample, dict) else None,
        "threshold_sample_id": (
            thresholds.get("sample_id") if isinstance(thresholds, dict) else None
        ),
        "shadow_only": True,
        "orders_attempted": False,
        "affects_strategy": False,
        "status": "unavailable",
        "reason": None,
    }
    if not isinstance(sample, dict):
        result["reason"] = "baseline_sample_unavailable"
        return result
    checks = (freshness or {}).get("checks") or {}
    required = ("sample_quality", "both_feeds_live", "lighter_continuity", "session_evidence", "quote_age_effective")
    failed = [name for name in required if checks.get(name) is not True]
    if failed:
        result["reason"] = "paired_quote_not_eligible"
        result["failed_checks"] = failed
        return result
    if not isinstance(book, dict):
        result["reason"] = "lighter_book_unavailable"
        return result

    book_age = _decimal(book.get("book_age_seconds"))
    if book_age is None or not Decimal(0) <= book_age <= BOOK_AGE_LIMIT_SECONDS:
        result["reason"] = "lighter_book_age_invalid"
        result["lighter_book_age_seconds"] = str(book_age) if book_age is not None else None
        return result
    if book.get("continuity_ok") is not True:
        result["reason"] = "lighter_book_continuity_invalid"
        return result
    if book.get("cold") is True:
        result["reason"] = "lighter_book_resync_cooldown"
        return result

    var_bid = _decimal(sample.get("var_bid"))
    var_ask = _decimal(sample.get("var_ask"))
    baseline_bid = _decimal(sample.get("lighter_bid"))
    baseline_ask = _decimal(sample.get("lighter_ask"))
    current_bid = _decimal(book.get("bid"))
    current_ask = _decimal(book.get("ask"))
    lighter_sell = _decimal(book.get("sell_price"))
    lighter_buy = _decimal(book.get("buy_price"))
    qty = _decimal(sample.get("lighter_qty"))
    if any(value is None or value <= 0 for value in (
        var_bid, var_ask, baseline_bid, baseline_ask, current_bid,
        current_ask, lighter_sell, lighter_buy, qty,
    )):
        result["reason"] = "paired_quote_or_depth_invalid"
        return result

    drift = max(
        abs(current_bid - baseline_bid) / baseline_bid * Decimal("10000"),
        abs(current_ask - baseline_ask) / baseline_ask * Decimal("10000"),
    )
    result.update({
        "quote_id": sample.get("quote_id"),
        "quote_timestamp": sample.get("quote_timestamp"),
        "quote_source": sample.get("quote_source", "collector_sample"),
        "var_quote_age_effective_seconds": checks.get("quote_age_effective_seconds"),
        "lighter_book_age_seconds": str(book_age),
        "lighter_nonce": book.get("nonce"),
        "lighter_qty": str(qty),
        "lighter_pair_drift_bps": str(drift),
    })
    if drift > PAIR_DRIFT_LIMIT_BPS:
        result["reason"] = "lighter_pair_drift_exceeds_1_5bps"
        return result

    long_edge = (lighter_sell - var_ask) / var_ask * Decimal("10000")
    short_edge = (var_bid - lighter_buy) / lighter_buy * Decimal("10000")
    threshold_map = thresholds.get("by_direction", {}) if isinstance(thresholds, dict) else {}
    result.update({
        "status": "scored",
        "reason": None,
        "long_var_short_lighter_edge_bps": str(long_edge),
        "short_var_long_lighter_edge_bps": str(short_edge),
        "thresholds_bps": threshold_map,
        "crossed_tiers": {
            "long_var_short_lighter": _crossed(
                long_edge, threshold_map.get("long_var_short_lighter")
            ),
            "short_var_long_lighter": _crossed(
                short_edge, threshold_map.get("short_var_long_lighter")
            ),
        },
        "thresholds_available": bool(threshold_map),
    })
    return result


class ShadowWindow:
    """One-second bounded aggregate; does not retain or replay individual ticks."""

    def __init__(self) -> None:
        self.count = 0
        self.scored = 0
        self.reasons: Counter[str] = Counter()
        self.sample_ids: set[str] = set()
        self.nonces: list[Any] = []
        self.edges: dict[str, dict[str, str]] = {}
        self.crossings: Counter[str] = Counter()
        self.started_at: str | None = None
        self.ended_at: str | None = None
        self.previous_at: datetime | None = None
        self.max_inter_sample_gap_seconds = Decimal(0)
        self.thresholds_bps: dict[str, Any] = {}
        self.max_pair_drift_bps = Decimal(0)
        self.max_quote_age_seconds = Decimal(0)
        self.max_book_age_seconds = Decimal(0)

    def add(self, observation: dict[str, Any]) -> None:
        self.count += 1
        at = str(observation.get("observed_at") or "")
        self.started_at = self.started_at or at
        self.ended_at = at or self.ended_at
        try:
            parsed_at = datetime.fromisoformat(at.replace("Z", "+00:00"))
            if parsed_at.tzinfo is not None:
                parsed_at = parsed_at.astimezone(timezone.utc)
                if self.previous_at is not None:
                    gap = Decimal(str((parsed_at - self.previous_at).total_seconds()))
                    self.max_inter_sample_gap_seconds = max(
                        self.max_inter_sample_gap_seconds, gap,
                    )
                self.previous_at = parsed_at
        except (TypeError, ValueError):
            pass
        sample_id = observation.get("sample_id")
        if sample_id:
            self.sample_ids.add(str(sample_id))
        thresholds = observation.get("thresholds_bps")
        if isinstance(thresholds, dict):
            self.thresholds_bps = thresholds
        for field, attr in (
            ("lighter_pair_drift_bps", "max_pair_drift_bps"),
            ("var_quote_age_effective_seconds", "max_quote_age_seconds"),
            ("lighter_book_age_seconds", "max_book_age_seconds"),
        ):
            value = _decimal(observation.get(field))
            if value is not None:
                setattr(self, attr, max(getattr(self, attr), value))
        nonce = observation.get("lighter_nonce")
        if nonce is not None:
            self.nonces.append(nonce)
        if observation.get("status") != "scored":
            self.reasons[str(observation.get("reason") or "unknown")] += 1
            return
        self.scored += 1
        for direction, field in (
            ("long_var_short_lighter", "long_var_short_lighter_edge_bps"),
            ("short_var_long_lighter", "short_var_long_lighter_edge_bps"),
        ):
            edge = _decimal(observation.get(field))
            if edge is None:
                continue
            slot = self.edges.setdefault(direction, {"min": str(edge), "max": str(edge)})
            slot["min"] = str(min(Decimal(slot["min"]), edge))
            slot["max"] = str(max(Decimal(slot["max"]), edge))
            tiers = (observation.get("crossed_tiers") or {}).get(direction) or []
            for tier in tiers:
                self.crossings[f"{direction}:tier_{tier}"] += 1

    def close(self) -> dict[str, Any]:
        return {
            "mode": "high_frequency_shadow_only",
            "shadow_only": True,
            "orders_attempted": False,
            "affects_strategy": False,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "observation_count": self.count,
            "scored_count": self.scored,
            "unscored_reasons": dict(self.reasons),
            "source_sample_ids": sorted(self.sample_ids),
            "lighter_nonce_first": self.nonces[0] if self.nonces else None,
            "lighter_nonce_last": self.nonces[-1] if self.nonces else None,
            "edge_extremes_bps": self.edges,
            "tier_crossing_observation_counts": dict(self.crossings),
            "thresholds_bps": self.thresholds_bps,
            "max_pair_drift_bps": str(self.max_pair_drift_bps),
            "max_quote_age_seconds": str(self.max_quote_age_seconds),
            "max_book_age_seconds": str(self.max_book_age_seconds),
            "sampling_interval_seconds": "0.20",
            "max_inter_sample_gap_seconds": str(self.max_inter_sample_gap_seconds),
            "safety_limits": {
                "quote_age_seconds_max": str(QUOTE_AGE_LIMIT_SECONDS),
                "book_age_seconds_max": str(BOOK_AGE_LIMIT_SECONDS),
                "pair_drift_bps_max": str(PAIR_DRIFT_LIMIT_BPS),
            },
        }
