"""WTI venue identity and session adapter for the unchanged median strategy."""

from datetime import datetime, timezone
from pathlib import Path
import json

from tools.lib.hedge_instruments import WTI_HEDGE, iter_positions, variational_position_qty
from tools.lib.market_sessions import session_observation_for_sample


def enabled(runtime):
    return bool(getattr(runtime, "live_inventory_frozen_median", False)
                and getattr(runtime, "live_allowed_assets", set()) == {"WTI"})


def request_instrument(asset):
    if str(asset).upper() != "WTI":
        return {"market": str(asset).upper()}
    v = WTI_HEDGE.var
    return {"market": v.symbol, "instrumentType": v.instrument_type,
            "kind": v.kind, "settlementAsset": v.settlement_asset,
            "fundingIntervalS": v.funding_interval_s}


def position_qty(payload):
    rows = iter_positions(payload)
    if rows is None:
        return None
    for row in rows:
        nested = row.get("position_info") if isinstance(row.get("position_info"), dict) else {}
        identity = row.get("instrument") or nested.get("instrument") or row
        if not isinstance(identity, dict):
            return None
        if str(identity.get("underlying") or identity.get("symbol") or "").upper() != "USOILP":
            continue
        if (identity.get("instrument_type") != "swap" or identity.get("kind") != "commodity"
                or identity.get("settlement_asset") != "USDC"
                or str(identity.get("funding_interval_s")) != "0"):
            return None
    return variational_position_qty(payload, WTI_HEDGE)


def session(runtime, now=None):
    if not enabled(runtime):
        return {"entry_allowed": True, "management_allowed": True, "reason": None}
    now = now or datetime.now(timezone.utc)
    path = getattr(runtime, "median_wti_session_health_path", None)
    path = Path(path) if path else Path(__file__).resolve().parents[2]/"log/oil_swap/basis_samples/health.json"
    denied = {"entry_allowed": False, "management_allowed": False,
              "reason": "wti_exchange_session_unverified"}
    try:
        h = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(h, dict):
            return denied
        updated = datetime.fromisoformat(h["updated_at"])
        age = (now-updated).total_seconds()
        heartbeat = datetime.fromisoformat(h["market_status_feed_heartbeat_at"])
        heartbeat_age = (now-heartbeat).total_seconds()
        if not (-5 <= age <= 30 and -5 <= heartbeat_age <= 30
                and h.get("status") == "running"
                and h.get("variational_underlying") == "USOILP"
                and h.get("market_status_feed_connected") is True):
            return denied
        if h.get("market_status") in ("closed", "halted_trading"):
            return {**denied, "reason": "wti_exchange_closed"}
        evidence = {
            "logged_at": now.isoformat(), "session_status": h.get("session_status"),
            "market_status": h.get("market_status"),
            "session_status_source": "variational_market_status_websocket",
            "session_schedule_source": h.get("session_schedule_source"),
            "session_status_feed_connected": h.get("market_status_feed_connected"),
            "session_status_observed_at": h.get("session_status_observed_at"),
            "session_status_feed_heartbeat_at": h.get("market_status_feed_heartbeat_at"),
            "session_closing_at": h.get("session_closing_at"), "session_id": h.get("session_id"),
        }
        observation = session_observation_for_sample(evidence)
        if observation is None:
            return denied
        entry = (observation.closing_at-now).total_seconds() > 1800
        return {"entry_allowed": entry, "management_allowed": True,
                "reason": None if entry else "wti_session_entry_cutoff",
                "session_id": observation.session_id, "closing_at": observation.closing_at.isoformat()}
    except (OSError, ValueError, TypeError, KeyError):
        return denied
