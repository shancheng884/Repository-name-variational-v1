"""Read-only fee renewal; independent from trading, alerts and remote desktop."""
from __future__ import annotations
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import read_json, write_json_atomic
from tools.lib.variational_fee_session import fetch_fee_capture
from tools.lib.wti_exit_cost import cost_policy, fingerprint
from tools.wti_exit_cost_audit import build


def invalidate(path, *, reason, now):
    try:
        write_json_atomic(path, {"schema_version": 1, "unavailable": True,
                                "reason": reason, "observed_at": now.isoformat()})
    except OSError:
        # A full disk must not leave an earlier discounted proof active.
        path.unlink(missing_ok=True)


def validate_paths(state_path, evidence_path):
    protected = {Path(state_path).resolve()}
    for name in ('wti_live_state.json', 'wti_live_state.telegram.json', 'wti_live_control.json',
                 'wti_live_risk_health.json', 'wti_live_state.sample_guard.json',
                 'live_inventory_state.json'):
        protected.add((ROOT/'log'/name).resolve())
    targets = (evidence_path, evidence_path.with_name(evidence_path.stem+'.last-good.json'),
               evidence_path.with_name(evidence_path.stem+'.renewal-health.json'))
    if any(p.is_symlink() or p.resolve() in protected for p in targets):
        raise ValueError('renewal_output_overlaps_protected_live_state')


async def renew_once(state_path, evidence_path, cookie_db, *, fetcher=fetch_fee_capture,
                     builder=build, loader=read_json):
    now = datetime.now(timezone.utc)
    validate_paths(state_path, evidence_path)
    cache_path = evidence_path.with_name(evidence_path.stem+".last-good.json")
    health_path = evidence_path.with_name(evidence_path.stem+".renewal-health.json")
    health = {"updated_at": now.isoformat(), "orders_attempted": False,
              "live_ledger_modified": False, "browser_reloaded": False,
              "remote_desktop_required": False, "status": "unavailable"}
    try:
        state = loader(state_path)
        if state.get('status') != 'open' or state.get('pending_action'):
            raise ValueError('awaiting_resolved_open_position')
        bound = fingerprint(state)
        cached = loader(cache_path)
        if cached.get('position_fingerprint') != bound:
            cached = None
        capture = await asyncio.to_thread(fetcher, state, cookie_db)
        proof, (cost, audit) = await builder(state, capture, cached=cached)
        current = loader(state_path)
        if (current.get('status') != 'open' or current.get('pending_action')
                or fingerprint(current) != bound):
            raise ValueError('position_changed_during_fee_renewal')
        cost, audit = cost_policy(current, proof)
        if audit['mode'] != 'real_cost_plus_remaining_reserve':
            raise ValueError('renewed_fee_proof_did_not_validate')
        write_json_atomic(cache_path, proof)
        write_json_atomic(evidence_path, proof)
        health.update(status='verified', applied_bps=str(cost), expires_at=proof['expires_at'],
                      position_fingerprint=bound, reason=None)
    except Exception as exc:
        message = str(exc)
        reason = message if re.fullmatch(r'[a-z0-9_:]+', message) else type(exc).__name__
        invalidate(evidence_path, reason=reason, now=datetime.now(timezone.utc))
        health.update(reason=reason, applied_bps='10')
    try:
        write_json_atomic(health_path, health)
    except OSError:
        invalidate(evidence_path, reason='renewal_health_write_failed', now=datetime.now(timezone.utc))
        health.update(status='unavailable', reason='renewal_health_write_failed', applied_bps='10')
    return health


def main():
    import fcntl
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-path', type=Path, default=ROOT/'log/wti_live_state.json')
    parser.add_argument('--evidence-path', type=Path, default=ROOT/'log/wti-exit-cost-20261030/active-evidence.json')
    parser.add_argument('--cookie-db', type=Path,
        default=Path.home()/'snap/chromium/common/chromium/Default/Cookies')
    parser.add_argument('--check-service-result', action='store_true')
    args = parser.parse_args()
    validate_paths(args.state_path, args.evidence_path)
    args.evidence_path.parent.mkdir(parents=True, exist_ok=True)
    if args.check_service_result:
        if os.getenv('SERVICE_RESULT') != 'success':
            invalidate(args.evidence_path, reason='renewal_service_failed', now=datetime.now(timezone.utc))
        return 0
    with args.evidence_path.with_suffix('.renewal.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        health = asyncio.run(renew_once(args.state_path, args.evidence_path, args.cookie_db))
    print(json.dumps(health, sort_keys=True))
    return 0 if health['status'] == 'verified' or health.get('reason') == 'awaiting_resolved_open_position' else 2


if __name__ == '__main__':
    raise SystemExit(main())
