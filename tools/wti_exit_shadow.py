#!/usr/bin/env python3
"""Offline exit-basis validation on a mandatory frozen snapshot directory."""
from __future__ import annotations

import argparse
from pathlib import Path
import hashlib
import json
import sys
from decimal import Decimal

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.wti_exit_shadow import exit_shadow
from tools.lib.wti_research import stamp
from tools.wti_research_audit import read_research_input, live_config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-log-root', required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        root = args.snapshot_log_root.resolve()
        if root == (ROOT / 'log').resolve():
            raise ValueError('use_frozen_snapshot_not_live_log_directory')
        state_path, health_path = root/'wti_live_state.json', root/'wti_live_risk_health.json'
        state_bytes, health_bytes = state_path.read_bytes(), health_path.read_bytes()
        state, health = json.loads(state_bytes), json.loads(health_bytes)
        pilot = state.get('gradient_pilot') or {}
        if pilot.get('market_id') != 145 or Decimal(str(pilot.get('multiplier'))) != 1:
            raise ValueError('frozen_contract_identity_unverified')
        as_of = stamp(health['updated_at'])
        rows, provenance = read_research_input(root/'oil_swap/basis_samples', as_of)
        if provenance['malformed_or_unverifiable_timestamp_rows']:
            raise ValueError('malformed_frozen_source')
        report = exit_shadow(rows, config=live_config(state), health=health, as_of=as_of)
        assert state_path.read_bytes() == state_bytes and health_path.read_bytes() == health_bytes
        report['input_provenance'] = provenance
        report['source_sha256'] = {'state': hashlib.sha256(state_bytes).hexdigest(),
                                   'health': hashlib.sha256(health_bytes).hexdigest()}
        report['research_code_sha256'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [Path(__file__), ROOT/'tools/lib/wti_exit_shadow.py', ROOT/'tools/lib/wti_replay.py']}
        print(json.dumps(report, ensure_ascii=True, sort_keys=True, default=str))
        return 0
    except (ValueError, OSError, KeyError, TypeError, ArithmeticError) as exc:
        print(json.dumps({'status': 'unavailable', 'error': str(exc), 'orders_attempted': False,
                          'state_modified': False, 'live_authorization': False}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
