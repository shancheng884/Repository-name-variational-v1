#!/usr/bin/env python3
"""Read-only supplemental entry health and offline same-parameter WTI replay."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import basis_sample_paths
from tools.lib.wti_entry_audit import entry_diagnostics, same_parameter_shadow, transport_failures
from tools.wti_research_audit import read_research_input, live_config


def recent_rows(root):
    rows = []
    paths = [p for p in basis_sample_paths(root, 'WTI') if p.suffix != '.gz']
    for path in sorted(paths, key=lambda p: p.stat().st_mtime)[-2:]:
        with path.open('rb') as f:
            end = f.seek(0, 2)
            start = max(0, end - 2000000)
            f.seek(start)
            if start:
                f.readline()
            data = f.read(max(0, end - f.tell()))
        for line in data.splitlines(keepends=True):
            if not line.endswith(b'\n'):
                continue
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    rows.append(row)
            except ValueError:
                continue
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--health-path', type=Path, default=ROOT / 'log/wti_live_risk_health.json')
    parser.add_argument('--state-path', type=Path, default=ROOT / 'log/wti_live_state.json')
    parser.add_argument('--sample-root', type=Path, default=ROOT / 'log/oil_swap/basis_samples')
    parser.add_argument('--shadow', action='store_true')
    parser.add_argument('--runtime-log', type=Path, default=ROOT / 'log/wti_live.log')
    args = parser.parse_args(argv)
    try:
        health = json.loads(args.health_path.read_text(encoding='utf-8'))
        now = datetime.now(timezone.utc)
        result = entry_diagnostics(health, recent_rows(args.sample_root), now=now)
        if args.runtime_log.is_file():
            with args.runtime_log.open('rb') as f:
                end = f.seek(0, 2)
                f.seek(max(0, end - 1000000))
                text = f.read(min(end, 1000000)).decode(errors='replace')
            result['refresh']['historical_transport_failures'] = transport_failures(text, now=now)
        result['refresh']['log_scan_scope'] = 'last_1MB_only_not_complete_failure_history'
        if args.shadow:
            state = json.loads(args.state_path.read_text(encoding='utf-8'))
            pilot = state.get('gradient_pilot') or {}
            if pilot.get('market_id') != 145 or str(pilot.get('multiplier')) not in {'1', '1.000000000000000000'}:
                raise ValueError('wti_contract_identity_unverified')
            # Historical snapshots are anchored to the saved health, not wall-clock now.
            from tools.lib.wti_research import stamp
            as_of = stamp(health['updated_at'])
            rows, provenance = read_research_input(args.sample_root, as_of)
            result['shadow'] = ({'status': 'malformed_source_not_accepted', 'eligible_for_live': False}
                if provenance['malformed_or_unverifiable_timestamp_rows'] else
                same_parameter_shadow(rows, health=health, config=live_config(state), as_of=as_of))
            result['input_provenance'] = provenance
        print(json.dumps(result, ensure_ascii=True, default=str, sort_keys=True))
        return 0
    except (ValueError, OSError, KeyError, TypeError, ArithmeticError) as exc:
        print(json.dumps({'status': 'diagnostic_unavailable', 'error': str(exc),
                          'orders_attempted': False, 'state_modified': False}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
