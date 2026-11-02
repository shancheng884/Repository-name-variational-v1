#!/usr/bin/env python3
"""Read-only threshold explanation, cost evidence and frozen OOS shadow replay."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
import gzip
import hashlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import basis_sample_paths
from tools.lib.wti_research import threshold_audit, shadow_validation, stamp, digest
from tools.lib.wti_reporting import number
from tools.wti_cost_audit import build_report


def read_research_input(root, as_of):
    rows, sources, malformed = [], [], 0
    paths = basis_sample_paths(root, 'WTI')
    if not paths:
        raise ValueError('sample_source_missing')
    for path in paths:
        checksum, read_bytes, incomplete = hashlib.sha256(), 0, 0
        opener = gzip.open if path.suffix == '.gz' else open
        with opener(path, 'rb') as source:
            for line in source:
                checksum.update(line)
                read_bytes += len(line)
                if not line.endswith(b'\n'):
                    incomplete += 1
                    continue
                try:
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        raise ValueError('non_object')
                    at = stamp(row.get('logged_at'))
                except (ValueError, TypeError, UnicodeDecodeError):
                    malformed += 1
                    continue
                if as_of - timedelta(days=7) < at < as_of:
                    rows.append(row)
                    if len(rows) >= 200000:
                        raise ValueError('sample_input_truncated')
        sources.append({'path': str(path), 'decompressed_snapshot_bytes': read_bytes,
                        'decompressed_snapshot_sha256': checksum.hexdigest(),
                        'incomplete_tail_lines': incomplete})
    rows.sort(key=lambda r: str(r['logged_at']))
    return rows, {'sources': sources, 'malformed_or_unverifiable_timestamp_rows': malformed}


def live_config(state):
    if state.get('asset') != 'WTI':
        raise ValueError('state_not_wti')
    config = dict((state.get('gradient_pilot') or {}).get('config') or {})
    for name in ('entry_threshold_bps', 'exit_target_bps', 'roundtrip_cost_bps',
                 'max_notional_usd', 'layer_notional_usd'):
        value = number(config.get(name))
        if value is None or value <= 0:
            raise ValueError(f'live_config_invalid:{name}')
    tiers = config.get('tier_thresholds_bps')
    if (not isinstance(tiers, list) or len(tiers) != 5
            or any(number(x) is None for x in tiers)
            or Decimal(tiers[0]) != Decimal(config['entry_threshold_bps'])
            or any(Decimal(a) >= Decimal(b) for a, b in zip(tiers, tiers[1:]))):
        raise ValueError('live_config_tiers_invalid')
    if (not 0 < float(config['max_hold_seconds']) <= 21600
            or float(config['entry_cutoff_seconds']) < 1800
            or Decimal(config['max_notional_usd']) > 25
            or Decimal(config['layer_notional_usd']) > Decimal(config['max_notional_usd'])):
        raise ValueError('live_config_outside_existing_bounds')
    config.update(market_id=145, multiplier='1')
    return config


def cost_evidence(state, exact):
    report = build_report(state)
    position = state.get('last_closed_position') or {}
    report['cost_budget_modified'] = False
    report['exact_order_audit'] = exact
    report['exact_fee_evidence_matches_position'] = bool(
        exact and exact.get('exit_execution_id') == position.get('exit_execution_id')
        and exact.get('entry_execution_id') == position.get('entry_execution_id')
        and exact.get('orders_attempted') is False and exact.get('state_modified') is False)
    report['remaining_blockers'] = []
    legs = exact.get('legs') or []
    fees = [number(x.get('fee_usd')) for x in legs]
    verified = bool(report['exact_fee_evidence_matches_position'] and exact.get('status') == 'confirmed'
        and legs and len(legs) == exact.get('expected_order_leg_count')
        and len({x.get('client_order_id') for x in legs}) == len(legs)
        and all(x.get('identity_matches_persisted_fill') is True and x.get('status') == 'confirmed_usd'
                and (x.get('fee_evidence') or {}).get('raw_fee_unit') == 'USD' for x in legs)
        and all(x is not None for x in fees)
        and sum(fees, Decimal(0)) == number(exact.get('fees_usd')))
    report['exact_usd_fee_total'] = exact.get('fees_usd') if verified else None
    if not verified:
        report['remaining_blockers'].append('exact_order_usd_fees_incomplete')
    if position.get('funding_cashflow_confirmed') is not True or number(position.get('funding_cashflow_usd')) is None:
        report['remaining_blockers'].append('funding_cashflow_unverified')
    if not report.get('execution_cost_evidence', {}).get('all_fills_verified'):
        report['remaining_blockers'].append('final_fill_evidence_incomplete')
    report['net_profit_verified'] = False
    gross = number(report.get('actual_gross_pnl_usd'))
    funding = number(position.get('funding_cashflow_usd'))
    if verified and report.get('execution_cost_evidence', {}).get('all_fills_verified') and gross is not None and funding is not None and position.get('funding_cashflow_confirmed') is True:
        report['net_profit_verified'] = True
        report['reconstructed_net_usd'] = str(gross - sum(fees, Decimal(0)) + funding)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sample-root', type=Path, default=ROOT / 'log/oil_swap/basis_samples')
    parser.add_argument('--state-path', type=Path, default=ROOT / 'log/wti_live_state.json')
    parser.add_argument('--fee-evidence', type=Path)
    parser.add_argument('--as-of', type=stamp)
    parser.add_argument('--shadow', action='store_true')
    parser.add_argument('--horizon-seconds', type=int, default=3600)
    args = parser.parse_args(argv)
    try:
        state = json.loads(args.state_path.read_text(encoding='utf-8'))
        config = live_config(state)
        as_of = args.as_of or datetime.now(timezone.utc)
        rows, provenance = read_research_input(args.sample_root, as_of)
        if not rows:
            raise ValueError('sample_input_missing_or_truncated')
        clean, audit = threshold_audit(rows, as_of=as_of, config=config)
        exact = json.loads(args.fee_evidence.read_text(encoding='utf-8')) if args.fee_evidence else {}
        result = {'schema_version': 1, 'mode': 'read_only_research', 'as_of': as_of.isoformat(),
                  'orders_attempted': False, 'state_modified': False, 'live_authorization': False,
                  'input_sha256': digest(rows), 'live_config_snapshot': config,
                  'input_provenance': provenance,
                  'threshold_audit': audit, 'cost_evidence': cost_evidence(state, exact)}
        if args.shadow:
            result['shadow_validation'] = (
                {'status': 'unverifiable_input_rows', 'live_authorization': False,
                 'eligible_for_live': False, 'orders_attempted': False}
                if provenance['malformed_or_unverifiable_timestamp_rows'] else
                shadow_validation(rows, clean, as_of=as_of, config=config,
                                  horizon_seconds=args.horizon_seconds))
        print(json.dumps(result, ensure_ascii=True, sort_keys=True, default=str))
        return 0
    except (ValueError, OSError, KeyError, TypeError, ArithmeticError) as exc:
        print(json.dumps({'mode': 'read_only_research', 'status': 'unavailable',
                          'error': f'{type(exc).__name__}:{exc}', 'orders_attempted': False,
                          'state_modified': False, 'live_authorization': False}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
