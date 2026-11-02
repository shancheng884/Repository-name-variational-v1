#!/usr/bin/env python3
"""Isolated, file-only observation of WTI evidence; cannot submit or retry orders."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.lib.wti_decision_audit import AuditWriter, sample_evidence, state_evidence, execution_evidence, pick


def read_object(path):
    if path.stat().st_size > 8*1024*1024:
        raise ValueError('source_too_large')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict): raise ValueError('object_required')
    return value


def save(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, sort_keys=True)+'\n')
    os.chmod(tmp, 0o600)
    tmp.replace(path)


class Observer:
    def __init__(self, log_root, audit_root, writer):
        self.log_root, self.audit_root, self.writer = Path(log_root), Path(audit_root), writer
        self.cursor_path = self.audit_root/'observer.cursor.json'
        self.cursor = read_object(self.cursor_path) if self.cursor_path.exists() else {}
        self.seen = self.cursor.get('seen', {})
        self.files = self.cursor.get('files', {})
        self.read_errors = 0
        self.source_run = None
        self.exact_active = False
        self.observed = 0

    def record(self, stage, value, identity):
        digest = hashlib.sha256(identity.encode()).hexdigest()
        ok = self.writer.emit(stage, {'observation_id': digest, **value},
            trace=f'observed:{self.source_run}:{value.get("sample_id") or "unknown"}', evidence='observed_only')
        if ok: self.observed += 1
        return ok

    def poll(self):
        for name in ('wti_live_risk_health.json', 'wti_live_state.json'):
            path = self.log_root/name
            try:
                obj = read_object(path)
                identity = hashlib.sha256(json.dumps(obj,sort_keys=True).encode()).hexdigest()
                if name.startswith('wti_live_risk'):
                    self.source_run = obj.get('run_id')
                    audit = obj.get('decision_audit') or {}
                    self.exact_active = audit.get('alive') is True and audit.get('run_id') == self.source_run
                    value = pick(obj, ('run_id','pid','updated_at','last_seen_sample_id',
                        'last_processed_sample_id','market_sample_at','collector_sample_at',
                        'gradient_threshold_profiles','gradient_threshold_source','last_action',
                        'wti_entry_authorized','entry_gate_block_reason','entry_block_reason',
                        'entry_authorization_permanently_revoked','account_risk_action',
                        'account_read_recovery_required','account_read_recovery_verified_snapshots','last_account_read_failure',
                        'account_risk_reason','account_snapshot_observed_at','startup_reconciled',
                        'runtime_cycle_error','market_sample_freshness_diagnostics',
                        'execution_refresh_status','execution_refresh_at','decision_audit'))
                    value['sample_id'] = obj.get('last_processed_sample_id')
                    value['unobserved_stages'] = ['per_branch_confirmation','exact_executor_call_sequence']
                    stage = 'observed_health'
                else:
                    value = {'state': state_evidence(obj), 'executions': execution_evidence(obj,self.cursor.get('last_execution')),
                             'confirmation_and_submission_are_not_inferred': True,
                             'initial_observation':name not in self.seen, 'new_trade_inferred':False}
                    stage = 'observed_execution_state'
                if self.seen.get(name) != identity and self.record(stage,value,name+identity):
                    self.seen[name] = identity
                    if stage == 'observed_execution_state':
                        history = obj.get('execution_history') or []
                        self.cursor['last_execution'] = history[-1].get('action_id') if history else None
            except (OSError,ValueError,TypeError):
                self.read_errors += 1
        folder = self.log_root/'oil_swap/basis_samples/WTI'
        for path in sorted(folder.glob('*.jsonl'))[-2:]:
            try:
                stat = path.stat(); mark = self.files.get(path.name)
                identity = f'{stat.st_dev}:{stat.st_ino}'
                if mark and (mark['identity'] != identity or mark['offset'] > stat.st_size):
                    if not self.record('source_discontinuity', {'source':path.name},identity+str(stat.st_size)):
                        continue
                    mark = None
                with path.open('rb') as handle:
                    offset = mark['offset'] if mark else max(0,stat.st_size-65536)
                    handle.seek(offset)
                    if not mark and offset: handle.readline()
                    until = min(stat.st_size,handle.tell()+262144)
                    while handle.tell() < until:
                        begin = handle.tell(); line = handle.readline()
                        if not line.endswith(b'\n') or handle.tell()>stat.st_size:
                            handle.seek(begin); break
                        try: row=json.loads(line)
                        except ValueError:
                            if not self.record('source_parse_gap',{'source':path.name,'offset':begin},identity+str(begin)):
                                handle.seek(begin);break
                            continue
                        value = sample_evidence(row)
                        value['source_file'] = path.name
                        value['runtime_association'] = 'unverified_sample_id_link_only'
                        if not self.record('observed_sample',value,identity+str(begin)+hashlib.sha256(line).hexdigest()):
                            handle.seek(begin);break
                    self.files[path.name]={'identity':identity,'offset':handle.tell()}
            except (OSError,ValueError,TypeError):
                self.read_errors+=1
        self.cursor.update(seen=self.seen,files=self.files)
        save(self.cursor_path,self.cursor)
        save(self.audit_root/'observer.status.json',{'updated_at':datetime.now(timezone.utc).isoformat(),
            'pid':os.getpid(),'status':'running','source_run_id':self.source_run,
            'exact_runtime_audit_active':self.exact_active,
            'activation':'runtime_hooks_active' if self.exact_active else 'exact_hooks_pending_planned_runtime_restart',
            'observed_events_this_process':self.observed,'source_read_errors':self.read_errors,
            'writer':self.writer.stats(),'evidence_level':'observed_only',
            'historical_coverage':'from_observer_start_initial_tail_only',
            'checkpoint_semantics':'queued_not_fsynced_crash_gaps_possible',
            'orders_attempted':False,'strategy_files_modified':False})


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--log-root',type=Path,default=ROOT/'log')
    parser.add_argument('--audit-root',type=Path,default=ROOT/'log/wti_decision_audit')
    parser.add_argument('--follow',action='store_true')
    parser.add_argument('--poll-seconds',type=float,default=2)
    args=parser.parse_args(argv)
    if not 1<=args.poll_seconds<=60:parser.error('poll interval must be between 1 and 60 seconds')
    if args.audit_root.resolve()==args.log_root.resolve():parser.error('dedicated audit directory required')
    args.audit_root.mkdir(parents=True,exist_ok=True)
    writer=AuditWriter(args.audit_root,name='observer',run_id=f'observer-{time.time_ns()}')
    writer.ready.wait(timeout=3)
    if writer.write_errors or not writer.thread.is_alive():
        writer.close()
        raise RuntimeError('observer_writer_lock_or_start_failed_sources_not_touched')
    observer=Observer(args.log_root,args.audit_root,writer)
    stop=False
    def halt(*_args):
        nonlocal stop
        stop=True
    for sig in (signal.SIGTERM,signal.SIGINT): signal.signal(sig,halt)
    try:
        while not stop:
            observer.poll()
            if writer.write_errors or not writer.thread.is_alive():
                raise RuntimeError('audit_writer_unavailable_no_trading_action_taken')
            if not args.follow:break
            time.sleep(args.poll_seconds)
        return 0
    finally:
        writer.close()
        try:
            path=args.audit_root/'observer.status.json'
            if path.exists():
                status=read_object(path);status['status']='stopped';save(path,status)
        except (OSError,ValueError): pass


if __name__=='__main__':
    raise SystemExit(main())
