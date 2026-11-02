import asyncio
from copy import deepcopy
import itertools
import json
from pathlib import Path
from queue import Full
from types import SimpleNamespace
import time

import pytest

import test_wti_live_runtime as runtime_tests
from tools.lib.wti_decision_audit import (
    AuditWriter, audited_cycle, emit, clean, safe_error, execution_evidence, crossing_evidence,state_evidence,
)
from tools.wti_decision_audit import Observer


def drain(writer):
    result=[]
    while not writer.queue.empty():
        result.append(writer.queue.get_nowait());writer.queue.task_done()
    return result


@pytest.mark.parametrize('edge,submits',[('40',0),('55',1)])
def test_exact_trace_preserves_real_strategy_refresh_and_submit_counts(tmp_path,monkeypatch,edge,submits):
    factory=runtime_tests._runtime_with_real_executor
    writer=AuditWriter(tmp_path/'audit',name='runtime',run_id='test-real-loop',start=False)
    def attach(*args,**kwargs):
        runtime=factory(*args,**kwargs);runtime.decision_audit=writer;return runtime
    monkeypatch.setattr(runtime_tests,'_runtime_with_real_executor',attach)
    runtime_tests.test_gradient_strong_signal_uses_linked_execution_refresh_before_open(
        monkeypatch,tmp_path,edge,submits)
    events=drain(writer)
    assert len({e['trace_id'] for e in events})==1
    stages=[e['stage'] for e in events]
    for stage in ('cycle_start','thresholds_evaluated','strategy_decision','signal_confirmation',
                  'strong_refresh_requested','strong_refresh_returned','execution_confirmation',
                  'pre_submission_gate','cycle_end'):
        assert stage in stages
    assert stages.count('executor_open_requested')==submits
    assert stages.count('executor_open_returned')==submits
    assert all(e['orders_attempted_by_auditor'] is False for e in events)
    thresholds=next(e for e in events if e['stage']=='thresholds_evaluated')
    assert thresholds['data']['crossings']['long_var_short_lighter']['crossed_tiers']==[1,2,3,4]
    end=events[-1]['data']
    assert end['state']['status']==('open' if submits else 'flat')
    if submits:
        returned=next(e for e in events if e['stage']=='executor_open_returned')
        assert returned['data']['executions']==[]  # No invented fills from a fake executor return.


def test_broken_auditor_never_retries_or_changes_a_real_strategy_decision(tmp_path,monkeypatch):
    factory=runtime_tests._runtime_with_real_executor
    class Broken:
        cycles=itertools.count(1)
        def emit(self,*args,**kwargs):raise OSError('audit disk unavailable')
    def attach(*args,**kwargs):
        runtime=factory(*args,**kwargs);runtime.decision_audit=Broken();return runtime
    monkeypatch.setattr(runtime_tests,'_runtime_with_real_executor',attach)
    runtime_tests.test_gradient_strong_signal_uses_linked_execution_refresh_before_open(
        monkeypatch,tmp_path,'55',1)


def test_exception_and_cancellation_identity_survive_audit():
    writer=AuditWriter(Path('unused'),name='runtime',run_id='r',start=False)
    exc=asyncio.CancelledError()
    class Runtime:
        run_id='r';decision_audit=writer
        @audited_cycle('test')
        async def cycle(self,row):raise exc
    with pytest.raises(asyncio.CancelledError) as raised:
        asyncio.run(Runtime().cycle({'sample_id':'s'}))
    assert raised.value is exc
    events=drain(writer)
    assert events[-1]['data']['error']['error_type']=='CancelledError'


def test_queue_is_nonblocking_and_reports_loss(tmp_path,monkeypatch):
    writer=AuditWriter(tmp_path,name='runtime',run_id='r',queue_size=1,start=False)
    assert writer.emit('one',{})
    assert writer.emit('two',{}) is False
    assert writer.stats()['dropped']==1 and writer.stats()['coverage_complete'] is False
    def nonblocking(item,block=True,timeout=None):
        assert block is False
        raise Full
    monkeypatch.setattr(writer.queue,'put',nonblocking)
    assert writer.emit('three',{}) is False


def test_rotation_keeps_complete_json_and_bounded_size(tmp_path):
    writer=AuditWriter(tmp_path,name='runtime',run_id='r',max_bytes=1200,backups=2)
    for n in range(40):writer.emit('event',{'n':n,'value':'a'*30})
    until=time.monotonic()+5
    while not writer.queue.empty() and time.monotonic()<until:time.sleep(.01)
    writer.close()
    assert writer.written==40 and writer.write_errors==0
    files=list(tmp_path.glob('runtime.jsonl*'))
    assert len(files)<=3 and sum(p.stat().st_size for p in files)<=3600
    for p in files:
        for line in p.read_text().splitlines():assert json.loads(line)['run_id']=='r'
    status=json.load(open(tmp_path/'runtime.health.json'))
    assert status['status']=='stopped' and status['alive'] is False


def test_writer_io_failure_is_visible_not_a_trading_exception(tmp_path,monkeypatch):
    from tools.lib import wti_decision_audit as audit
    def denied(*_args,**_kwargs):raise OSError('disk full')
    monkeypatch.setattr(audit.RotatingFileHandler,'_open',denied)
    writer=AuditWriter(tmp_path,name='runtime',run_id='r')
    writer.emit('record',{})
    until=time.monotonic()+3
    while writer.write_errors==0 and time.monotonic()<until:time.sleep(.01)
    writer.close()
    assert writer.write_errors>0 and not writer.stats()['coverage_complete']


def test_credentials_are_not_serialized_and_unknown_legs_stay_unknown():
    raw={'action_id':'a','kind':'open','legs':{'variational':{
        'submission_started':True,'request':{'side':'buy','qty':'0.2','private_key':'secret'},
        'result':{'classification':'unknown','terminal_confirmed':False,'token':'secret'}}}}
    source={'pending_action':raw,'execution_history':[]}
    before=deepcopy(source)
    evidence=execution_evidence(source,None)
    assert evidence[0]['legs']['variational']['result']['classification']=='unknown'
    assert state_evidence(source)['pending_execution_evidence'][0]['action_id']=='a'
    resolved=state_evidence({'execution_history':[{'action_id':'a','kind':'open','outcome':'filled'}]})
    assert resolved['latest_execution']['action_id']=='a' and not resolved['pending_execution_evidence']
    assert 'secret' not in json.dumps(clean(evidence)) and source==before
    assert 'secret' not in json.dumps(clean({'headers':{'token':'secret'},'value':'Bearer secret'}))
    assert safe_error(RuntimeError('HTTP 503 token=secret'))=={'error_type':'RuntimeError','http_status':503}
    from tools.lib.wti_execution import WTIAccountSnapshotUnavailable
    error=WTIAccountSnapshotUnavailable(phase='entry_preflight',retryable=True,
        diagnostic_code='service_unavailable',operation='positions',http_status=503)
    assert safe_error(error)['phase']=='entry_preflight' and safe_error(error)['http_status']==503


class Captured:
    def __init__(self):self.events=[]
    def emit(self,stage,value,**kwargs):self.events.append((stage,value,kwargs));return True
    def stats(self):return {'written':len(self.events)}


def sources(tmp_path):
    logs=tmp_path/'log';logs.mkdir()
    audit=logs/'audit';audit.mkdir()
    (logs/'wti_live_risk_health.json').write_text(json.dumps({
        'run_id':'r1','updated_at':'2026-10-29T12:00:00Z','last_processed_sample_id':'s1',
        'last_action':{'action':'hold','reason':'below_threshold'}}))
    (logs/'wti_live_state.json').write_text(json.dumps({'status':'flat','execution_history':[]}))
    folder=logs/'oil_swap/basis_samples/WTI';folder.mkdir(parents=True)
    sample=folder/'2026-10-29.jsonl';sample.write_text(json.dumps({'sample_id':'s1','sample_kind':'baseline'})+'\n')
    return logs,audit,sample


def test_observer_is_read_only_deduplicates_restart_and_never_invents_decision_stages(tmp_path,monkeypatch):
    def forbidden(*args,**kwargs):raise AssertionError('network forbidden')
    monkeypatch.setattr('socket.create_connection',forbidden)
    logs,audit,sample=sources(tmp_path)
    before={p:p.read_bytes() for p in (sample,logs/'wti_live_state.json',logs/'wti_live_risk_health.json')}
    writer=Captured();observer=Observer(logs,audit,writer)
    observer.poll();count=len(writer.events);observer.poll()
    assert len(writer.events)==count
    second=Captured();Observer(logs,audit,second).poll()
    assert not second.events
    assert all(e[2]['evidence']=='observed_only' for e in writer.events)
    assert not any('confirmation'==e[0] or 'submitted'==e[0] for e in writer.events)
    assert all(p.read_bytes()==b for p,b in before.items())
    status=json.load(open(audit/'observer.status.json'))
    assert status['exact_runtime_audit_active'] is False
    assert status['activation']=='exact_hooks_pending_planned_runtime_restart'


def test_partial_sample_is_not_consumed_or_duplicated(tmp_path):
    logs,audit,sample=sources(tmp_path);writer=Captured();observer=Observer(logs,audit,writer)
    observer.poll()
    with sample.open('a') as f:f.write('{"sample_id":"s2"')
    observer.poll();assert not any(e[1].get('sample_id')=='s2' for e in writer.events)
    with sample.open('a') as f:f.write('}\n')
    observer.poll();observer.poll()
    assert sum(e[1].get('sample_id')=='s2' for e in writer.events)==1


def test_source_rotation_is_an_explicit_gap_not_a_flat_or_recovery_assumption(tmp_path):
    logs,audit,sample=sources(tmp_path);writer=Captured();observer=Observer(logs,audit,writer)
    observer.poll();sample.unlink();sample.write_text('{"sample_id":"replacement"}\n')
    observer.poll()
    assert any(e[0]=='source_discontinuity' for e in writer.events)
    assert not any(e[0]=='recovered' for e in writer.events)


def test_crossing_capture_is_pure_and_handles_missing_thresholds():
    sample={'long_edge_bps':'84.85','short_edge_bps':'-85'}
    th={'long_var_short_lighter':tuple(map(str,('81','82','83','84','85')))}
    before=deepcopy((sample,th));value=crossing_evidence(sample,th)
    assert value['long_var_short_lighter']['crossed_tiers']==[1,2,3,4]
    assert value['short_var_long_lighter']['crossing_unverified'] is True
    assert (sample,th)==before
    writer=AuditWriter(Path('unused'),name='runtime',run_id='r',start=False)
    runtime=SimpleNamespace(decision_audit=writer,run_id='r')
    emit(runtime,'sample_admission_rejected',sample={'sample_id':'ingest-id'})
    assert drain(writer)[0]['trace_id']=='r:ingest:ingest-id'
