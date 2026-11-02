from copy import deepcopy
from datetime import datetime,timedelta,timezone,date
from decimal import Decimal as D
from types import SimpleNamespace
import pytest

from tools.lib.wti_equity_reporting import (
    snapshot,retry_snapshot,attribute_close,EquityJournal,fixed_trade_message,
    fixed_daily_message,record_daily_snapshot,daily_equity_period,market_tier,
)
from tools.lib.runtime_files import read_json

NOW=datetime.now(timezone.utc)
stamp=lambda seconds=0:(NOW+timedelta(seconds=seconds)).isoformat()


@pytest.fixture(autouse=True)
def current_test_clock(monkeypatch):
    monkeypatch.setitem(globals(),'NOW',datetime.now(timezone.utc))


def raw_snapshot():
    return {'observed_at':stamp(),'read_started_at':stamp(-.1),
        'variational_portfolio':{'result':{'balance':'145.932741000001578','upnl':'1.163751000001578'}},
        'variational_positions':{'ok':True,'result':{'positions':[
            {'position_info':{'qty':'6.141','company':'company','instrument':{'underlying':'USOILP','instrument_type':'swap'}},
             'maintenance_margin':'63.558736','value':'635.587359','upnl':'1.145328000001578'}]}},
        'lighter_positions':{'code':200,'accounts':[{
            'total_asset_value':'127.73356000000001','collateral':'129.045894',
            'cross_maintenance_margin_requirement':'19.204688','account_index':12,
            'positions':[{'market_id':145,'position':'6.141','sign':-1,
                          'unrealized_pnl':'-1.312334','position_value':'640.156263'}]}]}}


def context(reverse=False):
    sign=-1 if reverse else 1
    direction='short_var_long_lighter' if reverse else 'long_var_short_lighter'
    def event(key,kind,prices):
        legs={}
        for venue,price in zip(('variational','lighter'),prices):
            buy=(venue=='variational')==(kind=='open')
            if reverse:buy=not buy
            legs[venue]={'request':{'side':'buy' if buy else 'sell','submitted_at':stamp(.1 if venue=='variational' else .5)},
                         'response_received_at':stamp(.4 if venue=='variational' else .8),
                         'result':{'classification':'filled','terminal_confirmed':True,'filled_qty':'1','average_price':str(price)}}
        return {'action_id':key*32,'kind':kind,'direction':direction,'multiplier':'1','legs':legs}
    a,b=event('a','open',(100,102)),event('b','open',(110,112))
    def position(qty,v,l,layers):
        return {'direction':direction,'var_qty':str(qty),'lighter_qty':str(qty),
                'var_entry_price':str(v),'lighter_entry_price':str(l),
                'expected_var_qty':str(qty*sign),'expected_lighter_qty':str(-qty*sign),
                'layers':layers,'gradient_tier':1}
    la={'entry_execution_id':a['action_id'],'gradient_tier':1};lb={'entry_execution_id':b['action_id'],'gradient_tier':2}
    pending=event('c','close',(116,117))
    pending.update(previous_position=position(2,105,107,[la,lb]),close_position=position(1,100,102,[la]),
        remaining_position=position(1,110,112,[lb]),intent_at=stamp(),decision={'exit_gradient_tier':1,'reason':'net_target_reached'})
    state={'gradient_pilot':{'pilot_id':'pilot','layers_opened':[{'action_id':a['action_id']},{'action_id':b['action_id']}]},
           'execution_history':[a,b]}
    def snap(v_equity,l_equity,v_upnl,l_upnl,qty,seconds):
        return {'status':'verified','observed_at':stamp(seconds),'read_started_at':stamp(seconds-.1),
                'total_equity':str(v_equity+l_equity),'venues':{
                    'variational':{'equity':str(v_equity),'upnl':str(v_upnl),'qty':str(qty*sign),
                                   'maintenance_pct':'3.123456789123456789','gross_notional':'200'},
                    'lighter':{'equity':str(l_equity),'upnl':str(l_upnl),'qty':str(-qty*sign),
                               'maintenance_pct':'2','gross_notional':'204'}}}
    before=snap(200+20*sign,200-18*sign,20*sign,-18*sign,2,-.5)
    after=snap(200+46*sign,200-33*sign,35*sign,-23*sign,1,1.1)
    return state,pending,before,after


def test_actual_schema_var_nav_already_includes_upnl_and_lighter_mark_precision():
    result=snapshot(raw_snapshot())
    assert result['status']=='verified'
    assert result['venues']['variational']['equity']=='145.932741000001578'
    assert result['venues']['variational']['upnl']=='1.163751000001578'
    assert result['venues']['lighter']['equity']=='127.73356000000001'
    assert result['total_equity']=='273.666301000001588'


def test_explicit_flat_positions_can_prove_zero_upnl_but_open_position_cannot():
    raw=raw_snapshot();raw['variational_portfolio']['result'].pop('upnl')
    assert snapshot(raw)['status']=='unverified'
    raw['variational_positions']['result']['positions']=[]
    assert snapshot(raw)['status']=='verified'


@pytest.mark.parametrize('change',[
    lambda r:r['variational_portfolio']['result'].pop('balance'),
    lambda r:r['variational_portfolio']['result'].update(balance='NaN'),
    lambda r:r['variational_positions'].update(ok=False),
    lambda r:r['lighter_positions']['accounts'][0].update(cross_maintenance_margin_requirement='-1'),
    lambda r:r.update(observed_at=stamp(-20)),
    lambda r:r.update(read_started_at=stamp(-30)),
    lambda r:r['lighter_positions']['accounts'][0].update(collateral='1'),
    lambda r:r['lighter_positions']['accounts'][0]['positions'][0].update(market_id=0),
    lambda r:r['variational_positions']['result']['positions'][0]['position_info']['instrument'].update(underlying='ETH'),
])
def test_incomplete_or_other_asset_snapshot_never_becomes_equity_proof(change):
    raw=raw_snapshot();change(raw)
    assert snapshot(raw)['status']=='unverified'


@pytest.mark.parametrize('success',[1,2,None])
def test_exactly_one_snapshot_retry(success):
    calls=[]
    def read():
        calls.append(1)
        if len(calls)!=success:raise TimeoutError
        return raw_snapshot()
    result=retry_snapshot(read)
    assert len(calls)==(1 if success==1 else 2)
    assert result['attempts']==len(calls)
    assert result['status']==('verified' if success else 'unverified')


@pytest.mark.parametrize('reverse',[False,True])
def test_tier_attribution_removes_other_tiers_mark_movement_and_exchange_average_basis(reverse):
    state,pending,before,after=context(reverse)
    unchanged=deepcopy(state)
    result=attribute_close(state,pending,before,after)
    assert result['status']=='verified',result
    expected=D(-1 if reverse else 1)
    assert D(result['actual_pnl_usd'])==expected
    assert D(result['actual_pnl_bps'])==expected/D(101)*10000
    assert abs(D(after['total_equity'])-D(before['total_equity']))==11
    assert state==unchanged


@pytest.mark.parametrize('failure',['transfer','basis','qty','old','late','missing_fill','not_terminal'])
def test_unverified_attribution_is_never_pro_rata_allocated(failure):
    state,pending,before,after=context()
    if failure=='transfer':after['venues']['variational']['equity']='256'
    elif failure=='basis':state['execution_history'].pop()
    elif failure=='qty':after['venues']['variational']['qty']='2'
    elif failure=='old':before['observed_at']=stamp(-120)
    elif failure=='late':before['observed_at']=stamp(.6)
    elif failure=='missing_fill':pending['legs']['variational']['result'].pop('average_price')
    else:pending['legs']['variational']['result']['terminal_confirmed']=False
    assert attribute_close(state,pending,before,after)['status']=='unverified'


def test_journal_retry_is_bounded_and_survives_reentry_without_order_submission(tmp_path):
    calls=[]
    gateway=SimpleNamespace(latest_reporting_snapshot={'status':'unverified'},read_account=lambda:calls.append('read'))
    journal=EquityJournal(tmp_path);state,pending,_,_=context()
    journal.capture('before',state,pending,gateway)
    journal.capture('before',state,pending,gateway)
    record=read_json(tmp_path/(pending['action_id']+'.json'))
    assert calls==['read'] and record['before_attempts']==2
    assert record['before']['status']=='unverified'


def test_exact_frozen_close_template_and_unrounded_values():
    state,pending,before,after=context()
    execution={**pending,'outcome':'filled','completed_at':stamp(1.2)}
    record={'before':before,'after':after,'selected':pending['close_position'],
            'remaining':pending['remaining_position'],'market_tier':2,
            'attribution':attribute_close(state,pending,before,after)}
    assert fixed_trade_message(execution,state,{},record).splitlines()==[
        '[Var/Lighter] 档位平仓完成','资产：WTI｜平仓档位：1 / 5','本次平仓子单：1｜剩余子单：1',
        '实际盈亏：1 U',f'实际收益：{D(1)/101*10000} bps','当前市场档位：2 / 5',
        'Variational 权益：246 U','Lighter 权益：167 U',
        'Variational 保证金使用率：3.123456789123456789%','Lighter 保证金使用率：2%',
        '','平仓原因：收益达标']


def test_failed_snapshot_keeps_fixed_fields_and_reports_unverified_not_zero():
    state,pending,_,_=context()
    text=fixed_trade_message(pending,state,{},{}).splitlines()
    assert '实际盈亏：未核实' in text and '实际收益：未核实' in text
    assert 'Variational 权益：未核实' in text
    assert len(text)==12 and not any('费用' in line or 'ID' in line for line in text)


def test_open_template_has_only_user_frozen_fields():
    state,pending,_,after=context()
    e=state['execution_history'][0];e['decision']={'gradient_tier':2}
    record={'after':after,'before_children':1,'previous_position':{'var_qty':'1','var_entry_price':'110'}}
    lines=fixed_trade_message(e,state,{},record).splitlines()
    assert len(lines)==10
    assert lines[2]=='当前档位：2 / 5' and lines[3]=='已开子单：2 / 16'
    assert lines[4]=='单边总仓位：210 U'
    assert not any('动作ID' in x or '手续费' in x or '成交价' in x for x in lines)


def daily_state():
    return {'daily_capitals':{
        '2026-10-29':{'capital_usd':'390','observed_at':'2026-10-28T16:00:01+00:00','day_open_verified':True},
        '2026-10-30':{'capital_usd':'400','observed_at':'2026-10-29T16:00:01+00:00','day_open_verified':True},
        '2026-10-31':{'capital_usd':'425','observed_at':'2026-10-30T16:00:01+00:00','day_open_verified':True}}}


def test_daily_is_equity_delta_not_tier_sum_and_excludes_external_cash_without_subtracting_costs_again():
    reporting=daily_state()
    flows={'status':'verified','start':'2026-10-28T16:00:01+00:00','end':'2026-10-30T16:00:01+00:00',
           'flows':[{'at':'2026-10-30T12:00:00+00:00','net_inflow_usd':'20'}]}
    text=fixed_daily_message({'asset':'WTI'},date(2026,10,30),reporting,flows)
    assert text.splitlines()==['[Var/Lighter] 北京时间每日收益','日期：2026-10-30｜资产：WTI',
        '平仓子单：0 笔','双平台四腿成交量：0 U','','确认净盈亏：5 U','当日收益率：1.2500%',
        '当日简单年化：456.2500%','','累计账单已核实净收益：15 U','累计四腿成交量：0 U',
        f'累计收益率：{D(15)/390*100}%累计简单年化：{D(15)/390*100*365/2}%','','最新双边权益：未核实']


@pytest.mark.parametrize('missing',['opening','closing','flows','flow_coverage'])
def test_daily_missing_evidence_never_becomes_zero(missing):
    reporting=daily_state();flows={'status':'verified','start':'2026-10-28T16:00:01+00:00','end':'2026-10-30T16:00:01+00:00','flows':[]}
    if missing=='opening':reporting['daily_capitals']['2026-10-30']['day_open_verified']=False
    elif missing=='closing':reporting['daily_capitals'].pop('2026-10-31')
    elif missing=='flows':flows={}
    else:flows['end']='2026-10-30T12:00:00+00:00'
    assert '确认净盈亏：未核实' in fixed_daily_message({},date(2026,10,30),reporting,flows)


def test_cumulative_cannot_silently_reset_to_first_recent_snapshot():
    reporting=daily_state()
    receipt={'status':'verified','start':'2026-10-28T16:00:01+00:00','end':'2026-10-30T16:00:01+00:00','flows':[]}
    state={'execution_history':[{'kind':'open','outcome':'filled','action_id':'old',
                                 'intent_at':'2026-10-23T12:00:00+00:00'}]}
    text=fixed_daily_message(state,date(2026,10,30),reporting,receipt)
    assert '确认净盈亏：25 U' in text and '累计账单已核实净收益：未核实' in text


def test_snapshot_daily_boundary_and_staleness():
    record={};raw=raw_snapshot();normal=snapshot(raw)
    now=datetime(2026,10,30,16,0,5,tzinfo=timezone.utc)
    normal['observed_at']=now.isoformat();record_daily_snapshot(record,normal,now=now)
    assert record['daily_capitals']['2026-10-31']['day_open_verified'] is True
    old=deepcopy(record);normal['total_equity']='999';record_daily_snapshot(record,normal,now=now+timedelta(seconds=16))
    assert record==old
    assert daily_equity_period(record,date(2026,10,31)) is None


@pytest.mark.parametrize('edge,tier',[('29',0),('30',1),('45',2),('76',5)])
def test_market_tier_is_observation_only(edge,tier):
    assert market_tier({'long_edge_bps':edge},{'long_var_short_lighter':[30,45,55,65,75]},'long_var_short_lighter')==tier


def test_real_executor_hooks_keep_state_and_order_identity_during_restart(tmp_path):
    from test_wti_execution import SimulatedWTIGateway,_executor,_open,_sample,_session
    class FinancialGateway(SimulatedWTIGateway):
        def read_account(self):
            raw=super().read_account()
            self.latest_reporting_snapshot={'status':'verified','observed_at':datetime.now(timezone.utc).isoformat(),
                'read_started_at':datetime.now(timezone.utc).isoformat(),'total_equity':'400','venues':{
                    venue:{'equity':'200','upnl':'0','qty':str(qty),'gross_notional':str(abs(qty)*D('70.05')),
                           'maintenance_pct':'0'} for venue,qty in [('variational',self.var_qty),('lighter',self.lighter_qty)]}}
            return raw
    gateway=FinancialGateway();executor,_=_executor(tmp_path,gateway)
    executor.reporting_observer=EquityJournal(tmp_path/'proofs')
    _open(executor)
    reads=gateway.read_calls
    assert len(gateway.requests)==2
    restarted,_=_executor(tmp_path,gateway)
    restarted.reporting_observer=EquityJournal(tmp_path/'proofs')
    restarted.reconcile(sample=_sample(),now=datetime.now(timezone.utc))
    assert len(gateway.requests)==2
    closed=restarted.close(sample=_sample(),session=_session(),now=datetime.now(timezone.utc))
    assert len(gateway.requests)==4 and closed['status']=='flat' and not closed['pending_action']
    assert len({q['client_order_id'] for q in gateway.requests})==4
    proof=read_json(tmp_path/'proofs'/(closed['execution_history'][-1]['action_id']+'.json'))
    assert proof['attribution']['status']=='verified',proof
    assert D(proof['attribution']['actual_pnl_usd'])==0
    assert proof['before_attempts']==proof['after_attempts']==1
    assert gateway.read_calls==reads+3


def test_reporting_exception_does_not_change_real_executor_orders_or_risk(tmp_path):
    from test_wti_execution import _executor,_open
    executor,gateway=_executor(tmp_path)
    class BrokenObserver:
        def capture(self,*args):raise OSError('report disk unavailable')
    executor.reporting_observer=BrokenObserver()
    opened=_open(executor)
    assert opened['status']=='open' and not opened['pending_action']
    assert len(gateway.requests)==2


@pytest.mark.parametrize('mode',['fresh','retry_succeeds','retry_fails'])
def test_midnight_closed_market_snapshot_retries_once_without_polling_or_restart_duplicates(tmp_path,monkeypatch,mode):
    import tools.lib.wti_reporting as reporting
    import tools.lib.wti_equity_reporting as equity
    now=datetime(2026,10,30,16,0,15,tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return now.astimezone(tz) if tz else now.replace(tzinfo=None)
    monkeypatch.setattr(reporting,'datetime',Clock);monkeypatch.setattr(equity,'datetime',Clock)
    snap={'status':'verified','observed_at':now.isoformat(),'total_equity':'273.756447999999252'}
    current=[snap if mode=='fresh' else None];calls=[]
    def refresh():
        calls.append(1)
        if mode=='retry_fails':raise TimeoutError
        current[0]=snap
    args={'snapshot_reader':lambda:current[0],'snapshot_refresher':refresh}
    path=tmp_path/'outbox.json';notifier=SimpleNamespace(send_now=lambda _:pytest.fail('no trade to report'))
    reporter=reporting.TradeReporter(path,{},notifier,**args)
    reporter.poll({});reporter.poll({})
    reporting.TradeReporter(path,{},notifier,**args).poll({})
    assert len(calls)==(0 if mode=='fresh' else 1)
    saved=read_json(path)
    if mode=='retry_fails':
        assert not saved.get('daily_capitals') and saved['boundary_snapshot_attempts']['2026-10-31']==2
    else:assert saved['daily_capitals']['2026-10-31']['capital_usd']=='273.756447999999252'


@pytest.mark.parametrize('mode',['cached','retry_succeeds','retry_fails'])
def test_daily_footer_reuses_existing_health_once_without_network_or_mutating_outbox(tmp_path,mode):
    from tools.daily_pnl_report import reporting_with_equity_retry
    cache={'latest_equity':{'observed_at':stamp(0 if mode=='cached' else -20),'equity_usd':'273.123456789123456789'}}
    health={'asset':'WTI','risk_data_complete':True,'startup_reconciled':True,
            'account_snapshot_observed_at':stamp(0 if mode!='retry_fails' else -20),
            'variational_equity_usd':'145.932741000001578','lighter_equity_usd':'127.73356000000001'}
    calls=[];before=deepcopy(cache)
    def reader(path):
        calls.append(path.name)
        return health if path.name=='wti_live_risk_health.json' else cache
    result=reporting_with_equity_retry(tmp_path/'wti_live_state.telegram.json',reader=reader,now=NOW)
    assert len(calls)==(1 if mode=='cached' else 2) and cache==before
    expected='273.666301000001588' if mode=='retry_succeeds' else '273.123456789123456789'
    assert result['latest_equity']['equity_usd']==expected
