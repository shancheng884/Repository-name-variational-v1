"""Independent account-equity attribution. Never participates in trading gates."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
import re

from tools.lib.runtime_files import read_json, write_json_atomic

D = Decimal
UNKNOWN = '未核实'
BEIJING = timezone(timedelta(hours=8))
ROUNDING_TOLERANCE = D('0.000001')


def dec(value):
    if value is None or isinstance(value, bool):
        raise ValueError('report_decimal_missing')
    result = D(str(value))
    if not result.is_finite():
        raise ValueError('report_decimal_nonfinite')
    return result


def at(value):
    result = datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if result.tzinfo is None:
        raise ValueError('report_clock_unverified')
    return result.astimezone(timezone.utc)


def snapshot(raw):
    """Normalize the actual portfolio/account fields, retaining decimal precision."""
    try:
        observed,started = at(raw['observed_at']),at(raw['read_started_at'])
        if not 0 <= (observed-started).total_seconds() <= 15:
            raise ValueError('report_snapshot_window_unverified')
        portfolio = raw['variational_portfolio']['result']
        portfolio = portfolio.get('pool_portfolio_result',portfolio)
        rows=raw['variational_positions']
        if rows.get('ok') is not True or raw['lighter_positions'].get('code')!=200:
            raise ValueError('report_account_response_unverified')
        for _ in range(5):
            if isinstance(rows,list):break
            rows=rows.get('positions',rows.get('result',rows.get('data')))
        if not isinstance(rows,list):raise ValueError('report_var_positions_missing')
        var_qty,vm,vvalue=D(0),D(0),D(0)
        company=set()
        for row in rows:
            info=row['position_info'];q=dec(info['qty']);instrument=info['instrument']
            if not q:continue
            if instrument.get('underlying')!='USOILP' or instrument.get('instrument_type')!='swap':
                raise ValueError('report_other_asset_exposure')
            var_qty+=q;vm+=dec(row['maintenance_margin']);vvalue+=abs(dec(row['value']))
            company.add(info['company'])
        account=raw['lighter_positions']['accounts'][0]
        lqty,lupnl,lvalue=D(0),D(0),D(0)
        for row in account['positions']:
            q=dec(row['position'])*dec(row['sign']);lupnl+=dec(row['unrealized_pnl'])
            if not q:continue
            if row.get('market_id')!=145:raise ValueError('report_other_asset_exposure')
            lqty+=q;lvalue+=abs(dec(row['position_value']))
        vnav=dec(portfolio['balance'])
        vupnl=dec(portfolio.get('upnl','0' if not var_qty else None))
        lnav=dec(account['total_asset_value']);collateral=dec(account['collateral'])
        lm=dec(account['cross_maintenance_margin_requirement'])
        if abs(lnav-collateral-lupnl)>ROUNDING_TOLERANCE or min(vnav,lnav)<=0 or min(vm,lm)<0:
            raise ValueError('report_equity_identity_unverified')
        return {'status':'verified','observed_at':observed.isoformat(),'read_started_at':started.isoformat(),
                'company_ids':sorted(company),'lighter_account_index':account.get('account_index'),
                'venues':{
                    'variational':{'equity':str(vnav),'upnl':str(vupnl),'qty':str(var_qty),
                                   'maintenance_pct':str(vm/vnav*100),'gross_notional':str(vvalue)},
                    'lighter':{'equity':str(lnav),'upnl':str(lupnl),'qty':str(lqty),
                               'maintenance_pct':str(lm/lnav*100),
                               'gross_notional':str(lvalue)}},
                'total_equity':str(vnav+lnav)}
    except (KeyError,TypeError,ValueError,ArithmeticError,AttributeError,IndexError) as exc:
        return {'status':'unverified','reason':str(exc) if isinstance(exc,ValueError) else type(exc).__name__}


def retry_snapshot(reader):
    """Two attempts total. No trading action may be retried by this helper."""
    result={'status':'unverified','reason':'report_snapshot_unavailable'}
    for attempt in range(2):
        try:
            value=reader()
            result=value if isinstance(value,dict) and 'status' in value else snapshot(value)
        except Exception as exc:
            result={'status':'unverified','reason':type(exc).__name__}
        if result.get('status')=='verified':return {**result,'attempts':attempt+1}
    return {**result,'attempts':2}


def average_basis(state, pending, *, after):
    """Replay the current gradient episode's exchange average-cost basis."""
    pilot=state.get('gradient_pilot') or {}
    ids={x['action_id'] for x in pilot.get('layers_opened') or []}
    ids.update(x['entry_execution_id'] for x in pilot.get('layers_closed') or [])
    position=pending.get('previous_position') or state.get('open_position') or {}
    ids.update(x['entry_execution_id'] for x in position.get('layers') or [])
    closes={x['exit_execution_id'] for x in (state.get('closed_positions') or {}).values()
            if x.get('gradient_pilot_id')==pilot.get('pilot_id')}
    history=[e for e in state.get('execution_history',[]) if e.get('action_id') in ids|closes
             and e.get('action_id')!=pending['action_id']]
    if after:history.append(pending)
    basis={v:{'qty':D(0),'price':D(0)} for v in ('variational','lighter')}
    for event in history:
        for venue in basis:
            leg=event['legs'][venue];fill=leg['result']
            if fill.get('classification')!='filled' or fill.get('terminal_confirmed') is not True:
                raise ValueError('report_fill_identity_unverified')
            q=dec(fill['filled_qty'])*(1 if leg['request']['side']=='buy' else -1)
            price=dec(fill['average_price']);old=basis[venue];total=old['qty']+q
            if event['kind']=='open':
                if old['qty']*q<0:raise ValueError('report_direction_changed')
                old['price']=(abs(old['qty'])*old['price']+abs(q)*price)/abs(total)
            elif not old['qty'] or old['qty']*q>=0 or abs(q)>abs(old['qty']):
                raise ValueError('report_average_cost_unreconstructable')
            old['qty']=total
            if not total:old['price']=D(0)
    return basis


def attribute_close(state,pending,before,after):
    """Remove other tiers' mark movement; unexplained cash is never allocated."""
    try:
        if before.get('status')!='verified' or after.get('status')!='verified':
            raise ValueError('report_snapshot_unverified')
        first=min(at(x['request']['submitted_at']) for x in pending['legs'].values())
        if not 0 <= (first-at(before['observed_at'])).total_seconds() <= 15:
            raise ValueError('report_before_snapshot_late_or_stale')
        if dec(pending['multiplier'])!=1:raise ValueError('report_multiplier_unverified')
        latest=max(at(x['response_received_at']) for x in pending['legs'].values())
        if at(after['read_started_at'])<latest:raise ValueError('report_after_snapshot_early')
        if (at(after['observed_at'])-at(before['observed_at'])).total_seconds()>60:
            raise ValueError('report_close_window_too_wide')
        selected=pending.get('close_position') or pending['previous_position']
        previous=pending['previous_position'];remaining=pending.get('remaining_position') or {}
        if selected.get('direction') not in {'long_var_short_lighter','short_var_long_lighter'}:
            raise ValueError('report_direction_unknown')
        before_basis=average_basis(state,pending,after=False)
        after_basis=average_basis(state,pending,after=True)
        total,gross,denominator=D(0),D(0),D(0)
        evidence={}
        for venue,key in (('variational','var'),('lighter','lighter')):
            b,a=before['venues'][venue],after['venues'][venue]
            qb,qa=dec(b['qty']),dec(a['qty'])
            if qb!=dec(previous['expected_'+key+'_qty']) or qa!=dec(remaining.get('expected_'+key+'_qty','0')):
                raise ValueError('report_snapshot_position_mismatch')
            if before_basis[venue]['qty']!=qb or after_basis[venue]['qty']!=qa:
                raise ValueError('report_cost_basis_position_mismatch')
            q=dec(selected[key+'_qty'])*(1 if qb>0 else -1)
            if qb-qa!=q:raise ValueError('report_selected_quantity_mismatch')
            entry=dec(selected[key+'_entry_price'])
            mark_before=before_basis[venue]['price']+dec(b['upnl'])/qb
            selected_upnl=q*(mark_before-entry)
            other_change=D(0)
            if qa:
                mark_after=after_basis[venue]['price']+dec(a['upnl'])/qa
                other_change=qa*(mark_after-mark_before)
            fill=pending['legs'][venue]['result']
            if fill.get('classification')!='filled' or dec(fill['filled_qty'])!=abs(q):
                raise ValueError('report_terminal_fill_unverified')
            realised=q*(dec(fill['average_price'])-entry)
            delta=dec(a['equity'])-dec(b['equity'])
            attributed=delta+selected_upnl-other_change
            # A transfer, other cash flow or unexplained mark basis must not become tier PnL.
            if abs(attributed-realised)>ROUNDING_TOLERANCE:
                raise ValueError('report_unexplained_cash_or_basis_residual')
            total+=attributed;gross+=realised;denominator+=abs(q)*entry/2
            evidence[venue]={'equity_change':str(delta),'selected_pre_upnl':str(selected_upnl),
                             'other_upnl_change':str(other_change),'attributed':str(attributed)}
        return {'status':'verified','actual_pnl_usd':str(total),'actual_pnl_bps':str(total/denominator*10000),
                'fill_crosscheck_usd':str(gross),'venues':evidence,'method':'equity_change_minus_other_tiers_v1'}
    except (KeyError,TypeError,ValueError,ArithmeticError,IndexError) as exc:
        return {'status':'unverified','reason':str(exc) if isinstance(exc,ValueError) else type(exc).__name__}


class EquityJournal:
    """Separate reporting files; failures never change order or risk state."""
    def __init__(self,directory):
        self.directory=Path(directory)
        self.market_tier=None

    def path(self,action_id):
        if not re.fullmatch(r'[0-9a-f]{32}',str(action_id)):
            raise ValueError('report_action_id_invalid')
        return self.directory/(action_id+'.json')

    def capture(self,phase,state,pending,gateway):
        path=self.path(pending['action_id']);record=read_json(path)
        if path.exists() and not record:raise ValueError('report_journal_invalid_preserve_for_review')
        if record.get(phase,{}).get('status')=='verified':return
        if phase=='before':
            record.update(action_id=pending['action_id'],kind=pending['kind'],market_tier=self.market_tier,
                          previous_position=deepcopy(pending.get('previous_position')),
                          selected=deepcopy(pending.get('close_position') or pending.get('previous_position')),
                          remaining=deepcopy(pending.get('remaining_position')),
                          before_children=len((pending.get('previous_position') or {}).get('layers') or []))
        latest=getattr(gateway,'latest_reporting_snapshot',None) or {'status':'unverified'}
        if latest.get('status')=='verified':
            age=(datetime.now(timezone.utc)-at(latest['observed_at'])).total_seconds()
            if not -1<=age<=15:latest={'status':'unverified','reason':'report_snapshot_stale'}
        attempts=record.get(phase+'_attempts',0)
        if not attempts:
            record[phase]=deepcopy(latest);attempts=1
        if record.get(phase,{}).get('status')!='verified' and attempts<2:
            try:
                gateway.read_account()
                record[phase]=deepcopy(getattr(gateway,'latest_reporting_snapshot',{}) or {'status':'unverified'})
            except Exception as exc:record[phase]={'status':'unverified','reason':type(exc).__name__}
            attempts=2
        record[phase+'_attempts']=attempts
        if phase=='after' and pending['kind']=='close':
            record['attribution']=attribute_close(state,pending,record.get('before',{}),record.get('after',{}))
            if (record['attribution'].get('reason')=='report_unexplained_cash_or_basis_residual'
                    and record['after_attempts']<2):
                try:
                    gateway.read_account()
                    record['after']=deepcopy(gateway.latest_reporting_snapshot)
                except Exception as exc:
                    record['after']={'status':'unverified','reason':type(exc).__name__}
                record['after_attempts']=2
                record['attribution']=attribute_close(state,pending,record.get('before',{}),record.get('after',{}))
        write_json_atomic(path,record)


def value(x,suffix=''):
    return UNKNOWN if x is None else str(x)+suffix


def market_tier(row,thresholds,direction):
    try:
        values=thresholds[direction]
        edge=dec(row['long_edge_bps' if direction=='long_var_short_lighter' else 'short_edge_bps'])
        if len(values)!=5 or any(x is None for x in values):return None
        return max([i+1 for i,x in enumerate(values) if edge>=dec(x)] or [0])
    except (KeyError,TypeError,ValueError,ArithmeticError):return None


def record_daily_snapshot(reporting,snap,*,now=None):
    if not isinstance(snap,dict) or snap.get('status')!='verified':return
    observed=at(snap['observed_at']);now=now or datetime.now(timezone.utc)
    if not -1<=(now-observed).total_seconds()<=15:return
    day=observed.astimezone(BEIJING).date().isoformat()
    local=observed.astimezone(BEIJING)
    capital={'observed_at':observed.isoformat(),'capital_usd':snap['total_equity'],
             'day_open_verified':local.hour==0 and local.minute<2}
    days=reporting.setdefault('daily_capitals',{})
    old=days.get(day)
    if old is None or at(capital['observed_at'])<at(old['observed_at']):days[day]=capital
    reporting['latest_equity']={'observed_at':observed.isoformat(),'equity_usd':snap['total_equity']}
    reporting['latest_financial_snapshot']=deepcopy(snap)


def daily_equity_period(reporting,day):
    try:
        daily=reporting.get('daily_capitals') or {}
        start=daily.get(day.isoformat()) or {};end=daily.get((day+timedelta(days=1)).isoformat()) or {}
        if not start.get('day_open_verified') or not end.get('day_open_verified'):return None
        for item,wanted in ((start,day),(end,day+timedelta(days=1))):
            clock=at(item['observed_at']).astimezone(BEIJING)
            if clock.date()!=wanted or clock.hour!=0 or clock.minute>=2:return None
        if dec(start['capital_usd'])<=0 or at(end['observed_at'])<=at(start['observed_at']):return None
        return start,end
    except (KeyError,TypeError,ValueError,ArithmeticError):return None


def flow_sum(receipt,start,end):
    if receipt.get('status')!='verified' or at(receipt['start'])>start or at(receipt['end'])<end:
        raise ValueError('report_cashflow_coverage_unverified')
    return sum((dec(x['net_inflow_usd']) for x in receipt['flows'] if start<at(x['at'])<=end),D(0))


def fixed_daily_message(state,day,reporting,receipt):
    from tools.lib.wti_reporting import closed_rounds,number
    rows=closed_rounds(state)
    dated=[(x,at(x['closed_at']).astimezone(BEIJING).date()) for x in rows if x.get('closed_at')]
    selected=[x for x,t in dated if t==day];cumulative=[x for x,t in dated if t<=day]
    def volume(items):
        values=[number(x.get('four_leg_volume_usd')) for x in items]
        return str(sum(values,D(0)))+' U' if all(x is not None for x in values) else UNKNOWN
    period=daily_equity_period(reporting,day)
    daily_pnl=daily_pct=annual=total=total_pct=total_annual=None
    try:
        if period:
            start,end=period
            daily_pnl=dec(end['capital_usd'])-dec(start['capital_usd'])-flow_sum(receipt,at(start['observed_at']),at(end['observed_at']))
            daily_pct=daily_pnl/dec(start['capital_usd'])*100;annual=daily_pct*365
            starts=[v for k,v in (reporting.get('daily_capitals') or {}).items()
                    if k<=day.isoformat() and v.get('day_open_verified')]
            first=min(starts,key=lambda x:at(x['observed_at']))
            entries=[x for x in state.get('execution_history',[]) if x.get('kind')=='open'
                     and x.get('outcome')=='filled']
            if any(at(x.get('intent_at') or x['completed_at'])<at(first['observed_at']) for x in entries):
                raise ValueError('report_historical_opening_equity_unverified')
            total=dec(end['capital_usd'])-dec(first['capital_usd'])-flow_sum(receipt,at(first['observed_at']),at(end['observed_at']))
            total_pct=total/dec(first['capital_usd'])*100
            elapsed=D(str((at(end['observed_at'])-at(first['observed_at'])).total_seconds()))/86400
            total_annual=total_pct*365/elapsed
    except (ValueError,KeyError,TypeError,ArithmeticError):pass
    latest=reporting.get('latest_equity') or {};equity=None
    try:
        if -1<=(datetime.now(timezone.utc)-at(latest['observed_at'])).total_seconds()<=15:
            equity=dec(latest['equity_usd'])
    except (ValueError,KeyError,TypeError,ArithmeticError):pass
    return '\n'.join(['[Var/Lighter] 北京时间每日收益',f'日期：{day.isoformat()}｜资产：WTI',
        f'平仓子单：{sum(len(x.get("layers") or []) or 1 for x in selected)} 笔',
        '双平台四腿成交量：'+volume(selected),'',
        '确认净盈亏：'+value(daily_pnl,' U'),'当日收益率：'+value(daily_pct,'%'),
        '当日简单年化：'+value(annual,'%'),'','累计账单已核实净收益：'+value(total,' U'),
        '累计四腿成交量：'+volume(cumulative),
        '累计收益率：'+value(total_pct,'%')+'累计简单年化：'+value(total_annual,'%'),'',
        '最新双边权益：'+value(equity,' U')])


def fixed_trade_message(execution,state,health,record):
    """User-frozen template: no IDs, fill details, fees, or additional lines."""
    from tools.lib.wti_reporting import closed_rounds,number
    from tools.lib.wti_sizing import equity_gradient_slot_caps
    position=state.get('open_position') or {};decision=execution.get('decision') or {}
    row=next((x for x in closed_rounds(state) if x.get('exit_execution_id')==execution['action_id']),{})
    snap=record.get('after') or {};venues=snap.get('venues') or {}
    v,l=venues.get('variational') or {},venues.get('lighter') or {}
    if snap.get('status')!='verified':v,l={},{}
    if execution['kind']=='open':
        tier=decision.get('gradient_tier');caps=equity_gradient_slot_caps(variational_equity_usd=number(v.get('equity')),
            lighter_equity_usd=number(l.get('equity')),child_notional_usd=D(20))
        count=record.get('before_children')
        count=count+1 if count is not None else None
        qty=number(execution.get('legs',{}).get('variational',{}).get('result',{}).get('filled_qty'))
        fill=number(execution.get('legs',{}).get('variational',{}).get('result',{}).get('average_price'))
        old=record.get('previous_position') or {}
        notional=(dec(old.get('var_qty','0'))*dec(old.get('var_entry_price','0'))+qty*fill) if qty is not None and fill is not None and 'before_children' in record else None
        direction={'long_var_short_lighter':'做多 Variational / 做空 Lighter',
                   'short_var_long_lighter':'做空 Variational / 做多 Lighter'}.get(execution.get('direction'),UNKNOWN)
        leverage=max(dec(x['gross_notional'])/dec(x['equity']) for x in (v,l)) if v and l else None
        lines=['[Var/Lighter] 开仓/加仓确认',f'资产：WTI｜方向：{direction}',
               f'当前档位：{value(tier)} / 5',f'已开子单：{value(count)} / {value(caps[int(tier)-1] if caps and tier else None)}',
               f'单边总仓位：{value(notional," U")}',f'Variational 权益：{value(v.get("equity")," U")}',
               f'Lighter 权益：{value(l.get("equity")," U")}',
               f'Variational 保证金使用率：{value(v.get("maintenance_pct"),"%")}',
               f'Lighter 保证金使用率：{value(l.get("maintenance_pct"),"%")}',
               f'最高单边预计杠杆：{value(leverage,"x")} / 5x']
    else:
        result=record.get('attribution') or {};verified=result.get('status')=='verified'
        selected=record.get('selected') or row;remaining=record.get('remaining')
        reason=row.get('exit_reason') or execution.get('exit_reason') or decision.get('reason')
        reason={'net_target_reached':'收益达标','max_hold':'达到最长持仓时间','session_entry_cutoff':'临近休市',
                'sample_gap_requires_position_drain':'异常断档风控排空','account_risk_force_reduce':'账户风控减仓',
                'account_risk_emergency_exit':'账户紧急风控退出','session_boundary':'交易时段切换'}.get(reason,reason or UNKNOWN)
        tier=(decision.get('exit_gradient_tier') or selected.get('gradient_tier'))
        tiers=sorted({x.get('tier',x.get('gradient_tier')) for x in selected.get('layers') or []
                      if isinstance(x.get('tier',x.get('gradient_tier')),int)})
        if len(tiers)>1:tier=','.join(str(x) for x in tiers)
        child_count=(len(selected.get('layers') or []) or 1) if record.get('selected') else row.get('layers_count')
        lines=['[Var/Lighter] 档位平仓完成',f'资产：WTI｜平仓档位：{value(tier)} / 5',
               f'本次平仓子单：{value(child_count)}｜剩余子单：{len((remaining or {}).get("layers") or []) if record else UNKNOWN}',
               f'实际盈亏：{value(result.get("actual_pnl_usd") if verified else None," U")}',
               f'实际收益：{value(result.get("actual_pnl_bps") if verified else None," bps")}',
               f'当前市场档位：{value(record.get("market_tier"))} / 5',
               f'Variational 权益：{value(v.get("equity")," U")}',f'Lighter 权益：{value(l.get("equity")," U")}',
               f'Variational 保证金使用率：{value(v.get("maintenance_pct"),"%")}',
               f'Lighter 保证金使用率：{value(l.get("maintenance_pct"),"%")}', '',f'平仓原因：{reason}']
    return '\n'.join(lines)
