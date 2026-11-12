import asyncio
from datetime import datetime, timezone
from decimal import Decimal as D
import json
from types import SimpleNamespace

import pytest
from tools.eth_passive_collector import observation, Journal, eth_live_running
from tools.lib import frozen_median_live
from variational.listener import VariationalMonitor


URL="wss://omni-ws-server.prod.ap-northeast-1.variational.io/prices"


def fixtures(now=10000):
    stamp=datetime.fromtimestamp(now-.2,timezone.utc).isoformat()
    identity={"underlying":"ETH","instrument_type":"perpetual_future",
              "settlement_asset":"USDC","funding_interval_s":3600}
    quote={"timestamp":stamp,"received_at":stamp,"bid":"2000","ask":"2000.3",
           "raw":{"instrument":identity}}
    monitor=SimpleNamespace(reference_quotes={"ETH":{"timestamp":stamp,"received_at":stamp,
        "raw":{"__source_url":URL}}},signal_quotes={"ETH":quote})
    book={"continuity_ok":True,"cold":False,"server_timestamp_ms":(now-.1)*1000,
          "book_age_seconds":.1,"bid":D(2001),"ask":D('2001.1')}
    return monitor,book


def test_genuine_quotes_are_reused_each_second_without_renewing_source_time():
    m,b=fixtures();r,error=observation(m,b,10000)
    assert not error and r.size_key==URL
    r2,error=observation(m,{**b,"book_age_seconds":1.1},10001)
    assert not error and r2.source_at==r.source_at and r2.event_id==r.event_id
    assert observation(m,b,10006)[0] is None


@pytest.mark.parametrize("case",["wrong_asset","wrong_contract","wrong_currency","missing_time",
    "future_time","old_book","future_book","resync","cold","no_reference","no_quote"])
def test_reject_bad_or_missing_evidence(case):
    m,b=fixtures()
    if case=="wrong_asset":m.signal_quotes['ETH']['raw']['instrument']['underlying']='WTI'
    if case=="wrong_contract":m.signal_quotes['ETH']['raw']['instrument']['instrument_type']='swap'
    if case=="wrong_currency":m.signal_quotes['ETH']['raw']['instrument']['settlement_asset']='USDT'
    if case=="missing_time":m.signal_quotes['ETH']['timestamp']=None
    if case=="future_time":m.signal_quotes['ETH']['timestamp']=datetime.fromtimestamp(10001,timezone.utc).isoformat()
    if case=="old_book":b['server_timestamp_ms']=9997000
    if case=="future_book":b['server_timestamp_ms']=10001000
    if case=="resync":b['continuity_ok']=False
    if case=="cold":b['cold']=True
    if case=="no_reference":m.reference_quotes.clear()
    if case=="no_quote":m.signal_quotes.clear()
    assert observation(m,b,10000)[0] is None


def test_restart_never_duplicates_seconds_and_loader_accepts_same_real_journal(tmp_path,monkeypatch):
    m,b=fixtures();r,_=observation(m,b,10000)
    root=tmp_path/'median_samples';j=Journal(root)
    assert j.append(r) and not j.append(r)
    assert not Journal(root).append(r)
    monkeypatch.setattr(frozen_median_live.time,'time',lambda:10001)
    rt=SimpleNamespace(live_inventory_frozen_median=True,live_allowed_assets={'ETH'},
        output_dir=tmp_path,live_inventory_max_lighter_book_age_seconds=2)
    frozen_median_live.initialize(rt,{})
    assert list(rt.frozen_median_model.history)==[(10000,r.basis)]
    assert rt.frozen_median_model.signature==(r.instrument,r.size_key)
    assert not rt.frozen_median_model.confirmations


def test_browser_rest_frame_uses_same_monitor_schema():
    async def run():
        monitor=VariationalMonitor()
        payload={'instrument':{'underlying':'ETH','instrument_type':'perpetual_future',
            'settlement_asset':'USDC','funding_interval_s':3600},'bid':'2000','ask':'2000.3',
            'timestamp':'2026-11-12T00:00:00+00:00'}
        await monitor.process_rest_event({'kind':'rest_response',
            'url':'https://omni.variational.io/api/quotes/indicative','body':json.dumps(payload)})
        assert monitor.signal_quotes['ETH']['bid']=='2000'
    asyncio.run(run())
