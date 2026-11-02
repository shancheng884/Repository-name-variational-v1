import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import hashlib
import json
import sqlite3
from urllib.parse import urlsplit

import pytest
from Crypto.Cipher import AES

from test_wti_exit_cost import inputs, WALLET
from tools.lib.runtime_files import read_json, write_json_atomic
from tools.lib.variational_fee_session import decode_cookie, session_cookies, fetch_fee_capture
from tools.lib.wti_exit_cost import cost_policy, lighter_fee_capture
from tools.wti_exit_cost_audit import funding_debits
from tools.wti_exit_cost_renew import renew_once


def encrypted(host, value):
    raw = hashlib.sha256(host.encode()).digest()+value.encode()
    n = 16-len(raw)%16
    key = hashlib.pbkdf2_hmac('sha1', b'peanuts', b'saltysalt', 1, 16)
    return b'v10'+AES.new(key, AES.MODE_CBC, iv=b' '*16).encrypt(raw+bytes([n])*n)


def cookie_db(tmp_path, now):
    path = tmp_path/'Cookies'
    db = sqlite3.connect(path)
    db.execute('create table meta (key text, value text)')
    db.execute("insert into meta values ('version','24')")
    db.execute('create table cookies (host_key text,name text,encrypted_value blob,expires_utc integer)')
    expiry = int((now.timestamp()+11_644_473_600+3600)*1e6)
    for host, name, value in [('omni.variational.io','vr-connected-address',WALLET),
                              ('omni.variational.io','vr-token','synthetic-session-value'),
                              ('unrelated.invalid','vr-token','must-not-be-read'),
                              ('omni.variational.io','analytics-secret','must-not-be-read')]:
        db.execute('insert into cookies values (?,?,?,?)',(host,name,encrypted(host,value),expiry))
    db.commit();db.close()
    return path


def test_narrow_readonly_cookie_reader_and_origin_binding(tmp_path):
    now = datetime.now(timezone.utc);path = cookie_db(tmp_path, now)
    original = path.read_bytes()
    address, jar = session_cookies(path, now=now)
    assert address == WALLET and {c.name for c in jar} == {'vr-token','vr-connected-address'}
    assert path.read_bytes() == original
    db=sqlite3.connect(path);db.execute("update meta set value='25' where key='version'");db.commit();db.close()
    assert session_cookies(path,now=now)[0] == WALLET
    with pytest.raises(ValueError, match='origin_mismatch'):
        decode_cookie('other.invalid', encrypted('omni.variational.io','synthetic'))
    for raw in (b'v11'+bytes(32), b'v10', b'', b'v10'+bytes(16)):
        with pytest.raises(ValueError):decode_cookie('omni.variational.io',raw)
    with pytest.raises(ValueError, match='expired'):
        session_cookies(path, now=now+timedelta(hours=2))


@pytest.mark.parametrize('http', [200, 302, 401, 403, 503])
def test_fee_fetch_is_one_get_no_redirect_no_cookie_in_receipt(tmp_path, http):
    state,evidence,now=inputs();path=cookie_db(tmp_path,now);calls=[]
    class Response:
        status_code=http
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def iter_content(self,n):yield b'{"result":[],"pagination":{"object_count":0,"next_page":null}}'
    class Session:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def get(self,url,**kwargs):calls.append((url,kwargs,self.trust_env));return Response()
    if http==200:
        receipt=fetch_fee_capture(state,path,now=now,session_factory=Session)
        text=json.dumps(receipt)
        assert 'synthetic-session-value' not in text and 'Cookie' not in text and 'Authorization' not in text
    else:
        with pytest.raises(ValueError,match='var_fee_http_'+str(http)):
            fetch_fee_capture(state,path,now=now,session_factory=Session)
    assert len(calls)==1
    url,kwargs,trust=calls[0]
    assert urlsplit(url).hostname=='omni.variational.io' and urlsplit(url).path=='/api/transfers'
    assert kwargs['allow_redirects'] is False and trust is False
    assert 'type=fee' in url


def test_successive_renewals_revalidate_publish_without_touching_ledger(tmp_path):
    state,evidence,now=inputs();sp=tmp_path/'state.json';ep=tmp_path/'active.json'
    write_json_atomic(sp,state);original=sp.read_bytes();build_calls=[]
    def fetch(s,path):return evidence['fee_proofs']['variational']['raw']
    async def build(s,capture,**kwargs):
        build_calls.append(kwargs)
        return deepcopy(evidence),cost_policy(s,evidence)
    for _ in range(2):
        h=asyncio.run(renew_once(sp,ep,tmp_path/'unused',fetcher=fetch,builder=build))
        assert h['status']=='verified' and h['orders_attempted'] is False
        assert cost_policy(state,read_json(ep))[0]<10
    assert sp.read_bytes()==original and build_calls[1]['cached']
    assert all('renew_funding' not in x for x in build_calls)


def test_partial_tier_close_renews_current_position_without_historical_funding_rebuild(tmp_path):
    state,evidence,now=inputs();sp=tmp_path/'state.json';ep=tmp_path/'active.json'
    closed=deepcopy(state['execution_history'][1])
    closed['action_id']='previous-tier-close'
    state['execution_history'].append(closed)
    write_json_atomic(sp,state);seen=[]
    def fetch(current,path):
        seen.append(current)
        return evidence['fee_proofs']['variational']['raw']
    async def build(current,capture,**kwargs):
        assert any(x['kind']=='close' for x in current['execution_history'])
        proof=deepcopy(evidence)
        proof['position_fingerprint']=__import__('tools.lib.wti_exit_cost',fromlist=['fingerprint']).fingerprint(current)
        proof['fee_proofs']['variational']['position_fingerprint']=proof['position_fingerprint']
        proof['fee_proofs']['lighter']['position_fingerprint']=proof['position_fingerprint']
        return proof,cost_policy(current,proof)
    h=asyncio.run(renew_once(sp,ep,tmp_path/'unused',fetcher=fetch,builder=build))
    assert h['status']=='verified' and h['orders_attempted'] is False
    assert len(seen)==1 and read_json(ep)['funding_excluded_from_exit_cost'] is True
    assert read_json(sp)==state


@pytest.mark.parametrize('failure',['http','stale','position','pending','flat'])
def test_failure_or_state_change_immediately_invalidates_last_proof(tmp_path,failure):
    state,evidence,now=inputs();sp=tmp_path/'state.json';ep=tmp_path/'active.json'
    write_json_atomic(sp,state);write_json_atomic(ep,evidence)
    def fetch(s,path):
        if failure=='http':raise ValueError('var_fee_http_401')
        return evidence['fee_proofs']['variational']['raw']
    async def build(s,capture,**kwargs):
        e=deepcopy(evidence)
        if failure=='stale':e['expires_at']=(now-timedelta(seconds=1)).isoformat()
        if failure in ('position','pending','flat'):
            changed=deepcopy(state)
            if failure=='position':changed['open_position']['var_qty']='2'
            if failure=='pending':changed['pending_action']={'kind':'close'}
            if failure=='flat':changed.update(status='flat',open_position=None)
            write_json_atomic(sp,changed)
        return e,cost_policy(s,e)
    h=asyncio.run(renew_once(sp,ep,tmp_path/'unused',fetcher=fetch,builder=build))
    assert h['status']=='unavailable' and read_json(ep)['unavailable'] is True
    assert cost_policy(state,read_json(ep))[0]==10


def test_funding_debits_are_bound_and_cannot_be_lowered_by_credits():
    state,e,now=inputs();actions=state['execution_history'][:1];start=now-timedelta(minutes=30)
    actions[0]['legs']['lighter']['response_received_at']=(start-timedelta(minutes=5)).isoformat()
    row={'funding_id':1,'market_id':145,'timestamp':int(start.timestamp()),'position_size':'1',
         'position_side':'short','change':'-.001'}
    reply={'code':200,'position_fundings':[row,{**row,'funding_id':2,'change':'.002'}]}
    assert funding_debits(actions,reply,now)==D('.001')
    for altered in ({**row,'position_size':'2'},{**row,'position_side':'long'},{**row,'market_id':0}):
        with pytest.raises(ValueError):funding_debits(actions,{'code':200,'position_fundings':[altered]},now)


def test_adjacent_previous_round_csv_is_explicitly_out_of_scope():
    state,e,now=inputs();raw=deepcopy(e['fee_proofs']['lighter']['raw'])
    raw['trades_csv']+='WTI,41,1,100,9\n'
    proof=lighter_fee_capture(state,raw,account_index=7)
    assert proof['fee_usd']=='0' and proof['scoped_trade_ids']==['42']
    assert proof['out_of_scope_csv_trade_ids']==['41']


def test_renewal_cannot_overwrite_ledger_even_with_bad_output_argument(tmp_path):
    state,e,now=inputs();sp=tmp_path/'state.json';write_json_atomic(sp,state);before=sp.read_bytes()
    with pytest.raises(ValueError,match='protected_live_state'):
        asyncio.run(renew_once(sp,sp,tmp_path/'unused'))
    assert sp.read_bytes()==before


def test_disk_write_failure_removes_discounted_proof(tmp_path,monkeypatch):
    import tools.wti_exit_cost_renew as module
    state,e,now=inputs();ep=tmp_path/'evidence.json';write_json_atomic(ep,e)
    def full_disk(*a,**k):raise OSError('synthetic disk full')
    monkeypatch.setattr(module,'write_json_atomic',full_disk)
    module.invalidate(ep,reason='synthetic_failure',now=now)
    assert not ep.exists() and cost_policy(state,read_json(ep))[0]==10
