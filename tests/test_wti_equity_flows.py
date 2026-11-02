import asyncio,json
from datetime import datetime,timezone
from types import SimpleNamespace
import pytest
from tools.lib.wti_equity_flows import cashflow_receipt,var_flows,lighter_flows
from tools.lib.wti_equity_reporting import flow_sum

START=datetime(2026,10,29,tzinfo=timezone.utc)
END=datetime(2026,10,30,tzinfo=timezone.utc)


@pytest.mark.parametrize('success',[1,2,None])
def test_flow_receipt_retry_once_and_no_retry_after_persisted_failure_or_success(tmp_path,success):
    calls=[]
    def var(start,end):
        calls.append(1)
        if len(calls)!=success:raise TimeoutError
        return []
    async def lighter(start,end):return []
    path=tmp_path/'day.json'
    receipt=cashflow_receipt(START,END,path,fetch_var=var,fetch_lighter=lighter)
    assert receipt['attempts']==len(calls)==(1 if success==1 else 2)
    assert receipt['status']==('verified' if success else 'unverified')
    assert cashflow_receipt(START,END,path,fetch_var=var,fetch_lighter=lighter)==receipt
    assert len(calls)==(1 if success==1 else 2)


def test_var_flows_require_actual_pagination_and_explicit_external_flow_fields(monkeypatch,tmp_path):
    import tools.lib.variational_fee_session as session
    monkeypatch.setattr(session,'session_cookies',lambda *a,**k:('wallet',{}))
    monkeypatch.setattr('pathlib.Path.home',lambda:tmp_path)
    p=tmp_path/'snap/chromium/common/chromium/Last Version';p.parent.mkdir(parents=True)
    p.write_text('140.0.0.0')
    row={'id':'deposit','created_at':'2026-10-29T12:00:00+00:00','transfer_type':'deposit',
         'qty':'20.123456789123456789','asset':'USDC','status':'completed'}
    calls=[]
    class Response:
        status_code=200
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def iter_content(self,*args):yield json.dumps({'result':[row],'pagination':{'object_count':1,'next_page':None}}).encode()
    class Client:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def get(self,url,**kwargs):
            calls.append((url,kwargs));return Response()
    monkeypatch.setattr('requests.Session',Client)
    result=var_flows(START,END)
    assert result[0]['net_inflow_usd']=='20.123456789123456789'
    assert calls[0][1]['allow_redirects'] is False and calls[0][1]['stream'] is True
    assert calls[0][0].endswith('/api/transfers')
    row['transfer_type']='unknown'
    with pytest.raises(ValueError,match='kind_unverified'):var_flows(START,END)


def test_lighter_cursor_empty_page_is_complete_and_internal_transfer_nets_to_zero(monkeypatch):
    import lighter.signer_client as signer
    from lighter.api.account_api import AccountApi
    from lighter.api.transaction_api import TransactionApi
    import tools.wti_account_probe as probe
    monkeypatch.setattr(probe,'_load_env',lambda:(12,1,'not-a-real-key'))
    client=SimpleNamespace(api_client=SimpleNamespace(),create_auth_token_with_expiry=lambda **k:('not-a-real-token',None))
    monkeypatch.setattr(signer,'SignerClient',lambda **k:client)
    async def account(*args,**kwargs):return {'code':200,'accounts':[{'l1_address':'wallet'}]}
    calls=[]
    async def deposits(*args,**kwargs):
        calls.append(kwargs)
        if kwargs.get('cursor'):return {'code':200,'deposits':[],'cursor':'same-empty-cursor'}
        return {'code':200,'deposits':[{'id':'1','timestamp':int(START.timestamp()*1000)+1000,
                  'asset_id':3,'amount':'10.001','status':'completed'}],'cursor':'next'}
    async def withdrawals(*args,**kwargs):return {'code':200,'withdraws':[],'cursor':'nonempty'}
    async def transfers(*args,**kwargs):return {'code':200,'transfers':[{'id':'2',
        'timestamp':int(START.timestamp()*1000)+2000,'asset_id':3,'amount':'2','from_account_index':12,
        'to_account_index':12}],'cursor':''}
    monkeypatch.setattr(AccountApi,'account',account)
    monkeypatch.setattr(TransactionApi,'deposit_history',deposits)
    monkeypatch.setattr(TransactionApi,'withdraw_history',withdrawals)
    monkeypatch.setattr(TransactionApi,'transfer_history',transfers)
    rows=asyncio.run(lighter_flows(START,END))
    assert len(calls)==2 and calls[1]['cursor']=='next'
    assert [x['net_inflow_usd'] for x in rows]==['10.001','0']
    receipt={'status':'verified','start':START.isoformat(),'end':END.isoformat(),'flows':rows}
    assert str(flow_sum(receipt,START,END))=='10.001'
