"""Read-only external cash-flow coverage for equity reports, not trading."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import asyncio
import json
import re
import requests

from tools.lib.wti_equity_reporting import at, dec
from tools.lib.runtime_files import read_json,write_json_atomic


def var_flows(start,end):
    from tools.lib.variational_fee_session import session_cookies,ORIGIN
    p=Path.home()/'snap/chromium/common/chromium/Default/Cookies'
    address,jar=session_cookies(p,now=datetime.now(timezone.utc))
    version=(p.parent.parent/'Last Version').read_text().strip()
    if not re.fullmatch(r'[0-9]+(?:\.[0-9]+){3}',version):raise ValueError('report_browser_version_invalid')
    ua='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+version+' Safari/537.36'
    flows=[];seen=set()
    with requests.Session() as client:
        client.trust_env=False
        for page in range(10):
            with client.get(ORIGIN+'/api/transfers',params={'limit':100,'offset':page*100,
                'order_by':'created_at','order':'desc','created_at_gte':start.isoformat(),
                'created_at_lte':end.isoformat()},cookies=jar,headers={'vr-connected-address':address,
                'User-Agent':ua},timeout=(3,10),allow_redirects=False,stream=True) as response:
                if response.status_code!=200:raise ValueError('report_var_flows_http_'+str(response.status_code))
                parts=[];size=0
                for part in response.iter_content(65536):
                    size+=len(part)
                    if size>2*1024*1024:raise ValueError('report_var_flows_too_large')
                    parts.append(part)
                data=json.loads(b''.join(parts))
            rows=data['result'];pagination=data['pagination']
            if not isinstance(rows,list):raise ValueError('report_var_flow_schema_unverified')
            for row in rows:
                timestamp=at(row['created_at'])
                if not start<timestamp<=end:continue
                key=str(row['id'])
                if key in seen:raise ValueError('report_var_flow_duplicate')
                seen.add(key)
                kind=row.get('transfer_type',row.get('type'))
                if kind in {'fee','funding','realized_pnl'}:continue
                if kind not in {'deposit','withdrawal'}:raise ValueError('report_var_flow_kind_unverified')
                if row.get('status') in {'failed','canceled','rejected'}:continue
                if row.get('status') not in {'completed','confirmed','success'}:
                    raise ValueError('report_var_flow_status_unverified')
                if row.get('asset') not in {'USDC','USD'}:raise ValueError('report_var_flow_asset_unverified')
                amount=dec(row.get('qty',row.get('amount')))
                if amount<0:raise ValueError('report_var_flow_sign_unverified')
                flows.append({'venue':'variational','id':key,'at':timestamp.isoformat(),
                              'net_inflow_usd':str(amount if kind=='deposit' else -amount)})
            if pagination.get('next_page') is None and int(pagination['object_count'])<=page*100+len(rows):
                return flows
    raise ValueError('report_var_flows_truncated')


async def lighter_flows(start,end):
    from lighter.signer_client import SignerClient
    from lighter.api.account_api import AccountApi
    from lighter.api.transaction_api import TransactionApi
    from tools.wti_account_probe import _load_env,_model_dict,_close_client,DEFAULT_LIGHTER_URL
    account,key,secret=_load_env()
    client=SignerClient(url=DEFAULT_LIGHTER_URL,account_index=account,api_private_keys={key:secret})
    flows=[]
    try:
        token,error=client.create_auth_token_with_expiry(api_key_index=key)
        if error or not token:raise ValueError('report_lighter_flow_auth_unavailable')
        detail=_model_dict(await AccountApi(client.api_client).account(by='index',value=str(account),_request_timeout=8.0))
        if detail.get('code')!=200 or len(detail.get('accounts',[]))!=1:raise ValueError('report_lighter_flow_account_unverified')
        address=detail['accounts'][0]['l1_address'];api=TransactionApi(client.api_client)
        for kind,method,field in [('deposit',api.deposit_history,'deposits'),
                                  ('withdraw',api.withdraw_history,'withdraws'),
                                  ('transfer',api.transfer_history,'transfers')]:
            cursor=None;seen=set();cursors=set()
            for _ in range(10):
                args={'account_index':account,'auth':token,'_request_timeout':8.0}
                if cursor:args['cursor']=cursor
                if kind=='deposit':args['l1_address']=address
                data=_model_dict(await method(**args))
                rows=data.get(field)
                if data.get('code')!=200 or not isinstance(rows,list):raise ValueError('report_lighter_flow_schema_unverified')
                for row in rows:
                    milliseconds=int(row['timestamp'])
                    timestamp=datetime.fromtimestamp(milliseconds//1000,timezone.utc)+timedelta(milliseconds=milliseconds%1000)
                    if not start<timestamp<=end:continue
                    identity=str(row['id'])
                    if identity in seen:raise ValueError('report_lighter_flow_duplicate')
                    seen.add(identity)
                    if row.get('asset_id')!=3:raise ValueError('report_lighter_flow_asset_unverified')
                    amount=dec(row['amount'])
                    if amount<0:raise ValueError('report_lighter_flow_sign_unverified')
                    if kind!='transfer':
                        if row.get('status') in {'failed','canceled','rejected'}:continue
                        if row.get('status')!='completed':raise ValueError('report_lighter_flow_status_unverified')
                        net=amount if kind=='deposit' else -amount
                    else:
                        source,target=int(row['from_account_index']),int(row['to_account_index'])
                        if account not in {source,target}:raise ValueError('report_lighter_transfer_account_mismatch')
                        net=amount*((1 if target==account else 0)-(1 if source==account else 0))
                    flows.append({'venue':'lighter','id':kind+':'+identity,'at':timestamp.isoformat(),'net_inflow_usd':str(net)})
                cursor=data.get('cursor')
                if not rows or not cursor:break
                if cursor in cursors:raise ValueError('report_lighter_flow_cursor_repeated')
                cursors.add(cursor)
            else:raise ValueError('report_lighter_flows_truncated')
        return flows
    finally:
        await _close_client(client)


def cashflow_receipt(start,end,path,*,fetch_var=var_flows,fetch_lighter=lighter_flows):
    """Initial attempt plus exactly one retry; keep failure explicit and durable."""
    old=read_json(Path(path))
    if old.get('start')==start.isoformat() and old.get('end')==end.isoformat():return old
    receipt={'start':start.isoformat(),'end':end.isoformat(),'status':'unverified'}
    for attempt in range(2):
        receipt['attempts']=attempt+1
        try:
            v=fetch_var(start,end);l=asyncio.run(fetch_lighter(start,end))
            receipt.update(status='verified',flows=v+l)
            receipt.pop('reason',None)
            break
        except Exception as exc:
            receipt['reason']=str(exc) if isinstance(exc,ValueError) else type(exc).__name__
    write_json_atomic(Path(path),receipt)
    return receipt
