"""Small-size, two-venue live probe rounds with idempotent order identities."""
from __future__ import annotations
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR
from pathlib import Path
from typing import Any, Protocol
import asyncio, hashlib, inspect, json, os, time, uuid
import requests
from tools.lib.hedge_instruments import iter_positions, lighter_position_qty
from tools.lib.lighter_order_identity import lighter_client_order_index
from tools.lib.wti_readonly_adapter import VariationalForwarderClient
ZERO=Decimal("0"); BPS=Decimal("10000")
class ProbeError(RuntimeError): pass
def _d(v):
    if v in (None,""): return None
    try: out=Decimal(str(v))
    except Exception: return None
    return out if out.is_finite() else None
def _iso(v):
    return None if v is None else (v.astimezone(timezone.utc).isoformat() if isinstance(v,datetime) else str(v))
def _parse(v):
    if v is None: return None
    if isinstance(v,datetime): return v.astimezone(timezone.utc) if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        p=datetime.fromisoformat(str(v).replace("Z","+00:00")); return p.astimezone(timezone.utc) if p.tzinfo else p.replace(tzinfo=timezone.utc)
    except ValueError: return None
def _parse_epoch_ms(v):
    try:
        n=float(v)
        if n>1e11: n/=1000.0
        return datetime.fromtimestamp(n,timezone.utc)
    except (TypeError,ValueError,OSError):
        return None


class _EntropyCloid:
    def __init__(self,raw):
        raw=str(raw)
        if not raw.startswith("0x") or len(raw)!=34: raise ValueError("entropy_cloid_invalid")
        self.raw=raw
    def to_raw(self): return self.raw
    def __str__(self): return self.raw


def _model_dict(v):
    if isinstance(v,dict): return v
    for n in ("to_dict","model_dump"):
        m=getattr(v,n,None)
        if callable(m):
            o=m() if n=="to_dict" else m(mode="json"); return o if isinstance(o,dict) else None
    return None
def _rows(v,key):
    r=_model_dict(v)
    if r is None: return None
    code=r.get("code")
    if code is not None:
        try:
            if int(code) not in (0,200): return None
        except Exception: return None
    rows=r.get(key); return [x for x in rows if isinstance(x,dict)] if isinstance(rows,list) else None
def _var_rows(v):
    c=v
    for _ in range(12):
        if isinstance(c,list): return [x for x in c if isinstance(x,dict)]
        if not isinstance(c,dict): return None
        for k in ("orders","result","data"):
            if k in c: c=c[k]; break
        else: return None
    return None
def _underlying(row):
    ins=row.get("instrument") if isinstance(row.get("instrument"),dict) else {}
    for src in (ins,row):
        for k in ("underlying","symbol","market","asset"):
            if src.get(k) not in (None,""): return str(src[k]).strip().upper()
    return ""
@dataclass(frozen=True)
class ProbeRound:
    round_id:str; asset:str; buy_venue:str; sell_venue:str; sequence:int

class VariationalProbeExecutor:
    venue="variational"; available=True
    def __init__(self, command_url:str, *, timeout_seconds:float=8.0):
        self.command_url=command_url; self.timeout_seconds=timeout_seconds; self.client=None
    async def start(self):
        self.client=VariationalForwarderClient(self.command_url,timeout_seconds=self.timeout_seconds)
        await self.client.__aenter__()
    async def close(self):
        if self.client is not None:
            await self.client.__aexit__(None,None,None); self.client=None
    async def preflight(self,asset):
        assert self.client is not None
        pos=await self.client.request({"type":"VAR_API_POSITIONS","requestId":uuid.uuid4().hex,"requestTimeoutMs":int(self.timeout_seconds*1000)})
        positions=iter_positions(pos.get("result") if isinstance(pos,dict) else None)
        if positions is None: return {"ok":False,"error":"variational_positions_unverifiable"}
        symbol=asset.symbol("variational")
        for item in positions:
            nested=item.get("position_info") if isinstance(item.get("position_info"),dict) else {}
            ident=item.get("instrument") if isinstance(item.get("instrument"),dict) else nested.get("instrument") or item
            if str(ident.get("underlying") or ident.get("symbol") or "").upper()!=symbol: continue
            q=next((_d(src.get(k)) for src in (item,nested) for k in ("qty","quantity","size","position","position_size","base_amount","amount") if _d(src.get(k)) is not None),None)
            if q not in (None,ZERO): return {"ok":False,"error":"variational_position_not_flat","qty":str(q)}
        orders=await self.client.request({"type":"VAR_API_ORDERS","requestId":uuid.uuid4().hex,"status":"pending","limit":100,"offset":0,"orderBy":"created_at","order":"desc","requestTimeoutMs":int(self.timeout_seconds*1000)})
        rows=_var_rows(orders.get("result") if isinstance(orders,dict) else None)
        if rows is None: return {"ok":False,"error":"variational_orders_unverifiable"}
        active=[r for r in rows if _underlying(r)==symbol]
        if active: return {"ok":False,"error":"variational_active_order","count":len(active)}
        return {"ok":True,"qty":"0","active_orders":0,
                "contract":{"venue":"variational","symbol":asset.symbol("variational"),
                            "instrument_type":asset.var_instrument_type,"kind":asset.var_kind,
                            "settlement_asset":"USDC","funding_interval_s":0}}
    def normalize_qty(self,asset,qty):
        return qty.quantize(Decimal("0.00000001"),rounding=ROUND_DOWN) if qty>0 else None
    async def _execution_quote(self,asset,qty):
        assert self.client is not None
        req={"type":"VAR_API_QUOTE","requestId":uuid.uuid4().hex,"market":asset.symbol("variational"),"amount":format(qty,"f"),"instrumentType":asset.var_instrument_type,"settlementAsset":"USDC","kind":asset.var_kind,"fundingIntervalS":0,"confirm":False,"requestTimeoutMs":int(self.timeout_seconds*1000),"operationTimeoutMs":int(self.timeout_seconds*1000)}
        started=datetime.now(timezone.utc)
        try: message=await self.client.request(req)
        except Exception as exc: return None,f"{type(exc).__name__}:{exc}",started,datetime.now(timezone.utc)
        recv=datetime.now(timezone.utc); result=message.get("result") if isinstance(message,dict) else None
        if not isinstance(result,dict) or message.get("ok") is not True or result.get("ok") is not True:
            return None,str((result or {}).get("error") or (message or {}).get("error") or "variational_quote_unconfirmed")[:200],started,recv
        bid=_d(result.get("bid")); ask=_d(result.get("ask")); qid=str(result.get("quoteId") or result.get("quote_id") or "") or None
        qts=result.get("quoteTimestamp") or result.get("timestamp")
        if not bid or not ask or ask<=bid or not qid or not qts:
            return None,"variational_quote_shape_invalid",started,recv
        return {"bid":str(bid),"ask":str(ask),"mid":str((bid+ask)/2),"quote_id":qid,"quote_timestamp":str(qts),"request_started_at":started.isoformat(),"received_at":recv.isoformat(),"request_latency_ms":round((recv-started).total_seconds()*1000,3)},None,started,recv
    async def market_order(self,*,asset,side,qty,quote,client_order_id,reduce_only=False,max_slippage_bps=Decimal("8")):
        assert self.client is not None
        exec_quote,quote_error,submitted,quote_recv=await self._execution_quote(asset,qty)
        if exec_quote is None:
            return {"ok":False,"status":"rejected","error":quote_error,"client_order_id":client_order_id,"submitted_at":submitted.isoformat()}
        quote_id=exec_quote["quote_id"]; quote_at=exec_quote["quote_timestamp"]
        submitted=datetime.now(timezone.utc)
        payload={"type":"VAR_API_ORDER","requestId":client_order_id,"market":asset.symbol("variational"),"instrumentType":asset.var_instrument_type,"settlementAsset":"USDC","kind":asset.var_kind,"fundingIntervalS":0,"side":side.upper(),"amount":format(qty,"f"),"confirm":True,"reuseQuoteId":quote_id,"quoteTimestamp":quote_at,"expectedQuotePrice":exec_quote["mid"],"dispatchQuoteAgeLimitMs":2000,"maxSlippage":float(max_slippage_bps/BPS),"reduceOnly":bool(reduce_only),"requestTimeoutMs":int(self.timeout_seconds*1000),"operationTimeoutMs":int(self.timeout_seconds*1000)}
        try: message=await self.client.request(payload)
        except Exception as exc:
            return {"ok":False,"status":"unknown","error":f"{type(exc).__name__}:{exc}","client_order_id":client_order_id,"submitted_at":submitted.isoformat()}
        acked=datetime.now(timezone.utc); result=message.get("result") if isinstance(message,dict) else None
        if not isinstance(result,dict) or message.get("ok") is not True or result.get("ok") is not True:
            return {"ok":False,"status":"unknown","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"error":str((result or {}).get("error") or (message or {}).get("error") or "variational_order_unconfirmed")[:200]}
        rfq=str(result.get("rfqId") or result.get("rfq_id") or "") or None; oid=str(result.get("orderId") or result.get("order_id") or "") or None
        receipt=await self._poll(client_order_id,submitted,acked,rfq,oid,qty)
        receipt["execution_quote"]=exec_quote
        return receipt
    async def _poll(self,client_order_id,submitted,acked,rfq_id,order_id,qty):
        assert self.client is not None
        deadline=time.monotonic()+self.timeout_seconds
        while time.monotonic()<deadline:
            msg=await self.client.request({"type":"VAR_API_ORDERS","requestId":uuid.uuid4().hex,"status":"pending,canceled,cleared,rejected","createdAtGte":submitted.isoformat(),"limit":100,"offset":0,"orderBy":"created_at","order":"desc","requestTimeoutMs":int(self.timeout_seconds*1000)})
            rows=_var_rows(msg.get("result") if isinstance(msg,dict) else None)
            if rows is None: await asyncio.sleep(.15); continue
            matches=[r for r in rows if (rfq_id and str(r.get("rfq_id") or "")==rfq_id) or (order_id and str(r.get("order_id") or "")==order_id)]
            if len(matches)>1: return {"ok":False,"status":"unknown","client_order_id":client_order_id,"error":"variational_identity_ambiguous","submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"rfq_id":rfq_id}
            if matches:
                row=matches[0]; status=str(row.get("status") or "").lower()
                if status=="pending": await asyncio.sleep(.15); continue
                if status in {"canceled","cancelled","rejected","expired"}:
                    return {"ok":True,"status":"rejected","client_order_id":client_order_id,"filled_qty":"0","submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"rfq_id":rfq_id,"order_id":str(row.get("order_id") or "") or order_id}
                if status=="cleared" and str(row.get("clearing_status") or "")=="success_trades_booked_into_pool":
                    fill_at=_parse(row.get("execution_timestamp") or row.get("created_at"))
                    return {"ok":True,"status":"filled","client_order_id":client_order_id,"filled_qty":str(_d(row.get("qty")) or qty),"avg_price":str(_d(row.get("price")) or ""),"rfq_id":rfq_id,"order_id":str(row.get("order_id") or "") or order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"source_at":_iso(fill_at),"source_at_epoch":fill_at.timestamp() if fill_at else None,"fee_usd":None}
            await asyncio.sleep(.15)
        return {"ok":False,"status":"unknown","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"rfq_id":rfq_id,"order_id":order_id,"error":"variational_fill_timeout"}

class LighterProbeExecutor:
    def __init__(self,*,name,rest_url,account_index,api_key_index,private_key):
        self.venue=name; self.rest_url=rest_url.rstrip("/"); self.account_index=account_index; self.api_key_index=api_key_index; self.private_key=private_key
        self.available=bool(account_index is not None and api_key_index is not None and private_key); self.client=None; self.markets={}
    async def start(self):
        if not self.available: return
        from lighter.signer_client import SignerClient
        r=await asyncio.to_thread(requests.get,f"{self.rest_url}/api/v1/orderBooks",timeout=15); r.raise_for_status()
        rows=r.json().get("order_books") or []
        self.markets={str(x.get("symbol") or "").upper():x for x in rows if isinstance(x,dict)}
        self.client=SignerClient(url=self.rest_url,account_index=int(self.account_index),api_private_keys={int(self.api_key_index):self.private_key})
        if self.client.check_client() is not None: raise ProbeError(f"{self.venue}_sdk_preflight_failed")
    async def close(self):
        if self.client is not None:
            target=getattr(self.client,"api_client",self.client); close=getattr(target,"close",None)
            if callable(close):
                result=close()
                if inspect.isawaitable(result): await result
            self.client=None
    def market(self,asset): return self.markets.get(asset.symbol(self.venue).upper())
    async def preflight(self,asset):
        if not self.available or self.client is None: return {"ok":False,"error":f"{self.venue}_credentials_missing"}
        market=self.market(asset)
        if market is None: return {"ok":False,"error":f"{self.venue}_market_missing"}
        from lighter import AccountApi, OrderApi
        account=_model_dict(await AccountApi(self.client.api_client).account(by="index",value=str(self.account_index),_request_timeout=10.))
        if account is None: return {"ok":False,"error":f"{self.venue}_account_unverifiable"}
        qty=lighter_position_qty(account,market_id=int(market["market_id"]),symbol=asset.symbol(self.venue))
        if qty is None: return {"ok":False,"error":f"{self.venue}_position_unverifiable"}
        if qty!=ZERO: return {"ok":False,"error":f"{self.venue}_position_not_flat","qty":str(qty)}
        token,error=self.client.create_auth_token_with_expiry(api_key_index=int(self.api_key_index))
        if error is not None or not token: return {"ok":False,"error":f"{self.venue}_auth_token_unavailable"}
        rows=_rows(await OrderApi(self.client.api_client).account_active_orders(auth=token,account_index=int(self.account_index),market_id=int(market["market_id"]),_request_timeout=10.),"orders")
        if rows is None: return {"ok":False,"error":f"{self.venue}_active_orders_unverifiable"}
        if rows: return {"ok":False,"error":f"{self.venue}_active_order","count":len(rows)}
        return {"ok":True,"qty":"0","active_orders":0,
                "contract":{"venue":self.venue,"symbol":asset.symbol(self.venue),
                            "market_id":int(market["market_id"]),
                            "status":market.get("status"),"market_type":market.get("market_type"),
                            "size_decimals":int(market.get("supported_size_decimals") or 0),
                            "price_decimals":int(market.get("supported_price_decimals") or 0),
                            "min_base_amount":str(market.get("min_base_amount")),
                            "min_quote_amount":str(market.get("min_quote_amount")),
                            "multiplier":str(market.get("multiplier"))}}
    def normalize_qty(self,asset,qty):
        market=self.market(asset)
        if market is None or qty<=0: return None
        d=int(market.get("supported_size_decimals") or 0); step=Decimal(1).scaleb(-d)
        out=qty.quantize(step,rounding=ROUND_DOWN); minimum=_d(market.get("min_base_amount")) or ZERO
        return out if out>0 and out>=minimum else None
    async def market_order(self,*,asset,side,qty,quote,client_order_id,reduce_only=False,max_slippage_bps=Decimal("8")):
        if not self.available or self.client is None: return {"ok":False,"status":"rejected","error":f"{self.venue}_credentials_missing","client_order_id":client_order_id}
        market=self.market(asset)
        if market is None: return {"ok":False,"status":"rejected","error":f"{self.venue}_market_missing","client_order_id":client_order_id}
        qty=self.normalize_qty(asset,qty)
        if not qty: return {"ok":False,"status":"rejected","error":"lighter_qty_below_min","client_order_id":client_order_id}
        sd=int(market.get("supported_size_decimals") or 0); pd=int(market.get("supported_price_decimals") or 0)
        bbo=_d(quote.get("ask" if side=="buy" else "bid"))
        if not bbo: return {"ok":False,"status":"rejected","error":"lighter_quote_missing","client_order_id":client_order_id}
        direction=Decimal("-1") if side=="sell" else Decimal("1")
        limit=(bbo*(Decimal("1")+direction*max_slippage_bps/BPS)).quantize(Decimal(1).scaleb(-pd),rounding=ROUND_CEILING if side=="buy" else ROUND_FLOOR)
        if qty*limit < (_d(market.get("min_quote_amount")) or ZERO): return {"ok":False,"status":"rejected","error":"lighter_notional_below_min","client_order_id":client_order_id}
        base=qty*(Decimal(10)**sd); price=limit*(Decimal(10)**pd)
        if base!=base.to_integral_value() or price!=price.to_integral_value(): return {"ok":False,"status":"rejected","error":"lighter_precision_invalid","client_order_id":client_order_id}
        cidx=lighter_client_order_index(client_order_id); submitted=datetime.now(timezone.utc)
        try:
            _o,response,error=await self.client.create_order(market_index=int(market["market_id"]),client_order_index=cidx,base_amount=int(base),price=int(price),is_ask=side=="sell",order_type=self.client.ORDER_TYPE_MARKET,time_in_force=self.client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,order_expiry=self.client.DEFAULT_IOC_EXPIRY,reduce_only=bool(reduce_only),api_key_index=int(self.api_key_index))
        except Exception as exc:
            return {"ok":False,"status":"unknown","error":f"{type(exc).__name__}:{exc}","client_order_id":client_order_id,"submitted_at":submitted.isoformat()}
        acked=datetime.now(timezone.utc); body=_model_dict(response)
        if error is not None or body is None or _int(body.get("code")) not in (0,200):
            return {"ok":False,"status":"unknown","error":"lighter_sendtx_unconfirmed","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat()}
        return await self._poll(client_order_id,cidx,submitted,acked,int(market["market_id"]))
    async def _poll(self,client_order_id,cidx,submitted,acked,market_id):
        from lighter import OrderApi
        deadline=time.monotonic()+6.; token,error=self.client.create_auth_token_with_expiry(api_key_index=int(self.api_key_index))
        if error is not None or not token: return {"ok":False,"status":"unknown","error":"lighter_auth_token_unavailable","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat()}
        api=OrderApi(self.client.api_client)
        while time.monotonic()<deadline:
            rows=_rows(await api.account_inactive_orders(account_index=int(self.account_index),limit=100,auth=token,market_id=market_id,_request_timeout=5.),"orders") or []
            matches=[r for r in rows if _int(r.get("client_order_index"))==cid]
            if len(matches)>1: return {"ok":False,"status":"unknown","error":"lighter_identity_ambiguous","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat()}
            if matches:
                row=matches[0]; status=str(row.get("status") or "").lower(); filled=_d(row.get("filled_base_amount")) or ZERO; quote=_d(row.get("filled_quote_amount")) or ZERO
                if status=="filled" and filled>0 and quote>0:
                    fill_at=_parse(row.get("updated_at") or row.get("created_at"))
                    return {"ok":True,"status":"filled","client_order_id":client_order_id,"filled_qty":str(filled),"avg_price":str(quote/filled),"order_id":str(row.get("order_index") or row.get("order_id") or ""),"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"source_at":_iso(fill_at),"source_at_epoch":fill_at.timestamp() if fill_at else None,"fee_usd":None}
                if status.startswith("canceled"): return {"ok":True,"status":"rejected","client_order_id":client_order_id,"filled_qty":str(filled),"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"error":status}
            await asyncio.sleep(.12)
        return {"ok":False,"status":"unknown","error":"lighter_fill_timeout","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat()}
def _int(v):
    p=_d(v); return int(p) if p is not None and p==p.to_integral_value() else None
class EntropyProbeExecutor:
    venue="entropy"
    def __init__(self,*,info_url,account_address,private_key,dex="io",timeout_seconds=8.0,info=None,exchange=None):
        self.info_url=info_url.rstrip("/") if info_url else info_url
        self.account_address=account_address
        self.private_key=private_key
        self.dex=dex
        self.timeout_seconds=timeout_seconds
        self.info=info
        self.exchange=exchange
        self.available=bool(account_address and private_key)
        self.meta=None
        self.universe={}
    async def start(self):
        if not self.available: return
        if self.info is None or self.exchange is None:
            from eth_account import Account
            from hyperliquid.exchange import Exchange
            from hyperliquid.info import Info
            from hyperliquid.utils import constants
            base_url=self.info_url or constants.MAINNET_API_URL
            self.info=Info(base_url,skip_ws=True,perp_dexs=[self.dex])
            self.exchange=Exchange(Account.from_key(self.private_key),base_url,account_address=self.account_address,perp_dexs=[self.dex])
        self.meta=await asyncio.to_thread(self.info.meta,self.dex)
        self.universe={str(row.get("name")):dict(row) for row in (self.meta or {}).get("universe",[]) if isinstance(row,dict)}
        if not self.universe: raise ProbeError("entropy_meta_universe_missing")
    async def close(self): return None
    def _coin(self,asset): return asset.symbol("entropy")
    def _row(self,asset):
        row=self.universe.get(self._coin(asset))
        if row is None: raise ProbeError("entropy_market_missing")
        return row
    async def preflight(self,asset):
        if not self.available or self.info is None: return {"ok":False,"error":"entropy_credentials_missing"}
        coin=self._coin(asset)
        state=await asyncio.to_thread(self.info.user_state,self.account_address,self.dex)
        for item in (state or {}).get("assetPositions",[]) or []:
            pos=item.get("position") if isinstance(item,dict) else None
            if not isinstance(pos,dict) or pos.get("coin")!=coin: continue
            szi=_d(pos.get("szi")) or ZERO
            if szi!=ZERO: return {"ok":False,"error":"entropy_position_not_flat","qty":str(szi)}
        orders=await asyncio.to_thread(self.info.open_orders,self.account_address,self.dex)
        active=[row for row in (orders or []) if str(row.get("coin") or "")==coin]
        if active: return {"ok":False,"error":"entropy_active_order","count":len(active)}
        row=self._row(asset); asset_index=int(self.info.name_to_asset(coin))
        margin=(state or {}).get("marginSummary") or {}
        return {"ok":True,"qty":"0","active_orders":0,"account_value":margin.get("accountValue"),
                "contract":{"venue":"entropy","symbol":coin,"asset_index":asset_index,
                            "sz_decimals":int(row.get("szDecimals") or 0),
                            "max_leverage":row.get("maxLeverage"),"only_isolated":bool(row.get("onlyIsolated")),
                            "dex":self.dex,"fee_token":"USDC"}}
    def normalize_qty(self,asset,qty):
        if qty<=0 or not self.universe: return None
        step=Decimal(1).scaleb(-int(self._row(asset).get("szDecimals") or 0))
        return qty.quantize(step,rounding=ROUND_DOWN)
    @staticmethod
    def _cloid(client_order_id):
        return _EntropyCloid("0x"+hashlib.sha256(client_order_id.encode()).hexdigest()[:32])
    def _limit_price(self,asset,side,bbo,max_slippage_bps):
        row=self._row(asset); szd=int(row.get("szDecimals") or 0)
        direction=Decimal("-1") if side=="sell" else Decimal("1")
        px=bbo*(Decimal("1")+direction*max_slippage_bps/BPS)
        decimals=max(0,6-szd)
        return float(round(float(f"{float(px):.5g}"),decimals))
    async def market_order(self,*,asset,side,qty,quote,client_order_id,reduce_only=False,max_slippage_bps=Decimal("8")):
        if not self.available or self.exchange is None: return {"ok":False,"status":"rejected","error":"entropy_credentials_missing","client_order_id":client_order_id}
        qty=self.normalize_qty(asset,qty)
        if not qty: return {"ok":False,"status":"rejected","error":"entropy_qty_below_min","client_order_id":client_order_id}
        bbo=_d(quote.get("ask" if side=="buy" else "bid"))
        if not bbo: return {"ok":False,"status":"rejected","error":"entropy_quote_missing","client_order_id":client_order_id}
        limit_px=self._limit_price(asset,side,bbo,max_slippage_bps)
        cloid=self._cloid(client_order_id); coin=self._coin(asset)
        submitted=datetime.now(timezone.utc); start_ms=int(submitted.timestamp()*1000)-1000
        try:
            result=await asyncio.to_thread(self.exchange.order,coin,side=="buy",float(qty),limit_px,{"limit":{"tif":"Ioc"}},reduce_only=bool(reduce_only),cloid=cloid)
        except Exception as exc:
            return {"ok":False,"status":"unknown","error":f"{type(exc).__name__}:{exc}","client_order_id":client_order_id,"submitted_at":submitted.isoformat()}
        acked=datetime.now(timezone.utc)
        status=self._order_status(result)
        if status=="rejected": return {"ok":True,"status":"rejected","filled_qty":"0","error":self._order_error(result),"client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat()}
        deadline=time.monotonic()+self.timeout_seconds
        last=None
        while time.monotonic()<deadline:
            fills=await asyncio.to_thread(self.info.user_fills_by_time,self.account_address,start_ms)
            rows=[row for row in (fills or []) if str(row.get("cloid") or "").lower()==str(cloid).lower() and str(row.get("coin") or "")==coin]
            if rows:
                qty_sum=sum((_d(row.get("sz")) or ZERO for row in rows),ZERO); notional=sum(((_d(row.get("sz")) or ZERO)*(_d(row.get("px")) or ZERO) for row in rows),ZERO)
                fee=sum((_d(row.get("fee")) or ZERO for row in rows),ZERO)
                last=max((_parse_epoch_ms(row.get("time")) for row in rows),default=None)
                return {"ok":True,"status":"filled","filled_qty":str(qty_sum),"avg_price":str(notional/qty_sum) if qty_sum else None,"fee_usd":str(fee),"order_id":str(rows[0].get("oid") or ""),"cloid":str(cloid),"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"source_at":last.isoformat() if last else None,"source_at_epoch":last.timestamp() if last else None,"raw_status":status}
            await asyncio.sleep(.1)
        return {"ok":False,"status":"unknown","error":"entropy_fill_timeout","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"cloid":str(cloid)}
    @staticmethod
    def _order_status(result):
        try:
            statuses=result["response"]["data"]["statuses"]
            if not statuses: return "unknown"
            item=statuses[0]
            if "filled" in item or "resting" in item: return "accepted"
            if "error" in item: return "rejected"
        except (TypeError,KeyError,IndexError): pass
        return "unknown"
    @staticmethod
    def _order_error(result):
        try: return str(result["response"]["data"]["statuses"][0]["error"])[:200]
        except Exception: return "entropy_order_rejected"


class UnavailableProbeExecutor:
    def __init__(self,venue,reason): self.venue=venue; self.reason=reason; self.available=False
    async def start(self): return None
    async def close(self): return None
    async def preflight(self,asset): return {"ok":False,"error":self.reason}
    def normalize_qty(self,asset,qty): return None
    async def market_order(self,**kwargs): return {"ok":False,"status":"rejected","error":self.reason,"client_order_id":kwargs.get("client_order_id")}

def enumerate_probe_plan(assets,venues):
    plan=[]; seq=0
    for asset in assets:
        supported=[v for v in venues if asset.supports(v)]
        for i,left in enumerate(supported):
            for right in supported[i+1:]:
                for buy,sell in ((left,right),(right,left)):
                    seq+=1; plan.append(ProbeRound(f"{asset.name.lower()}-{buy}-{sell}-{seq:03d}",asset.name,buy,sell,seq))
    return plan
class ProbeState:
    def __init__(self,path):
        self.path=path; self.data={"schema":1,"phase":"idle","run_id":None,"cursor":0,"rounds":[],"current":None,"orders_attempted":False}
        if path.exists():
            try:
                loaded=json.loads(path.read_text())
                if isinstance(loaded,dict): self.data.update(loaded)
            except Exception: pass
    def save(self):
        self.path.parent.mkdir(parents=True,exist_ok=True); tmp=self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data,ensure_ascii=False,indent=2,default=str)+"\n"); os.replace(tmp,self.path)
class ProbeRunner:
    def __init__(self,*,hub,executors,state,writer_path,assets,venues,notional_usd=Decimal("20"),round_interval_seconds=1.0,max_quote_age_seconds=2.5,hold_seconds=3.1,run_id=None,horizons=(.25,.5,1.,3.),max_quote_age_by_venue=None):
        self.hub=hub; self.executors=executors; self.state=state; self.writer_path=writer_path; self.assets=assets; self.venues=venues; self.by_name={a.name:a for a in assets}; self.notional_usd=notional_usd; self.round_interval_seconds=max(.2,round_interval_seconds); self.max_quote_age_seconds=max_quote_age_seconds; self.max_quote_age_by_venue=dict(max_quote_age_by_venue or {}); self.hold_seconds=max(.5,hold_seconds); self.horizons=tuple(horizons); self.run_id=run_id or "probe-"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    def _write(self,row):
        self.writer_path.parent.mkdir(parents=True,exist_ok=True)
        with self.writer_path.open("a",encoding="utf-8") as f: f.write(json.dumps(row,ensure_ascii=False,default=str)+"\n")
    async def preflight_all(self):
        out={}
        for name,ex in self.executors.items():
            if name not in self.venues:
                continue
            if not getattr(ex,"available",False):
                out[name]={"ok":False,"error":getattr(ex,"reason",f"{name}_not_live_ready")}
                continue
            for asset in self.assets:
                if asset.supports(name): out[f"{name}:{asset.name}"]=await ex.preflight(asset)
        for asset in self.assets:
            for venue in self.venues:
                if not asset.supports(venue):
                    continue
                age=self.max_quote_age_by_venue.get(venue,self.max_quote_age_seconds)
                q=self.hub.latest(venue,asset.name,max_age=age)
                key=f"{venue}:{asset.name}:market"
                out[key]={"ok":q is not None and q.get("bid") is not None and q.get("ask") is not None,
                          "quote_kind":(q or {}).get("quote_kind"),
                          "age_s":(q or {}).get("age_s"),
                          "quote_id":(q or {}).get("quote_id"),
                          "error":None if q is not None else "quote_not_ready"}
        return out
    async def run(self,*,allow_live=False):
        plan=enumerate_probe_plan(self.assets,self.venues)
        self.state.data.update({"run_id":self.run_id,"planned_rounds":len(plan),"phase":"running" if allow_live else "dry_run","venues":list(self.venues),"assets":[a.name for a in self.assets]}); self.state.save()
        if not allow_live:
            self._write({"schema":1,"event":"probe_plan","run_id":self.run_id,"rounds":[asdict(r) for r in plan]})
            return {"ok":True,"mode":"dry_run","rounds":len(plan),"plan":[asdict(r) for r in plan]}
        health=await self.preflight_all(); blocked={k:v for k,v in health.items() if not v.get("ok")}; self.state.data["preflight"]=health
        if blocked:
            self.state.data["phase"]="blocked"; self.state.save(); return {"ok":False,"mode":"live","rounds":0,"blocked":blocked}
        completed=0
        for idx,item in enumerate(plan):
            if idx<int(self.state.data.get("cursor") or 0): continue
            if self.state.data.get("phase")=="manual_review_required": break
            row=await self._run_one(item); self._write(row)
            if row.get("status") in {"filled","rolled_back","no_fill"}:
                self.state.data["rounds"].append(row); self.state.data["cursor"]=idx+1; self.state.data["current"]=None; completed+=1; self.state.save()
            else:
                self.state.data["current"]=row; self.state.data["phase"]="manual_review_required"; self.state.save(); break
            await asyncio.sleep(self.round_interval_seconds)
        self.state.data["phase"]="completed" if self.state.data.get("phase")!="manual_review_required" else "manual_review_required"; self.state.save()
        return {"ok":self.state.data["phase"]=="completed","mode":"live","rounds":completed,"state":self.state.data["phase"]}
    async def _quote(self,venue,asset):
        q=self.hub.latest(venue,asset.name,max_age=self.max_quote_age_by_venue.get(venue,self.max_quote_age_seconds))
        if q is None or _d(q.get("mid")) is None or _d(q.get("bid")) is None or _d(q.get("ask")) is None: return None
        return q
    async def _wait_quotes(self,item,timeout=8.0):
        asset=self.by_name[item.asset]; deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            b=await self._quote(item.buy_venue,asset); s=await self._quote(item.sell_venue,asset)
            if b is not None and s is not None: return b,s
            await asyncio.sleep(.1)
        return None
    def _qty(self,item,buy,sell):
        asset=self.by_name[item.asset]; bm=_d(buy.get("mid")); sm=_d(sell.get("mid"))
        if not bm or not sm: return None
        raw=min(self.notional_usd/bm,self.notional_usd/sm); vals=[]
        for v in (item.buy_venue,item.sell_venue):
            q=self.executors[v].normalize_qty(asset,raw)
            if q is None: return None
            vals.append(q)
        qty=min(vals)
        return qty if all(self.executors[v].normalize_qty(asset,qty) is not None for v in (item.buy_venue,item.sell_venue)) else None

    async def _run_one(self,item):
        asset=self.by_name[item.asset]; pair=await self._wait_quotes(item)
        if pair is None: return {"schema":1,"event":"probe_round","round_id":item.round_id,"asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,"status":"no_quote"}
        buy,sell=pair; qty=self._qty(item,buy,sell)
        if qty is None: return {"schema":1,"event":"probe_round","round_id":item.round_id,"asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,"status":"blocked","reason":"qty_below_venue_minimum"}
        ids={item.buy_venue:f"{item.round_id}-entry-{item.buy_venue}",item.sell_venue:f"{item.round_id}-entry-{item.sell_venue}"}
        row={"schema":1,"event":"probe_round","round_id":item.round_id,"asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,"status":"submitting","notional_usd":str(self.notional_usd),"target_qty":str(qty),"decision":{"at":datetime.now(timezone.utc).isoformat(),"quotes":{item.buy_venue:buy,item.sell_venue:sell},"signal_edge_bps":str(((_d(sell["bid"])-_d(buy["ask"]))/((_d(sell["bid"])+_d(buy["ask"]))/2)*BPS) if _d(sell["bid"]) and _d(buy["ask"]) else ZERO),"entry_client_order_ids":ids}}
        self.state.data["current"]=row; self.state.data["orders_attempted"]=True; self.state.save()
        results=await asyncio.gather(self.executors[item.buy_venue].market_order(asset=asset,side="buy",qty=qty,quote=buy,client_order_id=ids[item.buy_venue]),self.executors[item.sell_venue].market_order(asset=asset,side="sell",qty=qty,quote=sell,client_order_id=ids[item.sell_venue]),return_exceptions=True)
        receipts={item.buy_venue:results[0],item.sell_venue:results[1]}
        row["entry"]={"legs":{v:{**(r if isinstance(r,dict) else {"ok":False,"status":"error","error":str(r)}),"side":"buy" if v==item.buy_venue else "sell","client_order_id":ids[v]} for v,r in receipts.items()},"quotes":{item.buy_venue:buy,item.sell_venue:sell}}
        filled=[v for v,r in receipts.items() if isinstance(r,dict) and r.get("status")=="filled"]
        unknown=[v for v,r in receipts.items() if not isinstance(r,dict) or r.get("status")=="unknown"]
        if unknown: row["status"]="manual_review_required"; row["reason"]="entry_terminal_status_unknown"; return row
        if len(filled)!=2:
            row["status"]="rolled_back" if filled else "no_fill"
            if filled:
                v=filled[0]; original=receipts[v]; cq=await self._quote(v,asset); cid=f"{item.round_id}-rollback-{v}"; cs="sell" if v==item.buy_venue else "buy"
                close=await self.executors[v].market_order(asset=asset,side=cs,qty=_d(original.get("filled_qty")) or qty,quote=cq or {},client_order_id=cid,reduce_only=True)
                row["rollback"]={"leg":v,"close":close}
                if close.get("status")!="filled": row["status"]="manual_review_required"; row["reason"]="rollback_not_confirmed"
            return row
        adverse=[]; start=time.monotonic()
        for delay in self.horizons:
            await asyncio.sleep(max(0.,delay-(time.monotonic()-start))); pts={}
            for v,r in receipts.items():
                q=await self._quote(v,asset)
                if q is None: continue
                mid=_d(q.get("mid")); fill=_d(r.get("avg_price")); side="buy" if v==item.buy_venue else "sell"
                if mid is None or fill is None: continue
                pts[v]={"mid":str(mid),"adverse_bps":str(((fill-mid) if side=="buy" else (mid-fill))/fill*BPS),"quote_at":q.get("recv_at")}
            adverse.append({"delay_s":delay,"points":pts})
        row["adverse"]=adverse
        close_ids={item.buy_venue:f"{item.round_id}-exit-{item.buy_venue}",item.sell_venue:f"{item.round_id}-exit-{item.sell_venue}"}
        cqs={v:await self._quote(v,asset) for v in receipts}
        closes=await asyncio.gather(self.executors[item.buy_venue].market_order(asset=asset,side="sell",qty=_d(receipts[item.buy_venue].get("filled_qty")) or qty,quote=cqs[item.buy_venue] or {},client_order_id=close_ids[item.buy_venue],reduce_only=True),self.executors[item.sell_venue].market_order(asset=asset,side="buy",qty=_d(receipts[item.sell_venue].get("filled_qty")) or qty,quote=cqs[item.sell_venue] or {},client_order_id=close_ids[item.sell_venue],reduce_only=True),return_exceptions=True)
        row["exit"]={"quotes_at_close":cqs,"legs":{item.buy_venue:{**(closes[0] if isinstance(closes[0],dict) else {"status":"error"}),"side":"sell","client_order_id":close_ids[item.buy_venue]},item.sell_venue:{**(closes[1] if isinstance(closes[1],dict) else {"status":"error"}),"side":"buy","client_order_id":close_ids[item.sell_venue]}}}
        closed=all(isinstance(r,dict) and r.get("status")=="filled" for r in closes)
        row["status"]="filled" if closed else "manual_review_required"; row["reason"]=None if closed else "exit_not_confirmed"; return row
