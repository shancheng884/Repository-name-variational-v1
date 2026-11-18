"""Small-size, two-venue live probe rounds with idempotent order identities."""
from __future__ import annotations
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR
from pathlib import Path
from typing import Any, Protocol
import asyncio, hashlib, inspect, json, math, os, time, uuid
import requests
from tools.lib.hedge_instruments import iter_positions, lighter_position_qty
from tools.lib.entropy_order_timing import instrumented_exchange, timed_order
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
    """Parse venue timestamps expressed in seconds, ms, us or ns."""
    try:
        n=float(v)
    except (TypeError,ValueError):
        return None
    if not math.isfinite(n) or n<=0: return None
    if n>=1e17: n/=1e9
    elif n>=1e14: n/=1e6
    elif n>=1e11: n/=1e3
    try:
        return datetime.fromtimestamp(n,timezone.utc)
    except (OSError,OverflowError,ValueError):
        return None


def _pid_alive(pid):
    """Liveness probe that never signals the target process (Windows-safe)."""
    if pid in (None,""): return False
    try: target=int(pid)
    except (TypeError,ValueError): return False
    if target<=0: return False
    if os.name=="nt":
        import ctypes
        from ctypes import wintypes
        kernel32=ctypes.WinDLL("kernel32",use_last_error=True)
        handle=kernel32.OpenProcess(0x1000,False,target)
        if not handle: return False
        try:
            code=wintypes.DWORD()
            if kernel32.GetExitCodeProcess(handle,ctypes.byref(code)):
                return code.value==259
            return True
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(target,0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError,ValueError,TypeError):
        return False
    return True


class ProbeLock:
    """Exclusive single-run guard so two processes cannot submit the same plan."""

    def __init__(self,path):
        self.path=Path(path); self.held=False

    def _read(self):
        try: return json.loads(self.path.read_text())
        except (OSError,ValueError): return None

    def acquire(self,run_id):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        for _ in range(2):
            try:
                fd=os.open(str(self.path),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o644)
            except FileExistsError:
                owner=self._read()
                if owner and not _pid_alive(owner.get("pid")):
                    try: self.path.unlink()
                    except FileNotFoundError: pass
                    continue
                return False,owner or {"path":str(self.path)}
            payload={"pid":os.getpid(),"run_id":run_id,"acquired_at":datetime.now(timezone.utc).isoformat()}
            with os.fdopen(fd,"w") as fh: json.dump(payload,fh)
            self.held=True
            return True,payload
        return False,self._read() or {"path":str(self.path)}

    def release(self):
        if not self.held: return
        owner=self._read()
        if isinstance(owner,dict) and owner.get("pid")==os.getpid():
            try: self.path.unlink()
            except FileNotFoundError: pass
        self.held=False


def _clock_offset_ms(execution_quote):
    """Venue-clock offset from an execution quote echo (local recv - venue ts)."""
    if not isinstance(execution_quote,dict): return None
    for local_key,exchange_key in (("received_at","quote_timestamp"),("quote_recv_at","quote_source_at"),("received_at","quote_source_at")):
        local=_parse(execution_quote.get(local_key)); exchange=_parse(execution_quote.get(exchange_key))
        if local is not None and exchange is not None:
            return round((local-exchange).total_seconds()*1000,3)
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
def _stamp_receipt(receipt):
    """Attach local completion time and per-stage latency to an order receipt."""
    if not isinstance(receipt, dict): return receipt
    completed = datetime.now(timezone.utc)
    submitted = _parse(receipt.get("submitted_at")); acked = _parse(receipt.get("acked_at"))
    receipt.setdefault("completed_at", completed.isoformat())
    receipt.setdefault("local_fill_at", receipt["completed_at"])
    offset=_clock_offset_ms(receipt.get("execution_quote"))
    if offset is not None: receipt.setdefault("clock_offset_ms",offset)
    if submitted is not None:
        receipt.setdefault("submit_to_complete_ms", round((completed - submitted).total_seconds() * 1000, 3))
    if submitted is not None and acked is not None:
        receipt.setdefault("submit_to_ack_ms", round((acked - submitted).total_seconds() * 1000, 3))
    if acked is not None:
        receipt.setdefault("ack_to_complete_ms", round((completed - acked).total_seconds() * 1000, 3))
    if receipt.get("status") == "filled" and receipt.get("source_at_epoch") is None and receipt.get("source_at") is None:
        receipt.setdefault("source_at_missing", True)
    source = receipt.get("source_at_epoch")
    if source is not None:
        try: receipt.setdefault("exchange_fill_to_local_ms", round((completed.timestamp() - float(source)) * 1000, 3))
        except (TypeError, ValueError): pass
    return receipt


def _qty_tolerance(qty):
    """Half-percent band: the smallest size gap worth correcting on a leg."""
    if qty is None or qty <= 0:
        return ZERO
    return max(abs(qty) * Decimal("0.002"), Decimal("0.00000001"))


@dataclass(frozen=True)
class ProbeRound:
    round_id:str; asset:str; buy_venue:str; sell_venue:str; sequence:int

class VariationalProbeExecutor:
    venue="variational"; available=True; timestamp_resolution_ms=1
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
        portfolio=await self.client.request({"type":"VAR_API_PORTFOLIO","requestId":uuid.uuid4().hex,"requestTimeoutMs":int(self.timeout_seconds*1000)})
        p=(portfolio.get("result") or {}).get("portfolio") or {}
        return {"ok":True,"qty":"0","active_orders":0,"available_margin":p.get("balance"),
                "contract":{"venue":"variational","symbol":asset.symbol("variational"),
                            "instrument_type":asset.var_instrument_type,"kind":asset.var_kind,
                            "settlement_asset":"USDC","funding_interval_s":0}}
    async def positions(self,asset,fresh=False):
        """Read-only position/order snapshot used for per-round reconciliation."""
        if self.client is None: return {"ok":False,"verified":False,"error":"variational_client_not_started"}
        try:
            pos=await self.client.request({"type":"VAR_API_POSITIONS","requestId":uuid.uuid4().hex,"requestTimeoutMs":int(self.timeout_seconds*1000)})
        except Exception as exc:
            return {"ok":False,"verified":False,"error":f"variational_positions_failed:{type(exc).__name__}:{exc}"}
        positions=iter_positions(pos.get("result") if isinstance(pos,dict) else None)
        if positions is None: return {"ok":False,"verified":False,"error":"variational_positions_unverifiable"}
        symbol=asset.symbol("variational"); qty=ZERO
        for item in positions:
            nested=item.get("position_info") if isinstance(item.get("position_info"),dict) else {}
            ident=item.get("instrument") if isinstance(item.get("instrument"),dict) else nested.get("instrument") or item
            if str(ident.get("underlying") or ident.get("symbol") or "").upper()!=symbol: continue
            part=next((_d(src.get(k)) for src in (item,nested) for k in ("qty","quantity","size","position","position_size","base_amount","amount") if _d(src.get(k)) is not None),None)
            if part is not None: qty+=part
        try:
            orders=await self.client.request({"type":"VAR_API_ORDERS","requestId":uuid.uuid4().hex,"status":"pending","limit":100,"offset":0,"orderBy":"created_at","order":"desc","requestTimeoutMs":int(self.timeout_seconds*1000)})
        except Exception as exc:
            return {"ok":False,"verified":False,"qty":str(qty),"error":f"variational_orders_failed:{type(exc).__name__}:{exc}"}
        rows=_var_rows(orders.get("result") if isinstance(orders,dict) else None)
        if rows is None: return {"ok":False,"verified":False,"qty":str(qty),"error":"variational_orders_unverifiable"}
        return {"ok":True,"verified":True,"qty":str(qty),"active_orders":len([r for r in rows if _underlying(r)==symbol])}

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
    async def market_order(self,**kwargs):
        return _stamp_receipt(await self._market_order(**kwargs))
    async def _market_order(self,*,asset,side,qty,quote,client_order_id,reduce_only=False,max_slippage_bps=Decimal("8")):
        assert self.client is not None
        submitted=datetime.now(timezone.utc)
        payload={"type":"VAR_API_ORDER","requestId":client_order_id,"market":asset.symbol("variational"),"instrumentType":asset.var_instrument_type,"settlementAsset":"USDC","kind":asset.var_kind,"fundingIntervalS":0,"side":side.upper(),"amount":format(qty,"f"),"confirm":True,"maxSlippage":float(max_slippage_bps/BPS),"reduceOnly":bool(reduce_only),"requestTimeoutMs":int(self.timeout_seconds*1000),"operationTimeoutMs":int(self.timeout_seconds*1000)}
        try: message=await self.client.request(payload)
        except Exception as exc:
            return {"ok":False,"status":"unknown","error":f"{type(exc).__name__}:{exc}","client_order_id":client_order_id,"submitted_at":submitted.isoformat()}
        acked=datetime.now(timezone.utc); result=message.get("result") if isinstance(message,dict) else None
        if not isinstance(result,dict) or message.get("ok") is not True or result.get("ok") is not True:
            return {"ok":False,"status":"unknown","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"error":str((result or {}).get("error") or (message or {}).get("error") or "variational_order_unconfirmed")[:200]}
        bid=_d(result.get("bid")); ask=_d(result.get("ask")); qid=str(result.get("quoteId") or result.get("quote_id") or "") or None
        qts=str(result.get("quoteTimestamp") or result.get("timestamp") or "") or None
        exec_quote={"bid":str(bid) if bid is not None else None,"ask":str(ask) if ask is not None else None,"mid":str((bid+ask)/2) if bid is not None and ask is not None else None,"quote_id":qid,"quote_timestamp":qts,"received_at":acked.isoformat(),"submitted_at":submitted.isoformat()}
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
                    filled=_d(row.get("filled_qty") or row.get("filled_amount") or row.get("executed_qty")) or ZERO
                    px=_d(row.get("average_price") or row.get("avg_price") or row.get("price"))
                    if filled>0 and px:
                        return {"ok":True,"status":"filled","partial":True,"client_order_id":client_order_id,"filled_qty":str(filled),"avg_price":str(px),"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"rfq_id":rfq_id,"order_id":str(row.get("order_id") or "") or order_id}
                    return {"ok":True,"status":"rejected","client_order_id":client_order_id,"filled_qty":"0","submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"rfq_id":rfq_id,"order_id":str(row.get("order_id") or "") or order_id}
                if status=="cleared" and str(row.get("clearing_status") or "")=="success_trades_booked_into_pool":
                    fill_at=_parse(row.get("execution_timestamp") or row.get("created_at"))
                    return {"ok":True,"status":"filled","client_order_id":client_order_id,"filled_qty":str(_d(row.get("qty")) or qty),"avg_price":str(_d(row.get("price")) or ""),"rfq_id":rfq_id,"order_id":str(row.get("order_id") or "") or order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"source_at":_iso(fill_at),"source_at_epoch":fill_at.timestamp() if fill_at else None,"fee_usd":None}
            await asyncio.sleep(.15)
        return {"ok":False,"status":"unknown","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"rfq_id":rfq_id,"order_id":order_id,"error":"variational_fill_timeout"}

LIGHTER_MAINNET_CHAIN_ID=304
LIGHTER_TESTNET_CHAIN_ID=300
LIGHTER_RH_MAINNET_CHAIN_ID=466324


def best_bid_ask(book: dict) -> tuple:
    """Best bid/ask out of a Lighter orderBookOrders payload."""
    bids = [_d(o.get("price")) for o in ((book or {}).get("bids") or []) if isinstance(o, dict)]
    asks = [_d(o.get("price")) for o in ((book or {}).get("asks") or []) if isinstance(o, dict)]
    bid = max([b for b in bids if b is not None], default=None)
    ask = min([a for a in asks if a is not None], default=None)
    return bid, ask


def leverage_from_imf_percent(value) -> float | None:
    """Translate Lighter's rendered margin fraction into leverage.

    The account endpoint reports the on-chain initial margin fraction as a
    percent of notional: ``50.00`` is 50% margin (2x), ``5.00`` is 20x.
    """
    imf = _d(value)
    if imf is None or imf <= 0:
        return None
    return float(Decimal("100") / imf)


def lighter_signing_chain_id(rest_url: str) -> int:
    url=(rest_url or "").lower()
    if "api.rh.lighter.xyz" in url:
        return LIGHTER_RH_MAINNET_CHAIN_ID
    if "testnet" in url or "rh-testnet" in url:
        return LIGHTER_TESTNET_CHAIN_ID
    return LIGHTER_MAINNET_CHAIN_ID


class LighterProbeExecutor:
    timestamp_resolution_ms=1000  # Lighter fill stamps are whole seconds
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
        chain_id=lighter_signing_chain_id(self.rest_url)
        if int(getattr(self.client,"chain_id",0))!=chain_id:
            self.client.chain_id=chain_id
            self.client.create_client(int(self.api_key_index))
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
        acc=(account.get("accounts") or [{}])[0] if isinstance(account.get("accounts"),list) and account.get("accounts") else {}
        return {"ok":True,"qty":"0","active_orders":0,
                "available_margin":acc.get("available_balance"),
                "contract":{"venue":self.venue,"symbol":asset.symbol(self.venue),
                            "market_id":int(market["market_id"]),
                            "status":market.get("status"),"market_type":market.get("market_type"),
                            "size_decimals":int(market.get("supported_size_decimals") or 0),
                            "price_decimals":int(market.get("supported_price_decimals") or 0),
                            "min_base_amount":str(market.get("min_base_amount")),
                            "min_quote_amount":str(market.get("min_quote_amount")),
                            "multiplier":str(market.get("multiplier"))}}
    async def positions(self,asset,fresh=False):
        """Read-only position/order snapshot used for per-round reconciliation."""
        if not self.available or self.client is None: return {"ok":False,"verified":False,"error":f"{self.venue}_credentials_missing"}
        market=self.market(asset)
        if market is None: return {"ok":False,"verified":False,"error":f"{self.venue}_market_missing"}
        from lighter import AccountApi, OrderApi
        try:
            account=_model_dict(await AccountApi(self.client.api_client).account(by="index",value=str(self.account_index),_request_timeout=10.))
        except Exception as exc:
            return {"ok":False,"verified":False,"error":f"{self.venue}_account_failed:{type(exc).__name__}:{exc}"}
        if account is None: return {"ok":False,"verified":False,"error":f"{self.venue}_account_unverifiable"}
        qty=lighter_position_qty(account,market_id=int(market["market_id"]),symbol=asset.symbol(self.venue))
        if qty is None: return {"ok":False,"verified":False,"error":f"{self.venue}_position_unverifiable"}
        token,error=self.client.create_auth_token_with_expiry(api_key_index=int(self.api_key_index))
        if error is not None or not token: return {"ok":False,"verified":False,"qty":str(qty),"error":f"{self.venue}_auth_token_unavailable"}
        try:
            rows=_rows(await OrderApi(self.client.api_client).account_active_orders(auth=token,account_index=int(self.account_index),market_id=int(market["market_id"]),_request_timeout=10.),"orders")
        except Exception as exc:
            return {"ok":False,"verified":False,"qty":str(qty),"error":f"{self.venue}_orders_failed:{type(exc).__name__}:{exc}"}
        if rows is None: return {"ok":False,"verified":False,"qty":str(qty),"error":f"{self.venue}_active_orders_unverifiable"}
        return {"ok":True,"verified":True,"qty":str(qty),"active_orders":len(rows)}

    async def _account(self):
        from lighter import AccountApi
        account = _model_dict(await AccountApi(self.client.api_client).account(
            by="index", value=str(self.account_index), _request_timeout=10.0))
        if account is None:
            raise ProbeError(f"{self.venue}_account_unverifiable")
        accounts = account.get("accounts") or []
        return accounts[0] if accounts and isinstance(accounts[0], dict) else {}

    async def account_state(self, asset):
        """Available margin and this market's margin setting, with no flat requirement."""
        if not self.available or self.client is None:
            return {"ok": False, "error": f"{self.venue}_credentials_missing"}
        market = self.market(asset)
        if market is None:
            return {"ok": False, "error": f"{self.venue}_market_missing"}
        try:
            account = await self._account()
        except Exception as exc:
            return {"ok": False, "error": f"{self.venue}_account_failed:{type(exc).__name__}:{exc}"}
        row = next((p for p in (account.get("positions") or [])
                    if str(p.get("market_id")) == str(market["market_id"])), None)
        if row is None:
            return {"ok": False, "error": f"{self.venue}_position_unverifiable"}
        imf = row.get("initial_margin_fraction")
        return {"ok": True, "available_margin": account.get("available_balance"),
                "position": row.get("position"), "initial_margin_fraction": imf,
                "leverage": leverage_from_imf_percent(imf),
                "margin_mode": row.get("margin_mode"),
                "margin_set_flag": row.get("margin_set_flag")}

    async def ensure_leverage(self, asset, leverage, *, margin_mode=0, attempts=12, delay=0.7):
        """Raise the market's leverage and prove it from a fresh account read.

        Returns ok only when the venue reports the requested leverage back; a
        submitted transaction that never shows up in the account is a failure.
        """
        target = float(leverage)
        market = self.market(asset)
        if market is None:
            return {"ok": False, "error": f"{self.venue}_market_missing"}
        before = await self.account_state(asset)
        if before.get("ok") and before.get("leverage") and abs(before["leverage"] - target) < 0.05:
            return {"ok": True, "changed": False, "leverage": before["leverage"],
                    "initial_margin_fraction": before.get("initial_margin_fraction")}
        if not self.available or self.client is None:
            return {"ok": False, "error": f"{self.venue}_credentials_missing"}
        try:
            _tx, response, error = await self.client.update_leverage(
                market_index=int(market["market_id"]), margin_mode=int(margin_mode),
                leverage=target, api_key_index=int(self.api_key_index))
        except Exception as exc:
            return {"ok": False, "error": f"lighter_update_leverage_failed:{type(exc).__name__}:{exc}"}
        if error is not None:
            return {"ok": False, "error": f"lighter_update_leverage_error:{error}"}
        code = _int(getattr(response, "code", None)) if response is not None else None
        if response is not None and code not in (0, 200):
            return {"ok": False, "error": "lighter_update_leverage_rejected", "response_code": code}
        last = None
        for _ in range(max(1, attempts)):
            await asyncio.sleep(delay)
            last = await self.account_state(asset)
            if last.get("ok") and last.get("leverage") and abs(last["leverage"] - target) < 0.05:
                return {"ok": True, "changed": True, "leverage": last["leverage"],
                        "initial_margin_fraction": last.get("initial_margin_fraction")}
        return {"ok": False, "error": "leverage_readback_mismatch", "target": target,
                "readback": (last or {}).get("leverage"),
                "initial_margin_fraction": (last or {}).get("initial_margin_fraction")}

    def normalize_qty(self,asset,qty):
        market=self.market(asset)
        if market is None or qty<=0: return None
        d=int(market.get("supported_size_decimals") or 0); step=Decimal(1).scaleb(-d)
        out=qty.quantize(step,rounding=ROUND_DOWN); minimum=_d(market.get("min_base_amount")) or ZERO
        return out if out>0 and out>=minimum else None
    async def market_order(self,**kwargs):
        receipt=await self._market_order(**kwargs)
        if isinstance(receipt,dict) and receipt.get("execution_quote") is None:
            receipt["execution_quote"]=getattr(self,"_last_exec_quote",None)
        return _stamp_receipt(receipt)
    async def _bbo(self, market):
        try:
            response = await asyncio.to_thread(
                requests.get, f"{self.rest_url}/api/v1/orderBookOrders",
                params={"market_id": int(market["market_id"]), "limit": 5}, timeout=10)
            response.raise_for_status()
            return best_bid_ask(response.json() or {})
        except Exception:
            return None, None

    async def _market_order(self,*,asset,side,qty,quote,client_order_id,reduce_only=False,max_slippage_bps=Decimal("8")):
        if not self.available or self.client is None: return {"ok":False,"status":"rejected","error":f"{self.venue}_credentials_missing","client_order_id":client_order_id}
        market=self.market(asset)
        if market is None: return {"ok":False,"status":"rejected","error":f"{self.venue}_market_missing","client_order_id":client_order_id}
        qty=self.normalize_qty(asset,qty)
        if not qty: return {"ok":False,"status":"rejected","error":"lighter_qty_below_min","client_order_id":client_order_id}
        sd=int(market.get("supported_size_decimals") or 0); pd=int(market.get("supported_price_decimals") or 0)
        bbo=_d(quote.get("ask" if side=="buy" else "bid"))
        if not bbo:
            # Flattening and settling pass no quote; fetch the live book instead of
            # refusing, otherwise a rescue order can never be sent.
            bid, ask = await self._bbo(market)
            bbo = ask if side == "buy" else bid
            if bbo is not None:
                quote = dict(quote or {})
                quote.setdefault("bid", str(bid) if bid is not None else "")
                quote.setdefault("ask", str(ask) if ask is not None else "")
                quote.setdefault("mid", str((bid + ask) / 2) if bid is not None and ask is not None else "")
        if not bbo: return {"ok":False,"status":"rejected","error":"lighter_quote_missing","client_order_id":client_order_id}
        direction=Decimal("-1") if side=="sell" else Decimal("1")
        limit=(bbo*(Decimal("1")+direction*max_slippage_bps/BPS)).quantize(Decimal(1).scaleb(-pd),rounding=ROUND_CEILING if side=="buy" else ROUND_FLOOR)
        if qty*limit < (_d(market.get("min_quote_amount")) or ZERO): return {"ok":False,"status":"rejected","error":"lighter_notional_below_min","client_order_id":client_order_id}
        self._last_exec_quote={"bid":str(_d(quote.get("bid"))) if _d(quote.get("bid")) is not None else None,"ask":str(_d(quote.get("ask"))) if _d(quote.get("ask")) is not None else None,"mid":str(_d(quote.get("mid"))) if _d(quote.get("mid")) is not None else None,"limit_price":str(limit),"quote_sequence":quote.get("sequence"),"quote_recv_at":quote.get("recv_at"),"quote_source_at":quote.get("source_at")}
        base=qty*(Decimal(10)**sd); price=limit*(Decimal(10)**pd)
        if base!=base.to_integral_value() or price!=price.to_integral_value(): return {"ok":False,"status":"rejected","error":"lighter_precision_invalid","client_order_id":client_order_id}
        cidx=lighter_client_order_index(client_order_id); submitted=datetime.now(timezone.utc)
        try:
            _o,response,error=await self.client.create_order(market_index=int(market["market_id"]),client_order_index=cidx,base_amount=int(base),price=int(price),is_ask=side=="sell",order_type=self.client.ORDER_TYPE_MARKET,time_in_force=self.client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,order_expiry=self.client.DEFAULT_IOC_EXPIRY,reduce_only=bool(reduce_only),api_key_index=int(self.api_key_index))
        except Exception as exc:
            return {"ok":False,"status":"unknown","error":f"{type(exc).__name__}:{exc}","client_order_id":client_order_id,"submitted_at":submitted.isoformat()}
        acked=datetime.now(timezone.utc); body=_model_dict(response)
        if error is not None or body is None or _int(body.get("code")) not in (0,200):
            return {"ok":False,"status":"unknown","error":"lighter_sendtx_unconfirmed","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"response_code":(_int(body.get("code")) if body else None),"sdk_error":(str(error)[:300] if error is not None else None)}
        receipt=await self._poll(client_order_id,cidx,submitted,acked,int(market["market_id"]))
        if isinstance(receipt,dict): receipt.setdefault("execution_quote",getattr(self,"_last_exec_quote",None))
        return receipt
    async def lookup_ioc(self, asset, client_order_id):
        market = self.market(asset)
        if market is None or self.client is None:
            return {"status": "unknown"}
        return await self._poll(client_order_id, lighter_client_order_index(client_order_id),
                                None, None, int(market["market_id"]), single_read=True)

    async def _poll(self,client_order_id,cidx,submitted,acked,market_id,*,single_read=False):
        from lighter import OrderApi
        deadline=time.monotonic()+6.; token,error=self.client.create_auth_token_with_expiry(api_key_index=int(self.api_key_index))
        identity={"client_order_id":client_order_id,"submitted_at":_iso(submitted),"acked_at":_iso(acked)}
        if error is not None or not token: return {**identity,"ok":False,"status":"unknown","error":"lighter_auth_token_unavailable"}
        api=OrderApi(self.client.api_client)
        limited=0
        while time.monotonic()<deadline:
            try:
                rows=_rows(await api.account_inactive_orders(account_index=int(self.account_index),limit=100,auth=token,market_id=market_id,_request_timeout=min(5.,max(.1,deadline-time.monotonic()))),"orders") or []
            except Exception as exc:
                limited+=1
                if single_read or not _is_rate_limit(exc) or limited>=4:
                    return {**identity,"ok":False,"status":"unknown","error":"lighter_receipt_rate_limited" if _is_rate_limit(exc) else "lighter_receipt_unavailable"}
                await asyncio.sleep(min(.35*2**(limited-1),max(0,deadline-time.monotonic())))
                continue
            matches=[r for r in rows if _int(r.get("client_order_index"))==cidx]
            if len(matches)>1: return {**identity,"ok":False,"status":"unknown","error":"lighter_identity_ambiguous"}
            if matches:
                row=matches[0]; status=str(row.get("status") or "").lower(); filled=_d(row.get("filled_base_amount")) or ZERO; quote=_d(row.get("filled_quote_amount")) or ZERO
                if (status=="filled" or status.startswith("canceled")) and filled>0 and quote>0:
                    fill_at=_parse_epoch_ms(row.get("updated_at") or row.get("created_at") or row.get("transaction_time"))
                    return {**identity,"ok":True,"status":"filled","partial":status!="filled","filled_qty":str(filled),"avg_price":format(quote/filled,"f"),"order_id":str(row.get("order_index") or row.get("order_id") or ""),"source_at":_iso(fill_at),"source_at_epoch":fill_at.timestamp() if fill_at else None,"fee_usd":None,"is_ask":row.get("is_ask"),"reduce_only":row.get("reduce_only")}
                if status.startswith("canceled") and filled==0:
                    return {**identity,"ok":True,"status":"rejected","filled_qty":"0","error":status}
            if single_read:
                break
            await asyncio.sleep(.12)
        return {**identity,"ok":False,"status":"unknown","error":"lighter_fill_timeout"}
def _int(v):
    p=_d(v); return int(p) if p is not None and p==p.to_integral_value() else None
async def _to_thread_retry(fn,*args,attempts=4,base_delay=0.4,**kwargs):
    """Run a blocking venue call, backing off while the venue rate-limits us.

    Hyperliquid answers 429 with an empty body, which surfaces as a bare
    ClientError.  Treating that as a hard failure is what aborted a live round.
    """
    last=None
    for index in range(max(1,attempts)):
        try:
            return await asyncio.to_thread(fn,*args,**kwargs)
        except Exception as exc:
            last=exc
            if index==attempts-1:
                break
            await asyncio.sleep(base_delay*(2**index))
    raise last


def _is_rate_limit(exc):
    text=str(exc).lower()
    return "429" in text or "rate limit" in text or "too many" in text


def _fill_window(*,submitted,acked,observed,exchange_at,resolution_ms):
    """Bound the true fill time instead of inventing a precise one.

    Venues stamp fills at a fixed resolution (Lighter: whole seconds).  A point
    estimate built from a truncated stamp is wrong by up to a full tick, which
    is why a 70ms round trip used to read as 1.1s.  The truth is the overlap of
    ``[exchange_at, exchange_at+resolution)`` with ``[ack, observed]``.
    """
    out={"resolution_ms":None if resolution_ms is None else int(resolution_ms),"lower_ms":None,"upper_ms":None,
         "exchange_at":_iso(exchange_at),"ack_at":_iso(acked),"observed_at":_iso(observed),"capped_by_poll":False}
    if submitted is None or observed is None: return out
    lower=acked or submitted
    if exchange_at is not None and resolution_ms:
        ex_lo=exchange_at; ex_hi=exchange_at+timedelta(milliseconds=float(resolution_ms))
        if ex_hi>lower: lower=max(lower,ex_lo)
        upper=min(observed,ex_hi)
        if upper<lower:
            lower=min(max(ex_lo,acked or submitted),observed); upper=observed
            out["capped_by_poll"]=True
    else:
        upper=observed
    out["lower_ms"]=round(max(0.,(lower-submitted).total_seconds()*1000),3)
    out["upper_ms"]=round(max(0.,(upper-submitted).total_seconds()*1000),3)
    return out
class EntropyProbeExecutor:
    venue="entropy"; timestamp_resolution_ms=1
    def __init__(self,*,info_url,account_address,private_key,dex="io",timeout_seconds=15.0,info=None,exchange=None,
                 fill_ws_url=None, enable_fill_stream=False):
        base=(info_url or "").rstrip("/")
        if base.endswith("/info"):
            base=base[:-5].rstrip("/")
        self.info_url=base
        self.account_address=(account_address or "").strip()
        self.private_key=(private_key or "").strip()
        self.dex=dex
        self.timeout_seconds=timeout_seconds
        self.info=info
        self.exchange=exchange
        address_ok=bool(self.account_address.startswith("0x") and len(self.account_address)==42)
        key_hex=self.private_key[2:] if self.private_key.startswith("0x") else self.private_key
        key_ok=bool(self.private_key.startswith("0x") and len(self.private_key)==66)
        try:
            if key_ok: bytes.fromhex(key_hex)
            else: key_ok=False
        except ValueError:
            key_ok=False
        if not self.account_address:
            self.reason="entropy_account_address_missing"
        elif not address_ok:
            self.reason="entropy_account_address_invalid"
        elif not self.private_key:
            self.reason="entropy_private_key_missing"
        elif not key_ok:
            self.reason="entropy_private_key_must_be_32_bytes"
        else:
            self.reason=None
        self.available=bool(address_ok and key_ok)
        self.meta=None
        self.universe={}
        self._positions_cache={}
        self._fill_stream = None
        self._fill_ws_url = fill_ws_url or "wss://api.hyperliquid.xyz/ws"
        self._enable_fill_stream = enable_fill_stream
    async def start(self):
        if not self.available: return
        if self.info is None or self.exchange is None:
            from eth_account import Account
            from hyperliquid.exchange import Exchange
            from hyperliquid.info import Info
            from hyperliquid.utils import constants
            base_url=self.info_url or constants.MAINNET_API_URL
            self.info=Info(base_url,skip_ws=True,perp_dexs=[self.dex])
            self.exchange=instrumented_exchange(Exchange,Account.from_key(self.private_key),base_url,account_address=self.account_address,perp_dexs=[self.dex])
        self.meta=await asyncio.to_thread(self.info.meta,self.dex)
        self.universe={str(row.get("name")):dict(row) for row in (self.meta or {}).get("universe",[]) if isinstance(row,dict)}
        if not self.universe: raise ProbeError("entropy_meta_universe_missing")
        if self._enable_fill_stream:
            from tools.lib.entropy_fill_stream import EntropyFillStream
            self._fill_stream = EntropyFillStream(self._fill_ws_url, self.account_address)
            self._fill_stream.start()
    async def close(self):
        if getattr(self, "_fill_stream", None):
            await self._fill_stream.close()
    def _coin(self,asset): return asset.symbol("entropy")
    def enrich_receipt(self, receipt, asset):
        stream = getattr(self, "_fill_stream", None)
        if not stream or receipt.get("fee_usd") is not None:
            return receipt
        ratio = Decimal(receipt.get("fee_allocation_ratio") or "1")
        expected = _d(receipt.get("filled_qty"))
        rows = stream.rows(receipt.get("order_id"), self._coin(asset), expected / ratio if expected else None)
        if rows and all(row.get("fee") is not None for row in rows):
            receipt = dict(receipt)
            fee = sum((_d(row.get("fee")) or ZERO for row in rows), ZERO)
            receipt["fee_usd"] = str(fee * Decimal(receipt.get("fee_allocation_ratio") or "1"))
            receipt["fee_pending"] = False
            receipt.update({k: v for k, v in self._exchange_fill_times(rows).items() if v is not None})
        return receipt
    def _row(self,asset):
        row=self.universe.get(self._coin(asset))
        if row is None: raise ProbeError("entropy_market_missing")
        return row
    async def preflight(self,asset):
        if not self.available or self.info is None: return {"ok":False,"error":self.reason or "entropy_credentials_missing"}
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
        spot=await asyncio.to_thread(self.info.spot_user_state,self.account_address)
        available=None
        for item in (spot or {}).get("tokenToAvailableAfterMaintenance",[]) or []:
            if isinstance(item,(list,tuple)) and len(item)>=2 and int(item[0])==0:
                available=item[1]; break
        return {"ok":True,"qty":"0","active_orders":0,"account_value":margin.get("accountValue"),
                "available_margin":available,
                "spot_usdc_total":next((row.get("total") for row in (spot or {}).get("balances",[]) if str(row.get("coin"))=="USDC"),None),
                "spot_usdc_hold":next((row.get("hold") for row in (spot or {}).get("balances",[]) if str(row.get("coin"))=="USDC"),None),
                "contract":{"venue":"entropy","symbol":coin,"asset_index":asset_index,
                            "sz_decimals":int(row.get("szDecimals") or 0),
                            "max_leverage":row.get("maxLeverage"),"only_isolated":bool(row.get("onlyIsolated")),
                            "dex":self.dex,"fee_token":"USDC"}}
    async def positions(self,asset,fresh=False):
        """Read-only position/order snapshot used for per-round reconciliation."""
        if not self.available or self.info is None: return {"ok":False,"verified":False,"error":self.reason or "entropy_credentials_missing"}
        coin=self._coin(asset)
        cached=self._positions_cache.get(coin)
        if not fresh and cached and time.monotonic()-cached[0]<2.0:
            return dict(cached[1])
        try:
            state=await _to_thread_retry(self.info.user_state,self.account_address,self.dex,attempts=6,base_delay=0.6)
            orders=await _to_thread_retry(self.info.open_orders,self.account_address,self.dex)
        except Exception as exc:
            return {"ok":False,"verified":False,"error":f"entropy_positions_failed:{type(exc).__name__}:{exc}"}
        if state is None or orders is None: return {"ok":False,"verified":False,"error":"entropy_positions_unverifiable"}
        qty=ZERO
        for item in (state or {}).get("assetPositions",[]) or []:
            pos=item.get("position") if isinstance(item,dict) else None
            if not isinstance(pos,dict) or pos.get("coin")!=coin: continue
            qty+=(_d(pos.get("szi")) or ZERO)
        active=len([row for row in (orders or []) if str(row.get("coin") or "")==coin])
        result={"ok":True,"verified":True,"qty":str(qty),"active_orders":active}
        self._positions_cache[coin]=(time.monotonic(),result)
        return dict(result)

    async def account_state(self, asset):
        """Available margin, position and per-asset leverage without a flat requirement."""
        if not self.available or self.info is None:
            return {"ok": False, "error": self.reason or "entropy_credentials_missing"}
        coin = self._coin(asset)
        try:
            state = await _to_thread_retry(self.info.user_state, self.account_address, self.dex,
                                           attempts=4, base_delay=0.5)
            spot = await _to_thread_retry(self.info.spot_user_state, self.account_address,
                                          attempts=4, base_delay=0.5)
        except Exception as exc:
            return {"ok": False, "error": f"entropy_account_failed:{type(exc).__name__}:{exc}"}
        if state is None:
            return {"ok": False, "error": "entropy_account_unverifiable"}
        qty = ZERO
        leverage = None
        for item in (state or {}).get("assetPositions", []) or []:
            pos = item.get("position") if isinstance(item, dict) else None
            if not isinstance(pos, dict) or pos.get("coin") != coin:
                continue
            qty += _d(pos.get("szi")) or ZERO
            raw = pos.get("leverage")
            if isinstance(raw, dict) and _d(raw.get("value")):
                leverage = float(_d(raw.get("value")))
        if leverage is None:
            try:
                max_lev = _d(self._row(asset).get("maxLeverage"))
            except ProbeError:
                max_lev = None
            leverage = float(max_lev) if max_lev else None
        available = None
        for item in (spot or {}).get("tokenToAvailableAfterMaintenance", []) or []:
            if isinstance(item, (list, tuple)) and len(item) >= 2 and int(item[0]) == 0:
                available = item[1]
                break
        return {"ok": True, "available_margin": available, "position": str(qty),
                "leverage": leverage}


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
    async def market_order(self,**kwargs):
        attempts = []
        receipt=await self._market_order(**kwargs, timing_attempts=attempts)
        if attempts:
            receipt["order_timing_attempts"] = attempts
        if isinstance(receipt,dict) and receipt.get("execution_quote") is None:
            receipt["execution_quote"]=getattr(self,"_last_exec_quote",None)
        return _stamp_receipt(receipt)
    async def _market_order(self,*,asset,side,qty,quote,client_order_id,reduce_only=False,max_slippage_bps=Decimal("8"),known_position_before=None,retry_rate_limit=True,timing_attempts=None):
        if not self.available or self.exchange is None: return {"ok":False,"status":"rejected","error":self.reason or "entropy_credentials_missing","client_order_id":client_order_id}
        qty=self.normalize_qty(asset,qty)
        if not qty: return {"ok":False,"status":"rejected","error":"entropy_qty_below_min","client_order_id":client_order_id}
        bbo=_d(quote.get("ask" if side=="buy" else "bid"))
        if not bbo:
            try:
                book=await _to_thread_retry(self.info.l2_snapshot,self._coin(asset),attempts=3)
                levels=(book or {}).get("levels") or [[],[]]
                side_levels=levels[1 if side=="buy" else 0]
                bbo=_d(side_levels[0].get("px")) if side_levels else None
            except Exception:
                bbo=None
        if not bbo: return {"ok":False,"status":"rejected","error":"entropy_quote_missing","client_order_id":client_order_id}
        limit_px=self._limit_price(asset,side,bbo,max_slippage_bps)
        self._last_exec_quote={"bid":str(_d(quote.get("bid"))) if _d(quote.get("bid")) is not None else None,"ask":str(_d(quote.get("ask"))) if _d(quote.get("ask")) is not None else None,"mid":str(bbo),"limit_price":str(limit_px),"quote_recv_at":quote.get("recv_at"),"quote_source_at":quote.get("source_at")}
        self._last_exec_quote.update({k: quote.get(k) for k in ("bid_size", "ask_size", "depth_audit", "time_alignment")})
        cloid=self._cloid(client_order_id); coin=self._coin(asset)
        before_qty=_d(known_position_before)
        if known_position_before is None:
            try:
                before=await self.positions(asset,fresh=True)
                if before.get("verified"): before_qty=_d(before.get("qty"))
            except Exception:
                before_qty=None
        submitted=datetime.now(timezone.utc); start_ms=int(submitted.timestamp()*1000)-5000
        result=None; attempts=0
        while True:
            attempts+=1
            timing = {"attempt": attempts}
            if timing_attempts is not None:
                timing_attempts.append(timing)
            try:
                result=await asyncio.to_thread(timed_order,timing,self.exchange.order,coin,side=="buy",float(qty),limit_px,{"limit":{"tif":"Ioc"}},reduce_only=bool(reduce_only),cloid=cloid)
                break
            except Exception as exc:
                timing["error_type"] = type(exc).__name__
                # The cloid is the order identity: a retry either replays the
                # same order or is rejected as a duplicate, never doubles it.
                if retry_rate_limit and _is_rate_limit(exc) and attempts<4:
                    await asyncio.sleep(0.5*attempts)
                    continue
                return {"ok":False,"status":"unknown","error":f"{type(exc).__name__}:{exc}","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"submit_attempts":attempts}
        acked=datetime.now(timezone.utc)
        status=self._order_status(result)
        if status=="rejected": return {"ok":True,"status":"rejected","filled_qty":"0","error":self._order_error(result),"client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat()}
        ack_fill = self._ack_fill(result, qty)
        oid = (ack_fill or {}).get("order_id")
        stream = getattr(self, "_fill_stream", None)
        async def rest_rows():
            while time.monotonic() < deadline:
                fills = await asyncio.to_thread(self.info.user_fills_by_time, self.account_address, start_ms)
                rows = [row for row in (fills or []) if str(row.get("coin") or "") == coin
                        and ((oid and str(row.get("oid")) == oid)
                             or str(row.get("cloid") or "").lower() == str(cloid).lower())]
                if rows and (not ack_fill or sum((_d(r.get("sz")) or ZERO for r in rows), ZERO) >= _d(ack_fill["filled_qty"])):
                    return rows
                await asyncio.sleep(.1)
            return []
        deadline=time.monotonic()+self.timeout_seconds
        last=None; delay=0.1; poll_errors=0
        while time.monotonic()<deadline:
            try:
                rows = stream.rows(oid, coin, _d(ack_fill["filled_qty"])) if stream and ack_fill else None
                details_source = "user_fills_ws" if rows else "rest_fallback"
                if not rows:
                    tasks = [asyncio.create_task(rest_rows())]
                    if stream and ack_fill:
                        tasks.append(asyncio.create_task(stream.wait(oid, coin, _d(ack_fill["filled_qty"]))))
                    try:
                        done, _ = await asyncio.wait(tasks, timeout=max(0, deadline-time.monotonic()),
                                                     return_when=asyncio.FIRST_COMPLETED)
                        if not done:
                            break
                        winner = next(iter(done))
                        rows = winner.result()
                        details_source = "user_fills_ws" if len(tasks)>1 and winner is tasks[1] else "rest_fallback"
                    finally:
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as exc:
                # A rate-limited fill poll must not abort a round whose order may
                # already have filled: keep polling until the deadline.
                poll_errors+=1
                await asyncio.sleep(min(delay,0.5)); delay=min(delay*2,0.5); continue
            if rows:
                qty_sum=sum((_d(row.get("sz")) or ZERO for row in rows),ZERO); notional=sum(((_d(row.get("sz")) or ZERO)*(_d(row.get("px")) or ZERO) for row in rows),ZERO)
                if ack_fill and qty_sum < _d(ack_fill["filled_qty"]):
                    await asyncio.sleep(.05)
                    continue
                fee=(sum((_d(row.get("fee")) or ZERO for row in rows),ZERO)
                     if all(row.get("fee") is not None for row in rows) else None)
                exchange_times = self._exchange_fill_times(rows)
                last = _parse(exchange_times["exchange_last_fill_at"])
                return {"ok":True,"status":"filled","filled_qty":str(qty_sum),"avg_price":str(notional/qty_sum) if qty_sum else None,"fee_usd":str(fee) if fee is not None else None,"order_id":str(rows[0].get("oid") or ""),"cloid":str(cloid),"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"source_at":last.isoformat() if last else None,"source_at_epoch":last.timestamp() if last else None,"raw_status":status,
                        "confirmation_source":"exchange_ack" if ack_fill else details_source,
                        **exchange_times,
                        "fill_details_source":details_source, "ack_fill":ack_fill, "fee_pending":fee is None}
            await asyncio.sleep(delay)
            delay=min(delay*1.5,0.5)
        if ack_fill:
            return {"ok": True, "status": "filled", **ack_fill, "fee_usd": None,
                    "fee_pending": True, "cloid": str(cloid), "submitted_at": submitted.isoformat(),
                    "acked_at": acked.isoformat(), "source_at": None, "source_at_epoch": None,
                    "exchange_first_fill_at": None, "exchange_last_fill_at": None,
                    "confirmation_source": "exchange_ack", "fill_details_source": "unavailable"}
        if not reduce_only and before_qty is not None and known_position_before is None:
            # A pre-existing position is not evidence that THIS order filled.
            # Only the signed change between the pre-order and post-timeout reads
            # can be attributed to it, and never more than the order allowed.
            state=None
            try:
                state=await _to_thread_retry(self.info.user_state,self.account_address,self.dex,attempts=3)
            except Exception:
                state=None
            qty_now=ZERO
            entry=None
            for item in (state or {}).get("assetPositions",[]) or []:
                pos=item.get("position") if isinstance(item,dict) else None
                if not isinstance(pos,dict) or pos.get("coin")!=coin: continue
                qty_now+=(_d(pos.get("szi")) or ZERO)
                entry=_d(pos.get("entryPx")) or entry
            delta=qty_now-before_qty
            expected_sign=Decimal("1") if side=="buy" else Decimal("-1")
            if delta!=ZERO and (delta>0)==(expected_sign>0) and abs(delta)<=abs(qty)*Decimal("1.5"):
                return {"ok":True,"status":"filled","filled_qty":str(abs(delta)),"avg_price":str(entry) if entry else None,"fee_usd":None,"order_id":None,"cloid":str(cloid),"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"source_at":None,"source_at_epoch":None,"raw_status":"position_delta","partial":True,"position_before":str(before_qty),"position_after":str(qty_now)}
        return {"ok":False,"status":"unknown","error":"entropy_fill_timeout","client_order_id":client_order_id,"submitted_at":submitted.isoformat(),"acked_at":acked.isoformat(),"cloid":str(cloid)}
    @staticmethod
    def _exchange_fill_times(rows):
        times = [_parse_epoch_ms(row.get("time")) for row in rows]
        if not times or any(t is None for t in times):
            return {"exchange_first_fill_at": None, "exchange_last_fill_at": None}
        return {"exchange_first_fill_at": _iso(min(times)), "exchange_last_fill_at": _iso(max(times))}

    @staticmethod
    def _ack_fill(result, requested):
        try:
            row = result["response"]["data"]["statuses"][0]["filled"]
            qty, price = _d(row["totalSz"]), _d(row["avgPx"])
            if qty is None or price is None or not qty.is_finite() or not price.is_finite():
                return None
            if qty <= 0 or qty > requested or price <= 0 or row.get("oid") is None:
                return None
            return {"filled_qty": str(qty), "avg_price": str(price), "order_id": str(row["oid"]),
                    "partial": qty < requested}
        except (KeyError, TypeError, IndexError):
            return None

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
    async def positions(self,asset,fresh=False): return {"ok":False,"verified":False,"error":self.reason}
    def normalize_qty(self,asset,qty): return None
    async def market_order(self,**kwargs): return {"ok":False,"status":"rejected","error":self.reason,"client_order_id":kwargs.get("client_order_id")}

def enumerate_probe_plan(assets,venues,pairs=None,rounds_per_pair=1):
    """Round-robin a plan.

    The default sweeps every ordered venue pair once.  ``pairs`` restricts the
    plan to explicit ``(asset,buy,sell)`` combinations and repeats them
    round-robin, so a targeted validation never touches a combination the
    operator did not ask for and never stacks ten identical rounds in a row.
    """
    plan=[]; seq=0
    if pairs is not None:
        repeats=max(1,int(rounds_per_pair or 1))
        for _ in range(repeats):
            for asset_name,buy,sell in pairs:
                seq+=1
                plan.append(ProbeRound(f"{asset_name.lower()}-{buy}-{sell}-{seq:03d}",asset_name,buy,sell,seq))
        return plan
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
    TERMINAL={"filled","rolled_back","no_fill","skipped"}
    def __init__(self,*,hub,executors,state,writer_path,assets,venues,notional_usd=Decimal("20"),round_interval_seconds=1.0,max_quote_age_seconds=2.5,hold_seconds=3.1,run_id=None,horizons=(.25,.5,1.,3.),max_quote_age_by_venue=None,max_rounds=0,lock_path=None,market_quote_wait_seconds=15.0,min_edge_bps=None,exit_on_revert=False,exit_edge_bps=0.0,max_hold_seconds=600.0,exit_poll_seconds=2.0,min_edge_by_pair=None,baseline_entry=False,baseline_minutes=30.0,min_baseline_samples=240,max_loss_usd=None):
        self.hub=hub; self.executors=executors; self.state=state; self.writer_path=writer_path; self.assets=assets; self.venues=venues; self.by_name={a.name:a for a in assets}; self.notional_usd=notional_usd; self.round_interval_seconds=max(.2,round_interval_seconds); self.max_quote_age_seconds=max_quote_age_seconds; self.max_quote_age_by_venue=dict(max_quote_age_by_venue or {}); self.market_quote_wait_seconds=float(market_quote_wait_seconds); self.min_edge_bps=None if min_edge_bps is None else Decimal(str(min_edge_bps)); self.exit_on_revert=bool(exit_on_revert); self.exit_edge_bps=Decimal(str(exit_edge_bps)); self.max_hold_seconds=max(5.0,float(max_hold_seconds)); self.exit_poll_seconds=max(0.5,float(exit_poll_seconds));
        self.min_edge_by_pair={k:Decimal(str(v)) for k,v in (min_edge_by_pair or {}).items()};
        self.baseline_entry=bool(baseline_entry); self.baseline_seconds=max(60.0,float(baseline_minutes)*60.0);
        self.min_baseline_samples=max(2,int(min_baseline_samples));
        self.max_loss_usd=None if max_loss_usd is None else Decimal(str(max_loss_usd));
        self._edge_history={}; self._realized_usd=Decimal("0"); self.stopped_reason=None; self.hold_seconds=max(.5,hold_seconds); self.horizons=tuple(horizons); self.max_rounds=max(0,int(max_rounds)); self.run_id=run_id or "probe-"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")+"-"+uuid.uuid4().hex[:4]; self.run_token=hashlib.sha256(self.run_id.encode()).hexdigest()[:8]
        self.lock=ProbeLock(lock_path or (Path(self.state.path).parent/"run.lock"))
        self._last_quote_reject=None
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
        required=self.notional_usd/Decimal("5")
        for key,value in list(out.items()):
            if not value.get("ok") or key.endswith(":market"):
                continue
            margin=_d(value.get("available_margin"))
            if margin is not None and margin < required:
                value.update({"ok":False,"error":f"{key.split(':')[0]}_insufficient_margin",
                              "required_margin":str(required)})
        for asset in self.assets:
            for venue in self.venues:
                if not asset.supports(venue):
                    continue
                age=self.max_quote_age_by_venue.get(venue,self.max_quote_age_seconds)
                key=f"{venue}:{asset.name}:market"
                q=None
                # A quiet or reconnecting venue needs a moment to publish its
                # first quote.  Failing preflight on the very first look turns
                # a cold feed into a blocked run.
                deadline=time.monotonic()+self.market_quote_wait_seconds
                while True:
                    q=self.hub.latest(venue,asset.name,max_age=age)
                    if q is not None and q.get("bid") is not None and q.get("ask") is not None:
                        break
                    if time.monotonic()>=deadline:
                        break
                    await asyncio.sleep(0.75)
                out[key]={"ok":q is not None and q.get("bid") is not None and q.get("ask") is not None,
                          "quote_kind":(q or {}).get("quote_kind"),
                          "age_s":(q or {}).get("age_s"),
                          "quote_id":(q or {}).get("quote_id"),
                          "error":None if q is not None else "quote_not_ready"}
        return out
    def _unresolved_round(self):
        state=self.state.data; current=state.get("current")
        if isinstance(current,dict) and state.get("orders_attempted") and current.get("status") not in self.TERMINAL:
            return current
        if state.get("phase")=="manual_review_required":
            return current if isinstance(current,dict) else {"status":"manual_review_required"}
        return None
    def _cid(self,item,kind,venue):
        return f"{self.run_token}-{item.round_id}-{kind}-{venue}"
    async def run(self,*,allow_live=False,resume=False):
        plan=enumerate_probe_plan(self.assets,self.venues,pairs=getattr(self,"pairs",None),
                                  rounds_per_pair=getattr(self,"rounds_per_pair",1))
        if not allow_live:
            self.state.data.update({"run_id":self.run_id,"planned_rounds":len(plan),"phase":"dry_run","venues":list(self.venues),"assets":[a.name for a in self.assets]}); self.state.save()
            self._write({"schema":1,"event":"probe_plan","run_id":self.run_id,"rounds":[asdict(r) for r in plan]})
            return {"ok":True,"mode":"dry_run","rounds":len(plan),"plan":[asdict(r) for r in plan]}
        acquired,owner=self.lock.acquire(self.run_id)
        if not acquired:
            self.state.data["blocked_reason"]="probe_already_running"; self.state.data["blocked_owner"]=owner; self.state.save()
            return {"ok":False,"mode":"live","rounds":0,"orders_attempted":False,"error":"probe_already_running","owner":owner}
        try:
            return await self._run_live(plan,resume=resume)
        finally:
            self.lock.release()

    async def _resume_pending(self,pending):
        """Clear a stalled round only when both accounts prove it is flat."""
        asset=self.by_name.get(str(pending.get("asset")))
        buy=pending.get("buy_venue"); sell=pending.get("sell_venue")
        if asset is None or buy not in self.executors or sell not in self.executors:
            return {"round_id":pending.get("round_id"),"error":"pending_round_unknown_venues"}
        check=await self.reconcile(asset,(buy,sell))
        if not check["flat"]:
            return {"round_id":pending.get("round_id"),"error":"pending_round_not_flat","reconcile":check}
        self.state.data.setdefault("resumed_rounds",[]).append({"round_id":pending.get("round_id"),"reason":pending.get("reason"),"reconcile":check})
        self.state.data["current"]=None; self.state.data["orders_attempted"]=False; self.state.data["phase"]="running"
        self.state.save()
        return None

    async def _run_live(self,plan,resume=False):
        previous=self.state.data.get("run_id")
        if previous and previous!=self.run_id and resume:
            pending=self._unresolved_round(); blocked=None
            if pending is not None:
                blocked=await self._resume_pending(pending)
            if blocked is not None:
                self.state.data["blocked_reason"]="pending_round_not_flat"; self.state.data["blocked_round"]=blocked; self.state.save()
                return {"ok":False,"mode":"live","rounds":0,"orders_attempted":False,"error":"pending_round_not_flat","round":blocked}
            self.state.data["resumed_from"]=previous
        elif previous and previous!=self.run_id:
            pending=self._unresolved_round()
            if pending is not None:
                blocked={"round_id":pending.get("round_id"),"status":pending.get("status"),"reason":pending.get("reason")}
                self.state.data["blocked_reason"]="unresolved_round_requires_review"; self.state.data["blocked_round"]=blocked; self.state.save()
                return {"ok":False,"mode":"live","rounds":0,"orders_attempted":False,"error":"unresolved_round_requires_review","round":blocked}
            self.state.data.update({"cursor":0,"rounds":[],"current":None,"orders_attempted":False,"previous_run_id":previous})
            self.state.data.pop("blocked_reason",None); self.state.data.pop("blocked_round",None)
        self.state.data.update({"run_id":self.run_id,"planned_rounds":len(plan),"phase":"running","pid":os.getpid(),"venues":list(self.venues),"assets":[a.name for a in self.assets]}); self.state.save()
        health=await self.preflight_all(); blocked={k:v for k,v in health.items() if not v.get("ok")}; self.state.data["preflight"]=health
        if blocked:
            self.state.data["phase"]="blocked"; self.state.save(); return {"ok":False,"mode":"live","rounds":0,"blocked":blocked}
        completed=0
        for idx,item in enumerate(plan):
            if idx<int(self.state.data.get("cursor") or 0): continue
            if self.max_rounds and completed>=self.max_rounds: break
            if self.state.data.get("phase")=="manual_review_required": break
            try:
                row=await self._run_one(item)
            except Exception as exc:
                current=self.state.data.get("current") or {"schema":1,"event":"probe_round","round_id":item.round_id,"asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue}
                current["status"]="manual_review_required"; current["reason"]=f"runtime_exception:{type(exc).__name__}:{exc}"
                self.state.data["current"]=current; self.state.data["phase"]="manual_review_required"; self.state.save(); self._write(current)
                return {"ok":False,"mode":"live","rounds":completed,"state":"manual_review_required","error":current["reason"]}
            self._write(row)
            if row.get("status")=="filled":
                self._realized_usd+=self._round_pnl_usd(row)
                if self.max_loss_usd is not None and self._realized_usd<=-abs(self.max_loss_usd):
                    self.state.data["current"]=row; self.state.data["realized_usd"]=str(self._realized_usd)
                    self.state.data["phase"]="stopped_max_loss"; self.state.data["stopped_reason"]="max_loss_reached"
                    self.state.save(); self.stopped_reason="max_loss_reached"
                    return {"ok":False,"mode":"live","rounds":completed,"state":"stopped_max_loss","realized_usd":str(self._realized_usd)}
            if row.get("status") in self.TERMINAL:
                self.state.data["rounds"].append(row); self.state.data["cursor"]=idx+1; self.state.data["current"]=None; completed+=1; self.state.save()
            else:
                self.state.data["current"]=row; self.state.data["phase"]="manual_review_required"; self.state.save(); break
            await asyncio.sleep(self.round_interval_seconds)
        self.state.data["realized_usd"]=str(self._realized_usd)
        self.state.data["phase"]="completed" if self.state.data.get("phase") not in ("manual_review_required","stopped_max_loss") else self.state.data.get("phase"); self.state.save()
        return {"ok":self.state.data["phase"]=="completed","mode":"live","rounds":completed,"state":self.state.data["phase"]}
    async def _quote(self,venue,asset,*,allow_stale=False):
        """Latest quote for a venue, rejecting ladders a print has disproved.

        `provably_stale` comes from the capture hub: a trade printed through the
        quoted top after the snapshot.  Trading on that ladder is a guaranteed
        adverse fill, so it is treated exactly like a missing quote.
        """
        q=self.hub.latest(venue,asset.name,max_age=self.max_quote_age_by_venue.get(venue,self.max_quote_age_seconds))
        if q is None or _d(q.get("mid")) is None or _d(q.get("bid")) is None or _d(q.get("ask")) is None:
            self._last_quote_reject={"venue":venue,"asset":asset.name,"reason":"quote_unavailable"}
            return None
        if q.get("provably_stale") and not allow_stale:
            self._last_quote_reject={"venue":venue,"asset":asset.name,"reason":"quote_provably_stale","evidence":q.get("staleness_evidence")}
            return None
        self._last_quote_reject=None
        return q
    async def _wait_quotes(self,item,timeout=8.0):
        asset=self.by_name[item.asset]; deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            b=await self._quote(item.buy_venue,asset); s=await self._quote(item.sell_venue,asset)
            if b is not None and s is not None: return b,s
            await asyncio.sleep(.1)
        return None
    @staticmethod
    def _round_pnl_usd(row):
        """Realised round-trip P&L in dollars, from the fills themselves."""
        try:
            buy=(row.get("entry") or {}).get("legs",{})
            sell=(row.get("exit") or {}).get("legs",{})
            bv=row.get("buy_venue"); sv=row.get("sell_venue")
            q=min(_d(buy[bv]["filled_qty"]),_d(buy[sv]["filled_qty"]))
            return ( _d(sell[bv]["avg_price"])-_d(buy[bv]["avg_price"]) )*q + ( _d(buy[sv]["avg_price"])-_d(sell[sv]["avg_price"]) )*q
        except Exception:
            return ZERO

    def _remember_edge(self,key,value,now_ts):
        from collections import deque
        history=self._edge_history.setdefault(key,deque())
        history.append((now_ts,float(value)))
        cutoff=now_ts-self.baseline_seconds*2
        while history and history[0][0]<cutoff: history.popleft()

    def _baseline(self,key,now_ts):
        """Rolling median of the pair's own edge: the level, not the dislocation."""
        history=self._edge_history.get(key) or ()
        window=[v for t,v in history if now_ts-t<=self.baseline_seconds]
        if len(window)<self.min_baseline_samples: return None
        ordered=sorted(window)
        return ordered[len(ordered)//2]

    @staticmethod
    def _cross_edge(asset,buy_quote,sell_quote):
        """Executable cross-venue edge in bps: sell at bid, buy at ask."""
        b=_d((buy_quote or {}).get("executable_buy")); s=_d((sell_quote or {}).get("executable_sell"))
        if b is None or s is None or b<=0: return None
        mid=(b+s)/2
        return (s-b)/mid*BPS

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

    async def reconcile(self,asset,venues):
        """Read-only flat check on both legs, used before and after every round."""
        out={"checked_at":datetime.now(timezone.utc).isoformat(),"flat":True,"venues":{}}
        for venue in venues:
            ex=self.executors.get(venue)
            source="positions"
            try:
                fn=getattr(ex,"positions",None)
                if callable(fn):
                    result=await fn(asset)
                elif ex is not None and hasattr(ex,"preflight"):
                    source="preflight"; result=await ex.preflight(asset)
                else:
                    source="none"; result={"ok":False,"verified":False,"error":"executor_missing"}
            except Exception as exc:
                source="exception"; result={"ok":False,"verified":False,"error":f"{type(exc).__name__}:{exc}"}
            payload=result if isinstance(result,dict) else {"ok":False,"error":"invalid_result"}
            qty=_d(payload.get("qty")); active=payload.get("active_orders")
            try: active_count=None if active in (None,"") else int(active)
            except (TypeError,ValueError): active_count=None
            verified=bool(payload.get("verified",payload.get("ok") is True)) and qty is not None
            flat=bool(verified and qty==ZERO and (active_count is None or active_count==0))
            out["venues"][venue]={"source":source,"verified":verified,"qty":None if qty is None else str(qty),"active_orders":active_count,"flat":flat,"error":payload.get("error")}
            if not flat: out["flat"]=False
        return out

    async def _run_one(self,item):
        asset=self.by_name[item.asset]
        pre=await self.reconcile(asset,(item.buy_venue,item.sell_venue))
        if not pre["flat"]:
            unverified=any((v or {}).get("qty") is None for v in (pre.get("venues") or {}).values())
            if unverified:
                # A rate-limited read is not a position: pause this round and try
                # again next time instead of stopping an unattended run.
                return {"schema":2,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,
                        "asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,
                        "status":"skipped","reason":"pre_round_account_unverified","orders_attempted":False,
                        "reconcile":{"pre":pre,"post":None}}
            return {"schema":2,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,"asset":item.asset,
                    "buy_venue":item.buy_venue,"sell_venue":item.sell_venue,"status":"manual_review_required",
                    "reason":"pre_round_position_not_flat","orders_attempted":False,"reconcile":{"pre":pre,"post":None}}
        row=await self._execute_round(item)
        if row.get("status")!="skipped":
            post=await self.reconcile(asset,(item.buy_venue,item.sell_venue))
            row["reconcile"]={"pre":pre,"post":post}
            if row.get("status") in self.TERMINAL and not post["flat"]:
                row["status"]="manual_review_required"; row["reason"]="post_round_position_not_flat"
        else:
            row["reconcile"]={"pre":pre,"post":None}
        return row

    async def _position_qty(self,asset,venue):
        """Verified position quantity, or None when the venue cannot prove it."""
        executor=self.executors.get(venue)
        fn=getattr(executor,"positions",None)
        if not callable(fn): return None
        try:
            result=await fn(asset)
        except Exception:
            return None
        if not isinstance(result,dict): return None
        verified=bool(result.get("verified",result.get("ok") is True))
        qty=_d(result.get("qty"))
        return qty if verified and qty is not None else None

    async def _close_leg(self,asset,item,venue,side,qty,tag):
        """One reduce-only flatten leg with its own deterministic client id."""
        cid=self._cid(item,tag,venue); quote=await self._quote(venue,asset)
        receipt=await self.executors[venue].market_order(asset=asset,side=side,qty=qty,quote=quote or {},client_order_id=cid,reduce_only=True,max_slippage_bps=Decimal("50"))
        return cid,receipt

    async def _resolve_unknown_entry(self,asset,item,qty):
        """Prove an unknown entry leg on the exchange before touching anything.

        An unknown status is not proof of a miss.  Reading both accounts first
        is what stops a recovery from closing the good leg while the unknown
        leg is actually open, which is how a probe becomes a naked position.
        """
        held={item.buy_venue:await self._position_qty(asset,item.buy_venue),
              item.sell_venue:await self._position_qty(asset,item.sell_venue)}
        out={"position_qty":{v:(None if q is None else str(q)) for v,q in held.items()},"decision":None,"flattened":{}}
        buy=held[item.buy_venue]; sell=held[item.sell_venue]
        if buy is None or sell is None:
            # Never flatten against an unreadable account: the unreadable venue
            # may be the leg that actually filled, and closing the other one
            # turns a hedged pair into a naked position (this is exactly how a
            # rate-limited read left a naked 0.125 NVDA long).
            out["decision"]="unresolved"; return out
        if buy is not None and sell is not None and buy==ZERO and sell==ZERO:
            out["decision"]="flat_confirmed"; return out
        if buy is not None and sell is not None and buy>ZERO and sell<ZERO and abs(abs(buy)-abs(sell))<=_qty_tolerance(qty):
            out["decision"]="both_legs_confirmed"; return out
        for venue,position in held.items():
            if position is None or position==ZERO: continue
            side="sell" if position>ZERO else "buy"
            cid,receipt=await self._close_leg(asset,item,venue,side,abs(position),"entry-unknown-flatten")
            out["flattened"][venue]={"client_order_id":cid,"receipt":receipt}
        out["decision"]="flattened"; return out

    async def _execute_round(self,item):
        asset=self.by_name[item.asset]; pair=await self._wait_quotes(item)
        if pair is None: return {"schema":1,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,"asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,"status":"skipped","reason":(self._last_quote_reject or {}).get("reason") or "quote_unavailable","quote_reject":self._last_quote_reject}
        buy,sell=pair; qty=self._qty(item,buy,sell)
        if qty is None: return {"schema":1,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,"asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,"status":"skipped","reason":"qty_below_venue_minimum"}
        # Decision quotes are the signal; submit quotes are what each order was
        # actually priced against.  Keeping both is what makes the cost split
        # half_spread / signal->exec / exec->fill complete instead of empty.
        signal_edge=((_d(sell["bid"])-_d(buy["ask"]))/((_d(sell["bid"])+_d(buy["ask"]))/2)*BPS) if _d(sell["bid"]) and _d(buy["ask"]) else ZERO
        threshold=self.min_edge_by_pair.get((item.asset,item.buy_venue,item.sell_venue),self.min_edge_bps) if self.min_edge_by_pair else self.min_edge_bps
        now_ts=time.time(); key=(item.asset,item.buy_venue,item.sell_venue)
        self._remember_edge(key,signal_edge,now_ts)
        baseline=self._baseline(key,now_ts) if self.baseline_entry else None
        if self.baseline_entry and baseline is None:
            return {"schema":2,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,
                    "asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,
                    "status":"skipped","reason":"baseline_warming_up","orders_attempted":False,
                    "signal_edge_bps":str(signal_edge),"notional_usd":str(self.notional_usd)}
        measured=(signal_edge-Decimal(str(baseline))) if self.baseline_entry else signal_edge
        if threshold is not None and measured<threshold:
            # Conditional entry: a validation run must only spend money when the
            # dislocation clears the entry bar, and must record what it saw on
            # the passes it skipped so the skip rate stays auditable.
            return {"schema":2,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,
                    "asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,
                    "status":"skipped","reason":"edge_below_threshold","orders_attempted":False,
                    "signal_edge_bps":str(signal_edge),"measured_bps":str(measured),
                    "baseline_bps":None if baseline is None else str(baseline),"threshold_bps":str(threshold),
                    "notional_usd":str(self.notional_usd),
                    "decision":{"at":datetime.now(timezone.utc).isoformat(),
                                "quotes":{item.buy_venue:buy,item.sell_venue:sell},
                                "submit_quotes":{item.buy_venue:buy,item.sell_venue:sell},
                                "signal_edge_bps":str(signal_edge)}}
        submit={}
        for venue,decision_quote in ((item.buy_venue,buy),(item.sell_venue,sell)):
            fresh=await self._quote(venue,asset)
            if fresh is None and (self._last_quote_reject or {}).get("reason")=="quote_provably_stale":
                return {"schema":1,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,
                        "asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,
                        "status":"skipped","reason":"quote_provably_stale_before_submit",
                        "quote_reject":self._last_quote_reject}
            submit[venue]=fresh or decision_quote
        # The decision quote and the submit quote are ~1s apart.  Re-prove the
        # edge on the prices the orders will actually be priced against, or a
        # dislocation that closed in that second is bought at a loss.
        submit_edge=self._cross_edge(asset,submit.get(item.buy_venue),submit.get(item.sell_venue))
        if threshold is not None and (submit_edge is None or submit_edge<threshold):
            return {"schema":2,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,
                    "asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,
                    "status":"skipped","reason":"edge_below_threshold_at_submit","orders_attempted":False,
                    "signal_edge_bps":str(signal_edge),
                    "submit_edge_bps":None if submit_edge is None else str(submit_edge),
                    "threshold_bps":str(threshold),"notional_usd":str(self.notional_usd),
                    "decision":{"at":datetime.now(timezone.utc).isoformat(),
                                "quotes":{item.buy_venue:buy,item.sell_venue:sell},
                                "submit_quotes":submit,"signal_edge_bps":str(signal_edge)}}
        ids={item.buy_venue:self._cid(item,"entry",item.buy_venue),item.sell_venue:self._cid(item,"entry",item.sell_venue)}

        row={"schema":2,"event":"probe_round","run_id":self.run_id,"round_id":item.round_id,"asset":item.asset,"buy_venue":item.buy_venue,"sell_venue":item.sell_venue,"status":"submitting","notional_usd":str(self.notional_usd),"target_qty":str(qty),"decision":{"at":datetime.now(timezone.utc).isoformat(),"quotes":{item.buy_venue:buy,item.sell_venue:sell},"submit_quotes":submit,"signal_edge_bps":str(signal_edge),"baseline_bps":None if baseline is None else str(baseline),
                "entry_client_order_ids":ids,
                "quote_audit":{v:{"age_s":(buy if v==item.buy_venue else sell).get("age_s"),
                                  "book_age_s":(buy if v==item.buy_venue else sell).get("book_age_s"),
                                  "bbo_age_s":(buy if v==item.buy_venue else sell).get("bbo_age_s"),
                                  "quote_kind":(buy if v==item.buy_venue else sell).get("quote_kind"),
                                  "provably_stale":bool((buy if v==item.buy_venue else sell).get("provably_stale"))}
                            for v in (item.buy_venue,item.sell_venue)}}}
        self.state.data["current"]=row; self.state.data["orders_attempted"]=True; self.state.save()
        results=await asyncio.gather(self.executors[item.buy_venue].market_order(asset=asset,side="buy",qty=qty,quote=submit[item.buy_venue],client_order_id=ids[item.buy_venue]),self.executors[item.sell_venue].market_order(asset=asset,side="sell",qty=qty,quote=submit[item.sell_venue],client_order_id=ids[item.sell_venue]),return_exceptions=True)
        receipts={item.buy_venue:results[0],item.sell_venue:results[1]}
        for venue,receipt in receipts.items():
            if not isinstance(receipt,dict): continue
            receipt["timestamp_resolution_ms"]=getattr(self.executors[venue],"timestamp_resolution_ms",None)
            receipt["fill_window"]=_fill_window(submitted=_parse(receipt.get("submitted_at")),
                                                acked=_parse(receipt.get("acked_at")),
                                                observed=_parse(receipt.get("completed_at")),
                                                exchange_at=_parse(receipt.get("source_at")),
                                                resolution_ms=getattr(self.executors[venue],"timestamp_resolution_ms",None))
        row["entry"]={"legs":{v:{**(r if isinstance(r,dict) else {"ok":False,"status":"error","error":str(r)}),"side":"buy" if v==item.buy_venue else "sell","client_order_id":ids[v]} for v,r in receipts.items()},"quotes":{item.buy_venue:buy,item.sell_venue:sell},"submit_quotes":submit}
        self.state.data["current"]=row; self.state.save()
        unknown=[v for v,r in receipts.items() if not isinstance(r,dict) or r.get("status") in ("unknown","error")]
        if unknown:
            resolution=await self._resolve_unknown_entry(asset,item,qty)
            row["entry"]["recovery"]=resolution
            if resolution["decision"]=="both_legs_confirmed":
                for venue in list(receipts):
                    if venue not in unknown: continue
                    held=abs(_d(resolution["position_qty"].get(venue)) or qty)
                    receipt=receipts[venue] if isinstance(receipts[venue],dict) else {}
                    price=_d(receipt.get("avg_price")) or _d(submit[venue].get("mid")) or _d(buy.get("mid") if venue==item.buy_venue else sell.get("mid"))
                    receipts[venue]={**receipt,"ok":True,"status":"filled","filled_qty":str(held),"avg_price":str(price) if price is not None else None,"recovered_from":"position_snapshot","price_inferred":True}
                    row["entry"]["legs"][venue]={**receipts[venue],"side":"buy" if venue==item.buy_venue else "sell","client_order_id":ids[venue]}
                unknown=[]
            elif resolution["decision"]=="flat_confirmed":
                row["status"]="no_fill"; row["reason"]="entry_unknown_but_accounts_flat"
                self.state.data["current"]=row; self.state.save(); return row
            else:
                confirmed=all((detail.get("receipt") or {}).get("status")=="filled" for detail in resolution["flattened"].values()) if resolution["flattened"] else False
                row["status"]="manual_review_required"
                row["reason"]="entry_unknown_flatten_confirmed" if confirmed and resolution["flattened"] else "entry_unknown_position_unresolved"
                self.state.data["current"]=row; self.state.save(); return row
        filled=[v for v,r in receipts.items() if isinstance(r,dict) and r.get("status")=="filled"]
        if len(filled)!=2:
            row["status"]="rolled_back" if filled else "no_fill"
            if filled:
                v=filled[0]; held=await self._position_qty(asset,v); size=_d(receipts[v].get("filled_qty"))
                if held is not None and held==ZERO:
                    row["rollback"]={"leg":v,"resolution":"position_flat_confirmed"}
                elif size is None and held is None:
                    row["status"]="manual_review_required"; row["reason"]="known_leg_size_unknown_no_rollback"
                else:
                    close_qty=abs(held) if held is not None else size
                    side="sell" if v==item.buy_venue else "buy"
                    cid,close=await self._close_leg(asset,item,v,side,close_qty,"rollback")
                    row["rollback"]={"leg":v,"client_order_id":cid,"close":close}
                    if not isinstance(close,dict) or close.get("status")!="filled":
                        row["status"]="manual_review_required"; row["reason"]="rollback_not_confirmed"
            self.state.data["current"]=row; self.state.save(); return row
        buy_qty=_d(receipts[item.buy_venue].get("filled_qty")); sell_qty=_d(receipts[item.sell_venue].get("filled_qty"))
        if buy_qty is None or sell_qty is None:
            row["status"]="manual_review_required"; row["reason"]="entry_fill_size_unknown"
            self.state.data["current"]=row; self.state.save(); return row
        if abs(buy_qty-sell_qty)>_qty_tolerance(qty):
            bigger=item.buy_venue if buy_qty>sell_qty else item.sell_venue
            excess=abs(buy_qty-sell_qty)
            normalized=self.executors[bigger].normalize_qty(asset,excess)
            if normalized is None or normalized<=0:
                row["status"]="manual_review_required"; row["reason"]="entry_leg_size_mismatch_unhedgeable"
                self.state.data["current"]=row; self.state.save(); return row
            side="sell" if bigger==item.buy_venue else "buy"
            cid,resize=await self._close_leg(asset,item,bigger,side,normalized,"entry-resize")
            row["entry"]["resize"]={"venue":bigger,"qty":str(normalized),"client_order_id":cid,"receipt":resize}
            if not isinstance(resize,dict) or resize.get("status")!="filled":
                row["status"]="manual_review_required"; row["reason"]="entry_leg_size_mismatch_resize_failed"
                self.state.data["current"]=row; self.state.save(); return row
            if bigger==item.buy_venue: buy_qty-=normalized; receipts[bigger]["filled_qty"]=str(buy_qty)
            else: sell_qty-=normalized; receipts[bigger]["filled_qty"]=str(sell_qty)
            receipts[bigger]["resized_qty"]=str(normalized)
            row["entry"]["legs"][bigger]={**receipts[bigger],"side":"buy" if bigger==item.buy_venue else "sell","client_order_id":ids[bigger]}
        adverse=[]; start=time.monotonic()
        first_fill=next((_parse(r.get("completed_at")) for r in receipts.values() if isinstance(r,dict) and r.get("completed_at")),None)
        for delay in self.horizons:
            await asyncio.sleep(max(0.,delay-(time.monotonic()-start))); pts={}
            for v,r in receipts.items():
                q=await self._quote(v,asset,allow_stale=True)
                if q is None: continue
                mid=_d(q.get("mid")); fill=_d(r.get("avg_price")); side="buy" if v==item.buy_venue else "sell"
                if mid is None or fill is None: continue
                quote_at=_parse(q.get("recv_at"))
                # A markout is only a measurement when the quote was published
                # after the fill.  Reusing the pre-fill ladder would report a
                # fake "no adverse move" for exactly the venues that lag most.
                predates=bool(first_fill is not None and quote_at is not None and quote_at<first_fill)
                stale=bool(q.get("provably_stale"))
                pts[v]={"mid":str(mid),"adverse_bps":str(((fill-mid) if side=="buy" else (mid-fill))/fill*BPS),
                        "quote_at":q.get("recv_at"),"quote_age_s":q.get("age_s"),
                        "book_age_s":q.get("book_age_s"),"bbo_age_s":q.get("bbo_age_s"),
                        "quote_kind":q.get("quote_kind"),
                        "valid":not predates and not stale,
                        "invalid_reason":("quote_predates_fill" if predates else ("quote_provably_stale" if stale else None))}
            adverse.append({"delay_s":delay,"points":pts})
        row["adverse"]=adverse
        close_ids={v:self._cid(item,"exit",v) for v in receipts}
        exit_reason="immediate"; held_from=time.monotonic(); exit_edge=None
        while True:
            cqs={v:await self._quote(v,asset) for v in receipts}
            exit_edge=self._cross_edge(asset,cqs.get(item.buy_venue),cqs.get(item.sell_venue))
            if not self.exit_on_revert:
                break
            target=self.exit_edge_bps if baseline is None else Decimal(str(baseline))+self.exit_edge_bps
            if exit_edge is not None and exit_edge<=target:
                exit_reason="reverted"; break
            if time.monotonic()-held_from>=self.max_hold_seconds:
                exit_reason="deadline"; break
            await asyncio.sleep(self.exit_poll_seconds)
        closes=await asyncio.gather(self.executors[item.buy_venue].market_order(asset=asset,side="sell",qty=_d(receipts[item.buy_venue].get("filled_qty")) or qty,quote=cqs[item.buy_venue] or {},client_order_id=close_ids[item.buy_venue],reduce_only=True,max_slippage_bps=Decimal("50")),self.executors[item.sell_venue].market_order(asset=asset,side="buy",qty=_d(receipts[item.sell_venue].get("filled_qty")) or qty,quote=cqs[item.sell_venue] or {},client_order_id=close_ids[item.sell_venue],reduce_only=True,max_slippage_bps=Decimal("50")),return_exceptions=True)
        row["exit"]={"reason":exit_reason,"hold_seconds":round(time.monotonic()-held_from,3),
                     "edge_at_exit_bps":None if exit_edge is None else str(exit_edge),
                     "quotes_at_close":cqs,"legs":{item.buy_venue:{**(closes[0] if isinstance(closes[0],dict) else {"status":"error"}),"side":"sell","client_order_id":close_ids[item.buy_venue]},item.sell_venue:{**(closes[1] if isinstance(closes[1],dict) else {"status":"error"}),"side":"buy","client_order_id":close_ids[item.sell_venue]}},"recovery":{}}
        closed=True
        for index,venue in enumerate((item.buy_venue,item.sell_venue)):
            leg=row["exit"]["legs"][venue]
            if leg.get("status")=="filled": continue
            held=await self._position_qty(asset,venue)
            detail={"first_status":leg.get("status"),"position_after":None if held is None else str(held)}
            if held==ZERO:
                leg.update({"status":"filled","recovered_from":"position_snapshot"}); detail["resolution"]="position_flat_confirmed"
                row["exit"]["recovery"][venue]=detail; continue
            if held is None:
                detail["resolution"]="position_unverifiable"; row["exit"]["recovery"][venue]=detail; closed=False; continue
            side="sell" if venue==item.buy_venue else "buy"
            cid,retry=await self._close_leg(asset,item,venue,side,abs(held),"exit-retry")
            detail["retry"]={"client_order_id":cid,"receipt":retry}
            if isinstance(retry,dict) and retry.get("status")=="filled":
                row["exit"]["legs"][venue]={**retry,"side":side,"client_order_id":cid,"retried":True}
                detail["resolution"]="retry_confirmed"
            else:
                detail["resolution"]="retry_unconfirmed"; closed=False
            row["exit"]["recovery"][venue]=detail
        row["status"]="filled" if closed else "manual_review_required"; row["reason"]=None if closed else "exit_not_confirmed"; return row
