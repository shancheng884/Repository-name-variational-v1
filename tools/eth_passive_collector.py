#!/usr/bin/env python3
"""Keep ETH median history warm using only existing browser traffic/public books."""
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal as D
import json
import logging
from pathlib import Path
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import websockets
from tools.basis_collector import MultiLighterBooks, parse_timestamp
from tools.lib.frozen_median import Observation
from variational.listener import VariationalMonitor, WS_PRICES_PATH

ETH_URL = "https://omni.variational.io/perpetual/ETH"
OUT = ROOT / "log/eth_passive"
JOURNAL = ROOT / "log/median_samples"


def eth_live_running():
    for p in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            args = p.read_bytes().split(b"\0")
        except OSError:
            continue
        if any(a.replace(b"\\",b"/").rsplit(b"/",1)[-1]==b"main.py" for a in args) and b"--live-allowed-assets" in args:
            i = args.index(b"--live-allowed-assets")
            if i+1 < len(args) and b"ETH" in args[i+1].split(b","):
                return True
    return False


def observation(monitor, book, now):
    reference = monitor.reference_quotes.get("ETH")
    quote = monitor.signal_quotes.get("ETH")
    if not reference or not quote or not book:
        return None, "waiting_for_both_sources"
    if not book.get("continuity_ok") or book.get("cold"):
        return None, "lighter_resyncing"
    try:
        identity = (quote.get("raw") or {}).get("instrument") or {}
        if (identity.get("underlying") != "ETH" or identity.get("instrument_type") != "perpetual_future"
                or identity.get("settlement_asset") != "USDC" or str(identity.get("funding_interval_s")) != "3600"):
            return None, "ETH_instrument_unverified"
        # Keep real source times, including body-capture latency; never renew a
        # cached price's timestamp just because this one-second timer fired.
        timestamps = [parse_timestamp(x.get(k)) for x in (reference, quote)
                      for k in ("timestamp", "received_at")]
        if any(t is None for t in timestamps):
            return None, "source_timestamp_missing"
        if any(t.timestamp() > now for t in timestamps):
            return None, "source_timestamp_future"
        source = min(t.timestamp() for t in timestamps)
        server = float(book["server_timestamp_ms"])/1000
        local = now-float(book["book_age_seconds"])
        if server > now+0.1:
            return None, "lighter_timestamp_future"
        raw = reference.get("raw") or {}
        size_key = raw.get("__source_url")
        if not size_key or WS_PRICES_PATH not in size_key:
            return None, "reference_provenance_missing"
        row = Observation(at=now, source_at=source,
            event_id=str(reference["timestamp"])+"|"+str(quote["timestamp"]),
            instrument="ETH:USDC:passive_indicative_bid_ask", size_key=size_key,
            var_bid=D(str(quote["bid"])), var_ask=D(str(quote["ask"])),
            lighter_bid=D(str(book["bid"])), lighter_ask=D(str(book["ask"])),
            lighter_at=min(server, local))
        return (row, None) if row.valid(var_max_age=5,lighter_max_age=2) else (None,"stale_or_invalid_pair")
    except (KeyError, ValueError, TypeError, ArithmeticError):
        return None, "invalid_pair_metadata"


class Journal:
    def __init__(self, root=JOURNAL):
        self.root = Path(root)
        self.last_second = -1
        for path in sorted(self.root.glob("*.jsonl"))[-1:]:
            with path.open("rb") as f:
                f.seek(max(0,path.stat().st_size-65536))
                for line in reversed(f.read().splitlines()):
                    try:
                        self.last_second = int(json.loads(line)["at"])
                        break
                    except (ValueError,KeyError,TypeError):
                        continue

    def append(self, row):
        if int(row.at) <= self.last_second:
            return False
        self.root.mkdir(parents=True,exist_ok=True)
        path=self.root/(datetime.fromtimestamp(row.at,timezone.utc).strftime("%Y-%m-%d")+".jsonl")
        with path.open("a",encoding="utf-8") as f:
            f.write(json.dumps({"schema":1,**asdict(row)},default=str)+"\n")
        self.last_second=int(row.at)
        return True


async def capture(monitor):
    tabs=await asyncio.to_thread(lambda:json.load(urllib.request.urlopen(
        "http://127.0.0.1:9222/json/list",timeout=5)))
    target=next((t for t in tabs if t.get("type")=="page" and t.get("url")==ETH_URL),None)
    if target is None:
        version=await asyncio.to_thread(lambda:json.load(urllib.request.urlopen(
            "http://127.0.0.1:9222/json/version",timeout=5)))
        async with websockets.connect(version["webSocketDebuggerUrl"]) as browser:
            await browser.send(json.dumps({"id":1,"method":"Target.createTarget",
                "params":{"url":ETH_URL,"background":True}}))
            while True:
                reply=json.loads(await asyncio.wait_for(browser.recv(),10))
                if reply.get("id")==1:
                    if reply.get("error"):raise RuntimeError("ETH_background_tab_create_failed")
                    target_id=reply["result"]["targetId"];break
        tabs=await asyncio.to_thread(lambda:json.load(urllib.request.urlopen(
            "http://127.0.0.1:9222/json/list",timeout=5)))
        target=next(t for t in tabs if t["id"]==target_id)
    async with websockets.connect(target["webSocketDebuggerUrl"],max_size=8*1024*1024) as ws:
        sequence=1;responses={};bodies={};sockets={}
        await ws.send(json.dumps({"id":sequence,"method":"Network.enable"}))
        # Only this non-trading ETH tab is reloaded once to observe its socket
        # creation. The attached WTI tab, extension and trading process stay put.
        sequence+=1
        await ws.send(json.dumps({"id":sequence,"method":"Page.reload"}))
        async for raw in ws:
            m=json.loads(raw);p=m.get("params") or {};method=m.get("method")
            if method=="Page.frameNavigated" and not p.get("frame",{}).get("parentId"):
                if p["frame"].get("url")!=ETH_URL:
                    raise RuntimeError("ETH_observation_tab_navigated_away")
            elif method=="Network.webSocketCreated":
                if str(p.get("url","")).endswith(WS_PRICES_PATH):
                    sockets[p["requestId"]]=p["url"]
            elif method=="Network.webSocketFrameReceived" and p.get("requestId") in sockets:
                await monitor.process_ws_event({"kind":"ws_frame","direction":"received",
                    "url":sockets[p["requestId"]],"timestamp":datetime.now(timezone.utc).isoformat(),
                    "payloadData":p["response"]["payloadData"],"opcode":p["response"].get("opcode",1)})
            elif method=="Network.responseReceived":
                response=p["response"]
                if response.get("status")==200 and response.get("url")=="https://omni.variational.io/api/quotes/indicative":
                    if len(responses)>256:responses.clear()
                    responses[p["requestId"]]=(response["url"],time.time())
            elif method=="Network.loadingFinished" and p.get("requestId") in responses:
                sequence+=1;bodies[sequence]=responses.pop(p["requestId"])
                if len(bodies)>256:raise RuntimeError("browser_body_backlog")
                await ws.send(json.dumps({"id":sequence,"method":"Network.getResponseBody",
                    "params":{"requestId":p["requestId"]}}))
            elif m.get("id") in bodies:
                url,captured=bodies.pop(m["id"])
                if time.time()-captured>2:continue
                body=m.get("result") or {}
                await monitor.process_rest_event({"kind":"rest_response","url":url,
                    "body":body.get("body"),"base64Encoded":body.get("base64Encoded",False),
                    "timestamp":datetime.fromtimestamp(captured,timezone.utc).isoformat()})


async def run():
    OUT.mkdir(parents=True,exist_ok=True)
    logger=logging.getLogger("eth_passive")
    books=MultiLighterBooks(("ETH",),logger)
    await books.load_markets()
    book_task=asyncio.create_task(books.run())
    capture_task=None;monitor=VariationalMonitor();journal=Journal();count=0;last=None
    try:
        while True:
            started=time.monotonic();reason=None
            if eth_live_running():
                if capture_task is not None:
                    capture_task.cancel();await asyncio.gather(capture_task,return_exceptions=True);capture_task=None
                monitor=VariationalMonitor();reason="paused_ETH_live_owns_journal"
            else:
                if capture_task is None:
                    monitor=VariationalMonitor();capture_task=asyncio.create_task(capture(monitor))
                if capture_task.done():
                    # Fail out and let systemd restart with bounded backoff.
                    await capture_task
                    raise RuntimeError("browser_capture_stopped")
                row,reason=observation(monitor,await books.snapshot("ETH",D(20)),time.time())
                if row is not None and journal.append(row):
                    count+=1;last=datetime.fromtimestamp(row.at,timezone.utc).isoformat()
            health={"updated_at":datetime.now(timezone.utc).isoformat(),"asset":"ETH",
                "status":"paused" if eth_live_running() else "running", "orders_attempted":False,
                "extra_rfq_requests":0,"samples_this_run":count,"last_sample_at":last,
                "block_reason":reason,"interval_seconds":1}
            p=OUT/"health.json";temp=p.with_suffix(".tmp");temp.write_text(json.dumps(health));temp.replace(p)
            await asyncio.sleep(max(0,1-(time.monotonic()-started)))
    finally:
        books.stop=True
        if capture_task is not None:capture_task.cancel()
        book_task.cancel()
        await asyncio.gather(*(t for t in (capture_task,book_task) if t is not None),return_exceptions=True)


if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
