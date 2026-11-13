#!/usr/bin/env python3
"""Prepare and (only with explicit flags) execute small two-venue probes."""
from __future__ import annotations
import argparse, asyncio, json, logging, os, sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from tools.market_capture.config import DEFAULT_ASSETS, CaptureConfig, load_environment
from tools.market_capture.runner import CaptureRunner
from tools.lib.venue_probe import (LighterProbeExecutor, ProbeRunner, ProbeState,
    UnavailableProbeExecutor, VariationalProbeExecutor, enumerate_probe_plan)

DEFAULT_ASSETS_NAMES="NVDA,SNDK,CRCL,AAPL,QQQ"
DEFAULT_VENUES="variational,lighter_mainnet,lighter_rh,entropy"

def _assets(raw):
    selected={a.name:a for a in DEFAULT_ASSETS}
    out=[]
    for name in [x.strip().upper() for x in raw.split(",") if x.strip()]:
        if name not in selected: raise SystemExit(f"unknown_asset:{name}")
        out.append(selected[name])
    return tuple(out)

def _config(assets,output):
    return CaptureConfig(output_dir=output,assets=assets,var_quote_seconds=1.0,book_snapshot_seconds=5.0,health_seconds=2.0,latency_seconds=5.0,system_seconds=60.0,fill_poll_seconds=2.0,edge_seconds=1.0,notional_usd=Decimal("20"),max_book_levels=20)

def _executors(env):
    ex={
      "variational":VariationalProbeExecutor(env.get("VAR_COMMAND_URL","ws://127.0.0.1:8768")),
      "lighter_mainnet":LighterProbeExecutor(name="lighter_mainnet",rest_url=env.get("LIGHTER_MAINNET_REST_URL","https://mainnet.zklighter.elliot.ai"),account_index=int(env["LIGHTER_ACCOUNT_INDEX"]) if env.get("LIGHTER_ACCOUNT_INDEX") else None,api_key_index=int(env["LIGHTER_API_KEY_INDEX"]) if env.get("LIGHTER_API_KEY_INDEX") else None,private_key=env.get("LIGHTER_PRIVATE_KEY","")),
      "lighter_rh":LighterProbeExecutor(name="lighter_rh",rest_url=env.get("LIGHTER_RH_REST_URL","https://api.rh.lighter.xyz"),account_index=int(env["LIGHTER_RH_ACCOUNT_INDEX"]) if env.get("LIGHTER_RH_ACCOUNT_INDEX") else None,api_key_index=int(env["LIGHTER_RH_API_KEY_INDEX"]) if env.get("LIGHTER_RH_API_KEY_INDEX") else None,private_key=env.get("LIGHTER_RH_API_PRIVATE_KEY","")),
    }
    # Entropy supports public quotes but no trading adapter until a signer key is provisioned.
    if env.get("ENTROPY_PRIVATE_KEY"):
        ex["entropy"]=UnavailableProbeExecutor("entropy","entropy_trade_adapter_not_installed")
    else:
        ex["entropy"]=UnavailableProbeExecutor("entropy","entropy_private_key_missing")
    return ex

async def _wait_ready(runner,assets,venues,timeout=35.0):
    deadline=asyncio.get_running_loop().time()+timeout
    needed=[(v,a.name) for a in assets for v in venues if a.supports(v)]
    while asyncio.get_running_loop().time()<deadline:
        if runner.hub is not None:
            ok=0
            for v,s in needed:
                q=runner.hub.latest(v,s,max_age=(12.0 if v=="entropy" else 5.0))
                if q and q.get("mid") is not None and q.get("bid") is not None and q.get("ask") is not None: ok+=1
            if ok>=len(needed): return {"ready":True,"quoted":ok,"needed":len(needed)}
        await asyncio.sleep(.25)
    return {"ready":False,"quoted":0,"needed":len(needed)}

async def _session(args,env,assets,venues):
    out=ROOT/"log"/"venue_probe"/"capture"
    cfg=_config(assets,out); cap=CaptureRunner(cfg); task=asyncio.create_task(cap.run(),name="probe-market-capture")
    await _wait_ready(cap,assets,tuple(venues))
    ex=_executors(env)
    for e in ex.values(): await e.start()
    return cap,task,ex

async def _stop(cap,task,ex):
    for e in ex.values():
        try: await e.close()
        except Exception: pass
    cap.request_stop();
    try: await asyncio.wait_for(task,timeout=15)
    except Exception: task.cancel()

async def main_async(args):
    assets=_assets(args.assets); venues=tuple(x.strip() for x in args.venues.split(",") if x.strip())
    env=load_environment()
    if args.mode=="plan":
        plan=enumerate_probe_plan(assets,venues)
        return {"ok":True,"mode":"plan","orders_attempted":False,"assets":[a.name for a in assets],"venues":list(venues),"rounds":len(plan),"plan":[r.__dict__ for r in plan],"live_gate":"requires --mode preflight, then --mode run --execute --confirm-live-probes --confirm-flat --i-understand-live-orders"}
    cap,task,ex=await _session(args,env,assets,venues)
    try:
        runner=ProbeRunner(hub=cap.hub,executors=ex,state=ProbeState(ROOT/"log"/"venue_probe"/"state.json"),writer_path=ROOT/"log"/"venue_probe"/"rounds.jsonl",assets=assets,venues=venues,notional_usd=Decimal("20"),round_interval_seconds=1.0,max_quote_age_seconds=3.0,max_quote_age_by_venue={"entropy":12.0})
        if args.mode=="preflight":
            health=await runner.preflight_all(); (ROOT/"log"/"venue_probe"/"preflight.json").write_text(json.dumps(health,ensure_ascii=False,indent=2,default=str)+"\n")
            return {"ok":all(v.get("ok") for v in health.values()),"mode":"preflight","orders_attempted":False,"health":health}
        if args.mode!="run": raise SystemExit("mode_must_be_plan_preflight_or_run")
        if not (args.execute and args.confirm_live_probes and args.confirm_flat and args.i_understand_live_orders):
            return {"ok":False,"mode":"run","orders_attempted":False,"error":"explicit_live_confirmation_flags_required"}
        return await runner.run(allow_live=True)
    finally:
        await _stop(cap,task,ex)

def parse_args(argv=None):
    p=argparse.ArgumentParser(description="RWA two-venue 20U probe planner/runner")
    p.add_argument("--mode",choices=("plan","preflight","run"),default="plan")
    p.add_argument("--assets",default=DEFAULT_ASSETS_NAMES)
    p.add_argument("--venues",default=DEFAULT_VENUES)
    p.add_argument("--execute",action="store_true")
    p.add_argument("--confirm-live-probes",action="store_true")
    p.add_argument("--confirm-flat",action="store_true")
    p.add_argument("--i-understand-live-orders",action="store_true")
    return p.parse_args(argv)

def main():
    logging.basicConfig(level=logging.WARNING,format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    try: result=asyncio.run(main_async(parse_args()))
    except KeyboardInterrupt: result={"ok":False,"error":"interrupted"}
    print(json.dumps(result,ensure_ascii=False,indent=2,default=str))
    return 0 if result.get("ok") else 1
if __name__=="__main__": raise SystemExit(main())
