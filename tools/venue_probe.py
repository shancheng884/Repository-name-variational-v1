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
from tools.lib.venue_probe import (EntropyProbeExecutor, LighterProbeExecutor, ProbeRunner, ProbeState,
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

def _pairs(raw,assets,venues):
    """Parse ``ASSET:buy>sell`` items and prove they are tradeable."""
    by_name={a.name:a for a in assets}
    out=[]; thresholds={}
    for item in [x.strip() for x in (raw or "").split(",") if x.strip()]:
        threshold=None
        if "@" in item:
            item,raw_threshold=item.rsplit("@",1)
            try: threshold=float(raw_threshold)
            except ValueError: raise SystemExit(f"bad_pair_threshold:{raw_threshold}")
        try:
            head,rest=item.split(":",1)
            buy,sell=rest.split(">",1)
        except ValueError:
            raise SystemExit(f"bad_pair_syntax:{item}")
        name=head.strip().upper(); buy=buy.strip(); sell=sell.strip()
        asset=by_name.get(name)
        if asset is None: raise SystemExit(f"pair_asset_not_selected:{name}")
        if buy==sell: raise SystemExit(f"pair_same_venue:{item}")
        for venue in (buy,sell):
            if venue not in venues: raise SystemExit(f"pair_venue_not_selected:{venue}")
            if not asset.supports(venue): raise SystemExit(f"pair_asset_unsupported:{name}:{venue}")
        out.append((name,buy,sell))
        if threshold is not None: thresholds[(name,buy,sell)]=threshold
    if not out: raise SystemExit("pair_list_empty")
    return out,thresholds


def _config(assets,output):
    return CaptureConfig(output_dir=output,assets=assets,var_quote_seconds=1.0,book_snapshot_seconds=5.0,health_seconds=2.0,latency_seconds=5.0,system_seconds=60.0,fill_poll_seconds=2.0,edge_seconds=1.0,notional_usd=Decimal("20"),max_book_levels=20)

def _executors(env):
    ex={
      "variational":VariationalProbeExecutor(env.get("VAR_COMMAND_URL","ws://127.0.0.1:8768")),
      "lighter_mainnet":LighterProbeExecutor(name="lighter_mainnet",rest_url=env.get("LIGHTER_MAINNET_REST_URL","https://mainnet.zklighter.elliot.ai"),account_index=int(env["LIGHTER_ACCOUNT_INDEX"]) if env.get("LIGHTER_ACCOUNT_INDEX") else None,api_key_index=int(env["LIGHTER_API_KEY_INDEX"]) if env.get("LIGHTER_API_KEY_INDEX") else None,private_key=env.get("LIGHTER_PRIVATE_KEY","")),
      "lighter_rh":LighterProbeExecutor(name="lighter_rh",rest_url=env.get("LIGHTER_RH_REST_URL","https://api.rh.lighter.xyz"),account_index=int(env["LIGHTER_RH_ACCOUNT_INDEX"]) if env.get("LIGHTER_RH_ACCOUNT_INDEX") else None,api_key_index=int(env["LIGHTER_RH_API_KEY_INDEX"]) if env.get("LIGHTER_RH_API_KEY_INDEX") else None,private_key=env.get("LIGHTER_RH_API_PRIVATE_KEY","")),
    }
    entropy_address=(env.get("ENTROPY_ACCOUNT_ADDRESS") or "").strip()
    entropy_key=(env.get("ENTROPY_PRIVATE_KEY") or env.get("HYPERLIQUID_PRIVATE_KEY") or "").strip()
    if entropy_address and entropy_key:
        ex["entropy"]=EntropyProbeExecutor(info_url=env.get("ENTROPY_INFO_URL","https://api.hyperliquid.xyz/info"),account_address=entropy_address,private_key=entropy_key,dex=env.get("ENTROPY_ALLOWED_DEX","io"))
    elif not entropy_address:
        ex["entropy"]=UnavailableProbeExecutor("entropy","entropy_account_address_missing")
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
                q=runner.hub.latest(v,s,max_age=(6.0 if v=="entropy" else 5.0))
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
    pairs,thresholds=(_pairs(args.pairs,assets,venues) if args.pairs else (None,{}))
    if args.mode=="plan":
        plan=enumerate_probe_plan(assets,venues,pairs=pairs,rounds_per_pair=args.rounds_per_pair)
        return {"ok":True,"mode":"plan","orders_attempted":False,"assets":[a.name for a in assets],"venues":list(venues),"rounds":len(plan),"plan":[r.__dict__ for r in plan],"live_gate":"requires --mode preflight, then --mode run --execute --confirm-live-probes --confirm-flat --i-understand-live-orders"}
    cap,task,ex=await _session(args,env,assets,venues)
    try:
        runner=ProbeRunner(hub=cap.hub,executors=ex,state=ProbeState(ROOT/"log"/"venue_probe"/"state.json"),writer_path=ROOT/"log"/"venue_probe"/"rounds.jsonl",assets=assets,venues=venues,notional_usd=Decimal("20"),round_interval_seconds=1.0,max_quote_age_seconds=3.0,max_quote_age_by_venue={"entropy":6.0},horizons=(.25,.5,1.,3.,5.,10.),max_rounds=args.max_rounds,min_edge_bps=args.min_edge_bps,exit_on_revert=args.exit_on_revert,exit_edge_bps=args.exit_edge_bps,max_hold_seconds=args.max_hold_seconds,exit_poll_seconds=args.exit_poll_seconds,min_edge_by_pair=thresholds,baseline_entry=args.baseline_entry,baseline_minutes=args.baseline_minutes,min_baseline_samples=args.min_baseline_samples,max_loss_usd=args.max_loss_usd)
        runner.pairs=pairs; runner.rounds_per_pair=args.rounds_per_pair
        if args.mode=="preflight":
            health=await runner.preflight_all(); (ROOT/"log"/"venue_probe"/"preflight.json").write_text(json.dumps(health,ensure_ascii=False,indent=2,default=str)+"\n")
            return {"ok":all(v.get("ok") for v in health.values()),"mode":"preflight","orders_attempted":False,"health":health}
        if args.mode!="run": raise SystemExit("mode_must_be_plan_preflight_or_run")
        if not (args.execute and args.confirm_live_probes and args.confirm_flat and args.i_understand_live_orders):
            return {"ok":False,"mode":"run","orders_attempted":False,"error":"explicit_live_confirmation_flags_required"}
        return await runner.run(allow_live=True,resume=args.resume)
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
    p.add_argument("--max-rounds",type=int,default=0,help="0 means the full plan")
    p.add_argument("--min-edge-bps",type=float,default=None,help="only enter when the executable cross-venue edge clears this many bps")
    p.add_argument("--exit-on-revert",action="store_true",help="hold until the cross-venue edge falls back to --exit-edge-bps instead of exiting on a timer")
    p.add_argument("--exit-edge-bps",type=float,default=0.0,help="edge level that counts as reverted for --exit-on-revert")
    p.add_argument("--max-hold-seconds",type=float,default=600.0,help="hard cap on holding time for --exit-on-revert")
    p.add_argument("--exit-poll-seconds",type=float,default=2.0,help="how often to re-read the edge while holding")
    p.add_argument("--baseline-entry",action="store_true",help="enter on the deviation from the pair rolling median instead of the raw edge")
    p.add_argument("--baseline-minutes",type=float,default=30.0)
    p.add_argument("--min-baseline-samples",type=int,default=240)
    p.add_argument("--max-loss-usd",type=float,default=None,help="hard stop after this much realised loss")
    p.add_argument("--pairs",default="",help="comma list of ASSET:buy>sell; restricts the plan to these combinations only")
    p.add_argument("--rounds-per-pair",type=int,default=1,help="repeat each requested pair this many times, round-robin")
    p.add_argument("--resume",action="store_true",help="continue a stalled run after both accounts prove flat")
    return p.parse_args(argv)

def main():
    logging.basicConfig(level=logging.WARNING,format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    try: result=asyncio.run(main_async(parse_args()))
    except KeyboardInterrupt: result={"ok":False,"error":"interrupted"}
    print(json.dumps(result,ensure_ascii=False,indent=2,default=str))
    return 0 if result.get("ok") else 1
if __name__=="__main__": raise SystemExit(main())
