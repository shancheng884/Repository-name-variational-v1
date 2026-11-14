#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,sys
from pathlib import Path
from decimal import Decimal
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from tools.lib.probe_metrics import calibrate, split_runs

def main():
 p=argparse.ArgumentParser(description="Analyze probe rounds and calibrate indicative triggers")
 p.add_argument("--input",default=str(ROOT/"log/venue_probe/rounds.jsonl"))
 p.add_argument("--output",default=str(ROOT/"log/venue_probe/calibration.json"))
 p.add_argument("--target-bps",type=float,default=1.0)
 p.add_argument("--by-run",action="store_true",help="also report each run id separately")
 a=p.parse_args(); rows=[]
 path=Path(a.input)
 if path.exists():
  for line in path.read_text(errors="ignore").splitlines():
   try:
    r=json.loads(line)
    if r.get("event")=="probe_round" and r.get("status") in {"filled","rolled_back","no_fill","skipped"}: rows.append(r)
   except ValueError: pass
 result=calibrate(rows,target_bps=Decimal(str(a.target_bps)))
 if a.by_run:
  result["runs"]={rid:calibrate(items,target_bps=Decimal(str(a.target_bps))) for rid,items in split_runs(rows).items()}
  result["runs"]={rid:{k:v for k,v in summary.items() if k!="summaries"} for rid,summary in result["runs"].items()}
 Path(a.output).parent.mkdir(parents=True,exist_ok=True); Path(a.output).write_text(json.dumps(result,ensure_ascii=False,indent=2))
 print(json.dumps({k:v for k,v in result.items() if k!="summaries"},ensure_ascii=False,indent=2))
 return 0
if __name__=="__main__": raise SystemExit(main())
