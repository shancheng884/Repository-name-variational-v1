"""Fee-adjusted per-asset summary of a live tier trial."""
from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path


def summarize(path: Path) -> dict:
    opened = closed = failed = 0
    wins = 0
    net = Decimal("0")
    rebate = Decimal("0")
    missing = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        event = row.get("event")
        if event == "opened":
            opened += 1
        elif event == "open_failed":
            failed += 1
        elif event == "closed":
            closed += 1
            pnl = row.get("pnl") or {}
            if pnl.get("complete"):
                value = Decimal(str(pnl.get("net_usd") or "0"))
                net += value
                if value > 0:
                    wins += 1
            else:
                missing += 1
            rebate += Decimal(str(row.get("rebate_usd") or "0"))
    return {"orders": str(path), "opened": opened, "closed": closed,
            "open_failed": failed, "incomplete_cost": missing,
            "win_rate_net": (wins / closed) if closed else None,
            "net_usd_fees_deducted": str(net.quantize(Decimal("0.000001"))),
            "rebate_usd": str(rebate.quantize(Decimal("0.000001"))),
            "net_usd_after_rebate": str((net + rebate).quantize(Decimal("0.000001")))}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=Path, action="append", required=True)
    args = parser.parse_args(argv)
    print(json.dumps([summarize(path) for path in args.orders], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
