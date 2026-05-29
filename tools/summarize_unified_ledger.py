import argparse
import csv
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "none":
        return ""
    return text


def fmt(value: Decimal | None, places: int = 6) -> str:
    if value is None:
        return "-"
    return f"{value:.{places}f}"


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def row_time(row: dict[str, Any]) -> str:
    return clean_text(row.get("logged_at", "")) or clean_text(row.get("entry_time", ""))


def summarize(rows: list[dict[str, Any]], assets: set[str], since: str) -> dict[str, Any]:
    data: dict[str, Any] = {
        "paper": {
            "closed": 0,
            "open": 0,
            "net_pnl": [],
            "gross_pnl": [],
            "fees": [],
            "by_asset": defaultdict(lambda: {"closed": 0, "open": 0, "net_pnl": [], "gross_pnl": []}),
            "by_status": Counter(),
        },
        "live": {
            "total": 0,
            "hedged": 0,
            "pending": 0,
            "naked": 0,
            "blocked": 0,
            "failed": 0,
            "by_asset": defaultdict(lambda: Counter()),
            "failure_reasons": Counter(),
            "rollback_actions": Counter(),
        },
    }

    for row in rows:
        if since and row_time(row) < since:
            continue
        asset = clean_text(row.get("asset", "")).upper()
        if assets and asset not in assets:
            continue

        record_kind = clean_text(row.get("record_kind", ""))
        if record_kind == "paper_opportunity":
            status = clean_text(row.get("status", "")) or clean_text(row.get("final_status", ""))
            net_pnl = to_decimal(row.get("net_pnl_usd")) or to_decimal(row.get("net_pnl_conservative_usd"))
            gross_pnl = to_decimal(row.get("gross_pnl_usd")) or to_decimal(row.get("entry_spread_pnl_usd"))
            fees = to_decimal(row.get("fees_usd"))
            bucket = data["paper"]
            bucket["by_status"][status or "unknown"] += 1
            bucket["by_asset"][asset]["closed" if status == "paper_closed" else "open"] += 1
            if status == "paper_closed":
                bucket["closed"] += 1
            else:
                bucket["open"] += 1
            if net_pnl is not None:
                bucket["net_pnl"].append(net_pnl)
                bucket["by_asset"][asset]["net_pnl"].append(net_pnl)
            if gross_pnl is not None:
                bucket["gross_pnl"].append(gross_pnl)
                bucket["by_asset"][asset]["gross_pnl"].append(gross_pnl)
            if fees is not None:
                bucket["fees"].append(fees)
            continue

        if record_kind != "execution_lifecycle":
            continue

        status = clean_text(row.get("hedge_completion_status", "")) or clean_text(row.get("processing_stage", ""))
        failure_stage = clean_text(row.get("failure_stage", ""))
        failure_reason = clean_text(row.get("failure_reason", ""))
        rollback_action = clean_text(row.get("rollback_action", ""))
        bucket = data["live"]
        bucket["total"] += 1
        bucket["by_asset"][asset][status or "unknown"] += 1
        if status == "hedged":
            bucket["hedged"] += 1
        elif failure_stage in {"hedge_plan", "mode_guard"} or status in {"blocked_by_mode", "hedge_blocked_before_submit"}:
            bucket["blocked"] += 1
        elif status == "naked_variational_leg":
            bucket["naked"] += 1
        elif failure_reason or status in {"fallback", "live_submit_failed", "live_submit_timed_out"}:
            bucket["failed"] += 1
        elif status in {"hedge_pending", "open"}:
            bucket["pending"] += 1
        if failure_reason:
            bucket["failure_reasons"][failure_reason] += 1
        if rollback_action:
            bucket["rollback_actions"][rollback_action] += 1

    return data


def print_report(data: dict[str, Any], ledger_path: Path, assets: set[str], verbose: bool) -> None:
    paper = data["paper"]
    live = data["live"]
    paper_closed = paper["closed"]
    paper_open = paper["open"]
    paper_avg_net = sum(paper["net_pnl"], Decimal("0")) / Decimal(len(paper["net_pnl"])) if paper["net_pnl"] else None
    paper_avg_gross = sum(paper["gross_pnl"], Decimal("0")) / Decimal(len(paper["gross_pnl"])) if paper["gross_pnl"] else None
    paper_total_net = sum(paper["net_pnl"], Decimal("0")) if paper["net_pnl"] else None
    live_success_rate = Decimal(live["hedged"]) / Decimal(live["total"]) * Decimal("100") if live["total"] else None
    live_risk_total = live["pending"] + live["naked"] + live["failed"]
    live_blocked_total = live["blocked"]
    if live["naked"] > 0 or live["failed"] > 0:
        overall_status = "WARN"
    elif live_risk_total == 0 and paper_closed == 0:
        overall_status = "WATCH"
    elif live_risk_total == 0 and paper_total_net is not None and paper_total_net > 0:
        overall_status = "PASS"
    else:
        overall_status = "WARN"

    asset_label = ",".join(sorted(assets)) if assets else "ALL"
    print(f"ledger: {ledger_path}")
    print(f"assets: {asset_label}")
    print(f"check: {overall_status}")
    print(
        f"paper: closed={paper_closed} open={paper_open} total_net_pnl={fmt(paper_total_net)} "
        f"avg_net_pnl={fmt(paper_avg_net)} avg_gross_pnl={fmt(paper_avg_gross)}"
    )
    if paper["by_status"]:
        status_text = ", ".join(f"{k}={v}" for k, v in paper["by_status"].most_common())
        print(f"paper_status: {status_text}")
    print(
        f"live: total={live['total']} hedged={live['hedged']} pending={live['pending']} naked={live['naked']} "
        f"blocked={live['blocked']} failed={live['failed']} hedged_success_rate={fmt(live_success_rate, 2)}%"
    )
    print(f"risk_open_items={live_risk_total}")
    print(f"blocked_before_submit={live_blocked_total}")
    if live["failure_reasons"]:
        failure_text = ", ".join(f"{k}={v}" for k, v in live["failure_reasons"].most_common())
        print(f"live_failures: {failure_text}")
    if live["rollback_actions"]:
        rollback_text = ", ".join(f"{k}={v}" for k, v in live["rollback_actions"].most_common())
        print(f"rollback_actions: {rollback_text}")
    if verbose:
        print("asset snapshot")
        all_assets = set(paper["by_asset"].keys()) | set(live["by_asset"].keys())
        for asset in sorted(all_assets):
            if assets and asset not in assets:
                continue
            paper_bucket = paper["by_asset"].get(asset, {"closed": 0, "open": 0, "net_pnl": [], "gross_pnl": []})
            live_bucket = live["by_asset"].get(asset, Counter())
            asset_net = sum(paper_bucket["net_pnl"], Decimal("0")) if paper_bucket["net_pnl"] else None
            asset_closed = paper_bucket["closed"]
            asset_open = paper_bucket["open"]
            live_status_text = ", ".join(f"{k}={v}" for k, v in live_bucket.most_common()) if live_bucket else "-"
            print(
                f"{asset}: paper_closed={asset_closed} paper_open={asset_open} paper_net_pnl={fmt(asset_net)} "
                f"live={live_status_text}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize unified ledger before VPS migration.")
    parser.add_argument("--ledger", default="log/unified_ledger.csv", help="Path to unified_ledger.csv")
    parser.add_argument("--assets", default="", help="Optional comma-separated asset filter, e.g. BTC,SOL,ETH")
    parser.add_argument("--since", default="", help="Only include rows whose logged_at/entry_time is >= this ISO prefix/string")
    parser.add_argument("--verbose", action="store_true", help="Show per-asset snapshot")
    args = parser.parse_args()

    ledger_path = Path(args.ledger)
    if not ledger_path.exists():
        raise SystemExit(f"Ledger file not found: {ledger_path}")

    assets = {asset.strip().upper() for asset in args.assets.split(",") if asset.strip()}
    rows = load_rows(ledger_path)
    data = summarize(rows, assets, args.since.strip())
    print_report(data, ledger_path, assets, args.verbose)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
