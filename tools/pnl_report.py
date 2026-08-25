#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.telegram_notifier import (  # noqa: E402
    TelegramNotifier,
    format_telegram_trade_message,
)


ORDER_METRICS = ROOT / "log" / "order_metrics.jsonl"
RELEVANT_EVENTS = {
    "live_inventory_run_config",
    "live_inventory_actual_pnl",
    "live_inventory_final_pnl",
    "live_inventory_account_snapshot",
    "live_inventory_runtime_stopped",
}


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = parse_time(value)
    if parsed is not None:
        return parsed
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--since must be YYYY-MM-DD or an ISO-8601 timestamp"
        ) from exc


def load_rows(path: Path, *, asset: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    needles = tuple(f'"event": "{event}"' for event in RELEVANT_EVENTS)
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not any(needle in line for needle in needles):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(row.get("event") or "") not in RELEVANT_EVENTS:
                continue
            row_asset = str(row.get("asset") or "").upper()
            if row_asset and row_asset != asset:
                continue
            rows.append(row)
    return rows


def row_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("run_id") or "unknown"),
        str(row.get("asset") or "unknown").upper(),
        str(row.get("lot_id") or row.get("cycle_id") or "unknown"),
    )


def deduplicated_actual_pnl(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        if row.get("event") != "live_inventory_actual_pnl":
            continue
        if row.get("actual_pnl_status") != "lighter_final_fill_confirmed":
            continue
        if to_decimal(row.get("actual_pnl_usd")) is None:
            continue
        selected[row_key(row)] = row
    return sorted(
        selected.values(),
        key=lambda row: parse_time(row.get("logged_at"))
        or datetime.min.replace(tzinfo=timezone.utc),
    )


def latest_final_pnl_by_cycle(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        if row.get("event") != "live_inventory_final_pnl":
            continue
        selected[row_key(row)] = row
    return selected


def complete_snapshots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    snapshots = [
        row
        for row in rows
        if row.get("event") == "live_inventory_account_snapshot"
        and row.get("snapshot_status") == "complete"
        and to_decimal(row.get("combined_equity_usd")) is not None
    ]
    return sorted(
        snapshots,
        key=lambda row: parse_time(row.get("snapshot_captured_at"))
        or parse_time(row.get("logged_at"))
        or datetime.min.replace(tzinfo=timezone.utc),
    )


def snapshot_for_cycle(
    snapshots: list[dict[str, Any]],
    cycle_key: tuple[str, str, str],
    stage: str,
) -> dict[str, Any] | None:
    matches = [
        row
        for row in snapshots
        if row_key(row) == cycle_key and row.get("snapshot_stage") == stage
    ]
    return matches[-1] if matches else None


def previous_flat_snapshot(
    snapshots: list[dict[str, Any]],
    exit_snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    exit_time = parse_time(exit_snapshot.get("snapshot_captured_at"))
    if exit_time is None:
        return None
    candidates = [
        row
        for row in snapshots
        if row.get("snapshot_stage")
        in {"startup_flat", "exit_confirmed_flat"}
        and row is not exit_snapshot
        and (
            parse_time(row.get("snapshot_captured_at")) is not None
            and parse_time(row.get("snapshot_captured_at")) < exit_time
        )
    ]
    return candidates[-1] if candidates else None


def decimal_text(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def latest_pnl_telegram_payload(
    rows: list[dict[str, Any]],
    report: Report,
    *,
    asset: str,
) -> dict[str, Any] | None:
    if not report.cycles:
        return None
    cycle = report.cycles[-1]
    cycle_key = row_key(cycle)
    run_id = cycle_key[0]
    cycle_time = parse_time(cycle.get("logged_at"))
    run_cycles = [
        row
        for row in report.cycles
        if row_key(row)[0] == run_id
        and (
            cycle_time is None
            or parse_time(row.get("logged_at")) is None
            or parse_time(row.get("logged_at")) <= cycle_time
        )
    ]
    run_actual_pnl = sum(
        (
            to_decimal(row.get("actual_pnl_usd")) or Decimal("0")
            for row in run_cycles
        ),
        Decimal("0"),
    )
    cycle_actual_pnl = to_decimal(cycle.get("actual_pnl_usd"))
    exit_snapshot = snapshot_for_cycle(
        report.snapshots,
        cycle_key,
        "exit_confirmed_flat",
    )
    exit_time = (
        parse_time(exit_snapshot.get("snapshot_captured_at"))
        if exit_snapshot
        else cycle_time
    )
    run_start_snapshots = [
        row
        for row in report.snapshots
        if str(row.get("run_id") or "unknown") == run_id
        and row.get("snapshot_stage") == "startup_flat"
        and (
            exit_time is None
            or parse_time(row.get("snapshot_captured_at")) is None
            or parse_time(row.get("snapshot_captured_at")) < exit_time
        )
    ]
    baseline_snapshot = (
        run_start_snapshots[-1]
        if run_start_snapshots
        else previous_flat_snapshot(report.snapshots, exit_snapshot)
        if exit_snapshot
        else None
    )
    baseline_equity = to_decimal(
        baseline_snapshot.get("combined_equity_usd")
        if baseline_snapshot
        else None
    )
    combined_equity = to_decimal(
        exit_snapshot.get("combined_equity_usd") if exit_snapshot else None
    )
    account_snapshot_flat = bool(
        exit_snapshot
        and exit_snapshot.get("account_snapshot_flat", True)
    )
    account_net_change = (
        combined_equity - baseline_equity
        if account_snapshot_flat
        and combined_equity is not None
        and baseline_equity is not None
        else None
    )
    capital_usd = (
        report.capital_usd
        if report.capital_source == "--capital-usd"
        else baseline_equity or report.capital_usd
    )
    pnl_for_return = (
        account_net_change
        if account_net_change is not None
        else run_actual_pnl
    )
    return_pct = (
        pnl_for_return / capital_usd * Decimal("100")
        if capital_usd is not None and capital_usd > 0
        else None
    )
    baseline_time = (
        parse_time(baseline_snapshot.get("snapshot_captured_at"))
        if baseline_snapshot
        else None
    )
    if baseline_time is None:
        run_configs = [
            parse_time(row.get("logged_at"))
            for row in rows
            if row.get("event") == "live_inventory_run_config"
            and str(row.get("run_id") or "unknown") == run_id
        ]
        baseline_time = next(
            (value for value in reversed(run_configs) if value is not None),
            None,
        )
    elapsed_days = (
        Decimal(str((exit_time - baseline_time).total_seconds()))
        / Decimal("86400")
        if baseline_time is not None
        and exit_time is not None
        and exit_time > baseline_time
        else None
    )
    annualized_pct = (
        return_pct * Decimal("365") / elapsed_days
        if return_pct is not None
        and elapsed_days is not None
        and elapsed_days > 0
        else None
    )
    return {
        "asset": asset.upper(),
        "lot_id": cycle.get("lot_id"),
        "summary_status": (
            "complete"
            if exit_snapshot is not None
            and account_snapshot_flat
            and baseline_equity is not None
            and cycle_actual_pnl is not None
            else "partial"
        ),
        "cycle_actual_pnl_usd": decimal_text(cycle_actual_pnl),
        "run_actual_pnl_usd": decimal_text(run_actual_pnl),
        "account_net_change_usd": decimal_text(account_net_change),
        "account_minus_fill_pnl_usd": decimal_text(
            account_net_change - run_actual_pnl
            if account_net_change is not None
            else None
        ),
        "variational_equity_usd": (
            exit_snapshot.get("variational_equity_usd")
            if exit_snapshot
            else None
        ),
        "lighter_equity_usd": (
            exit_snapshot.get("lighter_equity_usd")
            if exit_snapshot
            else None
        ),
        "combined_equity_usd": decimal_text(combined_equity),
        "capital_usd": decimal_text(capital_usd),
        "completed_cycles": len(run_cycles),
        "return_pct": decimal_text(return_pct),
        "return_pnl_source": (
            "account_equity_delta"
            if account_net_change is not None
            else "confirmed_pair_fills"
        ),
        "annualized_simple_pct": decimal_text(annualized_pct),
        "annualized_reliability": (
            "sample_under_30_days"
            if elapsed_days is not None and elapsed_days < 30
            else "observable"
            if elapsed_days is not None
            else "unavailable"
        ),
    }


def money(value: Decimal | None) -> str:
    return "-" if value is None else f"{value:+.6f} U"


def percent(value: Decimal | None) -> str:
    return "-" if value is None else f"{value:+.2f}%"


def timestamp(value: Any) -> str:
    parsed = parse_time(value)
    return parsed.isoformat() if parsed is not None else "-"


@dataclass(frozen=True)
class Report:
    cycles: list[dict[str, Any]]
    final_by_cycle: dict[tuple[str, str, str], dict[str, Any]]
    snapshots: list[dict[str, Any]]
    period_start: datetime | None
    period_end: datetime | None
    capital_usd: Decimal | None
    capital_source: str


def build_report(
    rows: list[dict[str, Any]],
    *,
    since: datetime | None,
    capital_usd: Decimal | None,
) -> Report:
    timed_rows = [
        (parse_time(row.get("logged_at")), row)
        for row in rows
    ]
    filtered_rows = [
        row
        for row_time, row in timed_rows
        if since is None or (row_time is not None and row_time >= since)
    ]
    snapshots = complete_snapshots(filtered_rows)
    capital_source = "--capital-usd"
    if capital_usd is None and snapshots:
        flat_snapshots = [
            row
            for row in snapshots
            if row.get("snapshot_stage")
            in {"startup_flat", "exit_confirmed_flat"}
        ]
        capital_snapshot = flat_snapshots[0] if flat_snapshots else snapshots[0]
        capital_usd = to_decimal(capital_snapshot.get("combined_equity_usd"))
        capital_source = "first_complete_account_snapshot"
    elif capital_usd is None:
        capital_source = "unavailable"

    cycles = deduplicated_actual_pnl(filtered_rows)
    final_by_cycle = latest_final_pnl_by_cycle(filtered_rows)
    times = [row_time for row_time, _ in timed_rows if row_time is not None]
    if since is not None:
        period_start = since
    else:
        run_times = [
            parse_time(row.get("logged_at"))
            for row in filtered_rows
            if row.get("event") == "live_inventory_run_config"
        ]
        period_start = min((value for value in run_times if value), default=None)
        if period_start is None and cycles:
            period_start = parse_time(cycles[0].get("logged_at"))
    filtered_times = [
        value for value in times if since is None or value >= since
    ]
    period_end = max(filtered_times, default=None)
    return Report(
        cycles=cycles,
        final_by_cycle=final_by_cycle,
        snapshots=snapshots,
        period_start=period_start,
        period_end=period_end,
        capital_usd=capital_usd,
        capital_source=capital_source,
    )


def print_report(report: Report, *, last_cycles: int) -> None:
    pnl_values = [
        to_decimal(row.get("actual_pnl_usd")) or Decimal("0")
        for row in report.cycles
    ]
    total_pnl = sum(pnl_values, Decimal("0"))
    wins = sum(value > 0 for value in pnl_values)
    losses = sum(value < 0 for value in pnl_values)
    gross_profit = sum((value for value in pnl_values if value > 0), Decimal("0"))
    gross_loss = sum((value for value in pnl_values if value < 0), Decimal("0"))
    average_pnl = total_pnl / len(pnl_values) if pnl_values else None
    median_pnl = Decimal(str(median(pnl_values))) if pnl_values else None

    running = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for value in pnl_values:
        running += value
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)

    elapsed_days = None
    if report.period_start is not None and report.period_end is not None:
        elapsed_days = Decimal(
            str(
                max(
                    0.0,
                    (report.period_end - report.period_start).total_seconds()
                    / 86400,
                )
            )
        )
    return_pct = None
    annualized_pct = None
    if report.capital_usd is not None and report.capital_usd > 0:
        return_pct = total_pnl / report.capital_usd * Decimal("100")
        if elapsed_days is not None and elapsed_days > 0:
            annualized_pct = return_pct * Decimal("365") / elapsed_days

    print("== 盈利汇总 ==")
    print(f"统计开始={timestamp(report.period_start)}")
    print(f"统计结束={timestamp(report.period_end)}")
    print(
        "统计天数="
        + (f"{elapsed_days:.3f}" if elapsed_days is not None else "-")
    )
    print(
        f"完成轮数={len(pnl_values)} 胜={wins} 负={losses} "
        f"胜率={(wins / len(pnl_values) * 100):.1f}%"
        if pnl_values
        else "完成轮数=0 胜=0 负=0 胜率=-"
    )
    print(f"实际成交盈亏={money(total_pnl)}")
    print(f"平均每轮={money(average_pnl)} 中位每轮={money(median_pnl)}")
    print(f"累计盈利={money(gross_profit)} 累计亏损={money(gross_loss)}")
    print(f"最大成交回撤={money(max_drawdown)}")
    print(
        f"策略本金={money(report.capital_usd)} "
        f"来源={report.capital_source}"
    )
    print(f"期间收益率={percent(return_pct)}")
    print(f"简单年化={percent(annualized_pct)}")
    reliability = (
        "样本不足30天_仅供观察"
        if elapsed_days is not None and elapsed_days < 30
        else "可观察"
        if elapsed_days is not None
        else "无法计算"
    )
    print(f"年化可信度={reliability}")
    print("成交盈亏口径=双方确认成交价计算_不单独扣手续费和资金费")

    print("\n== 双边账户权益 ==")
    print(f"完整快照数={len(report.snapshots)}")
    if report.snapshots:
        first = report.snapshots[0]
        latest = report.snapshots[-1]
        first_total = to_decimal(first.get("combined_equity_usd"))
        latest_total = to_decimal(latest.get("combined_equity_usd"))
        first_var = to_decimal(first.get("variational_equity_usd"))
        latest_var = to_decimal(latest.get("variational_equity_usd"))
        first_lighter = to_decimal(first.get("lighter_equity_usd"))
        latest_lighter = to_decimal(latest.get("lighter_equity_usd"))
        print(
            f"首个合计权益={money(first_total)} "
            f"时间={timestamp(first.get('snapshot_captured_at'))}"
        )
        print(
            f"最新Variational权益={money(latest_var)} "
            f"最新Lighter权益={money(latest_lighter)}"
        )
        print(f"最新合计权益={money(latest_total)}")
        account_delta = (
            latest_total - first_total
            if latest_total is not None and first_total is not None
            else None
        )
        var_delta = (
            latest_var - first_var
            if latest_var is not None and first_var is not None
            else None
        )
        lighter_delta = (
            latest_lighter - first_lighter
            if latest_lighter is not None and first_lighter is not None
            else None
        )
        print(
            f"账户净变化={money(account_delta)} "
            f"Variational变化={money(var_delta)} "
            f"Lighter变化={money(lighter_delta)}"
        )
        first_time = parse_time(first.get("snapshot_captured_at"))
        latest_time = parse_time(latest.get("snapshot_captured_at"))
        tracked_trade_pnl = sum(
            (
                to_decimal(row.get("actual_pnl_usd")) or Decimal("0")
                for row in report.cycles
                if (
                    first_time is not None
                    and latest_time is not None
                    and parse_time(row.get("logged_at")) is not None
                    and first_time
                    <= parse_time(row.get("logged_at"))
                    <= latest_time
                )
            ),
            Decimal("0"),
        )
        account_minus_trade = (
            account_delta - tracked_trade_pnl
            if account_delta is not None
            else None
        )
        print(f"快照期间成交盈亏={money(tracked_trade_pnl)}")
        print(f"账户变化减成交盈亏={money(account_minus_trade)}")
        print("账户净变化口径=含手续费_资金费_充值提现及其他账户活动")
    else:
        print("状态=尚无完整快照_部署新版并启动后自动开始记录")

    print("\n== 最近每轮 ==")
    for row in report.cycles[-max(0, last_cycles):]:
        key = row_key(row)
        final = report.final_by_cycle.get(key, {})
        entry_snapshot = snapshot_for_cycle(
            report.snapshots, key, "entry_confirmed"
        )
        exit_snapshot = snapshot_for_cycle(
            report.snapshots, key, "exit_confirmed_flat"
        )
        baseline_snapshot = (
            previous_flat_snapshot(report.snapshots, exit_snapshot)
            if exit_snapshot
            else None
        )
        entry_total = to_decimal(
            baseline_snapshot.get("combined_equity_usd")
            if baseline_snapshot
            else None
        )
        exit_total = to_decimal(
            exit_snapshot.get("combined_equity_usd")
            if exit_snapshot
            else None
        )
        cycle_equity_delta = (
            exit_total - entry_total
            if entry_total is not None and exit_total is not None
            else None
        )
        entry_var = to_decimal(
            entry_snapshot.get("variational_equity_usd")
            if entry_snapshot
            else None
        )
        entry_lighter = to_decimal(
            entry_snapshot.get("lighter_equity_usd")
            if entry_snapshot
            else None
        )
        exit_var = to_decimal(
            exit_snapshot.get("variational_equity_usd")
            if exit_snapshot
            else None
        )
        exit_lighter = to_decimal(
            exit_snapshot.get("lighter_equity_usd")
            if exit_snapshot
            else None
        )
        print(
            f"时间={timestamp(row.get('logged_at'))} "
            f"run={key[0]} lot={key[2]} "
            f"成交盈亏={money(to_decimal(row.get('actual_pnl_usd')))} "
            f"平仓到平仓权益变化={money(cycle_equity_delta)} "
            f"原因={final.get('exit_reason') or row.get('exit_reason') or '-'}"
        )
        print(
            f"  开仓后权益 Variational={money(entry_var)} "
            f"Lighter={money(entry_lighter)} | "
            f"平仓后权益 Variational={money(exit_var)} "
            f"Lighter={money(exit_lighter)}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize confirmed pair-fill PnL and account equity."
    )
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--since")
    parser.add_argument("--capital-usd", type=Decimal)
    parser.add_argument("--last", type=int, default=10)
    parser.add_argument("--path", type=Path, default=ORDER_METRICS)
    parser.add_argument(
        "--telegram-latest",
        action="store_true",
        help="Send the latest confirmed close as a Chinese Telegram summary.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    capital_usd = args.capital_usd
    if capital_usd is None:
        capital_usd = to_decimal(os.getenv("PNL_REPORT_CAPITAL_USD"))
    if capital_usd is not None and capital_usd <= 0:
        raise SystemExit("--capital-usd must be greater than zero")
    since = parse_since(args.since)
    rows = load_rows(args.path, asset=args.asset.upper())
    report = build_report(rows, since=since, capital_usd=capital_usd)
    print_report(report, last_cycles=args.last)
    if args.telegram_latest:
        payload = latest_pnl_telegram_payload(
            rows,
            report,
            asset=args.asset,
        )
        if payload is None:
            raise SystemExit("telegram_latest=FAILED no_confirmed_close")
        notifier = TelegramNotifier.from_env(
            logger=logging.getLogger("pnl_report.telegram")
        )
        message = format_telegram_trade_message(
            "live_inventory_pnl_summary",
            payload,
        )
        ok, detail = notifier.send_now(message)
        print(f"telegram_latest={'PASS' if ok else 'FAILED'} {detail}")
        if not ok:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
