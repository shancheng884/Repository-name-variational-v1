#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.pnl_baseline import (  # noqa: E402
    BEIJING_TIMEZONE,
    PNL_BASELINE_FILE_NAME,
    beijing_calendar_days,
    load_pnl_baseline,
    parse_timestamp,
    pnl_day_summary,
)
from tools.lib.telegram_notifier import (  # noqa: E402
    TelegramNotifier,
    format_telegram_trade_message,
)
from tools.lib.runtime_files import read_json  # noqa: E402
from tools.lib.wti_reporting import closed_rounds, number  # noqa: E402
from tools.lib.wti_billing_records import (  # noqa: E402
    load_verified_records, billing_rounds, publish_billing_updates, deliver_once,
    deliver_billing_correction_once,
)


DEFAULT_BASELINE = ROOT / "log" / PNL_BASELINE_FILE_NAME
DEFAULT_SEND_STATE = ROOT / "log" / "pnl_daily_telegram_state.json"


def decimal_value(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value)) if value not in (None, "") else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def load_send_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "sent_keys": []}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema_version": 1, "sent_keys": []}
    return value if isinstance(value, dict) else {"schema_version": 1, "sent_keys": []}


def resolve_day(value: str | None, *, now: datetime | None = None) -> date:
    observed = (now or datetime.now(timezone.utc)).astimezone(BEIJING_TIMEZONE)
    if value in (None, "yesterday"):
        return observed.date() - timedelta(days=1)
    if value == "today":
        return observed.date()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--day must be today, yesterday, or YYYY-MM-DD"
        ) from exc


def build_daily_payload(
    baseline: dict[str, Any],
    *,
    asset: str,
    day: date,
    now: datetime | None = None,
) -> dict[str, Any]:
    record = pnl_day_summary(baseline, day.isoformat())
    capital = decimal_value(os.getenv("PNL_REPORT_CAPITAL_USD"))
    capital_source = "PNL_REPORT_CAPITAL_USD"
    if capital is None or capital <= 0:
        capital = decimal_value(baseline.get("account_baseline_equity_usd"))
        capital_source = "tracking_account_baseline"
    daily_pnl = decimal_value(record.get("confirmed_pnl_usd")) or Decimal("0")
    cumulative_pnl = (
        decimal_value(baseline.get("confirmed_pnl_usd")) or Decimal("0")
    )
    daily_return = (
        daily_pnl / capital * Decimal("100")
        if capital is not None and capital > 0
        else None
    )
    cumulative_return = (
        cumulative_pnl / capital * Decimal("100")
        if capital is not None and capital > 0
        else None
    )
    observed = now or datetime.now(timezone.utc)
    covered_days = beijing_calendar_days(
        parse_timestamp(
            baseline.get("account_baseline_at") or baseline.get("started_at")
        ),
        observed,
    )
    cumulative_annualized = (
        cumulative_return * Decimal("365") / covered_days
        if cumulative_return is not None
        and covered_days is not None
        and covered_days > 0
        else None
    )
    return {
        "asset": asset.upper(),
        "summary_scope": "beijing_daily",
        "summary_status": "complete",
        "beijing_day": day.isoformat(),
        "reporting_timezone": "Asia/Shanghai",
        "daily_closed_child_lots": int(record.get("closed_child_lots") or 0),
        "daily_completed_close_groups": int(
            record.get("tracked_completed_cycles") or 0
        ),
        "daily_four_leg_volume_usd": str(
            record.get("four_leg_volume_usd") or "0"
        ),
        "beijing_day_actual_pnl_usd": str(daily_pnl),
        "beijing_day_return_pct": str(daily_return) if daily_return is not None else None,
        "daily_annualized_simple_pct": (
            str(daily_return * Decimal("365"))
            if daily_return is not None
            else None
        ),
        "run_actual_pnl_usd": str(cumulative_pnl),
        "cumulative_four_leg_volume_usd": str(
            baseline.get("confirmed_four_leg_volume_usd") or "0"
        ),
        "cumulative_closed_child_lots": int(
            baseline.get("tracked_closed_child_lots") or 0
        ),
        "return_pct": (
            str(cumulative_return) if cumulative_return is not None else None
        ),
        "annualized_simple_pct": (
            str(cumulative_annualized)
            if cumulative_annualized is not None
            else None
        ),
        "covered_beijing_days": (
            str(covered_days) if covered_days is not None else None
        ),
        "capital_usd": str(capital) if capital is not None else None,
        "capital_source": capital_source if capital is not None else "unavailable",
        "variational_equity_usd": baseline.get(
            "latest_variational_equity_usd"
        ),
        "lighter_equity_usd": baseline.get("latest_lighter_equity_usd"),
        "combined_equity_usd": baseline.get("latest_combined_equity_usd"),
        "account_snapshot_at": baseline.get("latest_account_snapshot_at"),
        "return_pnl_source": "beijing_daily_confirmed_pair_fills",
        "annualized_reliability": "beijing_daily_projection",
    }


def send_with_retry(
    notifier: TelegramNotifier,
    message: str,
    *,
    attempts: int = 3,
) -> tuple[bool, str]:
    detail = "not_attempted"
    for attempt in range(1, max(1, attempts) + 1):
        ok, detail = notifier.send_now(message)
        if ok:
            return True, detail
        if attempt < attempts:
            time.sleep(min(2 ** (attempt - 1), 4))
    return False, detail


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a deduplicated Beijing-day Telegram PnL report."
    )
    parser.add_argument("--asset", choices=('ETH', 'WTI', 'ALL'), default="ALL")
    parser.add_argument("--native-inventory", action="store_true",
                        help="Use the shared inventory engine's unchanged daily template and baseline.")
    parser.add_argument("--wti-state-path", type=Path, default=ROOT / 'log/wti_live_state.json')
    parser.add_argument('--wti-billing-dir', type=Path, default=ROOT / 'log/wti_billing_records')
    parser.add_argument('--billing-delivery-state', type=Path, default=ROOT / 'log/wti_billing_telegram_state.json')
    parser.add_argument("--day", default="yesterday")
    parser.add_argument("--baseline-path", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--state-path", type=Path, default=DEFAULT_SEND_STATE)
    parser.add_argument("--force", action="store_true")
    parser.add_argument('--correction', action='store_true',
                        help='Send one deduplicated corrected daily report without changing daily sent keys')
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--attempts", type=int, default=3)
    return parser


def build_platform_daily_message(
    baseline: dict[str, Any] | None, state: dict[str, Any], *, day: date, asset: str = 'ALL',
    billing_records: dict | None = None,
    reporting_state: dict | None = None,
    equity_flows: dict | None = None,
) -> str:
    if asset == 'WTI' and state and state.get('asset') == 'WTI':
        from tools.lib.wti_equity_reporting import fixed_daily_message
        return fixed_daily_message(state, day, reporting_state or {}, equity_flows or {})
    lines = ['[Var/Lighter] 北京时间每日收益', f'日期：{day.isoformat()}｜资产：{asset}',
             '口径：按双边最终成交记录；未知费用不按0处理']
    if asset in {'ALL', 'ETH'}:
        if baseline and baseline.get('asset') == 'ETH':
            record = pnl_day_summary(baseline, day.isoformat())
            lines.extend([f"ETH：平仓批次={record.get('tracked_completed_cycles') or 0}，"
                          f"已确认成交毛收益={record.get('confirmed_pnl_usd') or '0'} USD",
                          'ETH：手续费不计；资金费计入收益'])
        else:
            lines.append('ETH：账本不可用，收益未知')
    if asset in {'ALL', 'WTI'}:
        if not state or state.get('asset') != 'WTI':
            lines.append('WTI：账本不可用，不能按0收益统计')
        else:
            rows = billing_rounds(state, billing_records or {})
            dated = [(r, parse_timestamp(r.get('closed_at'))) for r in rows]
            selected = [r for r, at in dated if at and at.astimezone(BEIJING_TIMEZONE).date() == day]
            cumulative = [r for r, at in dated if at and at.astimezone(BEIJING_TIMEZONE).date() <= day]
            def summary(items: list[dict[str, Any]]) -> str:
                known = [number(x.get('actual_gross_pnl_usd')) for x in items]
                total = sum((v for v in known if v is not None), Decimal(0))
                return f'已确认毛收益={total} USD，毛收益缺证据={sum(v is None for v in known)}组'
            lines.append(f'WTI：当日平仓={len(selected)}组，{summary(selected)}')
            lines.append(f'WTI：累计平仓={len(cumulative)}组，{summary(cumulative)}')
            fees = [number(r.get('actual_fees_usd')) for r in selected]
            gross = [number(r.get('actual_gross_pnl_usd')) for r in selected]
            funding = [number(r.get('funding_cashflow_usd')) for r in selected]
            net_income = sum((v for v in gross if v is not None), Decimal(0)) + sum(
                (v for v in funding if v is not None), Decimal(0))
            lines.append('WTI：当日手续费=' + (str(sum(fees, Decimal(0))) + ' USD'
                         if all(v is not None for v in fees) else '未确认'))
            lines.append('WTI：当日净收益='
                         + (str(net_income) + ' USD（手续费不计；资金费计入收益）'
                            if all(v is not None for v in gross)
                            else '未确认（毛收益缺证据）'))
            audited = [r for r in cumulative if r.get('actual_net_pnl_status') == 'confirmed_from_independent_billing']
            known_net = sum((Decimal(r['actual_net_pnl_usd']) for r in audited), Decimal(0))
            lines.append(f'WTI：累计账单已核实净收益={known_net} USDC，'
                         f'已核实={len(audited)}组，账单待核实={len(cumulative) - len(audited)}组')
            lines.append(f"WTI当前状态：{state.get('status')}，待处理={bool(state.get('pending_action'))}")
    lines.append('权益/收益率/年化：本报告未获取新鲜账户快照，不展示旧值或推算值')
    return '\n'.join(lines)


def reporting_with_equity_retry(path: Path, *, reader=read_json, now=None) -> dict:
    """One cached-snapshot retry, with no network requests or shared-file writes."""
    reporting = reader(path)
    now = now or datetime.now(timezone.utc)
    def fresh(value):
        try:
            stamp = datetime.fromisoformat(value['observed_at'].replace('Z','+00:00'))
            return -1 <= (now-stamp).total_seconds() <= 15 and Decimal(value['equity_usd']).is_finite()
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return False
    if fresh(reporting.get('latest_equity') or {}):
        return reporting
    health = reader(path.with_name('wti_live_risk_health.json'))
    try:
        v,l = Decimal(str(health['variational_equity_usd'])),Decimal(str(health['lighter_equity_usd']))
        current = {'observed_at':health['account_snapshot_observed_at'],'equity_usd':str(v+l)}
        if (health.get('asset') == 'WTI' and health.get('risk_data_complete') is True
                and health.get('startup_reconciled') is True and v.is_finite() and l.is_finite()
                and min(v,l)>0 and fresh(current)):
            return {**reporting, 'latest_equity':current}
    except (KeyError, TypeError, ValueError, ArithmeticError):
        pass
    return reporting


def send_platform_daily(args: argparse.Namespace, baseline: dict[str, Any] | None) -> int:
    day = resolve_day(args.day)
    memory = load_send_state(args.state_path)
    if args.state_path.exists():
        memory = json.loads(args.state_path.read_text(encoding='utf-8'))
        if not isinstance(memory, dict) or not isinstance(memory.get('sent_keys'), list):
            raise ValueError('daily_delivery_state_invalid_preserve_for_review')
    key = f'{args.asset}:v2:{day.isoformat()}'
    if key in memory.get('sent_keys', []) and not args.force and not args.correction and not args.dry_run:
        print('daily_pnl=SKIP already_sent')
        return 0
    state = read_json(args.wti_state_path)
    try:
        records = load_verified_records(args.wti_billing_dir, state)
    except (OSError, ValueError, KeyError, TypeError, ArithmeticError):
        records = {}
        logging.getLogger('daily_pnl_report').error('wti_billing_archive_invalid_net_not_confirmed')
    reporting = reporting_with_equity_retry(args.wti_state_path.with_suffix('.telegram.json'))
    flows = {}
    if args.asset == 'WTI':
        from tools.lib.wti_equity_reporting import daily_equity_period, at
        from tools.lib.wti_equity_flows import cashflow_receipt
        period = daily_equity_period(reporting, day)
        if period:
            starts = [x for k,x in (reporting.get('daily_capitals') or {}).items()
                      if k<=day.isoformat() and x.get('day_open_verified')]
            first = min(starts,key=lambda x:at(x['observed_at']))
            flows = cashflow_receipt(at(first['observed_at']),at(period[1]['observed_at']),
                args.wti_state_path.parent/'wti_equity_daily'/(day.isoformat()+'.json'))
    message = build_platform_daily_message(baseline, state, day=day, asset=args.asset,
                                          billing_records=records, reporting_state=reporting, equity_flows=flows)
    print(message)
    if args.dry_run:
        return 0
    notifier = TelegramNotifier.from_env(logger=logging.getLogger('daily_pnl_report.telegram'))
    if args.correction:
        correction_key = 'daily-correction:' + key
        correction = '[更正补报；非新成交]\n' + message
        detail = deliver_billing_correction_once(
            args.billing_delivery_state, correction_key, correction, records, notifier)
        print(f"daily_correction={'PASS' if detail == 'sent' else 'NOT_SENT'} {detail}")
        return 0 if detail == 'sent' else 1
    if args.asset != 'WTI':
        publish_billing_updates(records, args.billing_delivery_state, notifier)
    if key in memory.get('sent_keys', []) and not args.force:
        print('daily_pnl=SKIP already_sent')
        return 0
    detail = deliver_once(args.billing_delivery_state, 'daily:' + key, message, notifier, strict_message=False)
    ok = detail == 'sent'
    print(f"daily_pnl={'PASS' if ok else 'FAILED'} {detail}")
    if not ok:
        return 1
    if not args.force:
        write_json_atomic(args.state_path, {
            'schema_version': 2, 'last_sent_day': day.isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'sent_keys': [*memory.get('sent_keys', []), key],
        })
    return 0


def main() -> int:
    args = build_parser().parse_args()
    load_dotenv(ROOT / ".env")
    baseline = load_pnl_baseline(args.baseline_path)
    if args.asset in {'ALL', 'WTI'} and not args.native_inventory:
        return send_platform_daily(args, baseline)
    if baseline is None:
        print("daily_pnl=SKIP baseline_missing")
        return 0
    if str(baseline.get("asset") or "").upper() not in {"", args.asset.upper()}:
        raise SystemExit("daily_pnl=FAILED baseline_asset_mismatch")
    target_day = resolve_day(args.day)
    started_day = date.fromisoformat(
        str(baseline.get("current_beijing_day") or target_day.isoformat())
    )
    baseline_started = parse_timestamp(baseline.get("started_at"))
    if baseline_started is not None:
        started_day = baseline_started.astimezone(BEIJING_TIMEZONE).date()
    if target_day < started_day:
        print("daily_pnl=SKIP target_before_baseline")
        return 0

    send_state = load_send_state(args.state_path)
    if args.day == "yesterday" and not args.force:
        try:
            last_sent_day = date.fromisoformat(
                str(send_state.get("last_sent_day") or "")
            )
        except ValueError:
            last_sent_day = None
        if (
            last_sent_day is not None
            and last_sent_day + timedelta(days=1) < target_day
        ):
            target_day = last_sent_day + timedelta(days=1)
    send_key = f"{args.asset.upper()}:{target_day.isoformat()}"
    sent_keys = [str(value) for value in send_state.get("sent_keys", [])]
    if send_key in sent_keys and not args.force:
        print(f"daily_pnl=SKIP already_sent key={send_key}")
        return 0

    payload = build_daily_payload(
        baseline,
        asset=args.asset,
        day=target_day,
    )
    message = format_telegram_trade_message(
        "live_inventory_pnl_summary",
        payload,
    )
    print(message)
    if args.dry_run:
        print("daily_pnl=DRY_RUN")
        return 0
    notifier = TelegramNotifier.from_env(
        logger=logging.getLogger("daily_pnl_report.telegram")
    )
    ok, detail = send_with_retry(
        notifier,
        message,
        attempts=max(1, args.attempts),
    )
    print(f"daily_pnl={'PASS' if ok else 'FAILED'} {detail}")
    if not ok:
        return 1
    if not args.force:
        write_json_atomic(
            args.state_path,
            {
                "schema_version": 1,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "last_sent_day": target_day.isoformat(),
                "sent_keys": [*sent_keys, send_key][-400:],
            },
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
