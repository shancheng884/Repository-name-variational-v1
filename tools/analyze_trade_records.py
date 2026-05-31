import argparse
import csv
import json
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


def median(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal("2")


def avg(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def fmt(value: Decimal | None, places: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{places}f}"


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "none":
        return ""
    return text


def clean_bool(value: Any) -> bool:
    text = clean_text(value).lower()
    return text in {"1", "true", "yes", "on"}


def infer_record_kind(row: dict[str, Any]) -> str:
    explicit = clean_text(row.get("record_kind", ""))
    if explicit:
        return explicit
    if clean_text(row.get("trade_key", "")) or clean_text(row.get("trade_id", "")):
        return "execution_lifecycle"
    return "unknown"


def infer_hedge_completion_status(row: dict[str, Any]) -> str:
    explicit = clean_text(row.get("hedge_completion_status", ""))
    if explicit:
        return explicit

    var_filled_at = clean_text(row.get("variational_filled_at", ""))
    lighter_filled_at = clean_text(row.get("lighter_filled_at", ""))
    processing_stage = clean_text(row.get("processing_stage", ""))
    failure_stage = clean_text(row.get("failure_stage", ""))
    synthetic_eager_fill = clean_bool(row.get("synthetic_eager_fill"))
    matched_variational_trade_id = clean_text(row.get("matched_variational_trade_id", ""))

    if synthetic_eager_fill and not matched_variational_trade_id:
        if lighter_filled_at or processing_stage == "lighter_filled":
            return "hedged_from_synthetic_eager_unmatched"
        return "synthetic_eager_waiting_for_var_fill"

    if not var_filled_at:
        return "no_variational_fill"
    if lighter_filled_at or processing_stage == "lighter_filled":
        return "hedged"
    if processing_stage in {"live_submit_started", "live_submit_sent"}:
        return "hedge_pending"
    if processing_stage in {"live_submit_failed", "live_submit_timed_out"}:
        return "naked_variational_leg"
    if failure_stage == "hedge_plan":
        return "hedge_blocked_before_submit"
    if processing_stage in {"dry_run_pending", "dry_run_planned"}:
        return "dry_run_only"
    if processing_stage == "blocked_by_mode":
        return "not_live_mode"
    return "open"


def infer_rollback_action(row: dict[str, Any]) -> str:
    explicit = clean_text(row.get("rollback_action", ""))
    if explicit:
        return explicit
    if infer_hedge_completion_status(row) == "naked_variational_leg":
        return "manual_review_required"
    return "none"


def load_from_csv(csv_path: Path, asset_filter: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            asset = clean_text(row.get("asset", "")).upper()
            if not asset:
                continue
            if asset_filter and asset not in asset_filter:
                continue
            rows.append(row)
    return rows


def load_from_jsonl(jsonl_path: Path, asset_filter: set[str], date_filter: str) -> list[dict[str, Any]]:
    latest_by_trade_key: dict[str, dict[str, Any]] = {}
    failures_by_trade_key: dict[str, Counter[str]] = defaultdict(Counter)
    with jsonl_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            event = json.loads(line)
            logged_at = clean_text(event.get("logged_at", ""))
            if date_filter and not logged_at.startswith(date_filter):
                continue

            mode = clean_text(event.get("mode", "")).lower()
            if mode != "live":
                continue

            asset = clean_text(event.get("asset", "")).upper()
            if not asset:
                continue
            if asset_filter and asset not in asset_filter:
                continue

            trade_key = clean_text(event.get("trade_key", ""))
            if not trade_key:
                continue

            failure_reason = clean_text(event.get("failure_reason", ""))
            hedge_error = clean_text(event.get("hedge_error", ""))
            if failure_reason:
                failures_by_trade_key[trade_key][failure_reason] += 1
            elif hedge_error:
                failures_by_trade_key[trade_key][hedge_error] += 1

            latest_by_trade_key[trade_key] = event

    rows = list(latest_by_trade_key.values())
    for row in rows:
        trade_key = clean_text(row.get("trade_key", ""))
        failures = failures_by_trade_key.get(trade_key)
        if failures:
            row["_observed_failure_reasons"] = failures

    return dedupe_rows_by_effective_trade_id(rows)


def dedupe_rows_by_effective_trade_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped_by_trade_id: dict[str, dict[str, Any]] = {}
    passthrough_rows: list[dict[str, Any]] = []

    for row in rows:
        effective_trade_id = clean_text(row.get("matched_variational_trade_id", "")) or clean_text(row.get("trade_id", ""))
        if not effective_trade_id:
            passthrough_rows.append(row)
            continue

        existing = deduped_by_trade_id.get(effective_trade_id)
        if existing is None:
            deduped_by_trade_id[effective_trade_id] = row
            continue

        row_is_synthetic = clean_bool(row.get("synthetic_eager_fill"))
        existing_is_synthetic = clean_bool(existing.get("synthetic_eager_fill"))
        preferred = existing
        secondary = row

        if existing_is_synthetic and not row_is_synthetic:
            preferred = row
            secondary = existing

        preferred_failures = preferred.get("_observed_failure_reasons")
        secondary_failures = secondary.get("_observed_failure_reasons")
        if isinstance(preferred_failures, Counter) and isinstance(secondary_failures, Counter):
            preferred["_observed_failure_reasons"] = preferred_failures + secondary_failures
        elif secondary_failures:
            preferred["_observed_failure_reasons"] = secondary_failures

        deduped_by_trade_id[effective_trade_id] = preferred

    return passthrough_rows + list(deduped_by_trade_id.values())


def calibration_edge_bps(row: dict[str, Any]) -> Decimal | None:
    var_price = to_decimal(row.get("variational_filled_price"))
    lighter_price = to_decimal(row.get("lighter_filled_price"))
    if var_price is None or lighter_price is None or var_price == 0:
        return None
    side = clean_text(row.get("side_raw", row.get("side", ""))).lower()
    if side == "buy":
        return (lighter_price - var_price) / var_price * Decimal("10000")
    if side == "sell":
        return (var_price - lighter_price) / var_price * Decimal("10000")
    return None


def fill_diff_var_minus_lighter(row: dict[str, Any]) -> Decimal | None:
    explicit = to_decimal(row.get("fill_diff_var_minus_lighter"))
    if explicit is not None:
        return explicit
    var_price = to_decimal(row.get("variational_filled_price"))
    lighter_price = to_decimal(row.get("lighter_filled_price"))
    if var_price is None or lighter_price is None:
        return None
    return var_price - lighter_price


def summarize(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "rows": 0,
            "filled_rows": 0,
            "record_kinds": Counter(),
            "latency_ms": [],
            "var_event_to_seen_ms": [],
            "var_seen_to_plan_start_ms": [],
            "plan_latency_ms": [],
            "plan_ready_to_submit_start_ms": [],
            "submit_call_latency_ms": [],
            "submit_sent_to_fill_ms": [],
            "var_seen_to_lighter_fill_ms": [],
            "edge_bps": [],
            "fill_diff": [],
            "plan_fill_diff": [],
            "var_notional": [],
            "lighter_notional": [],
            "failure_reasons": Counter(),
            "observed_failure_reasons": Counter(),
            "hedge_completion_statuses": Counter(),
            "rollback_actions": Counter(),
            "synthetic_unmatched_rows": 0,
            "synthetic_unmatched_hedged_rows": 0,
            "synthetic_matched_rows": 0,
            "by_side": defaultdict(
                lambda: {
                    "rows": 0,
                    "filled_rows": 0,
                    "latency_ms": [],
                    "calibration_edge_bps": [],
                    "fill_diff": [],
                }
            ),
        }
    )

    for row in rows:
        asset = clean_text(row.get("asset", "")).upper()
        if not asset:
            continue

        bucket = stats[asset]
        bucket["rows"] += 1

        record_kind = infer_record_kind(row)
        bucket["record_kinds"][record_kind] += 1

        processing_stage = clean_text(row.get("processing_stage", ""))
        if processing_stage == "lighter_filled":
            bucket["filled_rows"] += 1

        failure_reason = row.get("failure_reason")
        failure_reason_text = "" if failure_reason is None else str(failure_reason).strip()
        if failure_reason_text and failure_reason_text.lower() != "none":
            bucket["failure_reasons"][failure_reason_text] += 1

        observed_failures = row.get("_observed_failure_reasons")
        if isinstance(observed_failures, Counter):
            bucket["observed_failure_reasons"].update(observed_failures)

        hedge_completion_status = infer_hedge_completion_status(row)
        bucket["hedge_completion_statuses"][hedge_completion_status] += 1

        synthetic_eager_fill = clean_bool(row.get("synthetic_eager_fill"))
        matched_variational_trade_id = clean_text(row.get("matched_variational_trade_id", ""))
        if synthetic_eager_fill and not matched_variational_trade_id:
            bucket["synthetic_unmatched_rows"] += 1
            if processing_stage == "lighter_filled":
                bucket["synthetic_unmatched_hedged_rows"] += 1
        if matched_variational_trade_id:
            bucket["synthetic_matched_rows"] += 1

        rollback_action = infer_rollback_action(row)
        bucket["rollback_actions"][rollback_action] += 1

        for key, column in (
            ("latency_ms", "live_fill_latency_ms"),
            ("var_event_to_seen_ms", "live_var_event_to_seen_ms"),
            ("var_seen_to_plan_start_ms", "live_var_seen_to_plan_start_ms"),
            ("plan_latency_ms", "live_plan_latency_ms"),
            ("plan_ready_to_submit_start_ms", "live_plan_ready_to_submit_start_ms"),
            ("submit_call_latency_ms", "live_submit_call_latency_ms"),
            ("submit_sent_to_fill_ms", "live_submit_sent_to_fill_ms"),
            ("var_seen_to_lighter_fill_ms", "live_var_seen_to_lighter_fill_ms"),
            ("edge_bps", "live_edge_bps"),
            ("plan_fill_diff", "plan_vs_lighter_fill_diff"),
            ("var_notional", "variational_notional"),
            ("lighter_notional", "lighter_notional"),
        ):
            value = to_decimal(row.get(column))
            if value is not None:
                bucket[key].append(value)
        fill_diff = fill_diff_var_minus_lighter(row)
        if fill_diff is not None:
            bucket["fill_diff"].append(fill_diff)

        side = clean_text(row.get("side_raw", row.get("side", ""))).lower() or "unknown"
        side_bucket = bucket["by_side"][side]
        side_bucket["rows"] += 1
        if processing_stage == "lighter_filled":
            side_bucket["filled_rows"] += 1
        latency = to_decimal(row.get("live_fill_latency_ms"))
        if latency is not None:
            side_bucket["latency_ms"].append(latency)
        fill_diff = fill_diff_var_minus_lighter(row)
        if fill_diff is not None:
            side_bucket["fill_diff"].append(fill_diff)
        edge = calibration_edge_bps(row)
        if edge is not None:
            side_bucket["calibration_edge_bps"].append(edge)

    return stats


def collect_completed_details(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    completed: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("processing_stage", "")).strip() != "lighter_filled":
            continue
        variational_filled_at = clean_text(row.get("variational_filled_at", ""))
        lighter_filled_at = clean_text(row.get("lighter_filled_at", ""))
        if not variational_filled_at or not lighter_filled_at:
            continue
        completed.append(
            {
                "asset": clean_text(row.get("asset", "")).upper(),
                "trade_id": clean_text(row.get("matched_variational_trade_id", "")) or clean_text(row.get("trade_id", "")),
                "side": clean_text(row.get("side_raw", row.get("side", ""))),
                "qty": clean_text(row.get("qty", "")),
                "synthetic_eager_fill": clean_bool(row.get("synthetic_eager_fill")),
                "variational_filled_price": to_decimal(row.get("variational_filled_price")),
                "lighter_filled_price": to_decimal(row.get("lighter_filled_price")),
                "live_fill_latency_ms": to_decimal(row.get("live_fill_latency_ms")),
                "live_var_event_to_seen_ms": to_decimal(row.get("live_var_event_to_seen_ms")),
                "live_var_seen_to_plan_start_ms": to_decimal(row.get("live_var_seen_to_plan_start_ms")),
                "live_plan_latency_ms": to_decimal(row.get("live_plan_latency_ms")),
                "live_plan_ready_to_submit_start_ms": to_decimal(row.get("live_plan_ready_to_submit_start_ms")),
                "live_submit_call_latency_ms": to_decimal(row.get("live_submit_call_latency_ms")),
                "live_submit_sent_to_fill_ms": to_decimal(row.get("live_submit_sent_to_fill_ms")),
                "live_var_seen_to_lighter_fill_ms": to_decimal(row.get("live_var_seen_to_lighter_fill_ms")),
                "live_edge_bps": to_decimal(row.get("live_edge_bps")),
                "calibration_edge_bps": calibration_edge_bps(row),
                "variational_filled_at": variational_filled_at,
                "lighter_filled_at": lighter_filled_at,
            }
        )
    completed.sort(key=lambda item: (item["asset"], item["variational_filled_at"], item["trade_id"], item["synthetic_eager_fill"]))
    return completed


def collect_unmatched_synthetic_eager_details(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for row in rows:
        if not clean_bool(row.get("synthetic_eager_fill")):
            continue
        if clean_text(row.get("matched_variational_trade_id", "")):
            continue
        details.append(
            {
                "asset": clean_text(row.get("asset", "")).upper(),
                "side": clean_text(row.get("side_raw", row.get("side", ""))),
                "qty": clean_text(row.get("qty", "")),
                "trade_id": clean_text(row.get("trade_id", "")),
                "processing_stage": clean_text(row.get("processing_stage", "")),
                "variational_filled_at": clean_text(row.get("variational_filled_at", "")),
                "lighter_filled_at": clean_text(row.get("lighter_filled_at", "")),
                "failure_reason": clean_text(row.get("failure_reason", "")),
            }
        )
    details.sort(key=lambda item: (item["asset"], item["variational_filled_at"], item["trade_id"]))
    return details


def print_summary(stats: dict[str, dict[str, Any]], source_label: str) -> None:
    if not stats:
        print("No matching rows found.")
        return

    print(f"Source: {source_label}")
    print()
    print("record kind breakdown")
    for asset in sorted(stats):
        counter = stats[asset]["record_kinds"]
        breakdown = ", ".join(f"{kind}={count}" for kind, count in counter.most_common())
        print(f"{asset}: {breakdown}")
    print()
    print(
        "asset rows filled avg_latency_ms median_latency_ms avg_edge_bps "
        "avg_fill_diff avg_plan_fill_diff avg_var_notional avg_lighter_notional"
    )

    for asset in sorted(stats):
        bucket = stats[asset]
        print(
            "{asset} {rows} {filled} {avg_latency} {median_latency} {avg_edge} {avg_fill} {avg_plan_fill} {avg_var_notional} {avg_lighter_notional}".format(
                asset=asset,
                rows=bucket["rows"],
                filled=bucket["filled_rows"],
                avg_latency=fmt(avg(bucket["latency_ms"]), 3),
                median_latency=fmt(median(bucket["latency_ms"]), 3),
                avg_edge=fmt(avg(bucket["edge_bps"]), 3),
                avg_fill=fmt(avg(bucket["fill_diff"]), 4),
                avg_plan_fill=fmt(avg(bucket["plan_fill_diff"]), 4),
                avg_var_notional=fmt(avg(bucket["var_notional"]), 4),
                avg_lighter_notional=fmt(avg(bucket["lighter_notional"]), 4),
            )
        )

    print()
    print("latency breakdown")
    print(
        "asset avg_var_to_plan_ms avg_plan_ms avg_plan_to_submit_ms "
        "avg_submit_call_ms avg_submit_to_fill_ms avg_var_seen_to_fill_ms avg_var_event_to_seen_ms"
    )
    for asset in sorted(stats):
        bucket = stats[asset]
        print(
            "{asset} {var_to_plan} {plan} {plan_to_submit} {submit_call} {submit_to_fill} {var_to_fill} {event_to_seen}".format(
                asset=asset,
                var_to_plan=fmt(avg(bucket["var_seen_to_plan_start_ms"]), 3),
                plan=fmt(avg(bucket["plan_latency_ms"]), 3),
                plan_to_submit=fmt(avg(bucket["plan_ready_to_submit_start_ms"]), 3),
                submit_call=fmt(avg(bucket["submit_call_latency_ms"]), 3),
                submit_to_fill=fmt(avg(bucket["submit_sent_to_fill_ms"]), 3),
                var_to_fill=fmt(avg(bucket["var_seen_to_lighter_fill_ms"]), 3),
                event_to_seen=fmt(avg(bucket["var_event_to_seen_ms"]), 3),
            )
        )

    print()
    print("failure breakdown")
    for asset in sorted(stats):
        counter = stats[asset]["failure_reasons"]
        if not counter:
            print(f"{asset}: none")
            continue
        breakdown = ", ".join(f"{reason}={count}" for reason, count in counter.most_common())
        print(f"{asset}: {breakdown}")

    print()
    print("observed failure history")
    for asset in sorted(stats):
        counter = stats[asset]["observed_failure_reasons"]
        if not counter:
            print(f"{asset}: none")
            continue
        breakdown = ", ".join(f"{reason}={count}" for reason, count in counter.most_common())
        print(f"{asset}: {breakdown}")

    print()
    print("hedge completion breakdown")
    for asset in sorted(stats):
        counter = stats[asset]["hedge_completion_statuses"]
        if not counter:
            print(f"{asset}: none")
            continue
        breakdown = ", ".join(f"{status}={count}" for status, count in counter.most_common())
        print(f"{asset}: {breakdown}")

    print()
    print("rollback action breakdown")
    for asset in sorted(stats):
        counter = stats[asset]["rollback_actions"]
        if not counter:
            print(f"{asset}: none")
            continue
        breakdown = ", ".join(f"{action}={count}" for action, count in counter.most_common())
        print(f"{asset}: {breakdown}")

    print()
    print("synthetic eager merge breakdown")
    print("asset synthetic_unmatched synthetic_unmatched_hedged synthetic_matched")
    for asset in sorted(stats):
        bucket = stats[asset]
        print(
            "{asset} {unmatched} {unmatched_hedged} {matched}".format(
                asset=asset,
                unmatched=bucket["synthetic_unmatched_rows"],
                unmatched_hedged=bucket["synthetic_unmatched_hedged_rows"],
                matched=bucket["synthetic_matched_rows"],
            )
        )

    print()
    print("by side calibration")
    print("asset side rows filled avg_latency_ms median_latency_ms avg_calibration_edge_bps avg_fill_diff")
    for asset in sorted(stats):
        by_side = stats[asset]["by_side"]
        if not by_side:
            print(f"{asset} none")
            continue
        for side in sorted(by_side):
            bucket = by_side[side]
            print(
                "{asset} {side} {rows} {filled} {avg_latency} {median_latency} {avg_edge} {avg_fill_diff}".format(
                    asset=asset,
                    side=side,
                    rows=bucket["rows"],
                    filled=bucket["filled_rows"],
                    avg_latency=fmt(avg(bucket["latency_ms"]), 3),
                    median_latency=fmt(median(bucket["latency_ms"]), 3),
                    avg_edge=fmt(avg(bucket["calibration_edge_bps"]), 3),
                    avg_fill_diff=fmt(avg(bucket["fill_diff"]), 4),
                )
            )


def print_completed_details(rows: list[dict[str, Any]]) -> None:
    completed = collect_completed_details(rows)
    print()
    print("completed fill details")
    if not completed:
        print("none")
        return

    print(
        "asset side qty avg_edge_bps calibration_edge_bps latency_ms submit_call_ms submit_to_fill_ms "
        "var_seen_to_fill_ms var_event_to_seen_ms synthetic_eager_fill var_price lighter_price var_filled_at trade_id"
    )
    for item in completed:
        print(
            "{asset} {side} {qty} {edge} {calibration_edge} {latency} {submit_call} {submit_to_fill} "
            "{var_seen_to_fill} {event_to_seen} {synthetic_eager_fill} {var_price} {lighter_price} {filled_at} {trade_id}".format(
                asset=item["asset"],
                side=item["side"],
                qty=item["qty"],
                edge=fmt(item["live_edge_bps"], 3),
                calibration_edge=fmt(item["calibration_edge_bps"], 3),
                latency=fmt(item["live_fill_latency_ms"], 3),
                submit_call=fmt(item["live_submit_call_latency_ms"], 3),
                submit_to_fill=fmt(item["live_submit_sent_to_fill_ms"], 3),
                var_seen_to_fill=fmt(item["live_var_seen_to_lighter_fill_ms"], 3),
                event_to_seen=fmt(item["live_var_event_to_seen_ms"], 3),
                synthetic_eager_fill=str(item["synthetic_eager_fill"]).lower(),
                var_price=fmt(item["variational_filled_price"], 4),
                lighter_price=fmt(item["lighter_filled_price"], 4),
                filled_at=item["variational_filled_at"] or "-",
                trade_id=item["trade_id"] or "-",
            )
        )


def print_unmatched_synthetic_eager_details(rows: list[dict[str, Any]]) -> None:
    details = collect_unmatched_synthetic_eager_details(rows)
    print()
    print("unmatched synthetic eager details")
    if not details:
        print("none")
        return

    print("asset side qty processing_stage var_filled_at lighter_filled_at trade_id failure_reason")
    for item in details:
        print(
            "{asset} {side} {qty} {processing_stage} {var_filled_at} {lighter_filled_at} {trade_id} {failure_reason}".format(
                asset=item["asset"],
                side=item["side"] or "-",
                qty=item["qty"] or "-",
                processing_stage=item["processing_stage"] or "-",
                var_filled_at=item["variational_filled_at"] or "-",
                lighter_filled_at=item["lighter_filled_at"] or "-",
                trade_id=item["trade_id"] or "-",
                failure_reason=item["failure_reason"] or "-",
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze live hedge records by asset.")
    parser.add_argument(
        "--source",
        choices=("jsonl", "csv"),
        default="jsonl",
        help="Data source. Default: jsonl",
    )
    parser.add_argument(
        "--jsonl",
        default="log/order_metrics.jsonl",
        help="Path to order_metrics.jsonl. Default: log/order_metrics.jsonl",
    )
    parser.add_argument(
        "--csv",
        default="log/trade_records.csv",
        help="Path to trade_records.csv. Default: log/trade_records.csv",
    )
    parser.add_argument(
        "--assets",
        default="",
        help="Optional comma-separated asset filter, e.g. SOL,BTC,ETH",
    )
    parser.add_argument(
        "--date",
        default="",
        help="Optional logged_at date filter for jsonl, e.g. 2026-05-23",
    )
    args = parser.parse_args()

    asset_filter = {asset.strip().upper() for asset in args.assets.split(",") if asset.strip()}

    if args.source == "jsonl":
        jsonl_path = Path(args.jsonl)
        if not jsonl_path.exists():
            raise SystemExit(f"JSONL file not found: {jsonl_path}")
        rows = load_from_jsonl(jsonl_path, asset_filter, args.date.strip())
        stats = summarize(rows)
        source_label = str(jsonl_path)
        if args.date.strip():
            source_label += f" (date={args.date.strip()})"
        print_summary(stats, source_label)
        print_completed_details(rows)
        print_unmatched_synthetic_eager_details(rows)
        return 0

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")
    rows = load_from_csv(csv_path, asset_filter)
    stats = summarize(rows)
    print_summary(stats, str(csv_path))
    print_completed_details(rows)
    print_unmatched_synthetic_eager_details(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
