#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


DIRECTION_LONG = "long_var_short_lighter"
DIRECTION_SHORT = "short_var_long_lighter"
STATE_FLAT = "flat"


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def fmt(value: Decimal | None, places: str = "0.01") -> str:
    if value is None:
        return "-"
    return format(value.quantize(Decimal(places)), "f")


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def tail_jsonl(path: Path, limit: int) -> deque[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=max(1, limit))
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        return rows
    return rows


def max_present(*values: Decimal | None) -> Decimal | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def avg(values: list[Decimal]) -> Decimal | None:
    return sum(values) / Decimal(len(values)) if values else None


def percentile(values: list[Decimal], pct: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int((Decimal(len(ordered) - 1) * pct / Decimal("100")).to_integral_value(rounding="ROUND_HALF_UP"))
    return ordered[max(0, min(index, len(ordered) - 1))]


def depth_slippage(row: dict[str, Any], key: str) -> Decimal | None:
    value = row.get(key)
    if not isinstance(value, dict):
        return None
    return to_decimal(value.get("slippage_bps"))


@dataclass
class CandidateStats:
    asset: str
    samples: int = 0
    latest_at: str = ""
    latest_age_seconds: Decimal | None = None
    latest_reason: str = "-"
    reasons: Counter[str] = field(default_factory=Counter)
    regime_hits: int = 0
    pass_hits: int = 0
    long_normalized: list[Decimal] = field(default_factory=list)
    short_normalized: list[Decimal] = field(default_factory=list)
    long_raw: list[Decimal] = field(default_factory=list)
    short_raw: list[Decimal] = field(default_factory=list)
    long_roundtrip: list[Decimal] = field(default_factory=list)
    short_roundtrip: list[Decimal] = field(default_factory=list)
    sample_moves: list[Decimal] = field(default_factory=list)
    quote_ages_ms: list[Decimal] = field(default_factory=list)
    book_ages_ms: list[Decimal] = field(default_factory=list)
    entry_slippages: list[Decimal] = field(default_factory=list)
    exit_slippages: list[Decimal] = field(default_factory=list)

    latest_direction: str = "unknown"
    latest_norm_edge: Decimal | None = None
    latest_raw_edge: Decimal | None = None
    latest_roundtrip: Decimal | None = None
    latest_sample_move: Decimal | None = None
    latest_entry_slippage: Decimal | None = None
    latest_exit_slippage: Decimal | None = None
    recent_pass_count: int = 0
    recent_consecutive_pass_count: int = 0
    best_recent_net: Decimal | None = None

    def update(self, row: dict[str, Any]) -> None:
        self.samples += 1
        self.latest_at = str(row.get("logged_at") or self.latest_at)
        latest_time = parse_time(row.get("logged_at"))
        if latest_time is not None:
            self.latest_age_seconds = Decimal(str((datetime.now(timezone.utc) - latest_time).total_seconds()))
        self.latest_reason = str(row.get("reason") or row.get("stablecoin_regime_reason") or "-")
        self.reasons[self.latest_reason] += 1

        long_norm = to_decimal(row.get("normalized_long_edge_bps"))
        short_norm = to_decimal(row.get("normalized_short_edge_bps"))
        long_raw = to_decimal(row.get("long_edge_bps"))
        short_raw = to_decimal(row.get("short_edge_bps"))
        long_rt = to_decimal(row.get("long_roundtrip_pnl_bps"))
        short_rt = to_decimal(row.get("short_roundtrip_pnl_bps"))
        sample_move = to_decimal(row.get("basis_sample_move_bps"))
        quote_ms = to_decimal(row.get("quote_ms"))
        book_age_seconds = to_decimal(row.get("lighter_book_age_seconds"))

        long_entry_slippage = depth_slippage(row, "long_entry_lighter_depth")
        short_entry_slippage = depth_slippage(row, "short_entry_lighter_depth")
        long_exit_slippage = depth_slippage(row, "long_exit_lighter_depth")
        short_exit_slippage = depth_slippage(row, "short_exit_lighter_depth")

        if long_norm is not None:
            self.long_normalized.append(long_norm)
        if short_norm is not None:
            self.short_normalized.append(short_norm)
        if long_raw is not None:
            self.long_raw.append(long_raw)
        if short_raw is not None:
            self.short_raw.append(short_raw)
        if long_rt is not None:
            self.long_roundtrip.append(long_rt)
        if short_rt is not None:
            self.short_roundtrip.append(short_rt)
        if sample_move is not None:
            self.sample_moves.append(abs(sample_move))
        if quote_ms is not None:
            self.quote_ages_ms.append(quote_ms)
        if book_age_seconds is not None:
            self.book_ages_ms.append(book_age_seconds * Decimal("1000"))

        entry_slippage = max_present(long_entry_slippage, short_entry_slippage)
        exit_slippage = max_present(long_exit_slippage, short_exit_slippage)
        if entry_slippage is not None:
            self.entry_slippages.append(entry_slippage)
        if exit_slippage is not None:
            self.exit_slippages.append(exit_slippage)

        if row.get("long_stablecoin_regime_ok") or row.get("short_stablecoin_regime_ok"):
            self.regime_hits += 1

        long_score_edge = long_norm if long_norm is not None else long_raw
        short_score_edge = short_norm if short_norm is not None else short_raw
        if long_score_edge is not None and (short_score_edge is None or long_score_edge >= short_score_edge):
            self.latest_direction = DIRECTION_LONG
            self.latest_norm_edge = long_norm
            self.latest_raw_edge = long_raw
            self.latest_roundtrip = long_rt
            self.latest_entry_slippage = long_entry_slippage
            self.latest_exit_slippage = long_exit_slippage
        elif short_score_edge is not None:
            self.latest_direction = DIRECTION_SHORT
            self.latest_norm_edge = short_norm
            self.latest_raw_edge = short_raw
            self.latest_roundtrip = short_rt
            self.latest_entry_slippage = short_entry_slippage
            self.latest_exit_slippage = short_exit_slippage
        self.latest_sample_move = abs(sample_move) if sample_move is not None else None

    def expected_shortfall(self, fallback: Decimal) -> Decimal:
        parts = [value for value in [percentile(self.entry_slippages[-50:], Decimal("80")), percentile(self.exit_slippages[-50:], Decimal("80"))] if value is not None]
        if not parts:
            return fallback
        return max(fallback, sum(parts))

    def best_normalized_avg(self) -> Decimal | None:
        best: list[Decimal] = []
        for long_value, short_value in zip(self.long_normalized[-50:], self.short_normalized[-50:]):
            best.append(max(long_value, short_value))
        if not best:
            return None
        return avg(best)


def is_sane_row(row: dict[str, Any], args: argparse.Namespace) -> bool:
    basis = to_decimal(row.get("basis_bps"))
    if basis is not None and abs(basis) > Decimal(str(args.max_abs_basis_bps)):
        return False
    logged_at = parse_time(row.get("logged_at"))
    quote_at = parse_time(row.get("quote_timestamp"))
    if logged_at is not None and quote_at is not None:
        if abs(Decimal(str((logged_at - quote_at).total_seconds()))) > Decimal(str(args.max_log_quote_skew_seconds)):
            return False
    quote_ms = to_decimal(row.get("quote_ms"))
    if quote_ms is not None and quote_ms > Decimal(str(args.max_quote_ms_filter)):
        return False
    return True


def collect_stats(rows: list[dict[str, Any]], assets: set[str], args: argparse.Namespace) -> dict[str, CandidateStats]:
    stats: dict[str, CandidateStats] = {asset: CandidateStats(asset) for asset in assets}
    for row in rows:
        asset = str(row.get("asset") or "").upper()
        if asset not in stats:
            continue
        event = row.get("event")
        if event not in {"live_inventory_basis_state", "live_inventory_entry_blocked", "fresh_quote_basis_inventory_paper_state"}:
            continue
        if event == "fresh_quote_basis_inventory_paper_state" and not row.get("warm"):
            continue
        if not is_sane_row(row, args):
            continue
        stats[asset].update(row)
    return stats


def estimate_net_score(item: CandidateStats, *, fallback_shortfall_bps: Decimal, sample_move_penalty: Decimal) -> Decimal | None:
    edge = item.latest_norm_edge if item.latest_norm_edge is not None else item.latest_raw_edge
    if edge is None:
        return None
    shortfall = item.expected_shortfall(fallback_shortfall_bps)
    move = item.latest_sample_move or Decimal("0")
    roundtrip = item.latest_roundtrip or Decimal("0")
    return edge + min(roundtrip, Decimal("0")) - shortfall - (move * sample_move_penalty)


def is_candidate(
    item: CandidateStats,
    *,
    min_samples: int,
    min_normalized_edge_bps: Decimal,
    min_net_score_bps: Decimal,
    max_sample_move_bps: Decimal,
    max_quote_age_ms: Decimal,
    max_book_age_ms: Decimal,
    fallback_shortfall_bps: Decimal,
    sample_move_penalty: Decimal,
) -> tuple[bool, list[str], Decimal | None]:
    reasons: list[str] = []
    net = estimate_net_score(item, fallback_shortfall_bps=fallback_shortfall_bps, sample_move_penalty=sample_move_penalty)
    if item.samples < min_samples:
        reasons.append("few_samples")
    if item.latest_norm_edge is None or item.latest_norm_edge < min_normalized_edge_bps:
        reasons.append("normalized_edge_low")
    if item.latest_sample_move is None or item.latest_sample_move > max_sample_move_bps:
        reasons.append("sample_move_high")
    latest_quote_age = item.quote_ages_ms[-1] if item.quote_ages_ms else None
    latest_book_age = item.book_ages_ms[-1] if item.book_ages_ms else None
    if latest_quote_age is None or latest_quote_age > max_quote_age_ms:
        reasons.append("quote_age_high")
    if latest_book_age is None or latest_book_age > max_book_age_ms:
        reasons.append("book_age_high")
    if net is None or net < min_net_score_bps:
        reasons.append("net_score_low")
    return not reasons, reasons, net


def annotate_recent_passes(
    rows: list[dict[str, Any]],
    stats: dict[str, CandidateStats],
    args: argparse.Namespace,
) -> None:
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        asset = str(row.get("asset") or "").upper()
        if asset in stats and row.get("event") in {"live_inventory_basis_state", "live_inventory_entry_blocked", "fresh_quote_basis_inventory_paper_state"}:
            if row.get("event") == "fresh_quote_basis_inventory_paper_state" and not row.get("warm"):
                continue
            if not is_sane_row(row, args):
                continue
            by_asset[asset].append(row)

    for asset, item in stats.items():
        recent_rows = by_asset.get(asset, [])[-max(1, args.lookback_samples) :]
        pass_count = 0
        consecutive = 0
        best_net: Decimal | None = None
        for row in recent_rows:
            temp = CandidateStats(asset)
            temp.update(row)
            ok, _, net = is_candidate(
                temp,
                min_samples=0,
                min_normalized_edge_bps=Decimal(str(args.min_normalized_edge_bps)),
                min_net_score_bps=Decimal(str(args.min_net_score_bps)),
                max_sample_move_bps=Decimal(str(args.max_sample_move_bps)),
                max_quote_age_ms=Decimal(str(args.max_quote_age_ms)),
                max_book_age_ms=Decimal(str(args.max_book_age_ms)),
                fallback_shortfall_bps=Decimal(str(args.fallback_shortfall_bps)),
                sample_move_penalty=Decimal(str(args.sample_move_penalty)),
            )
            if net is not None and (best_net is None or net > best_net):
                best_net = net
            if ok:
                pass_count += 1
                consecutive += 1
            else:
                consecutive = 0
        item.recent_pass_count = pass_count
        item.recent_consecutive_pass_count = consecutive
        item.best_recent_net = best_net


def latest_run_filter(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_run_id = ""
    for row in reversed(rows):
        run_id = str(row.get("run_id") or "")
        if run_id:
            latest_run_id = run_id
            break
    if not latest_run_id:
        return rows
    return [row for row in rows if str(row.get("run_id") or "") == latest_run_id]


def run_once(args: argparse.Namespace) -> int:
    assets = {asset.strip().upper() for asset in args.assets.split(",") if asset.strip()}
    if not assets:
        raise SystemExit("--assets must include at least one asset")

    live_state = read_json(args.live_state)
    live_status = str(live_state.get("status") or "unknown")
    live_asset = str(live_state.get("asset") or args.current_asset or "-").upper()
    live_flat = live_status == STATE_FLAT and not live_state.get("open_lots") and not live_state.get("pending_actions")

    rows = list(tail_jsonl(args.probe_log, args.tail))
    if args.live_log and args.live_log != args.probe_log:
        rows.extend(tail_jsonl(args.live_log, max(1, args.tail // 5)))
    if args.latest_run_only:
        rows = latest_run_filter(rows)
    stats = collect_stats(rows, assets, args)
    annotate_recent_passes(rows, stats, args)

    ranked: list[tuple[Decimal, CandidateStats, bool, list[str]]] = []
    for item in stats.values():
        ok, reasons, net = is_candidate(
            item,
            min_samples=args.min_samples,
            min_normalized_edge_bps=Decimal(str(args.min_normalized_edge_bps)),
            min_net_score_bps=Decimal(str(args.min_net_score_bps)),
            max_sample_move_bps=Decimal(str(args.max_sample_move_bps)),
            max_quote_age_ms=Decimal(str(args.max_quote_age_ms)),
            max_book_age_ms=Decimal(str(args.max_book_age_ms)),
            fallback_shortfall_bps=Decimal(str(args.fallback_shortfall_bps)),
            sample_move_penalty=Decimal(str(args.sample_move_penalty)),
        )
        if item.latest_age_seconds is None or item.latest_age_seconds > Decimal(str(args.max_sample_age_seconds)):
            ok = False
            reasons = [*reasons, "stale_asset_samples"]
        if item.recent_pass_count < args.confirm_samples and item.recent_consecutive_pass_count < args.confirm_consecutive_samples:
            ok = False
            reasons = [*reasons, "confirm_samples_low"]
        ranked.append((net if net is not None else Decimal("-999999"), item, ok, reasons))
    ranked.sort(key=lambda row: row[0], reverse=True)

    current_score = None
    if live_asset in stats:
        current_score = estimate_net_score(
            stats[live_asset],
            fallback_shortfall_bps=Decimal(str(args.fallback_shortfall_bps)),
            sample_move_penalty=Decimal(str(args.sample_move_penalty)),
        )

    print(f"live_status={live_status} live_asset={live_asset} live_flat={str(live_flat).lower()} rows={len(rows)}")
    print("asset samples age_s pass/consec dir norm raw roundtrip move entry_slip exit_slip net best_recent ok reason")
    for net, item, ok, reasons in ranked:
        print(
            f"{item.asset} {item.samples} {fmt(item.latest_age_seconds, '0.1')} {item.recent_pass_count}/{item.recent_consecutive_pass_count} "
            f"{item.latest_direction} {fmt(item.latest_norm_edge)} {fmt(item.latest_raw_edge)} "
            f"{fmt(item.latest_roundtrip)} {fmt(item.latest_sample_move)} {fmt(item.latest_entry_slippage)} "
            f"{fmt(item.latest_exit_slippage)} {fmt(net)} {fmt(item.best_recent_net)} {str(ok).lower()} {','.join(reasons) or '-'}"
        )

    if not ranked:
        return 0
    write_ranking(args, ranked, live_status=live_status, live_asset=live_asset, live_flat=live_flat, current_score=current_score)
    best_net, best, best_ok, best_reasons = ranked[0]
    delta = None if current_score is None else best_net - current_score
    if best_ok and live_flat and best.asset != live_asset and (delta is None or delta >= Decimal(str(args.min_switch_delta_bps))):
        print()
        print(
            "SWITCH_CANDIDATE "
            f"asset={best.asset} direction={best.latest_direction} net={fmt(best_net)} "
            f"current_net={fmt(current_score)} delta={fmt(delta)} "
            f"normalized_edge={fmt(best.latest_norm_edge)} sample_move={fmt(best.latest_sample_move)} "
            "action=consider_stop_current_live_after_manual_flat_confirmation"
        )
        if args.print_live_command:
            print_live_command(best.asset, args)
    elif best_ok and best.asset == live_asset:
        print()
        print(f"KEEP_CANDIDATE asset={best.asset} net={fmt(best_net)} action=keep_current_asset")
    else:
        if best_ok and best.asset != live_asset and delta is not None and delta < Decimal(str(args.min_switch_delta_bps)):
            best_reasons = [*best_reasons, "switch_delta_low"]
        print()
        print(f"NO_SWITCH best_asset={best.asset} reasons={','.join(best_reasons) or '-'}")
    return 0


def write_ranking(
    args: argparse.Namespace,
    ranked: list[tuple[Decimal, CandidateStats, bool, list[str]]],
    *,
    live_status: str,
    live_asset: str,
    live_flat: bool,
    current_score: Decimal | None,
) -> None:
    if not args.ranking_output:
        return
    payload = {
        "event": "asset_candidate_ranking",
        "logged_at": datetime.now(timezone.utc).isoformat(),
        "live_status": live_status,
        "live_asset": live_asset,
        "live_flat": live_flat,
        "current_score": str(current_score) if current_score is not None else None,
        "assets": [],
    }
    for net, item, ok, reasons in ranked:
        payload["assets"].append(
            {
                "asset": item.asset,
                "samples": item.samples,
                "latest_age_seconds": str(item.latest_age_seconds) if item.latest_age_seconds is not None else None,
                "recent_pass_count": item.recent_pass_count,
                "recent_consecutive_pass_count": item.recent_consecutive_pass_count,
                "direction": item.latest_direction,
                "normalized_edge_bps": str(item.latest_norm_edge) if item.latest_norm_edge is not None else None,
                "raw_edge_bps": str(item.latest_raw_edge) if item.latest_raw_edge is not None else None,
                "roundtrip_pnl_bps": str(item.latest_roundtrip) if item.latest_roundtrip is not None else None,
                "sample_move_bps": str(item.latest_sample_move) if item.latest_sample_move is not None else None,
                "net_score_bps": str(net),
                "best_recent_net_score_bps": str(item.best_recent_net) if item.best_recent_net is not None else None,
                "ok": ok,
                "reasons": reasons,
            }
        )
    args.ranking_output.parent.mkdir(parents=True, exist_ok=True)
    with args.ranking_output.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def print_live_command(asset: str, args: argparse.Namespace) -> None:
    lines = [
        "",
        "suggested_live_command_after_manual_flat_confirmation:",
        "cd ~/Repository-name-variational-v1",
        "source .venv/bin/activate",
        "",
        "python main.py \\",
        "  --mode live \\",
        "  --confirm-live \\",
        f"  --live-allowed-assets {asset} \\",
        "  --variational-submit-transport api \\",
        "  --lighter-submit-transport ws \\",
        "  --lighter-order-mode market-ioc \\",
        "  --lighter-prewarm-submit-ws \\",
        f"  --live-max-notional-usd {args.live_max_notional_usd} \\",
        "  --live-inventory \\",
        "  --live-inventory-signal-mode basis \\",
        "  --live-inventory-basis-entry-mode concurrent \\",
        f"  --live-inventory-lot-notional-usd {args.live_inventory_lot_notional_usd} \\",
        "  --live-inventory-max-cycles 3 \\",
        "  --live-inventory-max-lots 1 \\",
        "  --live-inventory-max-total-lots 1 \\",
        "  --live-inventory-max-lighter-slippage-bps 6 \\",
        "  --live-inventory-lighter-submit-slippage-bps 15 \\",
        "  --live-inventory-lighter-exit-submit-slippage-bps 30 \\",
        "  --live-inventory-basis-min-entry-edge-bps 13 \\",
        "  --live-inventory-basis-min-abs-entry-bps 13 \\",
        "  --live-inventory-basis-min-exit-pnl-bps 8.0 \\",
        "  --live-inventory-basis-min-signal-reverted-exit-pnl-bps 8.0 \\",
        "  --live-inventory-basis-profit-take-pnl-bps 10.0 \\",
        "  --live-inventory-basis-entry-confirm-samples 2 \\",
        "  --live-inventory-basis-max-sample-move-bps 5 \\",
        "  --live-inventory-basis-stablecoin-normalization \\",
        "  --live-inventory-basis-use-normalized-edge-for-entry \\",
        "  --live-inventory-basis-stablecoin-regime-entry \\",
        f"  --live-inventory-basis-min-normalized-entry-edge-bps {args.suggested_min_normalized_entry_edge_bps} \\",
        "  --live-inventory-basis-min-normalized-filter-edge-bps 0.5 \\",
        "  --live-inventory-entry-lighter-fill-timeout-seconds 3 \\",
        "  --live-inventory-basis-auto-close-unhedged \\",
        "  --live-inventory-i-accept-basis-real-diagnostic \\",
        "  --live-inventory-i-accept-diagnostic-low-entry-bps \\",
        "  --live-inventory-ignore-recent-execution-loss-buffer-for-diagnostics \\",
        "  --live-inventory-i-confirm-flat-start \\",
        "  --live-inventory-reset-state-after-manual-flat",
    ]
    print("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only live asset candidate watcher.")
    parser.add_argument("--assets", default="BTC,ETH,SOL")
    parser.add_argument("--current-asset", default="")
    parser.add_argument("--probe-log", type=Path, default=Path("log_probe/order_metrics.jsonl"))
    parser.add_argument("--live-log", type=Path, default=Path("log/order_metrics.jsonl"))
    parser.add_argument("--live-state", type=Path, default=Path("log/live_inventory_state.json"))
    parser.add_argument("--tail", type=int, default=50000)
    parser.add_argument("--latest-run-only", action="store_true")
    parser.add_argument("--min-samples", type=int, default=120)
    parser.add_argument("--lookback-samples", type=int, default=30)
    parser.add_argument("--confirm-samples", type=int, default=2)
    parser.add_argument("--confirm-consecutive-samples", type=int, default=1)
    parser.add_argument("--min-switch-delta-bps", type=float, default=2.0)
    parser.add_argument("--min-normalized-edge-bps", type=float, default=1.0)
    parser.add_argument("--min-net-score-bps", type=float, default=1.0)
    parser.add_argument("--max-sample-move-bps", type=float, default=5.0)
    parser.add_argument("--max-quote-age-ms", type=float, default=250.0)
    parser.add_argument("--max-book-age-ms", type=float, default=250.0)
    parser.add_argument("--fallback-shortfall-bps", type=float, default=5.5)
    parser.add_argument("--sample-move-penalty", type=float, default=0.5)
    parser.add_argument("--max-sample-age-seconds", type=float, default=180.0)
    parser.add_argument("--max-abs-basis-bps", type=float, default=100.0)
    parser.add_argument("--max-log-quote-skew-seconds", type=float, default=30.0)
    parser.add_argument("--max-quote-ms-filter", type=float, default=1000.0)
    parser.add_argument("--ranking-output", type=Path, default=None)
    parser.add_argument("--print-live-command", action="store_true")
    parser.add_argument("--live-max-notional-usd", type=float, default=25)
    parser.add_argument("--live-inventory-lot-notional-usd", type=float, default=20)
    parser.add_argument("--suggested-min-normalized-entry-edge-bps", type=float, default=1.0)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    args = parser.parse_args()
    if args.tail <= 0:
        parser.error("--tail must be > 0")
    if args.min_samples < 0:
        parser.error("--min-samples must be >= 0")
    if args.lookback_samples <= 0:
        parser.error("--lookback-samples must be > 0")
    if args.confirm_samples < 0:
        parser.error("--confirm-samples must be >= 0")
    if args.confirm_consecutive_samples < 0:
        parser.error("--confirm-consecutive-samples must be >= 0")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be > 0")
    if args.max_sample_age_seconds <= 0:
        parser.error("--max-sample-age-seconds must be > 0")

    while True:
        code = run_once(args)
        if not args.watch:
            return code
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
