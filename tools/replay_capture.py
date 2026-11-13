#!/usr/bin/env python3
"""Download capture partitions from R2 and query them with DuckDB.

Examples:
  python tools/replay_capture.py --date 2026-11-13 --symbol NVDA
  python tools/replay_capture.py --date 2026-11-13 --symbol NVDA,SNDK \
      --streams quotes,edges,market_trades --venue lighter_mainnet
  python tools/replay_capture.py --date 2026-11-13 --symbol NVDA \
      --sql 'select count(*) as rows from {files}'
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = Path.home() / "replay" / "var-lighter"
VENUES = ("variational", "lighter_mainnet", "lighter_rh", "entropy")
STREAMS = ("quotes", "books", "market_trades", "edges", "fills", "adverse", "chains")


def load_env() -> dict[str, str]:
    env = dict(os.environ)
    for path in (Path.home() / ".config" / "market-capture" / "r2.env", ROOT / ".env"):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env.setdefault(key.strip(), value.strip().strip("'\""))
    return env


def parse_dates(value: str) -> list[date]:
    dates: list[date] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        dates.append(datetime.strptime(part, "%Y-%m-%d").date())
    if not dates:
        raise ValueError("date_required")
    return dates


def date_range(start: str | None, end: str | None) -> list[date]:
    if not start:
        raise ValueError("date_or_from_required")
    first = datetime.strptime(start, "%Y-%m-%d").date()
    last = datetime.strptime(end, "%Y-%m-%d").date() if end else first
    if last < first:
        raise ValueError("date_range_invalid")
    out = []
    current = first
    while current <= last:
        out.append(current)
        current += timedelta(days=1)
    return out


def parse_symbols(value: str) -> list[str]:
    symbols = [part.strip().upper() for part in value.split(",") if part.strip()]
    if not symbols:
        raise ValueError("symbol_required")
    return symbols


def parse_streams(value: str) -> list[str]:
    streams = [part.strip().lower() for part in value.split(",") if part.strip()]
    unknown = [stream for stream in streams if stream not in STREAMS]
    if unknown:
        raise ValueError("unknown_stream:" + ",".join(unknown))
    return streams


def partition_specs(stream: str, symbol: str, venue: str | None) -> list[tuple[str, str]]:
    """Return (venue_folder, local_subdir) for one stream/symbol."""
    if stream == "edges":
        return [("cross", "edges/cross")]
    venues = [venue] if venue else list(VENUES)
    return [(item, f"{stream}/{item}") for item in venues]


def remote_dir(env: dict[str, str], stream_dir: str, symbol: str) -> str:
    remote = env.get("R2_REMOTE", "r2").strip() or "r2"
    bucket = env.get("R2_BUCKET", "").strip()
    prefix = env.get("R2_PREFIX", "market_capture").strip().strip("/")
    if not bucket:
        raise ValueError("R2_BUCKET_missing")
    base = f"{remote}:{bucket}"
    if prefix:
        base += f"/{prefix}"
    return f"{base}/{stream_dir}/{symbol}"


def local_dir(cache: Path, day: date, stream_dir: str, symbol: str) -> Path:
    return cache / day.isoformat() / stream_dir / symbol


def rclone_command(
    source: str,
    destination: Path,
    day: date,
    *,
    low_priority: bool = True,
    bwlimit: str = "50M",
) -> list[str]:
    command: list[str] = []
    if low_priority and shutil.which("nice"):
        command += ["nice", "-n", "19"]
        if shutil.which("ionice"):
            command += ["ionice", "-c", "3"]
    command += [
        "rclone", "copy", source, str(destination),
        "--include", f"{day.strftime('%Y%m%d')}-*.jsonl.gz",
        "--transfers", "4", "--checkers", "4", "--no-traverse",
    ]
    if bwlimit:
        command += ["--bwlimit", bwlimit]
    return command


def download(
    source: str,
    destination: Path,
    day: date,
    *,
    dry_run: bool = False,
    low_priority: bool = True,
    bwlimit: str = "50M",
) -> dict[str, Any]:
    destination.mkdir(parents=True, exist_ok=True)
    command = rclone_command(source, destination, day, low_priority=low_priority, bwlimit=bwlimit)
    if dry_run:
        return {"ok": True, "dry_run": True, "command": command, "destination": str(destination)}
    result = subprocess.run(command, capture_output=True, text=True, timeout=1800)
    stderr = (result.stderr or "")[-800:]
    missing = result.returncode != 0 and "directory not found" in stderr.lower()
    return {
        "ok": result.returncode == 0 or missing,
        "skipped": missing,
        "returncode": result.returncode,
        "destination": str(destination),
        "stderr": stderr,
    }


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def summary_sql(stream: str, glob: str) -> str:
    files = sql_quote(glob)
    if stream == "quotes":
        return f"""
            SELECT venue, symbol, count(*) AS rows,
                   min(recv_at) AS first_at, max(recv_at) AS last_at,
                   avg(CAST(spread_bps AS DOUBLE)) AS avg_spread_bps
            FROM read_json_auto({files}, union_by_name=true)
            GROUP BY venue, symbol ORDER BY venue, symbol
        """
    if stream == "edges":
        return f"""
            SELECT buy_venue, sell_venue, mode, count(*) AS rows,
                   avg(CAST(edge_bps AS DOUBLE)) AS avg_edge_bps,
                   quantile_cont(CAST(edge_bps AS DOUBLE), 0.5) AS p50_edge_bps,
                   quantile_cont(CAST(edge_bps AS DOUBLE), 0.95) AS p95_edge_bps
            FROM read_json_auto({files}, union_by_name=true)
            GROUP BY buy_venue, sell_venue, mode ORDER BY buy_venue, sell_venue
        """
    if stream == "market_trades":
        return f"""
            SELECT venue, symbol, count(*) AS rows,
                   sum(CAST(usd_amount AS DOUBLE)) AS usd_notional,
                   min(source_at) AS first_at, max(source_at) AS last_at
            FROM read_json_auto({files}, union_by_name=true)
            GROUP BY venue, symbol ORDER BY venue, symbol
        """
    return f"""
        SELECT count(*) AS rows,
               min(recv_at) AS first_at, max(recv_at) AS last_at
        FROM read_json_auto({files}, union_by_name=true)
    """


def query_files(stream: str, files: list[Path], sql: str | None = None) -> list[dict[str, Any]]:
    if not files:
        return []
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError("duckdb_missing: pip install duckdb") from exc
    glob = str(files[0].parent / "*.jsonl.gz")
    statement = sql.replace("{files}", sql_quote(glob)) if sql else summary_sql(stream, glob)
    connection = duckdb.connect()
    try:
        result = connection.execute(statement)
        columns = [item[0] for item in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        connection.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull R2 capture partitions and query with DuckDB.")
    parser.add_argument("--date", default="", help="YYYY-MM-DD, comma separated")
    parser.add_argument("--from", dest="start", default="", help="start date YYYY-MM-DD")
    parser.add_argument("--to", dest="end", default="", help="end date YYYY-MM-DD")
    parser.add_argument("--symbol", required=True, help="NVDA,SNDK,...")
    parser.add_argument("--streams", default="quotes,edges", help="quotes,books,market_trades,edges,fills,adverse,chains")
    parser.add_argument("--venue", default="", help="optional single venue")
    parser.add_argument("--cache", default=str(DEFAULT_CACHE))
    parser.add_argument("--sql", default="", help="custom SQL; use {files} for the parquet/json glob")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-low-priority", action="store_true")
    parser.add_argument("--bwlimit", default="50M")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env = load_env()
    try:
        days = parse_dates(args.date) if args.date else date_range(args.start, args.end)
        symbols = parse_symbols(args.symbol)
        streams = parse_streams(args.streams)
    except ValueError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    if not env.get("R2_BUCKET"):
        print(json.dumps({"ok": False, "error": "R2_BUCKET_missing"}, ensure_ascii=False), file=sys.stderr)
        return 2
    cache = Path(args.cache).expanduser()
    summary: dict[str, Any] = {"ok": True, "downloaded": [], "queries": []}
    for day in days:
        for symbol in symbols:
            for stream in streams:
                for venue_folder, stream_dir in partition_specs(stream, symbol, args.venue or None):
                    source = remote_dir(env, stream_dir, symbol)
                    destination = local_dir(cache, day, stream_dir, symbol)
                    result = download(
                        source, destination, day,
                        dry_run=args.dry_run,
                        low_priority=not args.no_low_priority,
                        bwlimit=args.bwlimit,
                    )
                    summary["downloaded"].append(result)
                    if not result.get("ok"):
                        summary["ok"] = False
                        continue
                    files = sorted(destination.glob(f"{day.strftime('%Y%m%d')}-*.jsonl.gz"))
                    try:
                        rows = query_files(stream, files, args.sql or None)
                    except Exception as exc:
                        summary["ok"] = False
                        summary["queries"].append({"stream": stream, "symbol": symbol, "venue": venue_folder, "error": f"{type(exc).__name__}:{exc}"})
                        continue
                    summary["queries"].append({
                        "stream": stream, "symbol": symbol, "venue": venue_folder,
                        "files": len(files), "rows": rows,
                    })
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
