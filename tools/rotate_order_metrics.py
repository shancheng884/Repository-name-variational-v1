#!/usr/bin/env python3
"""Rotate a growing append-only jsonl log without stopping its writer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.log_rotation import (  # noqa: E402
    DEFAULT_KEEP_ARCHIVES,
    DEFAULT_MIN_BYTES,
    rotate_jsonl,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        default=str(ROOT / "log" / "order_metrics.jsonl"),
        help="jsonl file to rotate",
    )
    parser.add_argument(
        "--min-size-mb",
        type=float,
        default=DEFAULT_MIN_BYTES / 1024 / 1024,
        help="only rotate once the file is at least this large",
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=DEFAULT_KEEP_ARCHIVES,
        help="number of archives to keep after pruning",
    )
    parser.add_argument("--dry-run", action="store_true", help="report without rotating")
    args = parser.parse_args()

    path = Path(args.path)
    min_bytes = int(args.min_size_mb * 1024 * 1024)
    if args.dry_run:
        result = _dry_run(path, min_bytes, args.keep)
    else:
        result = rotate_jsonl(path, min_bytes=min_bytes, keep=args.keep)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


def _dry_run(path: Path, min_bytes: int, keep: int) -> dict:
    from tools.lib.log_rotation import existing_archives, plan_rotation

    plan = plan_rotation(path, min_bytes=min_bytes)
    return {**plan, "rotated": False, "dry_run": True, "archives_kept": len(existing_archives(path)), "keep": keep}


if __name__ == "__main__":
    raise SystemExit(main())
