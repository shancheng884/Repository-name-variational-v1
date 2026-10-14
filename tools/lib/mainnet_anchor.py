from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from tools.lib.basis_store import read_basis_samples


ANCHOR_FIELDS = frozenset({
    "logged_at", "asset", "venue", "sample_kind", "sample_quality",
    "sample_quality_version", "sample_pair_valid", "quote_size_mode",
    "quote_source", "quote_received_at", "source_quote_timestamp",
    "reference_price", "var_bid", "var_ask", "var_quote_age_seconds",
    "lighter_book_age_seconds", "long_edge_bps", "short_edge_bps",
})


def load_mainnet_anchor_rows(output_dir: Path, *, asset: str, now=None):
    observed = now or datetime.now(timezone.utc)
    rows = []
    for root in (output_dir / "basis_samples",
                 output_dir / "mainnet_anchor" / "basis_samples"):
        rows.extend(read_basis_samples(
            root, limit=90000, asset_filter=asset,
            sample_kind_filter="baseline", sample_quality_filter="valid",
            since_utc=observed - timedelta(days=30), until_utc=observed,
            row_fields=ANCHOR_FIELDS,
        ))
    return sorted(rows, key=lambda row: str(row.get("logged_at", "")))
