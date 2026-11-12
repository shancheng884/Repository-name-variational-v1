from __future__ import annotations

from pathlib import Path
import re

FORBIDDEN_CALLS = (
    "create_order", "place_order", "submit_order", "send_tx",
    "send_variational_place_order", "place_lighter_order",
    "submit_live_inventory_exit_pair",
)


def assert_no_order_submission_source(package_root: Path | None = None) -> list[str]:
    """Fail if capture code contains a known order-submission call."""
    root = package_root or Path(__file__).resolve().parent
    found = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for name in FORBIDDEN_CALLS:
            if re.search(rf"\b{re.escape(name)}\s*\(", text):
                found.append(f"{path.name}:{name}")
    return found
