"""Versioned Lighter client indexes; persisted orders retain their original identity."""

from __future__ import annotations

import hashlib
from typing import Any


MAX_LIGHTER_CLIENT_ORDER_INDEX = (1 << 48) - 1
LIGHTER_CLIENT_INDEX_SCHEME = "sha256_48_v1"
LEGACY_INDEX_REJECTION = "lighter_legacy_client_order_index_out_of_range"


def legacy_lighter_client_order_index(client_order_id: str) -> int:
    digest = hashlib.sha256(client_order_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1) or 1


def lighter_client_order_index(client_order_id: str) -> int:
    return legacy_lighter_client_order_index(client_order_id) & MAX_LIGHTER_CLIENT_ORDER_INDEX or 1


def lighter_order_identity(client_order_id: str) -> dict[str, str]:
    return {
        "native_client_order_id": str(lighter_client_order_index(client_order_id)),
        "lighter_client_index_scheme": LIGHTER_CLIENT_INDEX_SCHEME,
    }


def request_lighter_client_order_index(
    client_order_id: str, request: dict[str, Any], *, submitting: bool = False,
) -> int:
    scheme = request.get("lighter_client_index_scheme")
    native = request.get("native_client_order_id")
    if scheme is not None and scheme != LIGHTER_CLIENT_INDEX_SCHEME:
        raise ValueError("lighter_client_index_scheme_unknown")
    if native not in (None, ""):
        # Do not reinterpret a saved native identity after changing the hash mapping.
        text = str(native)
        if not text.isascii() or not text.isdecimal():
            raise ValueError("lighter_native_client_index_invalid")
        index = int(text)
        if not 1 <= index <= MAX_LIGHTER_CLIENT_ORDER_INDEX:
            raise ValueError("lighter_native_client_index_out_of_range")
        if scheme is not None and index != lighter_client_order_index(client_order_id):
            raise ValueError("lighter_native_client_index_mismatch")
        return index
    if scheme is not None:
        raise ValueError("lighter_native_client_index_missing")
    if submitting:
        return lighter_client_order_index(client_order_id)
    return legacy_lighter_client_order_index(client_order_id)


def legacy_lighter_index_was_unsendable(client_order_id: str, request: dict[str, Any]) -> bool:
    """The old SDK rejected this 63-bit value locally, before send_tx was called."""
    return (
        request.get("venue") == "lighter"
        and request.get("client_order_id") == client_order_id
        and all(request.get(key) in (None, "") for key in (
            "lighter_client_index_scheme", "native_client_order_id", "native_order_id",
            "transaction_hash",
        ))
        and legacy_lighter_client_order_index(client_order_id) > MAX_LIGHTER_CLIENT_ORDER_INDEX
    )
