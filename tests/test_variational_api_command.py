import argparse
from decimal import Decimal

from tools.variational_api_command import build_payload


def test_build_orders_payload() -> None:
    args = argparse.Namespace(
        action="orders",
        account=None,
        status="pending,canceled,cleared,rejected",
        instrument="P-ETH-USDC-3600",
        created_at_gte="2026-06-18T00:00:00.000Z",
        limit=50,
        offset=0,
        order_by="created_at",
        order="desc",
    )

    payload = build_payload(args, "ETH", Decimal("0"))

    assert payload == {
        "type": "VAR_API_ORDERS",
        "account": None,
        "status": "pending,canceled,cleared,rejected",
        "instrument": "P-ETH-USDC-3600",
        "createdAtGte": "2026-06-18T00:00:00.000Z",
        "limit": 50,
        "offset": 0,
        "orderBy": "created_at",
        "order": "desc",
    }
