from tools.summarize_variational_orders import extract_orders, summarize_orders


def test_extract_orders_from_command_response() -> None:
    response = {
        "ok": True,
        "result": {
            "orders": {
                "result": [
                    {"rfq_id": "1", "status": "rejected"},
                    "ignored",
                    {"rfq_id": "2", "status": "cleared"},
                ]
            }
        },
    }

    assert extract_orders(response) == [
        {"rfq_id": "1", "status": "rejected"},
        {"rfq_id": "2", "status": "cleared"},
    ]


def test_summarize_orders_groups_by_side_and_clearing_status() -> None:
    orders = [
        {
            "created_at": "2026-06-20T02:10:16Z",
            "instrument": {"underlying": "ETH"},
            "side": "buy",
            "status": "rejected",
            "clearing_status": "rejected_failed_taker_funding",
            "qty": "20",
            "rfq_id": "buy-rejected",
        },
        {
            "created_at": "2026-06-20T00:53:48Z",
            "instrument": {"underlying": "ETH"},
            "side": "sell",
            "status": "cleared",
            "clearing_status": "success_trades_booked_into_pool",
            "qty": "0.01141",
            "price": "1751.28",
            "rfq_id": "sell-cleared",
        },
    ]

    summary = summarize_orders(orders, recent_limit=1)

    assert summary["total_orders"] == 2
    assert summary["by_status"] == {"cleared": 1, "rejected": 1}
    assert summary["by_side_status"] == {"buy|rejected": 1, "sell|cleared": 1}
    assert summary["by_side_clearing_status"] == {
        "buy|rejected_failed_taker_funding": 1,
        "sell|success_trades_booked_into_pool": 1,
    }
    assert summary["by_asset_side_clearing_status"] == {
        "ETH|buy|rejected_failed_taker_funding": 1,
        "ETH|sell|success_trades_booked_into_pool": 1,
    }
    assert summary["recent_orders"] == [
        {
            "created_at": "2026-06-20T02:10:16Z",
            "asset": "ETH",
            "side": "buy",
            "status": "rejected",
            "clearing_status": "rejected_failed_taker_funding",
            "qty": "20",
            "price": None,
            "rfq_id": "buy-rejected",
            "order_id": None,
            "execution_timestamp": None,
            "cancel_reason": None,
            "failed_risk_checks": None,
        }
    ]
