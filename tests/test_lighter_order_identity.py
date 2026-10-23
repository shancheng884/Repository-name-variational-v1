from __future__ import annotations

import pytest

from tools.lib.lighter_order_identity import (
    MAX_LIGHTER_CLIENT_ORDER_INDEX,
    legacy_lighter_client_order_index,
    legacy_lighter_index_was_unsendable,
    lighter_client_order_index,
    lighter_order_identity,
    request_lighter_client_order_index,
)


def test_new_indexes_fit_native_range_and_persisted_legacy_identity_is_not_rehashed():
    cid = "legacy-live-action-lighter"
    old_index = legacy_lighter_client_order_index(cid)
    assert old_index > MAX_LIGHTER_CLIENT_ORDER_INDEX
    assert 1 <= lighter_client_order_index(cid) <= MAX_LIGHTER_CLIENT_ORDER_INDEX
    assert request_lighter_client_order_index(cid, {}) == old_index
    identity = lighter_order_identity(cid)
    assert request_lighter_client_order_index(cid, identity) == lighter_client_order_index(cid)
    assert request_lighter_client_order_index(cid, {"native_client_order_id": "123"}) == 123


@pytest.mark.parametrize("extra", [
    {"native_client_order_id": "123"}, {"native_order_id": "order-accepted"},
    {"transaction_hash": "transaction-accepted"}, {"lighter_client_index_scheme": "unknown"},
])
def test_legacy_range_proof_requires_no_conflicting_native_or_transaction_evidence(extra):
    cid = "legacy-live-action-lighter"
    request = {"venue": "lighter", "client_order_id": cid}
    assert legacy_lighter_index_was_unsendable(cid, request)
    assert not legacy_lighter_index_was_unsendable(cid, {**request, **extra})


@pytest.mark.parametrize("native", ["-1", "1.0", True, str(1 << 48)])
def test_invalid_saved_native_identity_is_never_replaced_by_another_hash(native):
    with pytest.raises(ValueError, match="lighter_native_client_index"):
        request_lighter_client_order_index("action-lighter", {"native_client_order_id": native})


def test_real_sdk_rejects_live_incident_index_before_send_and_accepts_48_bit_boundary():
    sdk = pytest.importorskip("lighter.signer_client")
    signer = sdk.get_signer()
    private_key, _, error = sdk.create_api_key()
    assert error is None
    # Synthetic local identity, no real credentials and no check_client/send_tx call.
    error = sdk.decode_and_free(signer.CreateClient(
        b"http://127.0.0.1:1", private_key.encode(), 304, 1, 10,
    ))
    assert error is None
    for index, rejected in (
        (797660596437583605, True),
        (MAX_LIGHTER_CLIENT_ORDER_INDEX, False),
        (MAX_LIGHTER_CLIENT_ORDER_INDEX + 1, True),
        (lighter_client_order_index("legacy-live-action-lighter"), False),
    ):
        result = signer.SignCreateOrder(145, index, 198, 100000, 1, 1, 0, 0, 0, 0, 1, 1, 10)
        error = sdk.decode_and_free(result.err)
        for field in ("txInfo", "txHash", "messageToSign"):
            sdk.decode_and_free(getattr(result, field))
        if rejected:
            assert error == "ClientOrderIndex should not be larger than 281474976710655"
        else:
            assert error is None
