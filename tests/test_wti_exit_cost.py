from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import json
from types import SimpleNamespace
import asyncio

import pytest

from tools.lib.wti_exit_cost import (cost_policy, fingerprint, exit_tail, scoped_cost_policy,
                                    var_fee_capture, lighter_fee_capture)
WALLET = '0x'+'a'*40


def inputs():
    now = datetime.now(timezone.utc)
    at = (now-timedelta(hours=1)).isoformat()
    legs = {}
    for venue, price, side in (("variational", "100", "buy"), ("lighter", "101", "sell")):
        legs[venue] = {"request": {"side": side, "qty": "1", "reference_price": price},
                       "result": {"classification": "filled", "terminal_confirmed": True,
                                  "native_order_id": "11", "native_client_order_id": "22",
                                  "filled_qty": "1", "average_price": price}}
    state = {"status": "open", "pending_action": None,
             "open_position": {"entry_execution_id": "entry", "gradient_pilot_id": "pilot",
                               "market_id": 145, "direction": "long_var_short_lighter", "multiplier": "1",
                               "var_qty": "1", "lighter_qty": "1", "var_entry_price": "100",
                               "lighter_entry_price": "101", "opened_at": at, "session_id": "session-1"},
             "execution_history": [{"action_id": "entry", "kind": "open", "intent_at": at, "legs": legs}]}
    base_action = state["execution_history"][0]
    for index, slip_bps in enumerate(("0.5", "1.0", "2.0")):
        close = deepcopy(base_action)
        close.update(action_id=f"close-{index}", kind="close", outcome="filled", market_id=145,
                     multiplier="1", direction="long_var_short_lighter",
                     completed_at=(now-timedelta(minutes=3-index)).isoformat())
        var_fill = D("100")-D(slip_bps)*D("100.5")/D("10000")
        close["legs"]["variational"]["request"].update(side="sell", reference_price="100")
        close["legs"]["variational"]["result"].update(native_order_id=f"v-{index}", average_price=str(var_fill))
        close["legs"]["lighter"]["request"].update(side="buy", reference_price="101")
        close["legs"]["lighter"]["result"].update(native_order_id=f"l-{index}", average_price="101")
        state["execution_history"].append(close)
    har = {"log": {"entries": [{"startedDateTime": now.isoformat(),
        "request": {"method": "GET", "url": "https://omni.variational.io/api/transfers?type=deposit,withdrawal,fee&limit=20&offset=0&created_at_gte="+(now-timedelta(days=1)).isoformat().replace('+00:00','Z')+"&created_at_lte="+(now+timedelta(days=1)).isoformat().replace('+00:00','Z'),
                    "headers": [{"name": "vr-connected-address", "value": WALLET}]},
        "response": {"status": 200, "content": {"text": json.dumps({"result": [], "pagination": {"object_count": 0, "next_page": None}})}}}]}}
    raw = {"trades_csv": "Market,Trade ID,Size,Price,Fee\nWTI,42,1,101,0\n",
           "trades": {"entry": {"code": 200, "trades": [{"trade_id": 42, "market_id": 145,
                      "ask_account_id": 7, "ask_id": 11, "size": "1", "price": "101"}]}}}
    var = {**var_fee_capture(state, har, expected_address=WALLET, now=now), "raw": har,
           "expected_address": WALLET}
    lighter = {**lighter_fee_capture(state, raw, account_index=7), "raw": raw, "account_index": 7}
    evidence = {"schema_version": 2, "position_fingerprint": fingerprint(state),
                "observed_at": now.isoformat(), "expires_at": (now+timedelta(minutes=15)).isoformat(),
                "fee_evidence_verified": True, "entry_fees_usd": "0", "exit_fee_reserve_usd": "0",
                "known_funding_debits_usd": "999", "funding_excluded_from_exit_cost": True,
                "fee_proofs": {"variational": var, "lighter": lighter}}
    return state, evidence, now


def test_real_costs_do_not_charge_entry_slippage_twice():
    state, evidence, now = inputs()
    value, audit = cost_policy(state, evidence, now=now)
    assert value == D('2')
    assert audit['mode'] == 'real_cost_plus_remaining_reserve'
    assert audit['entry_slippage_deducted_again'] is False
    assert D(audit['remaining_execution_reserve_bps']) == D('2')
    assert audit['exit_tail_samples'] == 3
    assert audit['funding_excluded_from_exit_cost'] is True
    state['execution_history'][0]['legs']['variational']['request']['reference_price'] = '99'
    assert cost_policy(state, evidence, now=now)[0] == value


@pytest.mark.parametrize('change', ['expired', 'future', 'position', 'price', 'fee_total', 'raw', 'missing_fee', 'funding_policy'])
def test_incomplete_or_wrong_evidence_never_reduces_original_budget(change):
    state, e, now = inputs()
    if change == 'expired': now += timedelta(minutes=16)
    if change == 'future': now -= timedelta(seconds=1)
    if change == 'position': state['open_position']['var_qty'] = '2'
    if change == 'price': state['open_position']['var_entry_price'] = '101'
    if change == 'fee_total': e['entry_fees_usd'] = '1'
    if change == 'raw': e['fee_proofs']['variational']['raw']['log']['entries'][0]['response']['status'] = 500
    if change == 'missing_fee': e['fee_proofs']['lighter']['complete'] = False
    if change == 'funding_policy': e['funding_excluded_from_exit_cost'] = False
    value, audit = cost_policy(state, e, now=now)
    assert value >= 10 and audit['mode'] == 'legacy_fallback'


def test_fee_zero_requires_complete_authenticated_window_not_absent_fields():
    state, evidence, now = inputs()
    har = evidence['fee_proofs']['variational']['raw']
    for update in ({'object_count': 1, 'next_page': None}, {'object_count': 0}, {'object_count': 0, 'next_page': {'offset': 20}}):
        broken = deepcopy(har)
        broken['log']['entries'][0]['response']['content']['text'] = json.dumps({'result': [], 'pagination': update})
        with pytest.raises(ValueError):
            var_fee_capture(state, broken, expected_address=WALLET, now=now)
    with pytest.raises(ValueError):
        var_fee_capture(state, har, expected_address='other', now=now)


def test_exit_tail_rejects_other_markets_unpaired_fills_and_future_data():
    state, evidence, now = inputs()
    state['execution_history'] = state['execution_history'][:1]
    close = deepcopy(state['execution_history'][0])
    close.update(action_id='close', kind='close', outcome='filled', market_id=145, multiplier='1',
                 completed_at=(now-timedelta(minutes=1)).isoformat(), direction='long_var_short_lighter')
    close['legs']['variational']['request'].update(side='sell', reference_price='100')
    close['legs']['variational']['result']['average_price'] = '99.9'
    close['legs']['lighter']['request'].update(side='buy', reference_price='101')
    close['legs']['lighter']['result']['average_price'] = '101.2'
    state['execution_history'].append(close)
    assert exit_tail(state, before=now)[0] > 10
    close['market_id'] = 0
    assert exit_tail(state, before=now) == (D('0'), 0)
    close['market_id'] = 145
    close['completed_at'] = (now+timedelta(seconds=1)).isoformat()
    assert exit_tail(state, before=now)[1] == 0


def test_actual_runtime_uses_same_evidence_policy_without_changing_entry_config(tmp_path):
    from tools.wti_live import WTILiveRuntime
    from test_wti_live_strategy import _config
    state, evidence, now = inputs()
    path = tmp_path/'proof.json'; path.write_text(json.dumps(evidence))
    runtime = object.__new__(WTILiveRuntime)
    runtime.args = SimpleNamespace(exit_cost_evidence=path)
    runtime.config = _config(roundtrip_cost_bps=D('10'))
    assert runtime._profit_exit_cost(state) == cost_policy(state, evidence, now=now)[0]
    assert runtime.config.roundtrip_cost_bps == 10
    path.write_text('{}')
    assert runtime._profit_exit_cost(state) >= 10


def test_full_size_refresh_revalidates_cost_before_any_close_submission(tmp_path):
    from test_wti_execution import _executor, _open, _sample, _session
    executor, gateway = _executor(tmp_path)
    _open(executor)
    seen = []
    def cost(state):
        seen.append(state['status'])
        return D('100')
    state = executor.close(sample=_sample(), session=_session(),
        decision={'action': 'close', 'reason': 'net_target_reached'},
        exit_target_bps=D('1'), roundtrip_cost_bps=D('0'), exit_cost_reader=cost,
        now=datetime.now(timezone.utc))
    assert seen == ['open'] and state['pending_action'] is None
    assert state['last_close_check']['applied_exit_cost_bps'] == '100'
    assert len(gateway.requests) == 2


@pytest.mark.parametrize('use_cached_lighter_proof', [False, True])
def test_real_audit_builder_protocol_and_fee_only_calls(monkeypatch, use_cached_lighter_proof):
    import tools.wti_exit_cost_audit as tool
    import lighter.signer_client
    state, evidence, now = inputs()
    state['execution_history'][0]['legs']['lighter']['response_received_at']=(now-timedelta(hours=1)).isoformat()
    raw = evidence['fee_proofs']['lighter']['raw']
    calls = []
    class Forwarder:
        def __init__(self, *a): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
        async def request(self, payload):
            assert len(payload['requestId']) == 32 and payload['type'] == 'VAR_API_ORDERS'
            assert payload['account'] == WALLET
            return {'ok': True, 'result': {'ok': True, 'addressUsed': True,
                'orders': {'result': [{'rfq_id': '22', 'order_id': '11', 'status': 'cleared',
                                       'qty': '1', 'price': '100', 'side': 'buy'}]}}}
    class Signer:
        def __init__(self, **kwargs): pass
        def create_auth_token_with_expiry(self, **kwargs): return 'test-only', None
    async def close(client): pass
    def get(path, params, token):
        calls.append((path, params))
        if path == 'export': return {'data_url': 'https://example.invalid/test-only'}
        if path == 'trades': return raw['trades']['entry']
        if path == 'orderBookDetails': return {'code': 200, 'order_book_details': [
            {'market_id': 145, 'taker_fee': '0', 'is_taker_fee_enabled': True}]}
        raise AssertionError(path)
    monkeypatch.setattr(tool, 'VariationalForwarderClient', Forwarder)
    monkeypatch.setattr(lighter.signer_client, 'SignerClient', Signer)
    monkeypatch.setattr(tool, '_close_client', close)
    monkeypatch.setattr(tool, '_load_env', lambda: (7, 2, 'test-only'))
    monkeypatch.setattr(tool, 'get_json', get)
    monkeypatch.setattr(tool, 'download_export', lambda url: raw['trades_csv'].encode())
    monkeypatch.setattr(tool.requests, 'get', lambda *a, **k: SimpleNamespace(
        status_code=200, text='There are no trading fees on Omni.', content=b'official-policy-test'))
    proof, (applied, audit) = asyncio.run(tool.build(state,
        evidence['fee_proofs']['variational']['raw'],
        cached=evidence if use_cached_lighter_proof else None))
    assert applied < 10 and audit['mode'] == 'real_cost_plus_remaining_reserve'
    assert proof['schema_version'] == 2 and proof['funding_excluded_from_exit_cost'] is True
    assert proof['funding_queries_attempted'] is False
    assert all(path != 'positionFunding' for path, _ in calls)
    if use_cached_lighter_proof:
        assert [x[0] for x in calls] == ['orderBookDetails']
    else:
        assert [x[0] for x in calls] == ['export', 'trades', 'orderBookDetails']
        assert calls[0][1]['type'] == 'trade'


def test_funding_amount_and_legacy_schema_cannot_change_the_dynamic_exit_reserve():
    state, evidence, now = inputs()
    baseline = cost_policy(state, evidence, now=now)[0]
    evidence['known_funding_debits_usd'] = '0'
    assert cost_policy(state, evidence, now=now)[0] == baseline
    evidence['schema_version'] = 1
    assert cost_policy(state, evidence, now=now)[0] == D('10')


def test_tier_scoped_exit_cost_excludes_funding_and_keeps_fill_reserve():
    state, evidence, now = inputs()
    state['open_position']['layers'] = [{
        'layer_id': 'tier-1-child-1', 'entry_execution_id': 'entry', 'tier': 1,
        'var_qty': '1', 'lighter_qty': '1', 'var_entry_price': '100',
        'lighter_entry_price': '101', 'opened_at': state['open_position']['opened_at'],
        'var_entry_fee_usd': '0', 'lighter_entry_fee_usd': '0',
    }]
    evidence['position_fingerprint'] = fingerprint(state)
    for venue in ('variational', 'lighter'):
        evidence['fee_proofs'][venue]['position_fingerprint'] = evidence['position_fingerprint']
    value = scoped_cost_policy(state, evidence, ['tier-1-child-1'], now=now)
    evidence['known_funding_debits_usd'] = '0'
    assert value == D('2')
    assert scoped_cost_policy(state, evidence, ['tier-1-child-1'], now=now) == value


def test_dynamic_reserve_requires_three_confirmed_paired_wti_closes():
    state, evidence, now = inputs()
    state['execution_history'] = state['execution_history'][:2]
    assert exit_tail(state, before=now)[1] == 1
    value, audit = cost_policy(state, evidence, now=now)
    assert value == D('10')
    assert audit['reason'] == 'exit_execution_evidence_insufficient'


def test_incident_exit_is_excluded_from_execution_calibration_only():
    state, _, now = inputs()
    assert exit_tail(state, before=now + timedelta(seconds=1))[1] == 3
    state['strategy_evidence_excluded_action_ids'] = ['close-2']
    assert exit_tail(state, before=now + timedelta(seconds=1))[1] == 2
    state.pop('strategy_evidence_excluded_action_ids')
    state['execution_history'][-1]['strategy_evidence_excluded'] = True
    assert exit_tail(state, before=now + timedelta(seconds=1))[1] == 2
