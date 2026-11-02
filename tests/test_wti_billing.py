from copy import deepcopy
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import hashlib
import io
import json
from types import SimpleNamespace

import pytest

from tools.lib.wti_billing import lighter_billing, variational_billing, net_audit, gross_from_fills, round_context, merge_cash_exports
from tools.wti_billing_audit import download_export, build_audit, main


def csv_bytes(rows, fields=None):
    f = io.StringIO()
    writer = csv.DictWriter(f, fieldnames=fields or list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return f.getvalue().encode()


@pytest.fixture
def example():
    start = datetime(2026, 10, 26, 5, 0, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=2)
    actions = []
    for key, kind, at, prices in [('entry', 'open', start, ('100', '101')),
                                  ('exit', 'close', end, ('100.2', '101.1'))]:
        legs = {}
        for venue, price in zip(('variational', 'lighter'), prices):
            side = 'buy' if (venue == 'variational') == (kind == 'open') else 'sell'
            legs[venue] = {'request': {'qty': '0.2', 'side': side, 'submitted_at': at.isoformat()},
                           'result': {'classification': 'filled', 'terminal_confirmed': True,
                                      'filled_qty': '0.2', 'average_price': price,
                                      'native_order_id': str(10 if kind == 'open' else 11)}}
        actions.append({'action_id': key, 'kind': kind, 'intent_at': at.isoformat(), 'legs': legs})
    state = {'asset': 'WTI', 'gradient_pilot': {'config': {'roundtrip_cost_bps': '10'}},
             'last_closed_position': {'market_id': 145, 'multiplier': '1',
                'direction': 'long_var_short_lighter', 'opened_at': start.isoformat(),
                'closed_at': end.isoformat(), 'entry_execution_id': 'entry', 'exit_execution_id': 'exit',
                'actual_gross_pnl_usd': '0.02'}, 'execution_history': actions}
    trade_rows, raw = [], {}
    for i, action in enumerate(actions):
        at = start if i == 0 else end
        price = action['legs']['lighter']['result']['average_price']
        trade_rows.append({'Market': 'WTI', 'Side': 'Open Short' if i == 0 else 'Close Short',
            'Date': at.strftime('%Y-%m-%d %H:%M:%S'), 'Size': '0.2', 'Price': price,
            'Fee': '0.0000', 'Trade ID': str(100+i), 'Role': 'Taker', 'Type': 'trade'})
        raw[action['action_id']] = {'code': 200, 'trades': [{
            'trade_id': 100+i, 'market_id': 145, 'ask_id': 10 if i == 0 else 99,
            'bid_id': 99 if i == 0 else 11, 'ask_account_id': 7 if i == 0 else 88,
            'bid_account_id': 88 if i == 0 else 7, 'is_maker_ask': bool(i),
            'timestamp': int(at.timestamp()*1000), 'size': '0.2', 'price': price}]}
    fat = start.replace(hour=6, minute=0, second=0)
    funds = [{'Market': 'WTI', 'Side': 'short', 'Date': fat.strftime('%Y-%m-%d %H:%M:%S'),
              'Position Size': '0.2', 'Payment': '0.00001'}]
    sources = {'account_index': 7, 'trades_csv': csv_bytes(trade_rows), 'funding_csv': csv_bytes(funds),
               'raw_trades': raw, 'raw_funding': {'code': 200, 'position_fundings': [{
                   'funding_id': 200, 'market_id': 145, 'timestamp': int(fat.timestamp()),
                   'position_side': 'short', 'position_size': '0.2', 'change': '0.00001'}]}}
    var_trades = [{'id': action['action_id'], 'created_at': action['intent_at'],
        'side': action['legs']['variational']['request']['side'], 'underlying': 'USOILP',
        'instrument_type': 'swap', 'price': action['legs']['variational']['result']['average_price'],
        'qty': '0.2', 'status': 'confirmed'} for action in actions]
    cash = [{'id': 'pnl', 'created_at': end.isoformat(), 'qty': '0.04', 'asset': 'USDC',
             'transfer_type': 'realized_pnl', 'status': 'confirmed', 'underlying': 'USOILP',
             'instrument_type': 'swap', 'fee_type': ''},
            {'id': 'fee', 'created_at': end.isoformat(), 'qty': '-0.002', 'asset': 'USDC',
             'transfer_type': 'fee', 'status': 'confirmed', 'underlying': 'USOILP',
             'instrument_type': 'swap', 'fee_type': 'trade'},
            {'id': 'fund', 'created_at': fat.isoformat(), 'qty': '0.00002', 'asset': 'USDC',
             'transfer_type': 'funding', 'status': 'confirmed', 'underlying': 'USOILP',
             'instrument_type': 'swap', 'fee_type': ''}]
    vt, vc = csv_bytes(var_trades), csv_bytes(cash)
    manifest = {'venue': 'variational_omni', 'account_scope': 'test-account', 'timezone': 'UTC',
                'settlement_asset': 'USDC', 'cashflow_sign_convention': 'signed_account_delta',
                'complete': True, 'all_pages_exported': True,
                'start_at': (start-timedelta(seconds=5)).isoformat(),
                'end_at': (end+timedelta(seconds=5)).isoformat(),
                'trades_sha256': hashlib.sha256(vt).hexdigest(),
                'transfers_sha256': hashlib.sha256(vc).hexdigest()}
    return state, sources, vt, vc, manifest


def check_lighter(state, sources):
    return lighter_billing(state, sources['trades_csv'], sources['funding_csv'],
                           sources['raw_funding'], sources['raw_trades'], account_index=7)


def test_explicit_csv_zero_fee_and_signed_funding_are_real_evidence(example):
    state, sources, *_ = example
    before = deepcopy(state)
    result = check_lighter(state, sources)
    assert result['status'] == 'verified' and D(result['fee_usdc']) == 0
    assert result['funding_usdc'] == '0.00001' and result['funding_ids'] == [200]
    assert len(result['trade_proofs']) == 2 and state == before


@pytest.mark.parametrize('bad', ['account', 'order', 'quantity', 'role', 'missing_fee', 'nonzero_unit', 'funding_qty', 'funding_amount', 'duplicate_funding', 'cursor'])
def test_wrong_or_incomplete_lighter_evidence_does_not_confirm(example, bad):
    state, sources, *_ = example
    if bad == 'account': sources['account_index'] = 88
    if bad == 'order': sources['raw_trades']['entry']['trades'][0]['ask_id'] = 999
    if bad == 'quantity': sources['raw_trades']['entry']['trades'][0]['size'] = '0.1'
    if bad == 'role': sources['raw_trades']['entry']['trades'][0]['is_maker_ask'] = True
    if bad == 'missing_fee': sources['trades_csv'] = sources['trades_csv'].replace(b'Fee,', b'Wrong,')
    if bad == 'nonzero_unit': sources['trades_csv'] = sources['trades_csv'].replace(b'0.0000', b'0.01')
    if bad == 'funding_qty': sources['raw_funding']['position_fundings'][0]['position_size'] = '0.3'
    if bad == 'funding_amount': sources['raw_funding']['position_fundings'][0]['change'] = '-0.1'
    if bad == 'duplicate_funding': sources['raw_funding']['position_fundings'] *= 2
    if bad == 'cursor': sources['raw_funding']['next_cursor'] = 'more'
    result = build_audit(state, sources)
    assert result['lighter']['status'] == 'unverified'
    assert result['actual_net_pnl_usdc'] is None


def test_complete_variational_ledger_and_fill_pnl_reconcile(example):
    state, sources, vt, vc, manifest = example
    report = build_audit(state, sources, var_trades=vt, var_transfers=vc, var_manifest=manifest)
    assert report['status'] == 'verified_from_exports_and_final_fills'
    assert D(report['actual_net_pnl_usdc']) == D('0.01803')
    assert report['slippage_already_in_gross_not_subtracted_again']
    assert not report['state_modified'] and not report['orders_attempted'] and not report['cost_budget_modified']


def test_official_csv_usoil_swap_alias_is_not_a_perpetual_alias(example):
    state, _, vt, vc, manifest = example
    vt = vt.replace(b'USOILP', b'USOIL')
    vc = vc.replace(b'USOILP', b'USOIL')
    manifest.update(trades_sha256=hashlib.sha256(vt).hexdigest(),
                    transfers_sha256=hashlib.sha256(vc).hexdigest())
    assert variational_billing(state, vt, vc, manifest)['status'].startswith('verified')
    vt = vt.replace(b'swap', b'perpetual_future')
    manifest['trades_sha256'] = hashlib.sha256(vt).hexdigest()
    with pytest.raises(ValueError, match='trade_match'):
        variational_billing(state, vt, vc, manifest)


def test_multiple_cash_exports_deduplicate_identical_ids_but_reject_conflicts(example):
    _, _, _, vc, _ = example
    from tools.lib.wti_billing import csv_rows
    rows = csv_rows(vc, {'id'})
    first, rest = csv_bytes(rows[:2]), csv_bytes(rows[1:])
    merged = merge_cash_exports([first, rest])
    assert len(csv_rows(merged, {'id'})) == 3
    changed = {**rows[1], 'qty': '-999'}
    with pytest.raises(ValueError, match='conflicting'):
        merge_cash_exports([first, csv_bytes([changed])])
    assert len(csv_rows(merge_cash_exports([vc, csv_bytes([], list(rows[0]))]), {'id'})) == 3
    with pytest.raises(ValueError, match='duplicate_cash_id'):
        merge_cash_exports([csv_bytes([rows[0], rows[0]])])


def test_round_window_includes_leg_submitted_before_hedge_completion(example):
    state, sources, vt, vc, manifest = example
    state['last_closed_position']['opened_at'] = '2026-10-26T05:30:00+00:00'
    report = build_audit(state, sources, var_trades=vt, var_transfers=vc, var_manifest=manifest)
    assert report['status'] == 'verified_from_exports_and_final_fills'
    assert round_context(state)[2].isoformat() == '2026-10-26T04:59:56+00:00'


def test_missing_maker_flag_cannot_prove_taker_fee(example):
    state, sources, *_ = example
    del sources['raw_trades']['entry']['trades'][0]['is_maker_ask']
    assert build_audit(state, sources)['lighter']['status'] == 'unverified'


def test_offline_multiple_csv_cli_binds_each_source_and_archives_provenance(example, tmp_path, capsys):
    state, sources, vt, vc, manifest = example
    from tools.lib.wti_billing import csv_rows
    rows = csv_rows(vc, {'id'})
    cash_sources = [csv_bytes(rows[:1]), csv_bytes(rows[1:])]
    manifest['cash_export_source_hashes'] = [hashlib.sha256(x).hexdigest() for x in cash_sources]
    manifest['transfers_sha256'] = hashlib.sha256(merge_cash_exports(cash_sources)).hexdigest()
    sp, bp, tp, mp = (tmp_path / x for x in ('state.json', 'bundle.json', 'trade.csv', 'coverage.json'))
    sp.write_text(json.dumps(state)); tp.write_bytes(vt); mp.write_text(json.dumps(manifest))
    bp.write_text(json.dumps({**sources, 'trades_csv': sources['trades_csv'].decode(),
                             'funding_csv': sources['funding_csv'].decode()}))
    paths = [tmp_path / f'cash-{i}.csv' for i in range(2)]
    for path, raw in zip(paths, cash_sources):
        path.write_bytes(raw)
    args = ['--state-path', str(sp), '--lighter-bundle', str(bp),
            '--variational-trades-csv', str(tp), '--variational-transfers-csv', *map(str, paths),
            '--variational-coverage-json', str(mp), '--output-dir', str(tmp_path / 'report')]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'verified_from_exports_and_final_fills'
    assert json.loads((tmp_path / 'report/variational-sources.json').read_text())['coverage'] == manifest
    manifest['cash_export_source_hashes'][0] = 'wrong'; mp.write_text(json.dumps(manifest))
    assert main(args) == 2
    assert json.loads(capsys.readouterr().out)['status'] == 'unavailable'


@pytest.mark.parametrize('field,value', [('complete', False), ('all_pages_exported', False),
    ('trades_sha256', 'wrong'), ('timezone', 'Asia/Shanghai'), ('account_scope', ''),
    ('cashflow_sign_convention', 'absolute')])
def test_variational_coverage_requires_explicit_scope_hashes_and_signs(example, field, value):
    state, sources, vt, vc, manifest = example
    manifest[field] = value
    result = build_audit(state, sources, var_trades=vt, var_transfers=vc, var_manifest=manifest)
    assert result['variational']['status'] == 'unverified' and result['actual_net_pnl_usdc'] is None


def test_missing_exports_do_not_become_zero_fee_or_funding(example):
    state, sources, *_ = example
    report = build_audit(state, sources)
    assert report['lighter']['status'] == 'verified'
    assert report['variational']['status'] == 'needs_exports'
    assert report['actual_net_pnl_usdc'] is None


def test_failed_fee_export_preserves_confirmed_components_but_never_certifies_net(example):
    state, sources, vt, vc, manifest = example
    from tools.lib.wti_billing import csv_rows
    vc = csv_bytes([r for r in csv_rows(vc, {'id'}) if r['transfer_type'] != 'fee'])
    manifest.update(complete=False, covered_cashflow_types=['funding', 'realized_pnl'],
                    transfers_sha256=hashlib.sha256(vc).hexdigest(),
                    export_failures=['account_transfers_export_failed'])
    result = build_audit(state, sources, var_trades=vt, var_transfers=vc, var_manifest=manifest)
    assert result['actual_net_pnl_usdc'] is None and result['status'] == 'partial_evidence'
    assert result['variational']['fee_usdc'] is None
    assert result['variational']['realized_pnl_usdc'] == '0.04'
    assert result['blockers'] == ['variational_fee_export_scope_incomplete']
    assert D(result['net_before_unverified_variational_fees_usdc']) == D('0.02003')
    manifest['complete'] = True
    assert build_audit(state, sources, var_trades=vt, var_transfers=vc,
                       var_manifest=manifest)['variational']['status'] == 'unverified'


def test_variational_zero_and_deposit_fee_exclusion_require_complete_window(example):
    state, sources, vt, vc, manifest = example
    from tools.lib.wti_billing import csv_rows
    cash = csv_rows(vc, {'id'})[:1]
    cash.append({**cash[0], 'id': 'deposit-fee', 'qty': '-0.1', 'underlying': '',
                 'instrument_type': '', 'transfer_type': 'fee', 'fee_type': 'deposit'})
    vc = csv_bytes(cash)
    manifest['transfers_sha256'] = hashlib.sha256(vc).hexdigest()
    var = variational_billing(state, vt, vc, manifest)
    assert var['fee_usdc'] == '0' and var['funding_usdc'] == '0'
    assert var['excluded_deposit_withdrawal_fee_ids'] == ['deposit-fee']


def test_conflicting_history_or_unbalanced_fills_fail_closed(example):
    state, *_ = example
    changed = deepcopy(state['execution_history'][0])
    changed['legs']['lighter']['result']['average_price'] = '999'
    state['execution_history'].append(changed)
    with pytest.raises(ValueError, match='conflicting'):
        gross_from_fills(state)


def test_download_retries_only_not_ready_export_and_never_forwards_auth():
    calls, sleeps = [], []
    responses = iter([SimpleNamespace(status_code=404, content=b'<Error>NoSuchKey</Error>'),
                      SimpleNamespace(status_code=200, content=b'Fee\n0\n')])
    def get(url, **kwargs):
        calls.append(kwargs)
        return next(responses)
    data = download_export('https://export.s3.amazonaws.com/signed?secret=not-logged', getter=get, sleeper=sleeps.append)
    assert data == b'Fee\n0\n' and sleeps == [2]
    assert all('headers' not in x and x['allow_redirects'] is False for x in calls)
    with pytest.raises(ValueError, match='host_not_allowed'):
        download_export('https://attacker.example/x', getter=get)
    with pytest.raises(ValueError, match='http_403'):
        download_export('https://export.s3.amazonaws.com/x', getter=lambda *a, **k: SimpleNamespace(status_code=403, content=b'bad'))
    with pytest.raises(ValueError, match='not_csv'):
        download_export('https://export.s3.amazonaws.com/x', getter=lambda *a, **k: SimpleNamespace(status_code=200, content=b'<?xml error>'))


def test_offline_cli_does_not_touch_live_state_or_access_network(example, tmp_path, monkeypatch, capsys):
    state, sources, *_ = example
    sp = tmp_path / 'state.json'
    sp.write_text(json.dumps(state))
    bundle = {**sources, 'trades_csv': sources['trades_csv'].decode(), 'funding_csv': sources['funding_csv'].decode()}
    bp = tmp_path / 'bundle.json'
    bp.write_text(json.dumps(bundle))
    before = sp.read_bytes()
    monkeypatch.setattr('requests.get', lambda *a, **k: (_ for _ in ()).throw(AssertionError('network forbidden')))
    assert main(['--state-path', str(sp), '--lighter-bundle', str(bp), '--output-dir', str(tmp_path / 'audit')]) == 2
    report = json.loads(capsys.readouterr().out)
    assert report['lighter']['status'] == 'verified'
    assert sp.read_bytes() == before
    assert json.load(open(tmp_path / 'audit' / 'audit.json'))['cost_budget_modified'] is False


@pytest.fixture
def raw_example(example):
    from urllib.parse import urlencode
    from tools.lib.wti_billing import csv_rows
    state, sources, vt, vc, manifest = deepcopy(example)
    wallet = '0x' + '1' * 40
    company = '7f696194-65db-484d-ade2-334c3ae159d1'
    for action in state['execution_history']:
        leg = action['legs']['variational']
        leg['request']['native_client_order_id'] = 'rfq-' + action['action_id']
        leg['result']['native_client_order_id'] = 'rfq-' + action['action_id']
    cash = [r for r in csv_rows(vc, {'id'}) if r['transfer_type'] != 'fee']
    vc = csv_bytes(cash)
    manifest.update(complete=False, covered_cashflow_types=['funding', 'realized_pnl'],
                    transfers_sha256=hashlib.sha256(vc).hexdigest(), export_failures=['empty_export'])
    groups = [(['deposit', 'withdrawal', 'fee'], []), (['funding'], [cash[1]]),
              (['realized_pnl'], [cash[0]])]
    entries = []
    for kinds, rows in groups:
        raw_rows = []
        for row in rows:
            raw_rows.append({**row, 'company': company, 'rfq_id': 'rfq-exit',
                'reference_instrument': {'underlying': 'USOILP', 'instrument_type': 'swap',
                                         'settlement_asset': 'USDC'}})
        query = {'order_by': 'created_at', 'order': 'desc', 'limit': '20', 'offset': '0',
                 'created_at_gte': manifest['start_at'], 'created_at_lte': manifest['end_at'],
                 'type': ','.join(kinds)}
        entries.append({'startedDateTime': '2026-10-29T04:00:00Z',
            'request': {'url': 'https://omni.variational.io/api/transfers?' + urlencode(query),
                        'method': 'GET', 'headers': [{'name': 'vr-connected-address', 'value': wallet}]},
            'response': {'status': 200, 'headers': [], 'content': {'text': json.dumps({
                'pagination': {'next_page': None, 'object_count': len(rows)}, 'result': raw_rows})}}})
    har = {'log': {'entries': entries}}
    manifest['raw_ledger_capture'] = {'wallet_address': wallet, 'company_id': company,
                                     'account_scope_confirmed': True}
    return state, sources, vt, vc, manifest, har


def raw_report(raw_example):
    state, sources, vt, vc, manifest, har = raw_example
    content = json.dumps(har).encode()
    manifest['raw_ledger_capture']['sha256'] = hashlib.sha256(content).hexdigest()
    return build_audit(state, sources, var_trades=vt, var_transfers=vc,
                       var_manifest=manifest, var_ledger_har=content)


def mutate_response(har, index, change):
    value = json.loads(har['log']['entries'][index]['response']['content']['text'])
    change(value)
    har['log']['entries'][index]['response']['content']['text'] = json.dumps(value)


def test_raw_zero_fees_require_wallet_company_rfq_and_complete_ledger(raw_example):
    before = deepcopy(raw_example[0])
    report = raw_report(raw_example)
    assert report['status'] == 'verified_from_exports_and_final_fills'
    assert report['variational']['status'] == 'verified_from_raw_ledger_and_exports'
    assert report['variational']['fee_usdc'] == '0'
    assert D(report['actual_net_pnl_usdc']) == D('0.02003')
    assert report['variational']['export_failures'] == ['empty_export']
    assert raw_example[0] == before


@pytest.mark.parametrize('bad', ['wallet', 'company', 'rfq', 'pagination', 'count', 'missing_page_field',
    'missing_category', 'hidden_filter', 'window', 'captured_before_close', 'capture_duration',
    'duplicate_id', 'cash_amount', 'currency', 'instrument', 'http', 'credentials', 'missing_rfq',
    'missing_attestation', 'wrong_hash', 'category', 'body_shape', 'origin'])
def test_raw_incomplete_or_wrong_ledger_never_certifies_net(raw_example, bad):
    state, sources, vt, vc, manifest, har = raw_example
    from urllib.parse import urlencode, urlsplit, parse_qs
    entries = har['log']['entries']
    if bad == 'wallet': entries[0]['request']['headers'][0]['value'] = '0x' + '2' * 40
    if bad == 'company': mutate_response(har, 2, lambda d: d['result'][0].update(company='wrong'))
    if bad == 'rfq': mutate_response(har, 2, lambda d: d['result'][0].update(rfq_id='wrong'))
    if bad == 'pagination': mutate_response(har, 0, lambda d: d['pagination'].update(next_page={'offset': '20'}))
    if bad == 'count': mutate_response(har, 2, lambda d: d['pagination'].update(object_count=2))
    if bad == 'missing_page_field': mutate_response(har, 0, lambda d: d['pagination'].pop('next_page'))
    if bad == 'missing_category': entries.pop(0)
    if bad in {'hidden_filter', 'window'}:
        query = parse_qs(urlsplit(entries[0]['request']['url']).query)
        query['asset' if bad == 'hidden_filter' else 'created_at_gte'] = ['ETH' if bad == 'hidden_filter' else '2026-10-27T00:00:00Z']
        entries[0]['request']['url'] = 'https://omni.variational.io/api/transfers?' + urlencode(query, doseq=True)
    if bad == 'captured_before_close': entries[0]['startedDateTime'] = '2026-10-26T05:30:00Z'
    if bad == 'capture_duration': entries[0]['startedDateTime'] = '2026-10-29T02:00:00Z'
    if bad == 'duplicate_id': mutate_response(har, 1, lambda d: d['result'][0].update(id='pnl'))
    if bad == 'cash_amount': mutate_response(har, 2, lambda d: d['result'][0].update(qty='0.05'))
    if bad == 'currency': mutate_response(har, 2, lambda d: d['result'][0].update(asset='ETH'))
    if bad == 'instrument': mutate_response(har, 2, lambda d: d['result'][0]['reference_instrument'].update(instrument_type='perpetual_future'))
    if bad == 'http': entries[0]['response']['status'] = 503
    if bad == 'credentials': entries[0]['request']['headers'].append({'name': 'Authorization', 'value': 'not-a-real-secret'})
    if bad == 'missing_rfq':
        for part in ('request', 'result'): state['execution_history'][1]['legs']['variational'][part].pop('native_client_order_id')
    if bad == 'missing_attestation': manifest['raw_ledger_capture']['account_scope_confirmed'] = False
    if bad == 'category': mutate_response(har, 1, lambda d: d['result'][0].update(transfer_type='realized_pnl'))
    if bad == 'body_shape': entries[0]['response']['content']['text'] = '[]'
    if bad == 'origin': entries[0]['request']['url'] = entries[0]['request']['url'].replace('omni.variational.io', 'example.invalid')
    if bad == 'wrong_hash':
        content = json.dumps(har).encode()
        manifest['raw_ledger_capture']['sha256'] = 'wrong'
        report = build_audit(state, sources, var_trades=vt, var_transfers=vc,
                             var_manifest=manifest, var_ledger_har=content)
    else:
        report = raw_report(raw_example)
    assert report['actual_net_pnl_usdc'] is None
    assert report['variational']['status'] == 'unverified'


def test_empty_export_failure_alone_is_not_zero_fee_evidence(raw_example):
    har = raw_example[-1]
    failure = deepcopy(har['log']['entries'][0])
    failure['request']['url'] = 'https://omni.variational.io/api/exports/job'
    failure['response']['content']['text'] = json.dumps({'id': 'job', 'status': 'failed', 'error': 'empty_export'})
    har['log']['entries'] = [failure]
    assert raw_report(raw_example)['actual_net_pnl_usdc'] is None


def test_identical_queries_deduplicate_but_conflicting_queries_fail(raw_example):
    har = raw_example[-1]
    har['log']['entries'].append(deepcopy(har['log']['entries'][2]))
    assert D(raw_report(raw_example)['actual_net_pnl_usdc']) == D('0.02003')
    mutate_response(har, 3, lambda d: d['result'][0].update(qty='0.5'))
    assert raw_report(raw_example)['actual_net_pnl_usdc'] is None


def test_actual_nonzero_raw_fee_is_subtracted_once_and_deposit_fee_is_excluded(raw_example):
    har = raw_example[-1]
    row = deepcopy(json.loads(har['log']['entries'][2]['response']['content']['text'])['result'][0])
    row.update(id='trade-fee', transfer_type='fee', qty='-0.002', fee_type='trade')
    deposit = {**row, 'id': 'deposit-fee', 'qty': '-1', 'fee_type': 'deposit', 'reference_instrument': None}
    mutate_response(har, 0, lambda d: d.update(result=[row, deposit], pagination={'next_page': None, 'object_count': 2}))
    report = raw_report(raw_example)
    assert D(report['variational']['fee_usdc']) == D('0.002')
    assert D(report['actual_net_pnl_usdc']) == D('0.01803')
    assert report['variational']['excluded_deposit_withdrawal_fee_ids'] == ['deposit-fee']


def archive_raw_round(raw_example, tmp_path, capsys, name='record'):
    state, sources, vt, vc, manifest, har = raw_example
    raw_report(raw_example)
    source = tmp_path / ('sources-' + name)
    source.mkdir(parents=True)
    files = {'state.json': json.dumps(state).encode(), 'trades.csv': vt, 'cash.csv': vc,
        'coverage.json': json.dumps(manifest).encode(), 'raw.har': json.dumps(har).encode(),
        'lighter.json': json.dumps({**sources, 'trades_csv': sources['trades_csv'].decode(),
                                  'funding_csv': sources['funding_csv'].decode()}).encode()}
    for key, content in files.items(): (source / key).write_bytes(content)
    target = tmp_path / 'records' / name
    assert main(['--state-path', str(source / 'state.json'), '--lighter-bundle', str(source / 'lighter.json'),
        '--variational-trades-csv', str(source / 'trades.csv'), '--variational-transfers-csv', str(source / 'cash.csv'),
        '--variational-coverage-json', str(source / 'coverage.json'), '--variational-ledger-har', str(source / 'raw.har'),
        '--output-dir', str(target)]) == 0
    capsys.readouterr()
    return target


def archive_registered_round(raw_example, tmp_path, capsys, name='registered', *, missing_var_close=False):
    state, sources, vt, vc, manifest, har = deepcopy(raw_example)
    if missing_var_close:
        close = state['execution_history'][1]['legs']['variational']['result']
        close.update(classification='unknown', status='unknown', terminal_confirmed=False,
                     filled_qty=None, average_price=None)
    content = json.dumps(har).encode()
    manifest['raw_ledger_capture']['sha256'] = hashlib.sha256(content).hexdigest()
    source = tmp_path / ('registered-sources-' + name)
    source.mkdir(parents=True)
    bundle = {**sources, 'trades_csv': sources['trades_csv'].decode(),
              'funding_csv': sources['funding_csv'].decode()}
    (source / 'state.json').write_text(json.dumps(state))
    (source / 'lighter.json').write_text(json.dumps(bundle))
    (source / 'trades.csv').write_bytes(vt)
    (source / 'cash.csv').write_bytes(vc)
    (source / 'coverage.json').write_text(json.dumps(manifest))
    (source / 'ledger.har').write_bytes(content)
    target = tmp_path / 'records' / name
    assert main(['--state-path', str(source / 'state.json'), '--lighter-bundle', str(source / 'lighter.json'),
        '--variational-trades-csv', str(source / 'trades.csv'), '--variational-transfers-csv', str(source / 'cash.csv'),
        '--variational-coverage-json', str(source / 'coverage.json'), '--variational-ledger-har', str(source / 'ledger.har'),
        '--register-for-reporting', '--output-dir', str(target)]) == 0
    capsys.readouterr()
    return target


def test_archive_recomputes_offline_overlays_without_writing_live_state(raw_example, tmp_path, capsys, monkeypatch):
    from tools.lib.wti_billing_records import load_verified_records, billing_rounds
    from tools.daily_pnl_report import build_platform_daily_message
    from datetime import date
    state = raw_example[0]
    before = deepcopy(state)
    archive_raw_round(raw_example, tmp_path, capsys)
    monkeypatch.setattr('requests.get', lambda *a, **k: pytest.fail('network forbidden'))
    records = load_verified_records(tmp_path / 'records', state)
    assert len(records) == 1 and D(records['exit']['net_usdc']) == D('0.02003')
    assert billing_rounds(state, records)[0]['actual_net_pnl_status'] == 'confirmed_from_independent_billing'
    message = build_platform_daily_message(None, state, day=date(2026, 10, 26), asset='WTI', billing_records=records)
    assert '确认净盈亏：未核实' in message
    assert '已核实=1组' not in message
    assert state == before


def test_registered_legacy_unknown_var_close_rebuilds_only_in_audit_snapshot(raw_example, tmp_path, capsys, monkeypatch):
    from tools.lib.wti_billing_records import load_verified_records, billing_rounds
    from tools.daily_pnl_report import build_platform_daily_message
    from datetime import date
    original = deepcopy(raw_example[0])
    original['execution_history'][1]['legs']['variational']['result'].update(
        classification='unknown', status='unknown', terminal_confirmed=False,
        filled_qty=None, average_price=None)
    close_before = deepcopy(original['execution_history'][1]['legs']['variational']['result'])
    archive_registered_round(raw_example, tmp_path, capsys, missing_var_close=True)
    monkeypatch.setattr('requests.get', lambda *a, **k: pytest.fail('network forbidden'))
    records = load_verified_records(tmp_path / 'records', original)
    proof = records['exit']
    assert D(proof['gross_usdc']) == D('0.02')
    assert D(proof['net_usdc']) == D('0.02003')
    assert D(proof['fees_usdc']) == 0
    snapshot = json.loads((tmp_path / 'records/registered/execution-snapshot.json').read_text())
    registration = json.loads((tmp_path / 'records/registered/registration.json').read_text())
    assert snapshot['execution_history'][1]['legs']['variational']['result']['source'].startswith('official_confirmed')
    assert registration['reconstruction']['rfq_id'] == 'rfq-exit'
    assert original['execution_history'][1]['legs']['variational']['result'] == close_before
    report = build_platform_daily_message(None, original, day=date(2026, 10, 26),
                                          asset='WTI', billing_records=records)
    assert '累计账单已核实净收益：未核实' in report
    assert billing_rounds(original, records)[0]['actual_gross_pnl_usd'] == '0.02'


def test_registered_confirmed_round_derives_missing_gross_without_mutating_source(raw_example, tmp_path, capsys):
    from tools.lib.wti_billing_records import load_verified_records
    state = deepcopy(raw_example[0])
    state['last_closed_position'].pop('actual_gross_pnl_usd')
    raw = list(deepcopy(raw_example))
    raw[0] = state
    archive_registered_round(raw, tmp_path, capsys, missing_var_close=False)
    records = load_verified_records(tmp_path / 'records', state)
    assert D(records['exit']['gross_usdc']) == D('0.02')
    assert 'actual_gross_pnl_usd' not in state['last_closed_position']


def test_registered_snapshot_rejects_tampering_and_live_identity_drift(raw_example, tmp_path, capsys):
    from tools.lib.wti_billing_records import load_verified_records
    target = archive_registered_round(raw_example, tmp_path, capsys, missing_var_close=True)
    snapshot_path = target / 'execution-snapshot.json'
    snapshot = json.loads(snapshot_path.read_text())
    snapshot['execution_history'][1]['legs']['variational']['result']['average_price'] = '999'
    snapshot_path.write_text(json.dumps(snapshot))
    with pytest.raises(ValueError, match='recomputation_mismatch|historical_reconstruction_mismatch|persisted_gross'):
        load_verified_records(target.parent, raw_example[0])

    target = archive_registered_round(raw_example, tmp_path / 'fresh', capsys, missing_var_close=True)
    live = deepcopy(raw_example[0])
    live['execution_history'][1]['legs']['variational']['request']['native_client_order_id'] = 'other-rfq'
    with pytest.raises(ValueError, match='live_round_mismatch'):
        load_verified_records(target.parent, live)


@pytest.mark.parametrize('bad', ['net', 'evidence', 'snapshot', 'live_fill', 'live_closed_at', 'source_hash'])
def test_tampered_archives_or_live_rounds_fail_closed(raw_example, tmp_path, capsys, bad):
    from tools.lib.wti_billing_records import load_verified_records
    state = raw_example[0]
    target = archive_raw_round(raw_example, tmp_path, capsys)
    if bad in {'net', 'evidence', 'snapshot', 'source_hash'}:
        file = target / {'net': 'audit.json', 'evidence': 'variational-sources.json',
                         'snapshot': 'execution-snapshot.json', 'source_hash': 'variational-sources.json'}[bad]
        value = json.loads(file.read_text())
        if bad == 'net': value['actual_net_pnl_usdc'] = '999'
        if bad == 'evidence': value['ledger_har'] = '{}'
        if bad == 'snapshot': value['last_closed_position']['actual_gross_pnl_usd'] = '999'
        if bad == 'source_hash': value['coverage']['raw_ledger_capture']['sha256'] = 'wrong'
        file.write_text(json.dumps(value))
    if bad == 'live_fill': state['execution_history'][0]['legs']['lighter']['result']['average_price'] = '999'
    if bad == 'live_closed_at': state['last_closed_position']['closed_at'] = '2026-10-27T00:00:00Z'
    with pytest.raises((ValueError, KeyError)):
        load_verified_records(tmp_path / 'records', state)


def test_duplicate_archives_count_one_round_and_unrelated_archive_is_not_applied(raw_example, tmp_path, capsys):
    import shutil
    from tools.lib.wti_billing_records import load_verified_records
    target = archive_raw_round(raw_example, tmp_path, capsys)
    shutil.copytree(target, target.parent / 'copy')
    assert len(load_verified_records(target.parent, raw_example[0])) == 1
    assert load_verified_records(target.parent, {'asset': 'WTI', 'execution_history': []}) == {}


def test_billing_notification_retries_only_explicit_rejection_and_never_ambiguous_delivery(tmp_path):
    from tools.lib.wti_billing_records import deliver_once
    path = tmp_path / 'outbox.json'
    calls = []
    replies = iter([(False, 'http_status_429'), (True, 'sent')])
    notifier = SimpleNamespace(send_now=lambda msg: (calls.append(msg), next(replies))[1])
    assert deliver_once(path, 'billing:exit', 'message', notifier) == 'pending'
    assert deliver_once(path, 'billing:exit', 'message', notifier) == 'sent'
    assert deliver_once(path, 'billing:exit', 'message', notifier) == 'sent'
    assert len(calls) == 2
    with pytest.raises(ValueError, match='changed_requires_review'):
        deliver_once(path, 'billing:exit', 'different', notifier)
    uncertain = SimpleNamespace(send_now=lambda msg: (calls.append(msg), (False, 'request_failed:Timeout'))[1])
    assert deliver_once(path, 'billing:new', 'new', uncertain) == 'delivery_unknown'
    assert deliver_once(path, 'billing:new', 'new', uncertain) == 'delivery_unknown'
    assert len(calls) == 3
    memory = json.loads(path.read_text());memory['events']['billing:new']['status'] = 'sending'
    path.write_text(json.dumps(memory))
    assert deliver_once(path, 'billing:new', 'new', uncertain) == 'delivery_unknown'
    assert len(calls) == 3


def test_daily_delivery_with_dynamic_status_cannot_resend_after_uncertain_result(tmp_path):
    from tools.lib.wti_billing_records import deliver_once
    calls = []
    notifier = SimpleNamespace(send_now=lambda msg: (calls.append(msg) is None and False, 'request_failed:Timeout'))
    path = tmp_path / 'outbox.json'
    assert deliver_once(path, 'daily:day', 'flat', notifier, strict_message=False) == 'delivery_unknown'
    assert deliver_once(path, 'daily:day', 'open', notifier, strict_message=False) == 'delivery_unknown'
    assert len(calls) == 1


def test_corrupt_billing_outbox_is_preserved_without_sending(tmp_path):
    from tools.lib.wti_billing_records import deliver_once
    path = tmp_path / 'outbox.json';path.write_text('{broken')
    with pytest.raises(ValueError):
        deliver_once(path, 'id', 'message', SimpleNamespace(send_now=lambda msg: pytest.fail('must not send')))
    assert path.read_text() == '{broken'


def test_concurrent_delivery_has_one_writer(tmp_path):
    from tools.lib.wti_billing_records import deliver_once, _delivery_lock
    path = tmp_path / 'outbox.json'
    with _delivery_lock(path.with_suffix('.json.lock')):
        with pytest.raises(OSError):
            deliver_once(path, 'id', 'message', SimpleNamespace(send_now=lambda msg: pytest.fail('must not send')))
    assert not path.exists()


def test_daily_keeps_unknown_rounds_separate_from_known_billing(raw_example, tmp_path, capsys):
    from datetime import date
    from tools.lib.wti_billing_records import load_verified_records
    from tools.daily_pnl_report import build_platform_daily_message
    archive_raw_round(raw_example, tmp_path, capsys)
    state = raw_example[0]
    state['closed_positions'] = {'unknown': {'exit_execution_id': 'unknown',
        'closed_at': state['last_closed_position']['closed_at'], 'actual_gross_pnl_usd': None}}
    records = load_verified_records(tmp_path / 'records', state)
    message = build_platform_daily_message(None, state, day=date(2026, 10, 26), billing_records=records)
    assert '当日净收益=未确认' in message
    assert '累计账单已核实净收益=0.02003' in message
    assert '已核实=1组，账单待核实=1组' in message
    earlier = build_platform_daily_message(None, state, day=date(2026, 10, 25), billing_records=records)
    assert '累计平仓=0组' in earlier and '累计账单已核实净收益=0 USDC' in earlier


def test_daily_timer_does_not_add_billing_notices_or_duplicate_frozen_daily(raw_example, tmp_path, capsys, monkeypatch):
    import tools.daily_pnl_report as daily
    archive_raw_round(raw_example, tmp_path, capsys)
    state_path = tmp_path / 'live.json';state_path.write_text(json.dumps(raw_example[0]))
    memory_path = tmp_path / 'daily.json'
    memory_path.write_text(json.dumps({'sent_keys': ['WTI:v2:2026-10-26']}))
    args = daily.build_parser().parse_args(['--asset', 'WTI', '--day', '2026-10-26',
        '--wti-state-path', str(state_path), '--state-path', str(memory_path),
        '--wti-billing-dir', str(tmp_path / 'records'), '--billing-delivery-state', str(tmp_path / 'outbox.json')])
    messages = []
    monkeypatch.setattr(daily.TelegramNotifier, 'from_env', lambda **kw:
        SimpleNamespace(send_now=lambda msg: (messages.append(msg) is None, 'sent')))
    before = state_path.read_bytes()
    assert daily.send_platform_daily(args, None) == 0
    assert daily.send_platform_daily(args, None) == 0
    assert not messages and state_path.read_bytes() == before


def test_daily_correction_cli_sends_one_report_without_rewriting_daily_or_live_state(
        raw_example, tmp_path, capsys, monkeypatch):
    import tools.daily_pnl_report as daily
    archive_registered_round(raw_example, tmp_path, capsys, missing_var_close=True)
    live = deepcopy(raw_example[0])
    live['execution_history'][1]['legs']['variational']['result'].update(
        classification='unknown', status='unknown', terminal_confirmed=False,
        filled_qty=None, average_price=None)
    wti_path = tmp_path / 'wti.json';wti_path.write_text(json.dumps(live))
    daily_path = tmp_path / 'daily.json'
    daily_path.write_text(json.dumps({'schema_version': 2, 'sent_keys': ['WTI:v2:2026-10-26']}))
    args = daily.build_parser().parse_args(['--asset', 'WTI', '--day', '2026-10-26', '--correction',
        '--wti-state-path', str(wti_path), '--state-path', str(daily_path),
        '--wti-billing-dir', str(tmp_path / 'records'),
        '--billing-delivery-state', str(tmp_path / 'outbox.json')])
    messages = []
    monkeypatch.setattr(daily.TelegramNotifier, 'from_env', lambda **kw:
        SimpleNamespace(send_now=lambda msg: (messages.append(msg), (True, 'sent'))[1]))
    before_wti, before_daily = wti_path.read_bytes(), daily_path.read_bytes()
    assert daily.send_platform_daily(args, None) == 0
    assert daily.send_platform_daily(args, None) == 0
    assert len(messages) == 1 and '更正补报' in messages[0]
    assert '确认净盈亏：未核实' in messages[0]
    assert wti_path.read_bytes() == before_wti and daily_path.read_bytes() == before_daily


def test_consolidated_daily_correction_is_once_and_covers_round_notices(tmp_path):
    from tools.lib.wti_billing_records import deliver_billing_correction_once, publish_billing_updates
    path = tmp_path / 'outbox.json'
    records = {'exit-a': {'exit_execution_id': 'exit-a', 'closed_at': '2026-10-26T05:00:00Z',
        'gross_usdc': '0.02', 'fees_usdc': '0.002', 'funding_usdc': '0', 'net_usdc': '0.018'}}
    sent = []
    notifier = SimpleNamespace(send_now=lambda message: (sent.append(message), (True, 'sent'))[1])
    message = '[补报]\n累计账单净收益=0.018 USDC'
    assert deliver_billing_correction_once(path, 'daily-correction:WTI:2026-10-29', message,
                                           records, notifier) == 'sent'
    assert deliver_billing_correction_once(path, 'daily-correction:WTI:2026-10-29', message,
                                           records, notifier) == 'sent'
    assert len(sent) == 1
    assert publish_billing_updates(records, path,
        SimpleNamespace(send_now=lambda _: pytest.fail('round notice duplicated correction'))) == {'exit-a': 'sent'}
    with pytest.raises(ValueError, match='changed_requires_review'):
        deliver_billing_correction_once(path, 'daily-correction:WTI:2026-10-29', 'changed', records, notifier)


def test_ambiguous_daily_correction_suppresses_round_level_retries(tmp_path):
    from tools.lib.wti_billing_records import deliver_billing_correction_once, publish_billing_updates
    path = tmp_path / 'outbox.json'
    records = {'exit-a': {'exit_execution_id': 'exit-a', 'closed_at': '2026-10-26T05:00:00Z',
        'gross_usdc': '0.02', 'fees_usdc': '0.002', 'funding_usdc': '0', 'net_usdc': '0.018'}}
    calls = []
    notifier = SimpleNamespace(send_now=lambda message: (calls.append(message), (False, 'request_failed:Timeout'))[1])
    assert deliver_billing_correction_once(path, 'daily-correction:key', 'message', records, notifier) == 'delivery_unknown'
    assert publish_billing_updates(records, path,
        SimpleNamespace(send_now=lambda _: pytest.fail('ambiguous report must not resend'))) == {'exit-a': 'delivery_unknown'}
    assert len(calls) == 1


def test_daily_unknown_delivery_does_not_retry_after_runtime_status_changes(tmp_path, monkeypatch):
    import tools.daily_pnl_report as daily
    state_path = tmp_path / 'live.json';state_path.write_text(json.dumps({'asset': 'WTI', 'status': 'flat'}))
    args = daily.build_parser().parse_args(['--asset', 'WTI', '--day', '2026-10-26',
        '--wti-state-path', str(state_path), '--state-path', str(tmp_path / 'daily.json'),
        '--wti-billing-dir', str(tmp_path / 'empty'), '--billing-delivery-state', str(tmp_path / 'outbox.json')])
    calls = []
    monkeypatch.setattr(daily.TelegramNotifier, 'from_env', lambda **kw:
        SimpleNamespace(send_now=lambda msg: (calls.append(msg) is not None, 'request_failed:Timeout')))
    assert daily.send_platform_daily(args, None) == 1
    state_path.write_text(json.dumps({'asset': 'WTI', 'status': 'open'}))
    assert daily.send_platform_daily(args, None) == 1
    assert len(calls) == 1


def test_billing_report_cli_dry_run_and_repeated_send_preserve_live_state(raw_example, tmp_path, capsys, monkeypatch):
    import tools.wti_billing_report as reporter
    archive_raw_round(raw_example, tmp_path, capsys)
    state_path = tmp_path / 'live.json';state_path.write_text(json.dumps(raw_example[0]))
    outbox = tmp_path / 'outbox.json'
    args = ['--billing-dir', str(tmp_path / 'records'), '--wti-state-path', str(state_path),
            '--delivery-state', str(outbox)]
    messages = []
    monkeypatch.setattr(reporter.TelegramNotifier, 'from_env', lambda **kw:
        SimpleNamespace(send_now=lambda msg: (messages.append(msg) is None, 'sent')))
    before = state_path.read_bytes()
    assert reporter.main([*args, '--dry-run']) == 0
    assert not messages and not outbox.exists()
    assert reporter.main(args) == 0
    assert reporter.main(args) == 0
    assert len(messages) == 1 and state_path.read_bytes() == before


def test_corrupt_daily_state_is_not_cleared_or_replayed(tmp_path, monkeypatch):
    import tools.daily_pnl_report as daily
    memory = tmp_path / 'daily.json';memory.write_text('{broken')
    args = daily.build_parser().parse_args(['--state-path', str(memory)])
    monkeypatch.setattr(daily.TelegramNotifier, 'from_env', lambda **kw: pytest.fail('must not send'))
    with pytest.raises(ValueError):
        daily.send_platform_daily(args, None)
    assert memory.read_text() == '{broken'


def test_conflicting_rfq_identities_cannot_certify_raw_ledger(raw_example):
    raw_example[0]['execution_history'][1]['legs']['variational']['result']['native_client_order_id'] = 'other-rfq'
    assert raw_report(raw_example)['actual_net_pnl_usdc'] is None


def test_signed_export_url_cannot_enter_private_evidence_archive(raw_example):
    har = raw_example[-1]
    entry = deepcopy(har['log']['entries'][0])
    entry['request']['url'] = 'https://omni.variational.io/api/exports/job'
    entry['response']['content']['text'] = json.dumps({'status': 'completed', 'download_url': 'https://example.invalid/signed'})
    har['log']['entries'].append(entry)
    assert raw_report(raw_example)['actual_net_pnl_usdc'] is None
