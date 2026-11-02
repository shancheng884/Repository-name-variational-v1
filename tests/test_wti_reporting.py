from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import pytest

from tools.daily_pnl_report import build_platform_daily_message
from tools.lib.runtime_files import read_json, write_json_atomic
from tools.lib.wti_reporting import TradeReporter, closed_rounds


def execution(key, kind, prices=(100, 101), qty='0.2', at='2026-10-28T16:30:00+00:00'):
    legs = {}
    for venue, price in zip(('variational', 'lighter'), prices):
        side = 'buy' if (venue == 'variational') == (kind == 'open') else 'sell'
        legs[venue] = {'request': {'side': side}, 'result': {
            'classification': 'filled', 'terminal_confirmed': True,
            'filled_qty': qty, 'average_price': str(price),
            'fee_usd': '0', 'fee_confirmed': True}}
    return {'action_id': key, 'kind': kind, 'legs': legs, 'completed_at': at,
            'outcome': 'filled', 'multiplier': '1', 'direction': 'long_var_short_lighter'}


def test_legacy_rounds_are_deduplicated_and_multi_layer_weighted():
    a = execution('a', 'open')
    b = execution('b', 'open', (102, 103))
    c = execution('c', 'close', (101, 101.5), qty='0.4')
    rows = closed_rounds({'execution_history': [a, a, b, c, c]})
    assert len(rows) == 1
    assert Decimal(rows[0]['actual_gross_pnl_usd']) == Decimal('0.2')
    assert rows[0]['actual_fees_usd'] == '0'
    assert rows[0]['actual_net_pnl_usd'] is None


def test_strategy_evidence_exclusion_preserves_actual_fills_and_daily_account_pnl():
    opened = execution('entry', 'open')
    closed = execution('gap-exit', 'close', (99, 101), qty='0.2')
    closed.update(strategy_evidence_excluded=True,
                  strategy_evidence_exclusion_reason='sample_gap_restart_lock_incident')
    state = {'asset': 'WTI', 'status': 'flat', 'execution_history': [opened, closed]}

    row = closed_rounds(state)[0]
    assert row['strategy_evidence_excluded'] is True
    assert row['actual_gross_pnl_usd'] is not None
    message = build_platform_daily_message(None, state, day=date(2026, 10, 29))
    assert 'WTI：当日平仓=1组' in message
    assert '已确认毛收益=-0.2 USD' in message


def test_unknown_fill_and_partial_qty_never_become_confirmed_profit():
    a, b = execution('a', 'open'), execution('b', 'close')
    b['legs']['variational']['result']['classification'] = 'unknown'
    assert closed_rounds({'execution_history': [a, b]})[0]['actual_gross_pnl_usd'] is None
    b = execution('b', 'close', qty='0.1')
    assert closed_rounds({'execution_history': [a, b]})[0]['actual_gross_pnl_usd'] is None


def test_reporter_imports_old_history_then_sends_new_action_once_after_restart(tmp_path):
    messages = []
    notifier = SimpleNamespace(send_now=lambda msg: (messages.append(msg) is None, 'sent'))
    path = tmp_path / 'outbox.json'
    state = {'execution_history': [execution('old', 'open')]}
    reporter = TradeReporter(path, state, notifier)
    reporter.poll(state)
    assert not messages
    state['execution_history'].append(execution('new', 'close'))
    reporter.poll(state)
    TradeReporter(path, state, notifier).poll(state)
    assert len(messages) == 1
    assert '实际盈亏：未核实' in messages[0] and '动作ID' not in messages[0]
    assert read_json(path)['events']['new']['status'] == 'sent'


def test_reporter_does_not_retry_ambiguous_delivery(tmp_path):
    path = tmp_path / 'outbox.json'
    calls = []
    notifier = SimpleNamespace(send_now=lambda msg: (calls.append(msg) is not None, 'request_failed:Timeout'))
    state = {'execution_history': []}
    reporter = TradeReporter(path, state, notifier)
    state['execution_history'].append(execution('new', 'open'))
    reporter.poll(state)
    reporter.poll(state)
    assert len(calls) == 1
    assert read_json(path)['events']['new']['status'] == 'delivery_unknown'
    saved = read_json(path)
    saved['events']['new']['status'] = 'sending'
    write_json_atomic(path, saved)
    TradeReporter(path, state, notifier).poll(state)
    assert len(calls) == 1


def test_explicit_rate_limit_retries_without_repeating_success(tmp_path):
    replies = iter([(False, 'http_status_429'), (True, 'sent')])
    reporter = TradeReporter(tmp_path / 'outbox.json', {}, SimpleNamespace(send_now=lambda msg: next(replies)))
    state = {'execution_history': [execution('a', 'open')]}
    reporter.poll(state)
    assert reporter.outbox['events']['a']['status'] == 'pending'
    reporter.poll(state)
    reporter.poll(state)
    assert reporter.outbox['events']['a']['status'] == 'sent'


def test_daily_uses_beijing_day_separates_assets_and_does_not_reuse_old_equity():
    state = {'asset': 'WTI', 'status': 'flat', 'execution_history': [
        execution('a', 'open'), execution('b', 'close', (100, 100.5))]}
    message = build_platform_daily_message({'asset': 'ETH', 'latest_combined_equity_usd': '273.749043'},
                                          state, day=date(2026, 10, 29))
    assert 'WTI：当日平仓=1组' in message
    assert 'ETH：平仓批次=0' in message
    assert '273.749043' not in message
    assert '手续费不计' in message
    previous = build_platform_daily_message(None, state, day=date(2026, 10, 28))
    assert 'WTI：当日平仓=0组' in previous
    assert 'WTI：累计平仓=0组' in previous


def test_daily_missing_ledger_does_not_claim_zero_profit():
    message = build_platform_daily_message(None, {}, day=date(2026, 10, 29))
    assert '账本不可用' in message
    assert '不能按0收益统计' in message


def test_partial_fee_evidence_does_not_block_net_income():
    a, b = execution('a', 'open'), execution('b', 'close')
    a['legs']['lighter']['result']['fee_confirmed'] = False
    state = {'asset': 'WTI', 'status': 'flat', 'execution_history': [a, b]}
    row = closed_rounds(state)[0]
    assert row['actual_fees_usd'] is None
    msg = build_platform_daily_message(None, state, day=date(2026, 10, 29))
    assert '手续费=未确认' in msg
    assert '手续费不计' in msg


def test_rejected_open_is_not_counted_as_a_filled_layer():
    rejected = execution('bad', 'open')
    rejected['outcome'] = 'no_fill'
    rows = closed_rounds({'execution_history': [rejected, execution('a', 'open'), execution('b', 'close')]})
    assert len(rows) == 1 and rows[0]['layers_count'] == 1


def test_corrupt_outbox_is_not_reset_or_replayed(tmp_path):
    path = tmp_path / 'outbox.json'
    path.write_text('{broken')
    with pytest.raises(ValueError, match='preserve_for_review'):
        TradeReporter(path, {}, SimpleNamespace())
    assert path.read_text() == '{broken'
