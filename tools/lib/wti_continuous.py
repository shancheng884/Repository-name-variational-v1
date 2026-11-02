"""Manual, equity-bounded WTI gradients. No asset rotation or order submission."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal as D


def position_subset(position, layer_ids):
    layers = position.get('layers') or []
    ids = list(layer_ids)
    if not ids or len(ids) != len(set(ids)) or len(layers) != len({x['layer_id'] for x in layers}):
        raise ValueError('gradient_close_layer_identity_invalid')
    selected = [x for x in layers if x['layer_id'] in ids]
    if len(selected) != len(ids):
        raise ValueError('gradient_close_layer_missing')
    result = deepcopy(position)
    result['layers'] = deepcopy(selected)
    for venue in ('var', 'lighter'):
        qty = sum((D(x[venue+'_qty']) for x in selected), D(0))
        if qty <= 0 or sum((D(x[venue+'_qty']) for x in layers), D(0)) != D(position[venue+'_qty']):
            raise ValueError('gradient_layer_quantity_mismatch')
        result[venue+'_qty'] = str(qty)
        result[venue+'_entry_price'] = str(sum((D(x[venue+'_qty'])*D(x[venue+'_entry_price']) for x in selected), D(0))/qty)
        fees = [x.get(venue+'_entry_fee_usd') for x in selected]
        result[venue+'_entry_fee_usd'] = str(sum((D(x) for x in fees), D(0))) if all(x is not None for x in fees) else None
    sign = D(1) if position['direction'] == 'long_var_short_lighter' else D(-1)
    result['expected_var_qty'] = str(sign*D(result['var_qty']))
    result['expected_lighter_qty'] = str(-sign*D(result['lighter_qty']))
    result['opened_at'] = min(x['opened_at'] for x in selected)
    result['gradient_tier'] = max(x['tier'] for x in selected)
    result['entry_execution_id'] = selected[0]['entry_execution_id']
    return result


def split_close(position, layer_ids):
    selected = position_subset(position, layer_ids)
    remaining_ids = [x['layer_id'] for x in position['layers'] if x['layer_id'] not in layer_ids]
    remaining = position_subset(position, remaining_ids) if remaining_ids else None
    return selected, remaining


def ensure_continuous(store, *, root_pilot_id, config, runtime_pid, runtime_run_id):
    """Adopt once, only after this runtime proves exact positions and no orders."""
    with store.exclusive():
        state = store.load()
        pilot = state.get('gradient_pilot') or {}
        session = state.get('manual_continuous_gradient')
        if state.get('status') not in {'flat', 'open'} or state.get('pending_action'):
            raise ValueError('continuous_requires_resolved_state')
        if pilot.get('continuous_rotation') or pilot.get('rotation_test_id') or state.get('gradient_rotation_session'):
            raise ValueError('manual_continuous_cannot_enable_rotation')
        if session is None:
            proof = state.get('last_account_reconciliation') or {}
            at = datetime.fromisoformat(proof['observed_at'])
            position = state.get('open_position') or {}
            if (proof.get('verified') is not True or proof.get('runtime_pid') != runtime_pid
                    or proof.get('runtime_run_id') != runtime_run_id or proof.get('active_order_count') != 0
                    or proof.get('market_id') != 145 or at.tzinfo is None
                    or not -5 <= (datetime.now(timezone.utc)-at).total_seconds() <= 15
                    or D(proof['variational_qty']) != D(position.get('expected_var_qty', '0'))
                    or D(proof['lighter_qty']) != D(position.get('expected_lighter_qty', '0'))):
                raise ValueError('continuous_requires_current_runtime_account_proof')
            old = pilot.get('config') or {}
            same = {k:v for k,v in config.items() if k not in {'manual_continuous_equity', 'elastic_capacity_enabled'}}
            previous = {k:v for k,v in old.items() if k not in {'manual_continuous_equity', 'elastic_capacity_enabled'}}
            if (pilot.get('pilot_id') != root_pilot_id or same != previous
                    or pilot.get('max_layers') != 5 or pilot.get('status') not in {'in_position','available','completed','exhausted'}
                    or (position and position.get('gradient_pilot_id') != root_pilot_id)):
                raise ValueError('continuous_adoption_identity_or_threshold_mismatch')
            session = {'root_pilot_id':root_pilot_id, 'last_pilot_id':root_pilot_id,
                       'config':deepcopy(config), 'started_at':datetime.now(timezone.utc).isoformat(),
                       'status':'active', 'capacity_mode':'smaller_venue_equity_1x_to_5x', 'child_notional_usd':'20'}
            state['manual_continuous_gradient'] = session
            state.setdefault('manual_wti_trial_history', []).append(deepcopy(state.get('manual_wti_trial')))
            if state.get('manual_wti_trial'):
                state['manual_wti_trial']['status'] = 'superseded_by_continuous_gradient'
            pilot['config'] = deepcopy(config)
            store.save(state)
        if (session.get('root_pilot_id') != root_pilot_id or session.get('config') != config
                or session.get('status') != 'active' or pilot.get('config') != config
                or pilot.get('max_layers') != 5):
            raise ValueError('continuous_session_configuration_mismatch')
        if pilot.get('pilot_id') != session['last_pilot_id']:
            if pilot.get('previous_pilot_id') != session['last_pilot_id'] or pilot.get('authorization_kind') != 'manual_new_round':
                raise ValueError('continuous_successor_identity_mismatch')
            session['last_pilot_id'] = pilot['pilot_id']
            store.save(state)
        return state
