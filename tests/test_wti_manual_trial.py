from copy import deepcopy
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from types import SimpleNamespace

import pytest

from tools.lib.wti_execution import WTIExecutionStore, WTIExecutionError
from tools.lib.wti_manual_trial import ensure_trial, advance_trial
from tools.wti_live import parse_args
from tools.wti_takeover import management_command


CONFIG = {
    "entry_threshold_bps": "30", "exit_target_bps": "1", "roundtrip_cost_bps": "10",
    "max_hold_seconds": 21600.0, "entry_cutoff_seconds": 1800.0,
    "max_notional_usd": "20", "max_slippage_bps": "8",
    "historical_readiness_mode": "bounded_pilot_history_volume_bypass",
    "tier_thresholds_bps": ["30", "45", "45.25", "45.50", "45.75"],
    "layer_notional_usd": "20", "max_layers": 5, "max_equity_multiple": "5",
    "max_completed_roundtrips": 1,
}


def setup_trial(tmp_path):
    store = WTIExecutionStore(tmp_path / "state.json")
    pilot = store.initialize_gradient_pilot(market_id=145, multiplier=Decimal(1),
        config=CONFIG, max_layers=5)
    opts = {"trial_id": "a"*32, "root_pilot_id": pilot["pilot_id"],
            "max_rounds": 3, "model_sha256": "b"*64, "config": CONFIG}
    ensure_trial(store, **opts)
    return store, opts


def complete_round(store, **proof_changes):
    state = store.load()
    state["gradient_pilot"].update(status="completed", completed_roundtrips=1)
    state["last_account_reconciliation"] = {
        "verified": True, "market_id": 145, "active_order_count": 0,
        "variational_qty": "0", "lighter_qty": "0",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "runtime_pid": 123, "runtime_run_id": "this-run", **proof_changes,
    }
    store.save(state)


def advance(store, opts):
    return advance_trial(store, **opts, runtime_pid=123, runtime_run_id="this-run")


def test_three_round_budget_is_persisted_and_never_renewed_by_restart(tmp_path):
    store, opts = setup_trial(tmp_path)
    roots = []
    for index in range(3):
        roots.append(store.load()["gradient_pilot"]["pilot_id"])
        restarted = WTIExecutionStore(store.path)
        before = restarted.path.read_bytes()
        ensure_trial(restarted, **opts)
        assert restarted.path.read_bytes() == before
        complete_round(restarted)
        state = advance(restarted, opts)
        session = state["manual_wti_trial"]
        assert len(session["completed_pilot_ids"]) == index + 1
        assert len(session["round_pilot_ids"]) <= 3
        assert not state["pending_action"] and not state["execution_history"]
        assert state.get("gradient_rotation_session") is None
    assert len(set(roots)) == 3
    assert session["status"] == "completed"
    assert advance(store, opts)["gradient_pilot"]["pilot_id"] == roots[-1]
    assert advance(store, opts)["manual_wti_trial"]["round_pilot_ids"] == roots


@pytest.mark.parametrize("changes", [
    {"trial_id": "c"*32}, {"max_rounds": 2}, {"root_pilot_id": "c"*32},
    {"model_sha256": "c"*64}, {"config": CONFIG | {"roundtrip_cost_bps": "0"}},
])
def test_existing_budget_and_model_identity_cannot_be_changed(tmp_path, changes):
    store, opts = setup_trial(tmp_path)
    before = store.path.read_bytes()
    with pytest.raises(ValueError):
        ensure_trial(store, **(opts | changes))
    assert store.path.read_bytes() == before


@pytest.mark.parametrize("proof", [
    {"runtime_pid": 124}, {"runtime_run_id": "old-run"},
    {"variational_qty": "0.1"}, {"active_order_count": 1},
    {"observed_at": (datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()},
])
def test_next_round_requires_current_runtime_fresh_dual_flat_proof(tmp_path, proof):
    store, opts = setup_trial(tmp_path)
    root = store.load()["gradient_pilot"]["pilot_id"]
    complete_round(store, **proof)
    with pytest.raises(WTIExecutionError, match="dual_flat_proof"):
        advance(store, opts)
    assert store.load()["gradient_pilot"]["pilot_id"] == root
    assert len(store.load()["manual_wti_trial"]["round_pilot_ids"]) == 1


@pytest.mark.parametrize("after_creation", [False, True])
def test_crash_before_or_after_successor_creation_is_idempotently_recovered(tmp_path, monkeypatch, after_creation):
    store, opts = setup_trial(tmp_path)
    complete_round(store)
    original = store.initialize_gradient_pilot

    def interrupted(**kwargs):
        if after_creation:
            original(**kwargs)
        raise RuntimeError("simulated_process_loss")

    monkeypatch.setattr(store, "initialize_gradient_pilot", interrupted)
    with pytest.raises(RuntimeError):
        advance(store, opts)
    resumed = WTIExecutionStore(store.path)
    ensure_trial(resumed, **opts)
    state = advance(resumed, opts)
    first = state["gradient_pilot"]["pilot_id"]
    for _ in range(3):
        ensure_trial(resumed, **opts)
        assert advance(resumed, opts)["gradient_pilot"]["pilot_id"] == first
    assert len(resumed.load()["manual_wti_trial"]["round_pilot_ids"]) == 2
    assert len(resumed.load()["manual_wti_trial"]["completed_pilot_ids"]) == 1
    assert not resumed.load()["execution_history"]


def test_pending_order_prevents_round_advance(tmp_path):
    store, opts = setup_trial(tmp_path)
    complete_round(store)
    state = store.load()
    state.update(status="pending", pending_action={"kind": "close", "action_id": "unresolved"})
    store.save(state)
    before = deepcopy(store.load())
    assert advance(store, opts) == before


def test_cli_has_explicit_bounded_wti_budget_and_never_enables_rotation(tmp_path):
    base = management_command(tmp_path, new_gradient_after="a"*32)[2:] + [
        "--experimental-exit-model", str(tmp_path/"model.json"),
        "--experimental-exit-model-sha256", "b"*64, "--confirm-experimental-exit-model",
        "--confirm-exploratory-entry", "--manual-wti-trial-id", "c"*32,
        "--manual-wti-root-pilot", "d"*32, "--manual-wti-max-rounds", "3",
    ]
    args = parse_args(base)
    assert args.manual_wti_max_rounds == 3
    assert not args.continuous_gradient_rotation and not args.rotation_test_id
    with pytest.raises(SystemExit):
        parse_args([*base, "--manual-wti-max-rounds", "4"])


@pytest.mark.parametrize("drain_during_read", [False, True])
def test_runtime_advances_only_after_account_read_and_clears_old_confirmation_votes(tmp_path, drain_during_read):
    from tools.wti_live import WTILiveRuntime
    store, opts = setup_trial(tmp_path)
    complete_round(store)
    runtime = object.__new__(WTILiveRuntime)
    runtime.args = SimpleNamespace(manual_wti_trial_id=opts["trial_id"])
    runtime.manage_existing_only = False
    runtime.entry_authorization_permanently_revoked = False
    runtime.rotation_drain_request = None
    runtime.experimental_exit_model = {"expires_at": (datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()}
    runtime.pilot_store = store
    runtime.run_id = "current-runtime"
    runtime.gradient_entry_confirmation_history = {"old": [("old-signal", 5)]}
    runtime._manual_trial_options = lambda: opts
    reads = []
    runtime._poll_rotation_drain = lambda: bool(reads) and drain_during_read

    async def read():
        reads.append(True)
        complete_round(store, runtime_pid=os.getpid(), runtime_run_id=runtime.run_id)
        return store.load()

    runtime._read_account_only_reconcile = read
    state = asyncio.run(runtime._advance_continuous_gradient_round(store.load()))
    assert reads == [True]
    if drain_during_read:
        assert len(state["manual_wti_trial"]["round_pilot_ids"]) == 1
        assert runtime.entry_authorized is False
        return
    assert len(state["manual_wti_trial"]["round_pilot_ids"]) == 2
    assert all(not v for v in runtime.gradient_entry_confirmation_history.values())
    assert runtime.entry_authorized is False
    assert runtime.last_action["action"] == "manual_wti_round_armed"


def test_completed_manual_budget_does_not_repeat_account_requests(tmp_path):
    from tools.wti_live import WTILiveRuntime
    store, opts = setup_trial(tmp_path)
    for _ in range(3):
        complete_round(store)
        advance(store, opts)
    runtime = object.__new__(WTILiveRuntime)
    runtime.args = SimpleNamespace(manual_wti_trial_id=opts["trial_id"])
    state = store.load()
    assert asyncio.run(runtime._advance_continuous_gradient_round(state)) == state


def test_real_initializer_resumes_multi_round_session_without_rearming(tmp_path, monkeypatch):
    import lighter.signer_client
    from tools import wti_account_probe
    from tools.lib import wti_readonly_adapter
    from test_wti_live_runtime import _runtime_with_real_executor, _FlatLoopGateway

    class Signer:
        def __init__(self, **kwargs):
            pass

        def check_client(self):
            return None

    class Forwarder:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

    runtime = _runtime_with_real_executor(tmp_path, SimpleNamespace(latest_sample=None),
                                          _FlatLoopGateway(), gradient=True)
    root = runtime.gradient_pilot["pilot_id"]
    runtime.args.manual_wti_trial_id = "a"*32
    runtime.args.manual_wti_root_pilot = root
    runtime.args.manual_wti_max_rounds = 3
    runtime.args.experimental_exit_model_sha256 = "b"*64

    async def readiness():
        runtime.readiness = {"ready": False, "market_data_ready": False,
                             "historical_volume_bypass_ready": True}

    runtime._load_startup_readiness = readiness
    market = {"market_id": 145, "symbol": "WTI", "market_type": "perp", "status": "active",
              "multiplier": Decimal(1), "supported_size_decimals": 3, "supported_price_decimals": 3,
              "min_base_amount": ".001", "min_quote_amount": "1"}
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "1")
    monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "2")
    monkeypatch.setenv("API_KEY_PRIVATE_KEY", "test-only-not-used")
    monkeypatch.setattr(lighter.signer_client, "SignerClient", Signer)
    monkeypatch.setattr(wti_account_probe, "fetch_lighter_market_metadata", lambda *a, **k: market)
    monkeypatch.setattr(wti_readonly_adapter, "VariationalForwarderClient", Forwarder)
    asyncio.run(runtime._initialize_execution())
    opts = runtime._manual_trial_options()
    state = runtime.pilot_store.load()
    assert state["manual_wti_trial"]["round_pilot_ids"] == [root]
    complete_round(runtime.pilot_store)
    advance(runtime.pilot_store, opts)
    before = runtime.pilot_store.path.read_bytes()
    successor = runtime.pilot_store.load()["gradient_pilot"]["pilot_id"]
    for _ in range(2):
        asyncio.run(runtime._initialize_execution())
        assert runtime.pilot_store.path.read_bytes() == before
        assert runtime.gradient_pilot["pilot_id"] == successor
    assert len(runtime.pilot_store.load()["manual_wti_trial"]["round_pilot_ids"]) == 2
