# Live Inventory V1 Design

## Goal

Build the first live version of the layered inventory strategy that has been validated in `paper inventory`.

The goal of V1 is not unattended trading. The goal is a very small, observable, recoverable live experiment that can prove whether the paper inventory edge survives real order submission, real fills, and partial failure modes.

## Current Evidence

Recent conservative paper inventory runs used:

- `--paper-inventory-latency-samples 1`
- `--paper-inventory-max-total-lots 5`
- `--paper-inventory-entry-bps 40`
- `--paper-inventory-exit-bps 10`
- `--paper-inventory-lot-notional-usd 50`

Latest useful sample:

- entered: `25`
- exited: `25`
- latest open lots: `0`
- realized PnL: about `+5.98 USD`
- losing exits: `0`
- long exits: `15`, avg about `44.66 bps`
- short exits: `10`, avg about `52.63 bps`

This supports the inventory direction, but live V1 must start much smaller than the paper run.

## Non-Goals

V1 must not be an unattended trading bot.

V1 must not run overnight.

V1 must not use `50 USD * 5 lots` live size.

V1 must not try to auto-recover from unknown real exchange state.

V1 must not ignore one-sided fill or submit failure.

V1 must not open a new lot after entering `manual_review_required`.

## V1 Scope

Asset:

- BTC only.

Mode:

- New explicit opt-in live inventory mode.
- It should only work with `--mode live --confirm-live`.
- It should be separate from current single-cycle `auto-live` behavior.

Suggested CLI flags:

- `--live-inventory`
- `--live-inventory-i-confirm-flat-start`
- `--live-inventory-reset-state-after-manual-flat`
- `--live-inventory-lot-notional-usd`, default disabled or `20`
- `--live-inventory-max-lots`, default `2`
- `--live-inventory-max-total-lots`, default `2`
- `--live-inventory-entry-bps`, default `45`
- `--live-inventory-exit-bps`, default `10`
- `--live-inventory-min-hold-samples`, default `3`
- `--live-inventory-max-hold-samples`, default `300`
- `--live-inventory-max-unrealized-loss-bps`, default `25`
- `--live-inventory-max-cycles`, default `1` for first live tests

Hard V1 caps:

- `lot_notional_usd <= 20` unless code is changed deliberately later.
- `max_total_lots <= 3`.
- `asset == BTC` only.
- `Lighter` order mode must be `market-ioc`.
- `Lighter` submit transport must be `ws`.
- `Variational` submit transport must be `api`.
- `--lighter-prewarm-submit-ws` should be required.

## State File

Use a separate file from current single-cycle auto-live state:

- `log/live_inventory_state.json`

State values:

- `flat`
- `open`
- `manual_review_required`

The state file must include:

- `status`
- `asset`
- `next_lot_id`
- `open_lots`
- `pending_actions`
- `realized_pnl_usd`
- `manual_review_reason`
- `updated_at`

Each open lot must include:

- `lot_id`
- `direction`
- `qty`
- `entry_var_order_id` if available
- `entry_lighter_order_id` if available
- `entry_var_fill_price`
- `entry_lighter_fill_price`
- `entry_edge_bps`
- `entered_at`
- `entered_sample_index`
- `status`

Supported lot statuses:

- `open`
- `exit_submitted`
- `closed`
- `manual_review_required`

## Startup Guards

Startup must fail unless all are true:

- `--mode live --confirm-live`
- `--live-inventory`
- `--live-inventory-i-confirm-flat-start`
- `--live-allowed-assets BTC`
- `--variational-submit-transport api`
- `--lighter-submit-transport ws`
- `--lighter-order-mode market-ioc`
- `--lighter-prewarm-submit-ws`
- `live_inventory_lot_notional_usd <= 10`
- `live_inventory_max_total_lots <= 3`

If `log/live_inventory_state.json` exists and status is not `flat`, startup must fail.

The only exception is `--live-inventory-reset-state-after-manual-flat`, and it is allowed only after the user manually confirms:

- Var BTC = 0
- Lighter BTC = 0

V1 should not try to query and trust balances as the only source of safety. Human confirmation remains required.

## Entry Logic

Use the same direction formulas as paper inventory:

- `long_var_short_lighter`: enter when `(lighter_bid - var_buy_price) / var_buy_price * 10000 >= entry_bps`
- `short_var_long_lighter`: enter when `(var_sell_price - lighter_ask) / var_sell_price * 10000 >= entry_bps`

Before entry:

- Check `open_lots(direction) < max_lots`.
- Check `open_lots(total) < max_total_lots`.
- Check no pending action for the same direction in V1.
- Check asset is BTC.
- Calculate qty from `lot_notional_usd / var_entry_price`.
- Reject if qty is below Lighter BTC minimum.
- Reject if qty exceeds hard V1 max qty.
- Run Lighter precheck with actionable Var price.
- Refresh Variational quote if available.

Submission order:

- Submit Var and Lighter concurrently, same as current low-latency auto-live.
- Var side and Lighter side must match current `place_lighter_order_from_plan` semantics.
- Use `reduceOnly=false` for entry on Variational.

Entry success requires:

- Var submit ok.
- Lighter order reached at least `live_submit_sent` or `lighter_filled`.
- Fill records can be matched or at least order submit records are present.

If either side fails:

- Write `manual_review_required`.
- Stop all new entries and exits.
- Log exact reason to `order_metrics.jsonl`.
- Do not assume the system is flat.

## Exit Logic

Exit one lot at a time, FIFO per direction.

Exit triggers:

- Current edge for that lot's direction is `<= exit_bps`.
- `holding_samples >= min_hold_samples`.
- Or `holding_samples >= max_hold_samples`.
- Or unrealized loss is worse than `max_unrealized_loss_bps`.

V1 should submit at most one exit at a time.

Use `reduceOnly=true` on Variational exit.

Exit side:

- Opposite of the Var entry side for the lot direction.

Exit success requires:

- Var exit submit ok.
- Lighter exit submit started/sent or filled.
- Lot status updated to `closed` only after both sides are known to have been submitted or filled.

If exit submit fails after one side may have moved:

- Write `manual_review_required`.
- Stop the runtime.
- Do not submit any more inventory actions.

## Manual Review Conditions

Enter `manual_review_required` immediately on:

- Var entry submit failed.
- Lighter entry submit failed.
- Var exit submit failed.
- Lighter exit submit failed.
- Duplicate exit attempt for the same lot.
- State file cannot be written.
- State file cannot be loaded.
- Unknown lot status.
- Asset mismatch.
- Qty mismatch above tolerance.
- Any exception during live submit.

When manual review is required:

- Write `log/live_inventory_state.json` with status `manual_review_required`.
- Append `live_inventory_manual_review_required` to `order_metrics.jsonl`.
- Log a clear runtime warning.
- Refuse all further live inventory actions until restart and explicit reset.

## Logs

New file:

- `log/live_inventory.jsonl`

Events:

- `live_inventory_enter_submitted`
- `live_inventory_entered`
- `live_inventory_exit_submitted`
- `live_inventory_exited`
- `live_inventory_manual_review_required`
- `live_inventory_guard_blocked`

Each event should include:

- `asset`
- `lot_id`
- `direction`
- `qty`
- `edge_bps`
- `var_side`
- `var_price`
- `lighter_price`
- `var_order_id` if available
- `lighter_order_id` if available
- `pnl_usd` for exits
- `pnl_bps` for exits
- `holding_samples` for exits
- `open_lots_total`
- `open_lots_direction`
- `realized_pnl_usd`
- `state_status`

## Analyzer

Add a live analyzer later, separate from paper:

- `tools/analyze.py`

It should output:

- entered
- exited
- manual review count
- current open lots
- realized PnL
- by-direction PnL
- losing exits
- max open lots
- latest events

## Implementation Plan

Step 1: state and engine layer.

- Add `LiveInventoryLot` and `LiveInventoryState` models.
- Add load/write helpers for `log/live_inventory_state.json`.
- Add unit tests for state roundtrip and startup blocking.

Step 2: CLI guards only.

- Add CLI flags.
- Validate V1 hard caps.
- Validate required transports.
- Validate BTC-only.
- Do not submit orders yet.

Step 3: live inventory decision engine dry path.

- Reuse paper inventory edge calculations.
- Log intended entry/exit decisions without submitting.
- Confirm decisions match paper inventory on the same stream.

Step 4: one-lot live entry only.

- Allow `max_total_lots=1` only.
- Submit entry concurrently.
- Persist open lot.
- Stop after one entry for manual observation.

Step 5: one-lot live exit only.

- Exit the persisted lot.
- Persist closed lot and realized PnL.
- Stop after one completed lot.

Step 6: allow `max_total_lots=2` or `3`.

- Only after one-lot entry/exit has worked several times.
- Keep one pending action at a time.

## First Live Test Parameters

Do not use the paper size.

First live test should use:

- `lot_notional_usd=10`
- `max_lots=1`
- `max_total_lots=1`
- `entry_bps=50`
- `exit_bps=10`
- `max_cycles=1`

Only after repeated successful one-lot tests:

- `max_total_lots=2`
- then `max_total_lots=3`

## Readiness Checklist

Live inventory implementation is not ready until all are true:

- Unit tests cover CLI guards.
- Unit tests cover state persistence.
- Unit tests cover manual review transitions.
- Paper inventory still passes.
- Live inventory dry decision mode matches paper inventory decisions.
- Startup blocks if state is not flat.
- Any submit failure writes manual review state.
- Analyzer can summarize live inventory logs.
