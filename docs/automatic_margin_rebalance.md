# Automatic margin rebalance design

## Scope

The rebalance controller moves collateral between the two venue accounts. It is
separate from order execution and is disabled until both venue adapters pass a
real-money integration test.

The controller never changes position direction or counts a transfer as PnL.

## Trigger and target

- Use fresh settled equity from both venues.
- Create a plan when the smaller/larger equity ratio is below `0.74`.
- Pause new child entries before creating a transfer.
- Target a post-transfer equity imbalance of `6.5%`, not exact equality.
- Do not use a time cooldown. A new plan is allowed immediately after the
  previous plan reaches a terminal state and fresh account data still requires
  another transfer.
- Only one transfer may be active at a time.

Open positions do not prevent planning. They do prevent risk limits from being
raised. After a confirmed transfer, both account snapshots and margin metrics
must be refreshed; adding may resume only when the normal risk calculation
allows it.

## State machine

1. `idle`: no transfer needed.
2. `pause_add`: persist the plan and block all new child entries.
3. `preflight`: verify fresh equity, destination allowlist, asset, chain, fee,
   minimum amount, open transfer history, idempotency key, source withdrawable
   balance, and source maintenance-margin plus safety buffer after transfer.
4. `submit_withdrawal`: submit one withdrawal from the source venue to the
   dedicated bound Arbitrum wallet. Any required wallet authorization remains
   manual in assisted mode.
5. `confirm_wallet_credit`: require a final withdrawal identifier and confirm
   the expected USDC credit in the dedicated wallet.
6. `submit_deposit`: prepare one deposit from the wallet to the destination
   venue. Any required wallet authorization remains manual in assisted mode.
7. `confirm_venue_credit`: require the destination venue to credit the expected
   amount within the configured fee tolerance.
8. `refresh_risk`: fetch both venues again and rerun leverage, maintenance
   margin and equity-balance checks.
9. `complete`: clear the active plan and allow entry only if risk is normal.
10. `manual_review`: keep entries blocked; never retry with a new idempotency key.

Process restarts resume the persisted non-terminal state. They never create a
second transfer.

There is no transfer cooldown. A rapid market reversal can create a new plan
only after the previous request is terminal and fresh post-transfer equity still
requires balancing. During an active request, add-ons remain paused; existing
positions may still reduce or exit.

## Adapter requirements

Each venue adapter must implement:

- `quote_transfer(asset, network, amount)`
- `prepare_withdrawal(idempotency_key, wallet_address, amount)`
- `get_withdrawal_status(withdrawal_id)`
- `prepare_deposit(idempotency_key, wallet_address, amount)`
- `find_venue_credit(wallet_tx_id, expected_net_amount)`
- `get_fresh_account_snapshot()`

The wallet bridge must independently verify the Arbitrum chain ID, USDC token,
allowlisted venue contracts and wallet balance changes. The strategy process
must not receive the wallet seed phrase or unrestricted private key. Assisted
mode may prepare browser or on-chain intents, but the user confirms each wallet
authorization. A future unattended signer must be a separate policy-restricted
service, not a key stored in the live strategy `.env`.

Secrets and destination addresses are configured outside the repository. An
execute-mode adapter must reject an address or network not present in the
allowlist.

Lighter supports signed secure withdrawals and transfers. Secure withdrawals
can only return to the account's controlling L1 address; other routes may
require the Ethereum private key. Variational execution remains disabled until
its official deposit/withdraw route, finality signal and limits are verified.

Before execute mode can be enabled, configure outside the repository:

- the allowed collateral asset and exact network in both directions;
- the controlling or allowlisted destination address for each venue;
- official source-transfer and destination-credit APIs or signed adapters;
- minimum transfer, fee tolerance, confirmation rule, and timeout;
- secret references in the host environment, never keys in source or chat;
- a USD 1-5 end-to-end transfer test in each supported direction.

## Accounting

Every plan records source debit, transfer fee and destination credit. Internal
movement is excluded from combined-account cash flow. Only the fee changes net
strategy equity. External deposits and withdrawals are recorded separately and
subtracted from account-equity return calculations.
