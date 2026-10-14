# Mainnet ETH anchor collection

`tools/mainnet_anchor_collect.py` uses the mainnet engine in observe mode, with
inventory decisions disabled and explicit order-method guards. It needs the
Variational browser forwarder and mainnet public market data; it does not need
Lighter trading keys. The Variational page must remain logged in and the browser
forwarder must remain connected.

New samples go to `log/mainnet_anchor/basis_samples/ETH`. The live mainnet history
loader merges this directory with `log/basis_samples/ETH`. It excludes records
explicitly tagged for another Lighter venue, filters timestamps to the latest
30 days, and retains only fields used by anchor calculations. Do not copy RH
state files or untagged RH market history into this project.

The collector records strategy-compatible, valid, unique passive observations
at least 31 seconds apart. This respects the strategy's 30-second reload sampler
without counting duplicate reference frames or manufacturing missing history.

## VPS installation

After synchronizing this version, run from the original mainnet checkout:

To restart RH trading and install the concurrent follower service together, use
`bash deploy/start_rh_and_mainnet_collection.sh`. It verifies local state, asks for
actual venue-position confirmation, resets a flat halted state through the
existing exchange-verification command, and preserves open positions by resuming
them. RH trading uses its existing configuration and exit logic; this collection
update does not fix RH's observed execution-price shortfall.

```bash
cd /home/ubuntu/Repository-name-variational-v1
sudo cp deploy/systemd/var-mainnet-anchor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now var-mainnet-anchor.service
```

The supplied unit uses this checkout path and follows fresh Var observations from
the RH strategy's `log/basis_samples` directory. It does not own any Var forwarder
ports, does not reuse RH book prices, and computes mainnet fills for the exact
base quantity of each Var observation. Both processes can run concurrently.
Without `--source-root`, the standalone mode owns ports 8866, 8867, and 8868.
Adjust the unit paths if the saved checkouts live elsewhere. Follower mode needs
its source process running; a running service with no advancing samples is not ready.

Verify the process and data separately:

```bash
systemctl is-active var-mainnet-anchor.service
cat log/mainnet_anchor/anchor_health.json
.venv/bin/python tools/mainnet_anchor_collect.py --status
```

An active process alone is not evidence of valid data. Check that `last_sample_at`
is advancing. The status command evaluates both directions using the actual
strategy threshold function, without bypassing the recent-health gate. Its
projection assumes continuous valid 30-second samples; the collector's real
31-second cadence, outages, and rejected observations can extend this time.

The anchor uses a rolling seven-day window. Current code requires at least
129600 effective sample-seconds in that window (36 hours), not seven complete
calendar days. Recent health and fresh latest samples must also qualify. Existing
compatible mainnet history can reduce the remaining warmup; expired history and
RH data cannot. `ready_now` describes data readiness only, not credentials,
balances, startup reconciliation, or permission to send real orders.

Before starting live mainnet trading, stop this collector. Keep both sample directories; the live engine loads them at
startup. Trading changes, account setup, and funding migration are separate from
this data-collection service.
