# RWA two-venue probe (20 USDC per leg)

This tool is separate from the read-only capture service. It is used to measure
the real price path, quote-to-fill slippage and post-fill adverse move for each
same-symbol venue pair.

## Instruments

Default: `NVDA`, `SNDK`, `CRCL`, `AAPL`, `QQQ` on
`variational`, `lighter_mainnet`, `lighter_rh`, `entropy`.

The first four are available on all four venues. QQQ is present on Var and both
Lighter books. Every asset leg is 20 USDC notional. The plan contains both
directions for every available venue pair: 54 rounds for the default set.

## Hard gates

```bash
# 1. Plan only; no network/order calls
.venv/bin/python tools/venue_probe.py --mode plan

# 2. Read-only account/market preflight; no orders
.venv/bin/python tools/venue_probe.py --mode preflight

# 3. Live, only after explicit confirmation and all preflight keys ok
.venv/bin/python tools/venue_probe.py --mode run   --execute --confirm-live-probes --confirm-flat --i-understand-live-orders
```

Before entry, both venue account positions and active orders must be verified
flat. A round writes its deterministic client order ids to
`log/venue_probe/state.json` before sending. Unknown terminal status moves the
run to `manual_review_required`; it never retries the same round. If one leg
fills and the other does not, the filled leg is closed reduce-only once.

The runner records entry quotes, order submit/ack timestamps, final fills,
0.25/0.5/1/3-second adverse marks, and both exit fills. Fees are reported
separately.

## Cost decomposition

`tools/probe_report.py` computes, per leg:

- half-spread paid against the contemporaneous mid;
- fill slippage against the contemporaneous executable BBO;
- entry and exit price cost without double counting;
- gross PnL in USDC and bps; fees separately;
- per-asset/venue p50/p80 and a suggested trigger floor.

```bash
.venv/bin/python tools/probe_report.py
```

The suggestion is research output. It does not change live thresholds.

## Entropy

Entropy quotes and public fills are captured. Live Entropy orders are blocked
until a trading signer private key is provisioned; `ENTROPY_ACCOUNT_ADDRESS`
alone is only sufficient for read-only `userFills`.
