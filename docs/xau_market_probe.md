# XAU Market Probe

`tools/probe_xau_markets.py` is the first-stage XAU integration probe. It does
not modify ETH state, does not import trading credentials, and never sends
`VAR_API_ORDER` or any order request.

## Safe default

The default mode checks the local extension command connection and records
whether the forwarded passive `/prices` event file contains XAU evidence. It
does not consume a Variational RFQ:

```bash
python tools/probe_xau_markets.py
```

The result is written to `log/xau_market_probe.json`.

## Optional indicative RFQ check

Only when explicitly requested, the probe sends exactly one indicative RFQ for
each instrument type, for a maximum of two RFQs total:

```bash
python tools/probe_xau_markets.py --allow-rfq --qty 0.004
```

The two payloads use `instrument_type=perpetual_future` and
`instrument_type=swap`. The tool stores only sanitized quote fields and never
persists the raw response or quote body.

This phase validates instrument payload support only. It does not add XAU to
the live strategy, does not change the ETH process, and does not route orders.
