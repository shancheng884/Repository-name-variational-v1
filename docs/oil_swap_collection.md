# Read-Only USOIL / WTI Collection

`tools/oil_swap_collector.py` collects indicative Variational USOIL swap quotes
(`USOILP`, instrument kind `commodity`) and Lighter mainnet WTI order-book depth.
It is a separate read-only process:
it does not start another forwarder, submit orders, or modify the ETH inventory
journal. Samples are written under `log/oil_swap/basis_samples/WTI`, separate
from ETH anchor history.

The collector requires the existing Variational browser extension command
forwarder on `127.0.0.1:8768`. Keep the authenticated Variational tab and the ETH
live process running. The collector sends only `VAR_API_QUOTE` requests with
`confirm: false`; it will refuse to start if the Lighter WTI market metadata is
not active or is frozen. It uses the current Lighter minimum size, precision,
multiplier, and minimum notional to choose a base quantity, then measures both
venues at the same quantity.

Start in a separate tmux session after the ETH live process is healthy:

```bash
cd /home/ubuntu/Repository-name-variational-v1
SESSION="oil-swap-$(date -u +%m%d%H%M%S)"
LOG="log/${SESSION}.log"
mkdir -p log
tmux new-session -d -s "$SESSION" \
  "cd /home/ubuntu/Repository-name-variational-v1 && exec .venv/bin/python tools/oil_swap_collector.py --reuse-forwarder --output-dir log/oil_swap --notional-usd 20 --interval-seconds 10 >> '$LOG' 2>&1"
echo "session=$SESSION log=$LOG"
```

Inspect `log/oil_swap/basis_samples/health.json` for `availability_state`,
`var_quote_state`, `lighter_book_state`, `samples_this_run`, and `last_sample_at`.
`both_feeds_live` means both feeds supplied fresh data; it is not a claim that
the exchange is open for trading. Feed failures are recorded as
`partial_or_unavailable`; the collector does not infer a holiday or scheduled
market closure from missing quotes.
