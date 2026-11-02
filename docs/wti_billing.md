# Read-Only WTI Billing Reconciliation

`tools/wti_billing_audit.py` is a separate audit, not an execution gateway.
It never writes live state, submits orders, changes the 10 bps cost budget,
restarts services, or enables rotation. Run it independently of the live process.

## Lighter Evidence

The authenticated `/api/v1/export` request uses millisecond bounds and market
145 for trade and funding CSVs. Download generation may initially return an S3
`NoSuchKey`; only that response is retried with a bounded backoff. Account auth
is never sent to download hosts, redirects are disabled, and signed links are
not printed or stored. Trade CSV rows must match raw exact-order trades by ID,
account, market, side, role, time, size and price. An explicit CSV zero fee is
valid evidence; a missing API fee is not. Nonzero fees need currency evidence.

Funding CSVs are reconciled with `/api/v1/positionFunding`, whose request bounds
are seconds and whose market parameter is `market_ids`. Signed payments must
match, and each funded position must match the final-fill exposure timeline.
Incomplete pages, ambiguous funding/fill boundaries and inconsistent evidence
remain unverified. CSV exports have UTC timestamps.

## Variational Evidence

Export Trades, Funding History, Realized PnL and account Transfers for a window
covering every leg, including earlier legs recovered before hedge completion.
Keep the active forwarding trading page unchanged; use an independent account
tab for account history. Never reload the extension just to obtain a bill.
The official CSV's `USOIL` with `instrument_type=swap` is explicitly mapped to
API `USOILP`; other instruments are not accepted by this alias.

Provide `--variational-trades-csv`, one or more paths after
`--variational-transfers-csv`, and `--variational-coverage-json`. The coverage
manifest must attest venue `variational_omni`, nonempty account scope,
timezone `UTC`, settlement `USDC`, `cashflow_sign_convention` equal to
`signed_account_delta`, `complete=true`, `all_pages_exported=true`, and
`start_at`/`end_at` covering the entire round. It binds `trades_sha256` and
`transfers_sha256`. Multiple cash CSVs additionally require ordered
`cash_export_source_hashes`; their aggregate hash is computed with
`merge_cash_exports`. Identical overlapping IDs across exports are deduplicated;
conflicting IDs and duplicate IDs inside a single export are rejected.

Completeness is an explicit operator attestation, not a claim established by a
file hash. Do not attest complete coverage if any category or export failed.
Absent fields, an empty UI view, a current fee tier, and a swap funding interval
are not proofs of historical zero fees or zero funding. Deposit/withdrawal fees
are excluded from round trading costs. Realized cash must equal final-fill PnL.

If the fee/account-transfer export fails, explicitly use `complete=false` and
`covered_cashflow_types=["funding","realized_pnl"]` for complete exports of just
those categories. Record the failure in `export_failures`. Matching trades,
realized PnL and funding remain individually auditable, but actual net and Var
fees remain null. `net_before_unverified_variational_fees_usdc` is explicitly
before the unknown Var cost, not a certified net profit. Never replace this
with zero simply to make the audit pass.

## Outputs

Use `--output-dir` with a new private artifact directory. The tool archives the
execution snapshot, source CSVs and raw Lighter responses, coverage and audit.
With `--lighter-bundle`, the audit can be reproduced offline. No credentials or
signed download links belong in artifacts or version control. Partial evidence
returns exit code 2 and does not invent net PnL. A fully verified result computes
net as final-fill gross minus actual fees plus signed funding; slippage is
already in the final-fill gross and must not be subtracted twice.

## Complete Raw Ledger When CSV Export Is Empty

An export job returning `error=empty_export` is diagnostic information, **not**
zero-fee evidence. An optional `--variational-ledger-har` accepts a sanitized,
filtered capture from the independent authenticated Portfolio tab. Keep only
the three `/api/transfers` queries and their optional export-job requests.
Never include Cookie/Authorization headers or signed download links.

The coverage manifest additionally requires `raw_ledger_capture` with `sha256`,
the explicitly observed `wallet_address`, `company_id`, and
`account_scope_confirmed=true`. Each request must use the same
`vr-connected-address`. Cash rows must belong to that company, and realized PnL
must match the persisted closing RFQ and CSV cash IDs, amounts, times and final
fills. The hash binds the capture, not its authenticity: operator confirmation
of the authenticated account remains required.

Required query categories are `deposit,withdrawal,fee`, `funding`, and
`realized_pnl`, without asset/status filters hiding records. Each must cover the
entire round, start at offset zero, return HTTP 200, include an explicit
`next_page=null` and `object_count` matching all returned rows. Currently only
complete first pages below their row limit are supported; additional/missing
pages fail closed. Duplicate identical responses are deduplicated; conflicting
responses fail. Captures must follow the closed round and span at most ten
minutes. A complete empty fee/funding response can prove zero; an empty export,
missing response or absent API field cannot. Keep the old CSV export failure
and `complete=false` declaration rather than rewriting them as successes.

## Independent Records And Telegram

Archive a verified result in a new private directory beneath
`log/wti_billing_records/`. Each archive contains `audit.json`,
`execution-snapshot.json`, `lighter-sources.json` and `variational-sources.json`.
Publish a finished directory atomically; never expose a partially copied
directory to the daily reader. Do not overwrite an earlier audit.

Historical records can be registered with `wti_billing_audit.py
--register-for-reporting` only when all source files and the sanitized raw
ledger are supplied. A legacy unknown closing receipt is reconstructed in the
archive only from one unique confirmed official trade matching the exact close
RFQ cashflow. The archive retains the original snapshot and a deterministic
reconstruction manifest; the live state is never repaired or rewritten.

The daily report recomputes the entire archived audit offline, then matches its
execution fingerprint to the current closed round before using its net value.
It never writes WTI state. Duplicate archives of the same round count once;
tampered sources/results and mismatching fills remain unverified. Other rounds
without evidence remain explicitly unknown. Cumulative **verified** net is
separate from the number of still-unverified rounds, and never includes future
Beijing-day closes.

`tools/wti_billing_report.py --dry-run` previews verified billing corrections.
Without `--dry-run` it sends a separately labelled billing confirmation, not a
new-trade notification. The existing daily timer also sends newly verified
billing updates, even when that day's daily report was already delivered.
Both use independent `log/wti_billing_telegram_state.json`, not the live trade
outbox. A process lock and durable `sending` marker prevent concurrent or
restarted senders repeating a notification. Unknown delivery is not retried;
only a definite 429 or missing local credentials leaves a retryable record.
Existing daily sent keys remain honored; deployment does not replay old daily
reports. A corrupt delivery file is preserved for manual review, not reset.
For a one-time consolidated correction, `daily_pnl_report.py --asset ALL
--day YYYY-MM-DD --correction` uses a separate durable key, leaves normal daily
sent keys unchanged, and suppresses per-round notices already covered by that
correction.

This result does not authorize live trading or alter thresholds. Existing live
fee fields remain unchanged. The 10 bps budget, two-second freshness gate,
120-second gap gate and position limits are not inputs rewritten by reporting.
Deploy only the audit/reporting files; no restart of WTI, the collector or the
watchdog is required. Future rounds without archived billing stay unverified.
