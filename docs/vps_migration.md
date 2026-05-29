# VPS Migration Guide

## Scope

This project is currently ready to migrate to a VPS for:

- `observe`
- `paper`
- log collection
- unified ledger export
- migration readiness checks

It is not ready for unattended `live` trading yet.

Do not run unattended `live` on the VPS until these are implemented and verified:

- real cancel flow
- automatic rollback flow
- partial-fill handling
- complete live order status reconciliation

## Recommended Workflow

Use the local machine as the source-of-truth codebase.

Recommended deployment flow:

1. Edit code locally.
2. Verify locally.
3. Commit and push with git.
4. Pull the latest version on the VPS.
5. Run only from the pulled version on the VPS.

Avoid editing core code directly on the VPS:

- `main.py`
- `paper_engine.py`
- `tools/*.py`
- trading logic
- risk logic
- ledger logic

VPS-local files that may be edited directly:

- `.env`
- shell startup scripts
- `systemd` / `tmux` / `screen` service wrappers
- log retention settings

## Local Preflight

From Windows PowerShell on the local machine:

```powershell
Set-Location "D:\my project\arbitrage-system\variational-v1"
.\.venv\Scripts\python.exe tools\preflight_migration_check.py --since 2026-05-28
```

The preflight should confirm:

- `.venv` Python exists
- `requirements.txt` exists
- core scripts compile
- `main.py --help` works
- unified ledger export works
- migration summary works
- no running `main.py` instance is detected unless expected

## VPS Setup

On the VPS, clone or pull the repository first.

Linux example:

```bash
git clone <REPO_URL> variational-v1
cd variational-v1
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python main.py --help
```

If updating an existing VPS checkout:

```bash
cd variational-v1
git pull
. .venv/bin/activate
pip install -r requirements.txt
python main.py --help
```

## Chrome / Forwarder Requirement

This runtime still depends on the Chrome extension forwarder.

Before trusting VPS results, confirm the VPS can run:

- Chrome or Chromium
- the unpacked extension from `chrome_extension/`
- Variational page login/session
- local forwarder ports:
  - `127.0.0.1:8766`
  - `127.0.0.1:8767`

The first VPS milestone is not profit. The first milestone is a stable data path:

```text
Chrome/Variational page -> extension -> Python runtime -> logs -> unified ledger -> summary
```

## First VPS Run

Start with `paper` or `observe` only.

Recommended first run:

```bash
. .venv/bin/activate
python main.py --mode paper
```

Safer observation-only run:

```bash
. .venv/bin/activate
python main.py --mode observe
```

Do not start a second `main.py` instance while one is running.

Do not run `live` unattended.

## VPS Ledger Check

After the VPS has collected data:

```bash
. .venv/bin/activate
python tools/export_unified_ledger.py
python tools/summarize_unified_ledger.py --since YYYY-MM-DD
```

Interpretation:

- `PASS`: no open risk and enough positive paper sample to proceed cautiously
- `WATCH`: no open risk, but not enough fresh paper sample yet
- `WARN`: do not proceed to live; inspect risk and failure fields first

Important fields:

- `risk_open_items`
- `naked`
- `failed`
- `pending`
- `blocked_before_submit`
- `paper_closed`
- `total_net_pnl`

## When To Consider Small Live Calibration

Only consider small live calibration after VPS `paper` / `observe` is stable and:

- `risk_open_items=0`
- `naked=0`
- `failed=0`
- fresh `paper_closed` samples exist
- paper result is not clearly negative
- the user is actively watching the run

First live calibration should remain semi-automatic:

- user manually trades on Variational
- the program only submits the Lighter hedge
- use one asset first, preferably `BTC`
- use a tiny notional, around `20-30u`
- run one trade, stop, then review logs

Stop immediately if any of these appear:

- `naked > 0`
- `failed > 0`
- abnormal `pending`
- unexpected asset
- unexpected notional or qty
- Chrome extension instability
- stale quotes or missing Lighter order book
