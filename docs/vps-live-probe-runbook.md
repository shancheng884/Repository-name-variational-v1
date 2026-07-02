# VPS Live + Probe Runbook

## 1. Update Code

```bash
cd ~/Repository-name-variational-v1
git pull --ff-only
source .venv/bin/activate
```

## 2. Start Probe Observe

Open a new SSH window for probe.

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate
bash tools/start_probe_observe.sh
```

Use a separate Chrome profile for probe. Configure the extension endpoints:

```text
wsEndpoint: ws://127.0.0.1:8776
restEndpoint: ws://127.0.0.1:8777
commandEndpoint: ws://127.0.0.1:8778
```

Do not use the live Chrome profile for probe.

## 3. Check Current Live + Candidates

Open another SSH window.

```bash
cd ~/Repository-name-variational-v1
source .venv/bin/activate
bash tools/check_live_and_candidates.sh SOL
```

If the tool prints `SWITCH_CANDIDATE`, manually confirm both exchanges are flat before using the printed live command.

## 4. Manual Safety Rules

Do not start a new live process if:

```text
status=manual_review_required
status is not flat and you did not intentionally resume open state
Variational SOL/BTC/ETH position is not 0
Lighter SOL/BTC/ETH position is not 0
another python main.py is running
```

Check process:

```bash
cd ~/Repository-name-variational-v1
ps aux | grep "python main.py" | grep -v grep
```

Do not delete `log/main.instance.lock` blindly.

## 5. Stop

Use `Ctrl+C` in the foreground window to stop live or probe.
