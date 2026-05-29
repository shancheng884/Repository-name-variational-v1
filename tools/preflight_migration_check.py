import argparse
import csv
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def venv_python() -> Path:
    if os.name == "nt":
        return ROOT / ".venv" / "Scripts" / "python.exe"
    return ROOT / ".venv" / "bin" / "python"


def run_step(name: str, command: list[str], timeout: int = 120) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except Exception as exc:
        return False, f"{name}: exception: {exc}"

    output = "\n".join(part for part in [proc.stdout.strip(), proc.stderr.strip()] if part)
    if proc.returncode != 0:
        return False, f"{name}: exit={proc.returncode}\n{output}"
    return True, output


def has_running_main_py() -> bool:
    if os.name != "nt":
        ok, output = run_step("process_check", ["ps", "-eo", "args"], timeout=30)
        return ok and "main.py" in output

    ps_command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { ($_.Name -eq 'python.exe' -or $_.Name -eq 'pythonw.exe') -and $_.CommandLine -match 'main.py' } | "
        "Select-Object -ExpandProperty CommandLine"
    )
    ok, output = run_step("process_check", ["powershell", "-NoProfile", "-Command", ps_command], timeout=30)
    return ok and bool(output.strip())


def ledger_has_rows(path: Path) -> bool:
    if not path.exists():
        return False
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return any(True for _ in reader)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local checks before migrating this project to a VPS.")
    parser.add_argument("--since", default="", help="Optional ISO date/string passed to summarize_unified_ledger.py")
    parser.add_argument(
        "--allow-running-main",
        action="store_true",
        help="Do not fail if an existing main.py process is detected",
    )
    args = parser.parse_args()

    py = venv_python()
    checks: list[tuple[str, bool, str]] = []

    checks.append(("project_root", ROOT.exists(), str(ROOT)))
    checks.append(("venv_python", py.exists(), str(py)))
    checks.append(("requirements", (ROOT / "requirements.txt").exists(), "requirements.txt"))
    checks.append(("main_py", (ROOT / "main.py").exists(), "main.py"))

    running_main = has_running_main_py()
    checks.append(("single_instance", args.allow_running_main or not running_main, "running main.py detected" if running_main else "none"))

    if py.exists():
        compile_cmd = [
            str(py),
            "-m",
            "py_compile",
            "main.py",
            "tools/export_unified_ledger.py",
            "tools/summarize_unified_ledger.py",
        ]
        ok, output = run_step("py_compile", compile_cmd)
        checks.append(("py_compile", ok, output))

        ok, output = run_step("main_help", [str(py), "main.py", "--help"])
        checks.append(("main_help", ok, output.splitlines()[0] if output else "ok"))

        ok, output = run_step("export_unified_ledger", [str(py), "tools/export_unified_ledger.py"])
        checks.append(("export_unified_ledger", ok, output))

        summary_cmd = [str(py), "tools/summarize_unified_ledger.py"]
        if args.since:
            summary_cmd.extend(["--since", args.since])
        ok, output = run_step("summarize_unified_ledger", summary_cmd)
        checks.append(("summarize_unified_ledger", ok, output))

        checks.append(("unified_ledger_rows", ledger_has_rows(ROOT / "log" / "unified_ledger.csv"), "log/unified_ledger.csv"))

    failed = [(name, detail) for name, ok, detail in checks if not ok]
    print("preflight migration check")
    for name, ok, detail in checks:
        status = "PASS" if ok else "FAIL"
        detail_one_line = str(detail).replace("\n", " | ")
        if len(detail_one_line) > 240:
            detail_one_line = detail_one_line[:237] + "..."
        print(f"{status} {name}: {detail_one_line}")

    if failed:
        print("result: FAIL")
        return 1
    print("result: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
