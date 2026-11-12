from pathlib import Path

from tools.market_capture.sysmon import (
    LoopLagMonitor, cpu_percentages, parse_proc_stat, read_processes,
)


def test_parse_proc_stat_and_cpu_percentages():
    before = parse_proc_stat("cpu  100 0 100 800 0 0 0 0 0 0\ncpu0 50 0 50 400 0 0 0 0 0 0\n")
    after = parse_proc_stat("cpu  200 0 200 1400 0 0 0 0 0 0\ncpu0 100 0 100 700 0 0 0 0 0 0\n")
    rows = cpu_percentages(before, after)
    assert rows[0]["cpu"] == "cpu"
    assert rows[0]["busy_pct"] == 25.0


def test_read_processes_matches_marker_and_cpu(tmp_path):
    proc = tmp_path / "proc"
    pid = proc / "123"
    pid.mkdir(parents=True)
    (pid / "cmdline").write_bytes(b"/usr/bin/python\0main.py\0")
    stat = ["0"] * 52
    stat[11] = "150"
    stat[12] = "50"
    (pid / "stat").write_text("123 (python) " + " ".join(stat))
    (pid / "status").write_text("Name:\tpython\nVmRSS:\t2048 kB\n")
    rows, ticks = read_processes(("main.py",), previous={123: 100}, elapsed=1.0, proc=proc)
    assert rows[0]["name"] == "main.py"
    assert rows[0]["cpu_pct_one_core"] == 100.0
    assert ticks[123] == 200


def test_loop_lag_snapshot_and_window():
    monitor = LoopLagMonitor()
    monitor.samples.extend([(100.0, 1.0), (101.0, 2.0), (102.0, 50.0)])
    snap = monitor.snapshot()
    assert snap["samples"] == 3 and snap["max_ms"] == 50.0
