from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
import asyncio
import math
import os
import platform
import time

PROC = Path("/proc")
CLOCK_TICKS = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096
PROCESS_MARKERS = (
    "main.py",
    "wti_live.py",
    "oil_swap_collector.py",
    "eth_passive_collector.py",
    "market_capture.py",
    "chrome",
)


def parse_proc_stat(text: str) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    fields = (
        "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal",
        "guest", "guest_nice",
    )
    for line in text.splitlines():
        if not line.startswith("cpu"):
            continue
        parts = line.split()
        name = parts[0]
        values = [int(x) for x in parts[1:] if x.isdigit()]
        row = {field: values[index] for index, field in enumerate(fields) if index < len(values)}
        out[name] = row
    return out


def cpu_percentages(previous: dict[str, dict[str, int]], current: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    rows = []
    for name, now in current.items():
        before = previous.get(name)
        if before is None:
            continue
        delta = {key: max(0, now.get(key, 0) - before.get(key, 0)) for key in now}
        total = sum(delta.values())
        if total <= 0:
            continue
        idle = delta.get("idle", 0) + delta.get("iowait", 0)
        rows.append({
            "cpu": name,
            "busy_pct": round((total - idle) / total * 100.0, 2),
            "steal_pct": round(delta.get("steal", 0) / total * 100.0, 2),
            "iowait_pct": round(delta.get("iowait", 0) / total * 100.0, 2),
            "user_pct": round(delta.get("user", 0) / total * 100.0, 2),
            "system_pct": round(delta.get("system", 0) / total * 100.0, 2),
        })
    return rows


def read_meminfo(path: Path = PROC / "meminfo") -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        text = path.read_text()
    except OSError:
        return out
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        parts = rest.split()
        if parts and parts[0].isdigit():
            out[key.strip()] = int(parts[0])
    return out


def read_loadavg(path: Path = PROC / "loadavg") -> tuple[float, float, float]:
    try:
        parts = path.read_text().split()
        return float(parts[0]), float(parts[1]), float(parts[2])
    except (OSError, ValueError, IndexError):
        return 0.0, 0.0, 0.0


def _process_name(cmdline: bytes) -> str | None:
    args = [part for part in cmdline.split(b"\0") if part]
    if not args:
        return None
    for arg in args:
        name = arg.replace(b"\\", b"/").rsplit(b"/", 1)[-1].decode("utf-8", "replace")
        if name.endswith(".py") or name in {"chrome", "chromium"}:
            return name
    return args[0].decode("utf-8", "replace")[:60]


def read_processes(
    markers: Iterable[str] = PROCESS_MARKERS,
    *,
    previous: dict[int, int] | None = None,
    elapsed: float = 60.0,
    proc: Path = PROC,
) -> tuple[list[dict[str, Any]], dict[int, int]]:
    markers = tuple(markers)
    rows: list[dict[str, Any]] = []
    ticks: dict[int, int] = {}
    for entry in proc.glob("[0-9]*"):
        pid = int(entry.name)
        try:
            cmdline = (entry / "cmdline").read_bytes()
            name = _process_name(cmdline)
            if not name or not any(marker in name for marker in markers):
                continue
            stat = (entry / "stat").read_text().split()
            utime, stime = int(stat[13]), int(stat[14])
            rss_pages = 0
            status = (entry / "status").read_text()
            for line in status.splitlines():
                if line.startswith("VmRSS:"):
                    rss_pages = int(line.split()[1])
                    break
        except (OSError, ValueError, IndexError):
            continue
        total_ticks = utime + stime
        ticks[pid] = total_ticks
        cpu_pct = 0.0
        if previous and pid in previous and elapsed > 0:
            cpu_pct = max(0.0, (total_ticks - previous[pid]) / CLOCK_TICKS / elapsed * 100.0)
        rows.append({
            "pid": pid,
            "name": name,
            "cpu_pct_one_core": round(cpu_pct, 2),
            "rss_mb": round(rss_pages * PAGE_SIZE / (1024 ** 2), 1)
            if rss_pages else None,
        })
    return rows, ticks


class SystemSampler:
    def __init__(self, *, system_seconds: float = 60.0) -> None:
        self.system_seconds = system_seconds
        self._previous_cpu: dict[str, dict[str, int]] = {}
        self._previous_process: dict[int, int] = {}
        self._last_sample = 0.0

    def sample(self, *, now: float | None = None) -> dict[str, Any]:
        now = now if now is not None else time.monotonic()
        elapsed = max(0.001, now - self._last_sample) if self._last_sample else self.system_seconds
        self._last_sample = now
        current_cpu: dict[str, dict[str, int]] = {}
        if platform.system() == "Linux":
            try:
                current_cpu = parse_proc_stat((PROC / "stat").read_text())
            except OSError:
                current_cpu = {}
        cpu_rows = cpu_percentages(self._previous_cpu, current_cpu) if self._previous_cpu else []
        self._previous_cpu = current_cpu
        try:
            processes, ticks = read_processes(previous=self._previous_process, elapsed=elapsed)
        except Exception:
            processes, ticks = [], {}
        self._previous_process = ticks
        mem = read_meminfo()
        load1, load5, load15 = read_loadavg()
        return {
            "schema": 1,
            "event": "system",
            "at": datetime.now(timezone.utc).isoformat(),
            "platform": platform.system(),
            "load1": load1,
            "load5": load5,
            "load15": load15,
            "mem_total_mb": round(mem.get("MemTotal", 0) / 1024, 1),
            "mem_available_mb": round(mem.get("MemAvailable", 0) / 1024, 1),
            "swap_total_mb": round(mem.get("SwapTotal", 0) / 1024, 1),
            "swap_free_mb": round(mem.get("SwapFree", 0) / 1024, 1),
            "cpu": cpu_rows,
            "processes": processes,
        }


class LoopLagMonitor:
    """Measure event-loop scheduling delay and record >100 ms stalls."""

    def __init__(self, *, interval: float = 0.05, stall_ms: float = 100.0, window_seconds: float = 300.0) -> None:
        self.interval = interval
        self.stall_ms = stall_ms
        self.window_seconds = window_seconds
        self.samples: deque[tuple[float, float]] = deque()
        self.stalls: deque[dict[str, Any]] = deque(maxlen=1000)
        self._task: asyncio.Task[Any] | None = None
        self._stop = asyncio.Event()
        self.writer = None
        self.total_samples = 0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="market-capture-loop-lag")

    async def _run(self) -> None:
        next_tick = time.monotonic() + self.interval
        while not self._stop.is_set():
            await asyncio.sleep(max(0.0, next_tick - time.monotonic()))
            now = time.monotonic()
            lag_ms = max(0.0, (now - next_tick) * 1000.0)
            next_tick += self.interval
            if next_tick < now - 1.0:
                next_tick = now + self.interval
            self.samples.append((now, lag_ms))
            self.total_samples += 1
            if lag_ms >= self.stall_ms:
                event = {
                    "schema": 1, "event": "event_loop_stall",
                    "at": datetime.now(timezone.utc).isoformat(),
                    "lag_ms": round(lag_ms, 3),
                }
                self.stalls.append(event)
                if self.writer is not None:
                    self.writer.append("stalls", "local", "event_loop", event)
            cutoff = now - self.window_seconds
            while self.samples and self.samples[0][0] < cutoff:
                self.samples.popleft()

    def snapshot(self) -> dict[str, Any]:
        values = sorted(value for _, value in self.samples)
        if not values:
            return {"samples": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None, "stalls": 0}
        def percentile(pct: float) -> float:
            index = min(len(values) - 1, max(0, int(math.ceil(pct * len(values))) - 1))
            return round(values[index], 3)
        return {
            "samples": len(values),
            "p50_ms": percentile(0.50),
            "p95_ms": percentile(0.95),
            "p99_ms": percentile(0.99),
            "max_ms": round(values[-1], 3),
            "stalls": len(self.stalls),
        }

    def max_in_window(self, start_epoch: float, end_epoch: float) -> dict[str, Any] | None:
        if not self.samples:
            return None
        wall_now = time.time()
        mono_now = time.monotonic()
        start_mono = mono_now - (wall_now - start_epoch)
        end_mono = mono_now - (wall_now - end_epoch)
        values = [lag for at, lag in self.samples if start_mono <= at <= end_mono]
        if not values:
            return None
        return {"max_ms": round(max(values), 3), "samples": len(values)}

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
