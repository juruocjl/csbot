#!/usr/bin/env python3
"""Bound CSBot memory with systemd restarts; stdlib only, no application imports."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import fcntl
import json
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, build_opener
from zoneinfo import ZoneInfo


SERVICE = "csbot.service"
MIB = 1024 * 1024
MIN_UPTIME = 600
DAILY_MIN_UPTIME = 6 * 3600


@dataclass(frozen=True)
class Sample:
    active: str
    memory: int
    available: int
    uptime: float
    now: datetime


def emit(event: str, **fields) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def systemctl(*args: str, timeout: int = 10) -> str:
    return subprocess.run(
        ["systemctl", *args], check=True, capture_output=True, text=True,
        timeout=timeout,
    ).stdout


def sample() -> Sample:
    props = dict(line.split("=", 1) for line in systemctl(
        "show", SERVICE, "--property=ActiveState,MemoryCurrent,ActiveEnterTimestampMonotonic",
    ).splitlines() if "=" in line)
    active = props["ActiveState"]
    # An inactive service may report MemoryCurrent=[not set]. Never start a
    # deliberately stopped service just because the host has little memory.
    if active != "active":
        return Sample(active, 0, 0, 0, datetime.now(ZoneInfo("Asia/Shanghai")))
    memory = int(props["MemoryCurrent"])
    if memory >= (1 << 63):
        raise ValueError("systemd memory accounting is unavailable")
    meminfo = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    available = int(meminfo["MemAvailable"].split()[0]) * 1024
    entered = int(props["ActiveEnterTimestampMonotonic"]) / 1_000_000
    if entered <= 0:
        raise ValueError("service start timestamp is unavailable")
    return Sample(active, memory, available, max(0, time.monotonic() - entered),
                  datetime.now(ZoneInfo("Asia/Shanghai")))


def restart_reason(current: Sample) -> str | None:
    if current.active != "active" or current.uptime < MIN_UPTIME:
        return None
    if current.memory >= 1600 * MIB:
        return "backend_memory"
    if current.available <= 384 * MIB and current.memory >= 1024 * MIB:
        return "host_memory_pressure"
    local = current.now.astimezone(ZoneInfo("Asia/Shanghai"))
    # One restart per window: a successful restart resets the service uptime.
    if local.hour == 5 and 10 <= local.minute < 50 and current.uptime >= DAILY_MIN_UPTIME:
        return "daily_maintenance"
    return None


def log_sample(event: str, current: Sample, **fields) -> None:
    emit(event, at=current.now.isoformat(), active=current.active,
         memory_mib=round(current.memory / MIB, 1),
         available_mib=round(current.available / MIB, 1),
         uptime_seconds=round(current.uptime), **fields)


def wait_for_backend(timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    opener = build_opener(ProxyHandler({}))
    while time.monotonic() < deadline:
        try:
            if systemctl("is-active", SERVICE).strip() == "active":
                # This deliberately nonexistent route avoids database queries
                # and expensive API work. A 404 proves the HTTP loop responds,
                # not that protected APIs or downstream services are healthy.
                try:
                    with opener.open("http://127.0.0.1:8888/__memory_guard_probe__", timeout=2) as response:
                        status = response.status
                except HTTPError as exc:
                    status = exc.code
                    exc.close()
                if 200 <= status < 500:
                    return
        except (OSError, URLError, subprocess.SubprocessError):
            pass
        time.sleep(2)
    raise RuntimeError("backend did not become active and HTTP-responsive after restart")


def run(dry_run: bool) -> None:
    before = sample()
    reason = restart_reason(before)
    log_sample("sample", before, restart_reason=reason, dry_run=dry_run)
    if dry_run or reason is None:
        return
    # Recheck after the sample so an operator's stop during inspection is
    # respected. try-restart also leaves an already inactive unit stopped.
    current = sample()
    if current.active != "active" or current.uptime < MIN_UPTIME:
        emit("restart_skipped", reason="service_state_changed")
        return
    emit("restart_requested", reason=reason)
    systemctl("try-restart", SERVICE, timeout=65)
    wait_for_backend()
    log_sample("restart_completed", sample(), reason=reason)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="sample and log; never restart or write files")
    args = parser.parse_args()
    try:
        if args.dry_run:
            run(True)
        else:
            # RuntimeDirectory is created by the guard's systemd unit. flock
            # also prevents overlap with a manually invoked copy of the guard.
            with Path("/run/csbot-memory-guard/guard.lock").open("a") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    emit("skipped", reason="another_guard_is_running")
                    return 0
                run(False)
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as exc:
        # Do not print subprocess output, which could include application logs.
        emit("guard_failed", error_type=type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
