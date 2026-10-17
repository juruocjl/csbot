#!/usr/bin/env python3
"""Independent, read-only host probes and private QQ health alerts (stdlib only)."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import time
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

MIB = 1024**2
GIB = 1024**3
WARNING = 1
CRITICAL = 2
COOLDOWN = {WARNING: 6 * 3600, CRITICAL: 3600}
SERVICES = ("csbot.service", "mihomo.service", "docker.service", "csbot-memory-guard.timer")
CONTAINERS = ("csbot-database", "csbot-napcat", "steam_monitor", "csbot-homepage", "csbot-nginx")


@dataclass(frozen=True)
class Issue:
    level: int
    message: str
    samples: int = 2


def emit(event: str, **fields) -> None:
    # Never log HTTP responses, exception text, credentials, or message payloads.
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def command(args: list[str], timeout: int = 5) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True,
                          timeout=timeout).stdout


def request_json(opener, url: str, payload=None, headers=None):
    request = Request(url, data=None if payload is None else json.dumps(payload).encode(),
                      headers=headers or {})
    with opener.open(request, timeout=5) as response:
        return json.loads(response.read(1024 * 1024))


class NapCat:
    """Use the existing authenticated loopback WebUI; no OneBot port changes."""

    def __init__(self, root: Path):
        config = json.loads((root / "napcat/config/webui.json").read_text())
        port = int(config.get("port", 6099))
        if not 1 <= port <= 65535 or not config.get("token"):
            raise ValueError("invalid NapCat configuration")
        self.base = f"http://127.0.0.1:{port}/api/"
        self.opener = build_opener(ProxyHandler({}))
        self.credential = ""
        result = self.post("auth/login", {
            "hash": hashlib.sha256((config["token"] + ".napcat").encode()).hexdigest(),
        })
        self.credential = result["Credential"]

    def post(self, path: str, payload: dict):
        headers = {"Content-Type": "application/json"}
        if self.credential:
            headers["Authorization"] = "Bearer " + self.credential
        result = request_json(self.opener, self.base + path, payload, headers)
        if result.get("code") != 0:
            raise RuntimeError("NapCat request failed")
        return result.get("data")

    def online(self) -> bool:
        result = self.post("QQLogin/CheckLoginStatus", {})
        return result.get("isLogin") is True and result.get("coreReady") is True

    def send(self, recipient: str, message: str) -> None:
        result = self.post("Debug/call", {
            "action": "send_private_msg",
            "params": {"user_id": int(recipient), "message": message, "auto_escape": True},
        })
        # NapCat 4.18.30 Debug/call returns the OneBot response envelope.
        if not isinstance(result, dict) or result.get("retcode") != 0 or result.get("status") != "ok":
            raise RuntimeError("private alert was not acknowledged")
        if not isinstance(result.get("data"), dict) or result["data"].get("message_id") is None:
            raise RuntimeError("private alert has no message id")


def resource_issues(disk: dict, memory: dict) -> dict[str, Issue]:
    issues = {}
    available = disk["available"]
    used = disk["used_ratio"]
    if available < 800 * MIB or used >= .97:
        issues["disk"] = Issue(CRITICAL, f"根分区可用 {available / GIB:.2f} GiB，使用率 {used:.1%}", 1)
    elif available < 2 * GIB or used >= .90:
        issues["disk"] = Issue(WARNING, f"根分区可用 {available / GIB:.2f} GiB，使用率 {used:.1%}")
    if disk["inode_ratio"] <= .01:
        issues["inodes"] = Issue(CRITICAL, f"根分区剩余 inode {disk['inode_ratio']:.1%}", 1)
    elif disk["inode_ratio"] <= .02:
        issues["inodes"] = Issue(WARNING, f"根分区剩余 inode {disk['inode_ratio']:.1%}")
    if memory["available"] <= 256 * MIB:
        issues["host_memory"] = Issue(CRITICAL, f"整机可用内存仅 {memory['available'] / MIB:.0f} MiB", 1)
    elif memory["available"] <= 512 * MIB:
        issues["host_memory"] = Issue(WARNING, f"整机可用内存仅 {memory['available'] / MIB:.0f} MiB")
    return issues


def collect(root: Path, previous: dict, now: float) -> tuple[dict[str, Issue], dict]:
    issues: dict[str, Issue] = {}
    metrics: dict = {"at": now}

    def probe(key, function):
        try:
            function()
        except Exception as exc:
            issues[key] = Issue(CRITICAL, f"{key} 检查失败（{type(exc).__name__}）")

    def resources():
        disk = os.statvfs("/")
        available = disk.f_bavail * disk.f_frsize
        used = (disk.f_blocks - disk.f_bfree) * disk.f_frsize
        info = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
        metrics["disk_available"] = available
        metrics["memory_available"] = int(info["MemAvailable"].split()[0]) * 1024
        issues.update(resource_issues(
            {"available": available, "used_ratio": used / max(1, used + available),
             "inode_ratio": disk.f_favail / max(1, disk.f_files)},
            {"available": metrics["memory_available"]},
        ))
    probe("host_resources", resources)

    def services():
        output = command(["systemctl", "show", *SERVICES,
                          "--property=Id,ActiveState,MemoryCurrent,NRestarts"])
        for block in output.strip().split("\n\n"):
            props = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
            name = props["Id"]
            if props.get("ActiveState") != "active":
                issues["service:" + name] = Issue(CRITICAL, f"{name} 未运行")
            if name == "csbot.service" and props.get("ActiveState") == "active":
                memory = int(props["MemoryCurrent"])
                if memory >= 1 << 63:
                    raise ValueError("memory accounting unavailable")
                if memory >= 1600 * MIB:
                    issues["backend_memory"] = Issue(CRITICAL, f"后端内存达到 {memory / MIB:.0f} MiB", 1)
                elif memory >= 1400 * MIB:
                    issues["backend_memory"] = Issue(WARNING, f"后端内存达到 {memory / MIB:.0f} MiB")
                restarts = int(props["NRestarts"])
                if restarts - previous.get("backend_restarts", restarts) >= 2:
                    issues["backend_restarts"] = Issue(CRITICAL, "后端在一次采样间隔内自动重启至少两次", 1)
                metrics["backend_restarts"] = restarts
    probe("systemd", services)

    def containers():
        format_ = '{{json .Name}} {{.State.Running}} {{.State.Restarting}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}'
        lines = command(["docker", "inspect", "--format", format_, *CONTAINERS]).splitlines()
        if len(lines) != len(CONTAINERS):
            raise ValueError("missing container state")
        for name, line in zip(CONTAINERS, lines):
            _, running, restarting, health = line.split()
            if running != "true" or restarting == "true" or health == "unhealthy":
                issues["container:" + name] = Issue(CRITICAL, f"容器 {name} 异常（running={running}, health={health}）")
    probe("containers", containers)

    def database():
        command(["docker", "exec", "csbot-database", "sh", "-c",
                 'exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t 2'])
    probe("postgresql", database)

    def backend_http():
        opener = build_opener(ProxyHandler({}))
        try:
            with opener.open("http://127.0.0.1:8888/__health_alert_probe__", timeout=3) as response:
                status = response.status
        except HTTPError as exc:
            status = exc.code
            exc.close()
        if not 200 <= status < 500:
            raise RuntimeError("backend HTTP unavailable")
    probe("backend_http", backend_http)

    def steam():
        result = request_json(build_opener(ProxyHandler({})), "http://127.0.0.1:5555/api/health")
        if not all(result.get(key) is True for key in ("ok", "loggedOn", "friendStatusReady")):
            issues["steam_ready"] = Issue(WARNING, "Steam Monitor 未就绪", 3)
    probe("steam_api", steam)

    def logs():
        current = {}
        for name in ("app.log", "err.log"):
            path = root / name
            if not path.exists():
                continue
            stat = path.stat()
            current[name] = {"inode": stat.st_ino, "size": stat.st_size}
            if stat.st_size >= 500 * MIB:
                issues["log_size:" + name] = Issue(CRITICAL, f"{name} 已达到 {stat.st_size / MIB:.0f} MiB", 1)
            elif stat.st_size >= 100 * MIB:
                issues["log_size:" + name] = Issue(WARNING, f"{name} 已达到 {stat.st_size / MIB:.0f} MiB")
        elapsed = now - previous.get("at", now)
        growth = 0
        for name, stat in current.items():
            old = previous.get("logs", {}).get(name, {})
            if stat["inode"] == old.get("inode"):
                growth += max(0, stat["size"] - old["size"])
        if elapsed >= 30 and growth * 60 / elapsed >= 5 * MIB:
            issues["log_growth"] = Issue(WARNING, f"两个运行日志合计增长 {growth * 60 / elapsed / MIB:.1f} MiB/分钟")
        metrics["logs"] = current
    probe("logs", logs)

    def outbox():
        path = root / "data/ai/state.sqlite3"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1) as db:
            db.execute("PRAGMA query_only=ON")
            count, oldest = db.execute("SELECT count(*),min(created_at) FROM outbound WHERE status='sent'").fetchone()
        if oldest is not None and now - oldest >= 1800:
            age = (now - oldest) / 3600
            issues["outgoing_index"] = Issue(CRITICAL if age >= 2 else WARNING,
                f"已发消息的归档重试积压 {count} 条，最老 {age:.1f} 小时；不会重发群消息")
    probe("outbox", outbox)
    return issues, metrics


def notifications(issues: dict[str, Issue], state: dict, recipients: list[str], now: float) -> dict[str, list[tuple[str, str]]]:
    records = state.setdefault("issues", {})
    events = {recipient: [] for recipient in recipients}
    for key in sorted(set(records) | set(issues)):
        record = records.setdefault(key, {"bad": 0, "clear": 0, "notified": {}, "recovered": []})
        if key in issues:
            issue = issues[key]
            record.update(level=issue.level, message=issue.message, clear=0, recovered=[])
            record["bad"] += 1
            if record["bad"] < issue.samples:
                continue
            record["active"] = True
            for recipient in recipients:
                sent = record["notified"].get(recipient)
                if not sent or issue.level > sent["level"] or now - sent["at"] >= COOLDOWN[issue.level]:
                    label = "严重" if issue.level == CRITICAL else "预警"
                    events[recipient].append((key, f"[{label}] {issue.message}"))
        else:
            record["bad"] = 0
            record["clear"] += 1
            if record["clear"] < 2:
                continue
            if not record.get("active"):
                del records[key]
                continue
            for recipient in recipients:
                if recipient not in record["recovered"]:
                    label = "恢复" if recipient in record["notified"] else "已恢复，异常期间未能送达"
                    events[recipient].append((key, f"[{label}] {record['message']}"))
    return events


def deliver(events, state: dict, recipients: list[str], now: float, sender) -> None:
    attempts = state.setdefault("attempts", {})
    for recipient, items in events.items():
        if not items or now - attempts.get(recipient, 0) < 300:
            continue
        message = "CSBot 服务器状态通知\n" + datetime.fromtimestamp(now, ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")
        message += "\n主机：" + socket.gethostname()
        included = []
        for item in items:
            if len(message) + len(item[1]) + 1 > 3500:
                break
            message += "\n" + item[1]
            included.append(item)
        if not included:
            continue
        try:
            sender(recipient, message)
        except Exception as exc:
            attempts[recipient] = now
            emit("notification_failed", error_type=type(exc).__name__)
            continue
        attempts.pop(recipient, None)
        for key, _ in included:
            record = state["issues"][key]
            if record["clear"] >= 2:
                record["recovered"].append(recipient)
            else:
                record["notified"][recipient] = {"at": now, "level": record["level"]}
        emit("notification_delivered", issue_count=len(included))
    for key, record in list(state["issues"].items()):
        if record["clear"] >= 2 and all(r in record["recovered"] for r in recipients):
            del state["issues"][key]


def save_state(path: Path, state: dict) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as file:
        os.chmod(temporary, 0o600)
        json.dump(state, file)
        file.flush()
        os.fsync(file.fileno())
    temporary.replace(path)


def run(root: Path, recipients: list[str], state: dict, dry_run: bool) -> None:
    now = time.time()
    issues, metrics = collect(root, state.get("metrics", {}), now)
    client = None
    try:
        client = NapCat(root)
        if not client.online():
            issues["qq_online"] = Issue(CRITICAL, "NapCat 的 QQ 客户端离线；QQ 告警通道也受影响")
    except Exception as exc:
        issues["qq_channel"] = Issue(CRITICAL, f"QQ 告警通道不可用（{type(exc).__name__}）")
    emit("sample", at=datetime.fromtimestamp(now, ZoneInfo("Asia/Shanghai")).isoformat(),
         issues={key: {"level": issue.level, "message": issue.message} for key, issue in issues.items()},
         disk_available_mib=round(metrics.get("disk_available", 0) / MIB),
         memory_available_mib=round(metrics.get("memory_available", 0) / MIB), dry_run=dry_run)
    if dry_run:
        return
    events = notifications(issues, state, recipients, now)
    state["metrics"] = metrics
    if client is not None:
        deliver(events, state, recipients, now, client.send)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/home/ubuntu/csbot"))
    parser.add_argument("--config", type=Path, default=Path("/etc/csbot-health-alert.json"))
    parser.add_argument("--state-dir", type=Path, default=Path("/run/csbot-health-alert"))
    parser.add_argument("--dry-run", action="store_true", help="read-only probes; no notifications or state writes")
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text())
        recipients = config["recipients"]
        if not isinstance(recipients, list) or not recipients or any(not isinstance(r, str) or not r.isdecimal() for r in recipients):
            raise ValueError("explicit QQ recipients are required")
        recipients = list(dict.fromkeys(recipients))
        if args.dry_run:
            run(args.root.resolve(), recipients, {}, True)
        else:
            args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            with (args.state_dir / "lock").open("a") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    emit("skipped", reason="another_monitor_is_running")
                    return 0
                path = args.state_dir / "state.json"
                try:
                    state = json.loads(path.read_text()) if path.exists() else {}
                except (ValueError, OSError):
                    state = {}
                    emit("state_reset", reason="previous_state_unreadable")
                run(args.root.resolve(), recipients, state, False)
                save_state(path, state)
    except Exception as exc:
        emit("monitor_failed", error_type=type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
