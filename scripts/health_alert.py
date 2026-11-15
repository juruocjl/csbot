#!/usr/bin/env python3
"""Independent, read-only host probes and Matrix health alerts (stdlib only)."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import html
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import time
from urllib.error import HTTPError
import urllib.parse
from urllib.request import ProxyHandler, Request, build_opener
import uuid
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows unit test environment
    fcntl = None

MIB = 1024**2
GIB = 1024**3
STATE_VERSION = 2
TARGET_KEY = "matrix"
WARNING = 1
CRITICAL = 2
COOLDOWN = {WARNING: 6 * 3600, CRITICAL: 3600}
WAKE_COOLDOWN = 600
RETRY_BACKOFF = 300
DEFAULT_HOMESERVER = "http://127.0.0.1:6167"
DEFAULT_MENTION_USER_ID = "@csbot:ave-mujica.top"
DEFAULT_MENTION_LABEL = "@csbot"
SERVICES = ("csbot.service", "mihomo.service", "docker.service", "csbot-memory-guard.timer")
CONTAINERS = ("csbot-database", "csbot-napcat", "steam_monitor", "csbot-homepage", "csbot-nginx")

EventItem = tuple[str, str, str, bool]


@dataclass(frozen=True)
class Issue:
    level: int
    message: str
    samples: int = 2


@dataclass(frozen=True)
class MatrixConfig:
    homeserver: str
    room_id: str
    access_token: str
    mention_user_id: str = DEFAULT_MENTION_USER_ID
    mention_label: str = DEFAULT_MENTION_LABEL


def _shanghai_tz():
    try:
        return ZoneInfo("Asia/Shanghai")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=8))


def emit(event: str, **fields) -> None:
    # Never log HTTP responses, exception text, credentials, or message payloads.
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def sanitize_text(text: str) -> str:
    # Prevent dynamic probe text from accidentally matching @mentions in MatrixHub.
    return str(text).replace("@", "＠")


def command(args: list[str], timeout: int = 5) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True,
                          timeout=timeout).stdout


def request_json(opener, url: str, payload=None, headers=None, method: str | None = None, timeout: int = 5):
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(url, data=data, headers=headers or {}, method=method)
    with opener.open(request, timeout=timeout) as response:
        return json.loads(response.read(1024 * 1024))


def load_config(path: Path) -> MatrixConfig:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("invalid config object")
    room_id = str(data.get("room_id") or "").strip()
    if not room_id.startswith("!"):
        raise ValueError("invalid Matrix room_id")
    access_token = str(data.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("missing Matrix access_token")
    homeserver = str(data.get("homeserver") or DEFAULT_HOMESERVER).strip().rstrip("/")
    if not (homeserver.startswith("http://127.0.0.1:") or homeserver.startswith("https://")):
        raise ValueError("invalid Matrix homeserver URL")
    mention_user_id = str(data.get("mention_user_id") or DEFAULT_MENTION_USER_ID).strip()
    if not mention_user_id.startswith("@") or ":" not in mention_user_id:
        raise ValueError("invalid Matrix mention_user_id")
    mention_label = str(data.get("mention_label") or DEFAULT_MENTION_LABEL).strip()
    if not mention_label.startswith("@"):
        raise ValueError("invalid Matrix mention_label")
    return MatrixConfig(
        homeserver=homeserver,
        room_id=room_id,
        access_token=access_token,
        mention_user_id=mention_user_id,
        mention_label=mention_label,
    )


def napcat_online(root: Path) -> bool:
    """Read-only check of the local NapCat QQ client via its loopback WebUI."""
    config = json.loads((root / "napcat/config/webui.json").read_text(encoding="utf-8"))
    port = int(config.get("port", 6099))
    token = config.get("token")
    if not 1 <= port <= 65535 or not token:
        raise ValueError("invalid NapCat configuration")
    base = f"http://127.0.0.1:{port}/api/"
    opener = build_opener(ProxyHandler({}))

    def post(path: str, payload: dict):
        headers = {"Content-Type": "application/json"}
        if credential:
            headers["Authorization"] = "Bearer " + credential
        result = request_json(opener, base + path, payload, headers)
        if result.get("code") != 0:
            raise RuntimeError("NapCat request failed")
        return result.get("data")

    credential = ""
    login_data = post("auth/login", {
        "hash": hashlib.sha256((str(token) + ".napcat").encode("utf-8")).hexdigest(),
    })
    credential = str(login_data["Credential"])
    status = post("QQLogin/CheckLoginStatus", {})
    return status.get("isLogin") is True and status.get("coreReady") is True


class MatrixNotifier:
    """Send alerts to a configured Matrix room using Client-Server API."""

    def __init__(self, config: MatrixConfig):
        self.config = config
        self.opener = build_opener(ProxyHandler({}))

    def send(self, body: str, formatted_body: str, *, wake_supervisor: bool = False) -> str:
        if not wake_supervisor and ("@" in body or "matrix.to/#/@" in formatted_body):
            raise ValueError("unexpected mention in non-wake message")
        txn_id = uuid.uuid4().hex
        enc_room = urllib.parse.quote(self.config.room_id, safe="")
        url = f"{self.config.homeserver.rstrip('/')}/_matrix/client/v3/rooms/{enc_room}/send/m.room.message/{txn_id}"
        payload = {
            "msgtype": "m.text",
            "body": body,
            "format": "org.matrix.custom.html",
            "formatted_body": formatted_body,
            "m.mentions": {
                "user_ids": [self.config.mention_user_id] if wake_supervisor else [],
            },
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.config.access_token,
        }
        result = request_json(self.opener, url, payload, headers, method="PUT", timeout=10)
        if not isinstance(result, dict) or not isinstance(result.get("event_id"), str) or not result["event_id"].startswith("$"):
            raise RuntimeError("Matrix alert was not acknowledged")
        return result["event_id"]


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
            if name == "steam_monitor":
                # steam_api inspects /api/health directly with rich state & 503 grading.
                if running != "true" or restarting == "true":
                    issues["container:" + name] = Issue(CRITICAL, f"容器 {name} 未运行（running={running}, restarting={restarting}）")
            elif running != "true" or restarting == "true" or health == "unhealthy":
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
        opener = build_opener(ProxyHandler({}))
        try:
            result = request_json(opener, "http://127.0.0.1:5555/api/health")
        except HTTPError as exc:
            if exc.code != 503:
                exc.close()
                raise
            raw = exc.read(1024 * 1024)
            exc.close()
            result = json.loads(raw)
        if all(result.get(key) is True for key in ("ok", "loggedOn", "friendStatusReady")) and not result.get("reconnectStopped"):
            return
        stopped = bool(result.get("reconnectStopped"))
        recovery = bool(result.get("recoveryMode"))
        attempt = int(result.get("recoveryAttempt") or 0)
        failures = int(result.get("consecutiveLoginFailures") or 0)
        err = sanitize_text(str(result.get("lastLoginError") or result.get("recoveryKind") or "unready"))[:80]
        if stopped:
            issues["steam_ready"] = Issue(CRITICAL, f"Steam Monitor 已停止自动重连（error={err}）", 1)
        elif (recovery and attempt >= 3) or (not recovery and failures >= 3):
            issues["steam_ready"] = Issue(CRITICAL, f"Steam Monitor 连续重连未恢复（attempt={attempt}, failures={failures}, error={err}）", 2)
        else:
            issues["steam_ready"] = Issue(WARNING, f"Steam Monitor 自动重连中（attempt={attempt}, error={err}）", 2)
    probe("steam_api", steam)

    def napcat_qq():
        if not napcat_online(root):
            issues["qq_online"] = Issue(CRITICAL, "NapCat QQ 客户端离线")
    probe("napcat_qq", napcat_qq)

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


def notifications(issues: dict[str, Issue], state: dict, now: float) -> list[EventItem]:
    if state.get("version") != STATE_VERSION:
        state.clear()
        state["version"] = STATE_VERSION
    records = state.setdefault("issues", {})
    events: list[EventItem] = []
    for key in sorted(set(records) | set(issues)):
        record = records.setdefault(key, {"bad": 0, "clear": 0, "notified": None, "recovered": False})
        if key in issues:
            issue = issues[key]
            record.update(level=issue.level, message=issue.message, clear=0, recovered=False)
            record["bad"] += 1
            if record["bad"] < issue.samples:
                continue
            record["active"] = True
            sent = record.get("notified")
            if not sent or issue.level > sent["level"] or now - sent["at"] >= COOLDOWN[issue.level]:
                is_critical = issue.level == CRITICAL
                badge = "严重" if is_critical else "预警"
                events.append((key, badge, sanitize_text(issue.message), is_critical))
        else:
            record["bad"] = 0
            record["clear"] += 1
            if record["clear"] < 2:
                continue
            if not record.get("active"):
                del records[key]
                continue
            if not record.get("recovered"):
                badge = "恢复" if record.get("notified") else "已恢复，异常期间未能送达"
                events.append((key, badge, sanitize_text(record["message"]), False))
    return events


def format_matrix_message(
    items: list[EventItem],
    host: str,
    now: float,
    *,
    wake_supervisor: bool,
    wake_throttled: bool,
    mention_user_id: str = DEFAULT_MENTION_USER_ID,
    mention_label: str = DEFAULT_MENTION_LABEL,
) -> tuple[str, str]:
    ts = datetime.fromtimestamp(now, _shanghai_tz()).isoformat(timespec="seconds")
    safe_host = sanitize_text(host)
    if any(item[3] for item in items):
        title = "CSBot 严重告警"
    elif all(item[1].startswith("恢复") or item[1].startswith("已恢复") for item in items):
        title = "CSBot 恢复通知"
    else:
        title = "CSBot 状态预警"

    plain_lines = [f"**[{title}]** `{safe_host}` · `{ts}`"]
    html_items = []
    for _, badge, detail, _ in items:
        safe_detail = sanitize_text(detail)
        plain_lines.append(f"- **[{badge}]** {safe_detail}")
        html_items.append(f"<li><strong>[{html.escape(badge)}]</strong> {html.escape(safe_detail)}</li>")

    html_parts = [
        f"<p><strong>[{html.escape(title)}]</strong> <code>{html.escape(safe_host)}</code> · <code>{html.escape(ts)}</code></p>",
        "<ul>" + "".join(html_items) + "</ul>",
    ]
    if wake_supervisor:
        plain_lines.append("")
        plain_lines.append(f"请 {mention_label} 诊断处理")
        enc_uid = urllib.parse.quote(mention_user_id, safe="@:")
        html_parts.append(
            f'<p>请 <a href="https://matrix.to/#/{enc_uid}">{html.escape(mention_label)}</a> 诊断处理</p>'
        )
    elif wake_throttled:
        plain_lines.append("")
        plain_lines.append("（距上次唤醒不足 10m，本次仅留痕防风暴）")
        html_parts.append("<p>（距上次唤醒不足 10m，本次仅留痕防风暴）</p>")

    return "\n".join(plain_lines), "".join(html_parts)


def deliver(
    events: list[EventItem],
    state: dict,
    now: float,
    sender,
    *,
    host: str | None = None,
    mention_user_id: str = DEFAULT_MENTION_USER_ID,
    mention_label: str = DEFAULT_MENTION_LABEL,
) -> None:
    if not events or now - state.get("last_attempt_at", 0) < RETRY_BACKOFF:
        return
    hostname = host or socket.gethostname()
    last_wake_at = state.get("last_wake_at")
    can_wake = last_wake_at is None or (now - float(last_wake_at)) >= WAKE_COOLDOWN

    included: list[EventItem] = []
    body = ""
    html_body = ""
    wake_supervisor = False

    for item in events:
        candidate = [*included, item]
        cand_critical = any(x[3] for x in candidate)
        cand_wake = bool(cand_critical and can_wake)
        cand_throttled = bool(cand_critical and not can_wake)
        cand_body, cand_html = format_matrix_message(
            candidate,
            hostname,
            now,
            wake_supervisor=cand_wake,
            wake_throttled=cand_throttled,
            mention_user_id=mention_user_id,
            mention_label=mention_label,
        )
        if len(cand_body) > 3000 and included:
            break
        included = candidate
        body = cand_body
        html_body = cand_html
        wake_supervisor = cand_wake

    if not included:
        return

    try:
        sender(body, html_body, wake_supervisor=wake_supervisor)
    except Exception as exc:
        state["last_attempt_at"] = now
        emit("notification_failed", error_type=type(exc).__name__)
        return

    state.pop("last_attempt_at", None)
    if wake_supervisor:
        state["last_wake_at"] = now
    for key, _, _, _ in included:
        record = state["issues"][key]
        if record["clear"] >= 2:
            record["recovered"] = True
            del state["issues"][key]
        else:
            record["notified"] = {"at": now, "level": record["level"]}
    emit("notification_delivered", issue_count=len(included), wake_supervisor=wake_supervisor)


def save_state(path: Path, state: dict) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as file:
        os.chmod(temporary, 0o600)
        json.dump(state, file, ensure_ascii=False)
        file.flush()
        os.fsync(file.fileno())
    temporary.replace(path)


def run(root: Path, config: MatrixConfig | None, state: dict, dry_run: bool) -> None:
    now = time.time()
    issues, metrics = collect(root, state.get("metrics", {}), now)
    emit("sample", at=datetime.fromtimestamp(now, _shanghai_tz()).isoformat(),
         issues={key: {"level": issue.level, "message": issue.message} for key, issue in issues.items()},
         disk_available_mib=round(metrics.get("disk_available", 0) / MIB),
         memory_available_mib=round(metrics.get("memory_available", 0) / MIB), dry_run=dry_run)
    if dry_run:
        return
    if config is None:
        raise ValueError("Matrix config is required when not in dry-run mode")
    events = notifications(issues, state, now)
    state["metrics"] = metrics
    notifier = MatrixNotifier(config)
    deliver(
        events,
        state,
        now,
        notifier.send,
        mention_user_id=config.mention_user_id,
        mention_label=config.mention_label,
    )


def send_test_notification(config: MatrixConfig, host: str | None = None, now: float | None = None) -> str:
    ts_now = time.time() if now is None else now
    safe_host = sanitize_text(host or socket.gethostname())
    ts = datetime.fromtimestamp(ts_now, _shanghai_tz()).isoformat(timespec="seconds")
    body = f"**[CSBot 告警通道自检]** `{safe_host}` · `{ts}`\n- Matrix 告警通道连通正常（旧 QQ 私聊告警链路已移除）"
    html_body = (
        f"<p><strong>[CSBot 告警通道自检]</strong> <code>{html.escape(safe_host)}</code> · <code>{html.escape(ts)}</code></p>"
        "<ul><li>Matrix 告警通道连通正常（旧 QQ 私聊告警链路已移除）</li></ul>"
    )
    return MatrixNotifier(config).send(body, html_body, wake_supervisor=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/home/ubuntu/csbot"))
    parser.add_argument("--config", type=Path, default=Path("/etc/csbot-health-alert.json"))
    parser.add_argument("--state-dir", type=Path, default=Path("/run/csbot-health-alert"))
    parser.add_argument("--dry-run", action="store_true", help="read-only probes; no notifications or state writes")
    parser.add_argument("--send-test", action="store_true", help="send one non-mention Matrix test message")
    args = parser.parse_args()
    try:
        if args.send_test:
            config = load_config(args.config)
            event_id = send_test_notification(config)
            emit("test_notification_delivered", event_id=event_id)
            return 0
        if args.dry_run:
            config = load_config(args.config) if args.config.exists() else None
            run(args.root.resolve(), config, {}, True)
            return 0
        config = load_config(args.config)
        args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        with (args.state_dir / "lock").open("a", encoding="utf-8") as lock:
            if fcntl is not None:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    emit("skipped", reason="another_monitor_is_running")
                    return 0
            path = args.state_dir / "state.json"
            try:
                state = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"version": STATE_VERSION}
            except (ValueError, OSError):
                state = {"version": STATE_VERSION}
                emit("state_reset", reason="previous_state_unreadable")
            run(args.root.resolve(), config, state, False)
            save_state(path, state)
    except Exception as exc:
        emit("monitor_failed", error_type=type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
