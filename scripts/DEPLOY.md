# Deploy

Backend deploys should use the PowerShell script from Windows:

```powershell
cd C:\Users\cjlqwq\Documents\csbot\csbot
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1
```

What it does:

1. Verifies there are no tracked local changes.
2. Fetches `origin/main` and refuses to deploy if the branch is behind or diverged.
3. Pushes `main` to GitHub.
4. Runs `git pull --ff-only origin main` on `ubuntu@cgserver:/home/ubuntu/csbot`.
5. Pulls frontend build artifacts from `origin/build-output` in
   `/home/ubuntu/csbot/dist`.
6. Restarts and verifies the `csbot` systemd service:

```bash
sudo systemctl restart csbot
sudo systemctl status csbot
sudo journalctl -u csbot -n 80 --no-pager
```

The server's systemd unit runs the bot from `/home/ubuntu/csbot` with
`ENVIRONMENT=prod` and `/home/ubuntu/.local/bin/uv run python bot.py`.

## Production config

The server reads production settings from `/home/ubuntu/csbot/.env.prod`.
Keep these two lists intentionally separate:

```env
# Groups that receive scheduled pushes and reports.
CS_GROUP_LIST = ["832126798"]

# Groups whose events are allowed to reach matchers.
CS_EVENT_GROUP_LIST = ["832126798"]
```

`CS_EVENT_GROUP_LIST` is the response whitelist. Events outside this list are
ignored before matchers run. If this list is empty or missing, all incoming
events are blocked.

When the response whitelist should mirror the current push list, run this on
the server before restarting:

```bash
cd /home/ubuntu/csbot
cp .env.prod ".env.prod.bak-event-whitelist-$(date +%Y%m%d%H%M%S)"
python3 - <<'PY'
from pathlib import Path

p = Path(".env.prod")
lines = p.read_text().splitlines()
group_line = next(
    (line for line in reversed(lines) if line.strip().startswith("CS_GROUP_LIST") and "=" in line),
    None,
)
if group_line is None:
    raise SystemExit("CS_GROUP_LIST not found")

event_line = "CS_EVENT_GROUP_LIST=" + group_line.split("=", 1)[1]
for i, line in enumerate(lines):
    if line.strip().startswith("CS_EVENT_GROUP_LIST") and "=" in line:
        lines[i] = event_line
        break
else:
    if lines and lines[-1].strip():
        lines.append("")
    lines.append(event_line)

p.write_text("\n".join(lines) + "\n")
PY
grep -nE '^[[:space:]]*CS_(EVENT_)?GROUP_LIST[[:space:]]*=' .env.prod
```

## Frontend deploy

Push frontend changes from `C:\Users\cjlqwq\Documents\csbot\csbot-front` to
`main`. GitHub Actions builds the frontend and pushes the static output to the
`build-output` branch. After that finishes, either run the backend deploy script
without `-SkipFrontend`, or pull the output manually:

```bash
cd /home/ubuntu/csbot/dist
git fetch origin build-output
git pull --ff-only origin build-output
```

## Verify

After deploy, check:

```bash
cd /home/ubuntu/csbot
git rev-parse --short HEAD
grep -nE '^[[:space:]]*CS_(EVENT_)?GROUP_LIST[[:space:]]*=' .env.prod
systemctl is-active csbot
journalctl -u csbot -n 80 --no-pager
curl -sI http://127.0.0.1:1234/major-homework | head
```

Useful variants:

```powershell
# Server already pulled, only restart.
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1 -SkipPush -SkipPull -SkipFrontend

# Only validate local state and SSH config, without changing the server.
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1 -SkipPush -SkipPull -SkipFrontend -SkipRestart

# Backend-only deploy when frontend build-output is not needed.
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1 -SkipFrontend
```
