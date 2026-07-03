# Deploy

Use the PowerShell deploy script from Windows:

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

Useful variants:

```powershell
# Server already pulled, only restart.
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1 -SkipPush -SkipPull -SkipFrontend

# Only validate local state and SSH config, without changing the server.
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1 -SkipPush -SkipPull -SkipFrontend -SkipRestart

# Backend-only deploy when frontend build-output is not needed.
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1 -SkipFrontend
```
