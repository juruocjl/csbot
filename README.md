# csbot

## How to start

1. `uv sync` .
4. `uv run nb run` .

## Deploy

Use the backend deploy runbook in [`scripts/DEPLOY.md`](scripts/DEPLOY.md).

The server SSH target is:

```bash
ssh ubuntu@42.193.244.178
```

Short version:

1. Commit backend changes from local `csbot`.
2. If frontend changed, commit and push `csbot-front/main`, then wait for GitHub Actions to update `build-output`.
3. Deploy backend from Windows:

```powershell
cd C:\Users\cjlqwq\Documents\csbot\csbot
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1
```

Backend-only deploy:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_backend.ps1 -SkipFrontend
```

The bot is managed by systemd. Do not use `screen` for csbot anymore.

```bash
sudo systemctl restart csbot
sudo systemctl status csbot
sudo journalctl -u csbot -f
```

Useful checks:

```bash
grep -n "MAJOR_STAGE" ~/csbot/.env.prod
grep -n "CS_EVENT_GROUP_LIST" ~/csbot/.env.prod
curl -sI http://127.0.0.1:1234/major-homework | head
```
