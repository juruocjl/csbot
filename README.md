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

## Local delivery check

Use the remote test database `csbot_backup` and the existing test token `dinner123321`.
Do not create a new token for local delivery checks. Provide the real remote database URL through
`CS_DATABASE`; it should point at `csbot_backup`.
If the remote PostgreSQL is reached through SSH, forward it locally first and rewrite the host/port
in `CS_DATABASE` to the forwarded address.

Backend:

```powershell
cd C:\Users\cjlqwq\Documents\csbot\csbot
# Example shape only; use the real testing credential.
$env:CS_DATABASE="postgresql+asyncpg://<user>:<password>@<host>:5432/csbot_backup"
$env:CS_SERVER_SKIP_STARTUP_CACHE="1"
$env:CS_SKIP_SCHEMA_CHECK="1"
$env:CS_DISABLE_BACKGROUND_JOBS="1"
$env:CS_WATCH_STAGE_ENABLE_PROFILE_REFRESH="0"
uv run python bot_minimal.py
```

Remote watch-stage test instances should keep background crawling disabled. Only set
`CS_WATCH_STAGE_ENABLE_PROFILE_REFRESH=1` for a run where the explicit purpose is to verify
watch-stage-triggered player profile crawling.

Frontend:

```powershell
cd C:\Users\cjlqwq\Documents\csbot\csbot-front
$env:VITE_API_BASE_URL="http://127.0.0.1:1234"
cmd /c npm run dev -- --host 127.0.0.1 --port 5173
```

Smoke-test a protected API with:

```powershell
curl.exe -H "Authorization: Bearer dinner123321" -H "Content-Type: application/json" `
  -d "{\"steamId\":\"76561199388088405\"}" `
  http://127.0.0.1:1234/api/watch-stage/snapshot
```

For watch-stage delivery, verify that the response is not `401`, contains a `status`, and returns
running match data when the tested Steam ID is currently in a watch-stage match. The profile fields
such as `legacyScore` may be `null` on the first response because player base data is crawled
asynchronously and rate-limited.
