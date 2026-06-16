# csbot

## How to start

1. `uv sync` .
4. `uv run nb run` .

## Deploy

Use the public IP SSH target when deploying:

```bash
ssh ubuntu@42.193.244.178
```

Deployment order:

1. Commit and push backend changes from local `backend`.
2. Commit and push frontend changes from local `csbot-front`.
3. Pull backend on the server:

```bash
cd ~/csbot
git pull --ff-only
```

4. Pull the built frontend output on the server after GitHub Actions finishes:

```bash
cd ~/csbot/dist
git fetch origin build-output
git pull --ff-only
```

5. Restart and verify the bot with systemd. Do not use `screen` for csbot anymore.

```bash
sudo systemctl restart csbot
sudo systemctl status csbot
sudo journalctl -u csbot -f
```

Useful checks:

```bash
grep -n "MAJOR_STAGE" ~/csbot/.env.prod
curl -sI http://127.0.0.1:1234/major-homework | head
```
