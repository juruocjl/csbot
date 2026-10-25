#!/usr/bin/env bash
# Persist a tailnet-only listener while NapCat stays bound to loopback.
# Tailscale Serve preserves this background configuration across restarts.
set -euo pipefail
exec tailscale serve --bg --yes --tcp=6099 tcp://127.0.0.1:6099
