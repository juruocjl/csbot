#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || { echo 'This installer targets Linux x86_64'; exit 1; }
node_prefix="${CS_AI_NODE_PREFIX:-$PWD/.tools/node}"
temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
curl --fail --location --retry 2 --max-time 180 \
  https://nodejs.org/dist/v22.23.4/node-v22.23.4-linux-x64.tar.xz -o "$temporary/node.tar.xz"
echo "1805bcd35e6815559bd2eb97db438d3fdb0eb3a5e6a977b7c1badca76337e9ce  $temporary/node.tar.xz" | sha256sum --check
mkdir -p "$node_prefix"
tar -xJf "$temporary/node.tar.xz" -C "$node_prefix" --strip-components=1
export PATH="$node_prefix/bin:$PATH"
npm ci --prefix ai_runtime/dsh --ignore-scripts --no-audit --no-fund
echo 'DSH/Mneme installed. Build the script image and run ai_preflight.py as the service identity.'
