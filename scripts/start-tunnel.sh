#!/usr/bin/env bash
# Start a free Cloudflare quick tunnel to the local API (port 8000).
# Requires: cloudflared — https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/
#
# Usage (from repo root):
#   chmod +x scripts/start-tunnel.sh
#   ./scripts/start-tunnel.sh

set -euo pipefail

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "cloudflared not found. Install from:"
  echo "  https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/"
  exit 1
fi

echo ""
echo "Ensure the backend is running: docker compose up -d"
echo "Tunneling http://localhost:8000 to the public internet..."
echo "Use the https:// URL for EXPO_PUBLIC_API_URL and wss://.../ws/live for EXPO_PUBLIC_WS_URL"
echo ""

exec cloudflared tunnel --url http://localhost:8000
