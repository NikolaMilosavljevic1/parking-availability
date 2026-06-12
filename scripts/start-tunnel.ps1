# Start a free Cloudflare quick tunnel to the local API (port 8000).
# Requires: cloudflared — https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/
#
# Usage (from repo root):
#   .\scripts\start-tunnel.ps1
#
# Copy the https://*.trycloudflare.com URL, then:
#   curl https://YOUR-URL/health
# Set EAS env vars (see DEPLOY.md) before building the APK.

$ErrorActionPreference = "Stop"

if (-not (Get-Command cloudflared -ErrorAction SilentlyContinue)) {
    Write-Host "cloudflared not found. Install from:" -ForegroundColor Red
    Write-Host "  https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/"
    exit 1
}

Write-Host ""
Write-Host "Ensure the backend is running: docker compose up -d" -ForegroundColor Cyan
Write-Host "Tunneling http://localhost:8000 to the public internet..." -ForegroundColor Cyan
Write-Host "Use the https:// URL for EXPO_PUBLIC_API_URL and wss://.../ws/live for EXPO_PUBLIC_WS_URL"
Write-Host ""

cloudflared tunnel --url http://localhost:8000
