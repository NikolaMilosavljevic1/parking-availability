# Deployment — APK + Cloudflare Tunnel (free)

Use this guide to run the backend on your home PC and install a standalone Android APK that works on any network (not just home WiFi).

**Stack:** Docker Compose (API on port 8000) → Cloudflare Tunnel (free HTTPS) → EAS Build (free tier APK).

---

## Overview

| Piece | Role |
|-------|------|
| `docker compose up -d` | Postgres, Redis, scraper, API on your PC |
| `cloudflared` | Exposes `localhost:8000` as a public `https://` URL |
| EAS Build | Compiles APK with `EXPO_PUBLIC_API_URL` baked in |
| Android phone | Install APK once; open app like any other app |

The phone never opens the tunnel URL in a browser — the installed app calls it in the background.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- [Node.js](https://nodejs.org/) 18+
- Free [Expo account](https://expo.dev/signup)
- [cloudflared](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/) installed
- Android phone (sideload APK)

---

## Step 1 — Start the backend

```bash
# repo root
copy .env.example .env    # Windows
# edit .env — set POSTGRES_PASSWORD

docker compose up -d
curl http://localhost:8000/health
```

---

## Step 2 — Expose API with Cloudflare Tunnel

**Windows (PowerShell):**

```powershell
.\scripts\start-tunnel.ps1
```

**macOS / Linux:**

```bash
chmod +x scripts/start-tunnel.sh
./scripts/start-tunnel.sh
```

Copy the `https://something.trycloudflare.com` URL from the output.

**Verify from your phone browser (optional):**

```
https://YOUR-TUNNEL-URL/health
```

Should return JSON OK.

**WebSocket URL for the app:**

```
wss://YOUR-TUNNEL-URL/ws/live
```

> **Quick tunnel caveat:** The URL changes every time you restart `cloudflared`. For a stable URL, set up a [named Cloudflare tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/) on a domain you control (still free).

Keep the tunnel terminal open (or run as a Windows service / systemd unit).

---

## Step 3 — Configure EAS environment variables

Standalone APKs bake the API URL at **build time**. Set variables in Expo (not `mobile/.env`, which is not uploaded to EAS).

```bash
cd mobile
npm install -g eas-cli
eas login
eas init    # links project to expo.dev — follow prompts
```

Create env vars for the **preview** profile (replace with your tunnel URL):

```bash
eas env:create --name EXPO_PUBLIC_API_URL --value https://YOUR-TUNNEL-URL --environment preview --visibility plaintext
eas env:create --name EXPO_PUBLIC_WS_URL --value wss://YOUR-TUNNEL-URL/ws/live --environment preview --visibility plaintext
```

List to confirm:

```bash
eas env:list --environment preview
```

See `mobile/.env.production.example` for the variable names.

---

## Step 4 — Build the APK

```bash
cd mobile
npm install
eas build --platform android --profile preview
```

- Build runs on Expo cloud (~10–20 min).
- When done, open the link in the terminal or go to [expo.dev](https://expo.dev) → your project → **Builds**.
- Download the **APK**.

Shortcut script:

```bash
npm run build:apk
```

---

## Step 5 — Install on Android

1. Transfer the APK to the phone (download link on phone, Drive, USB, etc.).
2. Tap the file in **Downloads**.
3. Allow install from unknown sources if prompted.
4. Install → open **Belgrade Parking** from the app drawer.

---

## Daily operation

1. PC on, not sleeping
2. `docker compose up -d`
3. Tunnel running (`.\scripts\start-tunnel.ps1`)
4. Open the app on the phone

If you use a **quick tunnel** and the URL changed, update EAS env vars and rebuild the APK. Named tunnels avoid this.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| App shows loading forever | Tunnel down, Docker down, or wrong URL in APK — rebuild with correct `EXPO_PUBLIC_*` |
| `/health` works in browser, app fails | WS must be `wss://` (not `ws://`) when using HTTPS tunnel |
| Live badge never turns green | WebSocket blocked — confirm `wss://YOUR-TUNNEL/ws/live` in EAS env |
| Quick tunnel URL changed | Update `eas env:create` values and run `eas build` again |

---

## Optional — stable URL (named tunnel)

1. Cloudflare account + a domain on Cloudflare DNS
2. `cloudflared tunnel create belgrade-parking`
3. Route `parking.yourdomain.com` → `http://localhost:8000`
4. Set EAS **production** env vars with that hostname
5. `eas build --platform android --profile production`

---

## Optional — local APK build (no EAS cloud)

Requires Android Studio + JDK. Set env vars in the shell, then:

```bash
cd mobile
$env:EXPO_PUBLIC_API_URL="https://YOUR-TUNNEL-URL"    # PowerShell
$env:EXPO_PUBLIC_WS_URL="wss://YOUR-TUNNEL-URL/ws/live"
npx expo prebuild --platform android
cd android
.\gradlew.bat assembleRelease    # Windows
```

APK path:

```
mobile/android/app/build/outputs/apk/release/app-release.apk
```
