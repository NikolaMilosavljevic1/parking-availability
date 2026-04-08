/**
 * config.ts — Central place for environment-specific settings.
 *
 * HOW TO CHANGE THE API URL:
 *   • Expo Go on the same machine (iOS Simulator / Android Emulator):
 *       API_URL = 'http://localhost:8000'         ← default, works as-is
 *
 *   • Expo Go on a physical device (phone on same WiFi):
 *       Replace 'localhost' with your machine's local IP address.
 *       Find it with: ipconfig (Windows) or ifconfig (Mac/Linux)
 *       Example: API_URL = 'http://192.168.1.42:8000'
 *
 *   • Android emulator specifically needs:
 *       API_URL = 'http://10.0.2.2:8000'
 */

export const API_URL = 'http://192.168.1.104:8000';
export const WS_URL  = 'ws://192.168.1.104:8000/ws/live';

/** How long to wait (ms) before marking a location's data as stale. */
export const STALE_THRESHOLD_MS = 5 * 60 * 1000; // 5 minutes
