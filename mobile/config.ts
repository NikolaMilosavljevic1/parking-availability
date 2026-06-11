/**
 * config.ts — Central place for environment-specific settings.
 *
 * Set API_URL and WS_URL via mobile/.env (see mobile/.env.example).
 * Restart Expo after changes: npx expo start --clear
 */

export const API_URL =
  process.env.EXPO_PUBLIC_API_URL ?? 'http://localhost:8000';
export const WS_URL =
  process.env.EXPO_PUBLIC_WS_URL ?? 'ws://localhost:8000/ws/live';

/** How long to wait (ms) before marking a location's data as stale. */
export const STALE_THRESHOLD_MS = 5 * 60 * 1000; // 5 minutes
