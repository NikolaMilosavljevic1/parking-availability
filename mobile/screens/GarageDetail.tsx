import React, { useCallback, useEffect, useState } from "react";
import {
  ActivityIndicator,
  Linking,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { NativeStackScreenProps } from "@react-navigation/native-stack";

import { API_URL } from "../config";
import OccupancyBar, { occupancyColor } from "../components/OccupancyBar";
import HourlyOccupancyChart from "../components/HourlyOccupancyChart";
import { CityEvent, Location, Snapshot } from "../types";
import { RootStackParamList } from "../App";

type Props = NativeStackScreenProps<RootStackParamList, "GarageDetail">;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString("sr-RS", { hour: "2-digit", minute: "2-digit" });
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString("sr-RS", {
    weekday: "short",
    day: "numeric",
    month: "short",
  });
}

/** Open the native maps app to navigate to a location. */
function openDirections(lat: number, lng: number, name: string) {
  const label = encodeURIComponent(name);
  const url = Platform.select({
    ios: `maps:0,0?q=${label}@${lat},${lng}`,
    android: `geo:${lat},${lng}?q=${lat},${lng}(${label})`,
    default: `https://www.google.com/maps/dir/?api=1&destination=${lat},${lng}`,
  });
  Linking.openURL(url!).catch(() =>
    Linking.openURL(
      `https://www.google.com/maps/dir/?api=1&destination=${lat},${lng}`,
    ),
  );
}

const EVENT_TYPE_EMOJI: Record<string, string> = {
  concert: "🎵",
  sports: "⚽",
  theatre: "🎭",
  religious: "⛪",
  festival: "🎪",
  other: "📅",
};

// ---------------------------------------------------------------------------
// Screen
// ---------------------------------------------------------------------------

export default function GarageDetail({ route }: Props) {
  // The location passed from GarageList — we update it with fresher data below
  const [location, setLocation] = useState<Location>(route.params.location);
  const [history, setHistory] = useState<Snapshot[]>([]);
  const [events, setEvents] = useState<CityEvent[]>([]);
  const [historyLoading, setHistoryLoading] = useState(true);

  // ---------- Fetch fresh location data + history ----------

  const loadDetail = useCallback(async () => {
    try {
      const [locResp, histResp, eventsResp] = await Promise.all([
        fetch(`${API_URL}/locations/${location.id}`),
        fetch(`${API_URL}/locations/${location.id}/history?period=24h`),
        fetch(`${API_URL}/events`),
      ]);

      if (locResp.ok) {
        const fresh: Location = await locResp.json();
        setLocation(fresh);
      }

      if (histResp.ok) {
        const histData: { snapshots: Snapshot[] } = await histResp.json();
        setHistory(histData.snapshots);
      }

      if (eventsResp.ok) {
        const allEvents: CityEvent[] = await eventsResp.json();
        // Filter to events near this location (within 5 km of any of our known venues)
        // We use the pre-computed dist_to_* fields on the location for proximity check
        const nearbyVenueEvents = allEvents.filter((e) => {
          if (!e.venue_lat || !e.venue_lng) return false;
          // Show events from venues that are within 3 km of this parking location
          const dists = [
            location.dist_to_arena_km,
            location.dist_to_hram_km,
            location.dist_to_marakana_km,
            location.dist_to_partizan_km,
            location.dist_to_narodno_pozoriste_km,
            location.dist_to_sava_centar_km,
          ].filter((d): d is number => d != null);
          return dists.some((d) => d < 3);
        });
        setEvents(nearbyVenueEvents.slice(0, 5));
      }
    } catch (e) {
      console.warn("GarageDetail fetch error:", e);
    } finally {
      setHistoryLoading(false);
    }
  }, [location.id]);

  useEffect(() => {
    loadDetail();
  }, []);

  // ---------- Prediction ----------

  const [prediction, setPrediction] = useState<number | null>(null);

  useEffect(() => {
    fetch(`${API_URL}/locations/${location.id}/predict`)
      .then((r) => (r.ok ? r.json() : null))
      .then((data) =>
        data?.predicted_occupancy_pct != null
          ? setPrediction(data.predicted_occupancy_pct)
          : null,
      )
      .catch(() => null);
  }, [location.id]);

  // ---------- Render ----------

  const color = occupancyColor(location.occupancy_pct);

  return (
    <ScrollView style={styles.container} contentContainerStyle={styles.content}>
      {/* Live counter card */}
      <View style={styles.card}>
        <View style={styles.liveRow}>
          <View>
            <Text style={styles.cardLabel}>Free spots right now</Text>
            <Text style={[styles.bigNumber, { color }]}>
              {location.free_spots ?? "—"}
            </Text>
            {location.total_spots && (
              <Text style={styles.totalLabel}>
                of {location.total_spots} total
              </Text>
            )}
          </View>

          <View style={styles.liveRight}>
            {location.live && (
              <View style={styles.liveBadge}>
                <Text style={styles.liveBadgeText}>● LIVE</Text>
              </View>
            )}
            {prediction !== null && (
              <View style={styles.predBadge}>
                <Text style={styles.predLabel}>2h forecast</Text>
                <Text
                  style={[
                    styles.predValue,
                    { color: occupancyColor(prediction) },
                  ]}
                >
                  {Math.round(prediction)}%
                </Text>
              </View>
            )}
          </View>
        </View>

        <View style={{ marginTop: 12 }}>
          <OccupancyBar
            occupancyPct={location.occupancy_pct}
            freeSpots={location.free_spots}
            totalSpots={location.total_spots}
          />
        </View>

        {location.scraped_at && (
          <Text style={styles.updatedAt}>
            Updated {formatTime(location.scraped_at)}
          </Text>
        )}
      </View>

      {/* Location info */}
      <View style={styles.card}>
        <Text style={styles.sectionTitle}>Location</Text>
        {location.address && (
          <Text style={styles.infoText}>{location.address}</Text>
        )}
        {location.neighborhood && (
          <Text style={styles.infoText}>{location.neighborhood}</Text>
        )}
        <Text style={styles.infoText}>
          {location.location_type === "garage"
            ? "🏗 Garage"
            : "🅿 Open parking lot"}
        </Text>
      </View>

      {/* Directions button */}
      {location.latitude != null && location.longitude != null && (
        <Pressable
          style={({ pressed }: { pressed: boolean }) => [
            styles.directionsBtn,
            pressed && { opacity: 0.75 },
          ]}
          onPress={() =>
            openDirections(
              location.latitude!,
              location.longitude!,
              location.name,
            )
          }
        >
          <Text style={styles.directionsBtnText}>Get Directions</Text>
        </Pressable>
      )}

      {/* 24h occupancy chart */}
      <View style={styles.card}>
        <Text style={styles.sectionTitle}>Occupancy — last 24 hours</Text>
        {historyLoading ? (
          <ActivityIndicator color="#1d4ed8" style={{ marginVertical: 24 }} />
        ) : (
          <HourlyOccupancyChart snapshots={history} />
        )}
        {!historyLoading && history.length > 0 && (
          <View style={styles.legend}>
            {(
              [
                { label: "Available", color: "#22c55e" },
                { label: "Filling up", color: "#f59e0b" },
                { label: "Almost full", color: "#ef4444" },
              ] as const
            ).map(({ label, color }) => (
              <View key={label} style={styles.legendItem}>
                <View style={[styles.legendDot, { backgroundColor: color }]} />
                <Text style={styles.legendLabel}>{label}</Text>
              </View>
            ))}
          </View>
        )}
      </View>

      {/* Nearby events */}
      {events.length > 0 && (
        <View style={styles.card}>
          <Text style={styles.sectionTitle}>Nearby events</Text>
          {events.map((ev) => (
            <View key={ev.id} style={styles.eventRow}>
              <Text style={styles.eventEmoji}>
                {EVENT_TYPE_EMOJI[ev.event_type ?? ""] ?? "📅"}
              </Text>
              <View style={styles.eventInfo}>
                <Text style={styles.eventName} numberOfLines={2}>
                  {ev.event_name}
                </Text>
                <Text style={styles.eventMeta}>
                  {formatDate(ev.event_date)}
                  {ev.event_time ? ` · ${ev.event_time.slice(0, 5)}` : ""}
                  {ev.venue_name ? ` · ${ev.venue_name}` : ""}
                </Text>
                {ev.expected_attendance && (
                  <Text style={styles.eventAttend}>
                    ~{ev.expected_attendance.toLocaleString()} expected
                  </Text>
                )}
              </View>
            </View>
          ))}
        </View>
      )}
    </ScrollView>
  );
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#f9fafb",
  },
  content: {
    padding: 16,
    gap: 12,
    paddingBottom: 40,
  },
  card: {
    backgroundColor: "#ffffff",
    borderRadius: 16,
    padding: 16,
    ...Platform.select({
      ios: {
        shadowColor: "#000",
        shadowOffset: { width: 0, height: 1 },
        shadowOpacity: 0.06,
        shadowRadius: 6,
      },
      android: { elevation: 2 },
    }),
  },
  liveRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  cardLabel: {
    fontSize: 13,
    color: "#6b7280",
    fontWeight: "500",
  },
  bigNumber: {
    fontSize: 56,
    fontWeight: "800",
    lineHeight: 64,
  },
  totalLabel: {
    fontSize: 13,
    color: "#9ca3af",
  },
  liveRight: {
    alignItems: "flex-end",
    gap: 8,
  },
  liveBadge: {
    backgroundColor: "#dcfce7",
    borderRadius: 8,
    paddingHorizontal: 8,
    paddingVertical: 4,
  },
  liveBadgeText: {
    fontSize: 11,
    fontWeight: "700",
    color: "#16a34a",
  },
  predBadge: {
    backgroundColor: "#eff6ff",
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 6,
    alignItems: "center",
  },
  predLabel: {
    fontSize: 10,
    color: "#6b7280",
  },
  predValue: {
    fontSize: 20,
    fontWeight: "700",
  },
  updatedAt: {
    marginTop: 8,
    fontSize: 11,
    color: "#9ca3af",
    textAlign: "right",
  },
  sectionTitle: {
    fontSize: 15,
    fontWeight: "700",
    color: "#111827",
    marginBottom: 8,
  },
  infoText: {
    fontSize: 14,
    color: "#374151",
    marginBottom: 4,
  },
  directionsBtn: {
    backgroundColor: "#1d4ed8",
    borderRadius: 12,
    paddingVertical: 14,
    alignItems: "center",
  },
  directionsBtnText: {
    color: "#ffffff",
    fontSize: 16,
    fontWeight: "700",
  },
  legend: {
    flexDirection: "row",
    justifyContent: "center",
    gap: 16,
    marginTop: 10,
  },
  legendItem: {
    flexDirection: "row",
    alignItems: "center",
    gap: 5,
  },
  legendDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
  },
  legendLabel: {
    fontSize: 11,
    color: "#6b7280",
  },
  eventRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    gap: 10,
    paddingVertical: 8,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#f3f4f6",
  },
  eventEmoji: {
    fontSize: 22,
    lineHeight: 28,
  },
  eventInfo: {
    flex: 1,
  },
  eventName: {
    fontSize: 14,
    fontWeight: "600",
    color: "#111827",
  },
  eventMeta: {
    fontSize: 12,
    color: "#6b7280",
    marginTop: 2,
  },
  eventAttend: {
    fontSize: 11,
    color: "#9ca3af",
    marginTop: 2,
  },
});
