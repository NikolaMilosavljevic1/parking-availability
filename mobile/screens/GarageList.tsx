import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  FlatList,
  Platform,
  Pressable,
  RefreshControl,
  StyleSheet,
  Text,
  View,
} from "react-native";
import * as ExpoLocation from "expo-location";
import { NativeStackScreenProps } from "@react-navigation/native-stack";

import { API_URL, WS_URL } from "../config";
import OccupancyBar, { occupancyColor } from "../components/OccupancyBar";
import { Location, WsLocationUpdate, WsSnapshot } from "../types";
import { RootStackParamList } from "../App";

type Props = NativeStackScreenProps<RootStackParamList, "GarageList">;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function haversineKm(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function formatDistance(km: number): string {
  if (km < 1) return `${Math.round(km * 1000)} m`;
  return `${km.toFixed(1)} km`;
}

function applyLiveUpdate(
  locations: Location[],
  update: WsLocationUpdate,
): Location[] {
  return locations.map((loc) =>
    loc.id === update.location_id
      ? {
          ...loc,
          free_spots: update.free_spots,
          total_spots: update.total_spots ?? loc.total_spots,
          occupancy_pct: update.occupancy_pct,
          scraped_at: update.scraped_at,
          live: true,
        }
      : loc,
  );
}

function sortByFreeSpots(locs: Location[]): Location[] {
  if (!Array.isArray(locs)) return [];
  // Use .slice() instead of spread — spread can fail in Hermes on React-managed arrays
  return locs.slice().sort((a, b) => {
    if (a.free_spots === null && b.free_spots === null) return 0;
    if (a.free_spots === null) return 1;
    if (b.free_spots === null) return -1;
    return b.free_spots - a.free_spots;
  });
}

// ---------------------------------------------------------------------------
// Row component
// ---------------------------------------------------------------------------

interface RowProps {
  location: Location;
  onPress: () => void;
}

const LocationRow = React.memo(({ location, onPress }: RowProps) => {
  const color = occupancyColor(location.occupancy_pct);

  return (
    <Pressable
      style={({ pressed }: { pressed: boolean }) => [
        styles.row,
        pressed && styles.rowPressed,
      ]}
      onPress={onPress}
    >
      {/* Left: name + type */}
      <View style={styles.rowLeft}>
        <Text style={styles.rowName} numberOfLines={1}>
          {location.name}
        </Text>
        <Text style={styles.rowMeta}>
          {location.neighborhood ?? ""}
          {location.neighborhood && location.distanceKm != null ? "  ·  " : ""}
          {location.distanceKm != null
            ? formatDistance(location.distanceKm)
            : ""}
        </Text>
        <OccupancyBar
          occupancyPct={location.occupancy_pct}
          freeSpots={location.free_spots}
          totalSpots={location.total_spots}
        />
      </View>

      {/* Right: free spots count */}
      <View style={styles.rowRight}>
        <Text style={[styles.freeCount, { color }]}>
          {location.free_spots ?? "—"}
        </Text>
        <Text style={styles.freeLabel}>free</Text>
      </View>
    </Pressable>
  );
});

// ---------------------------------------------------------------------------
// Screen
// ---------------------------------------------------------------------------

export default function GarageList({ navigation }: Props) {
  const [locations, setLocations] = useState<Location[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [wsStatus, setWsStatus] = useState<"connecting" | "live" | "error">(
    "connecting",
  );
  const [userCoords, setUserCoords] = useState<{
    lat: number;
    lng: number;
  } | null>(null);

  const wsRef = useRef<WebSocket | null>(null);

  // ---------- Fetch from REST (initial load + pull-to-refresh) ----------

  const fetchLocations = useCallback(async () => {
    try {
      const resp = await fetch(`${API_URL}/locations`);
      const data: Location[] = await resp.json();
      if (!Array.isArray(data)) {
        console.warn("Unexpected /locations response:", data);
        return;
      }
      setLocations((prev) => {
        // Merge existing distance info if GPS is already available
        if (userCoords) {
          return sortByFreeSpots(
            data.map((loc) => ({
              ...loc,
              distanceKm:
                loc.latitude != null && loc.longitude != null
                  ? haversineKm(
                      userCoords.lat,
                      userCoords.lng,
                      loc.latitude,
                      loc.longitude,
                    )
                  : undefined,
            })),
          );
        }
        return sortByFreeSpots(data);
      });
    } catch (e) {
      console.warn("Failed to fetch locations:", e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [userCoords]);

  // ---------- GPS ----------

  useEffect(() => {
    (async () => {
      const { status } = await ExpoLocation.requestForegroundPermissionsAsync();
      if (status !== "granted") return;
      const pos = await ExpoLocation.getCurrentPositionAsync({});
      setUserCoords({ lat: pos.coords.latitude, lng: pos.coords.longitude });
    })();
  }, []);

  // Recalculate distances when GPS comes in
  useEffect(() => {
    if (!userCoords) return;
    setLocations((prev) =>
      sortByFreeSpots(
        prev.map((loc) => ({
          ...loc,
          distanceKm:
            loc.latitude != null && loc.longitude != null
              ? haversineKm(
                  userCoords.lat,
                  userCoords.lng,
                  loc.latitude,
                  loc.longitude,
                )
              : undefined,
        })),
      ),
    );
  }, [userCoords]);

  // ---------- WebSocket ----------

  const connectWs = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close();
    }

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;
    setWsStatus("connecting");

    ws.onopen = () => setWsStatus("live");

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);

        if (msg.type === "snapshot") {
          // Initial snapshot — replace all live data
          const updates: WsLocationUpdate[] = msg.payload;
          setLocations((prev) => {
            // Plain object lookup — avoids Map constructor iterator issues in Hermes
            const updateMap: Record<string, WsLocationUpdate> = {};
            updates.forEach((u) => {
              updateMap[u.location_id] = u;
            });
            return sortByFreeSpots(
              prev.map((loc) => {
                const u = updateMap[loc.id];
                if (!u) return loc;
                return {
                  ...loc,
                  free_spots: u.free_spots,
                  total_spots: u.total_spots ?? loc.total_spots,
                  occupancy_pct: u.occupancy_pct,
                  scraped_at: u.scraped_at,
                  live: true,
                };
              }),
            );
          });
        } else if (msg.location_id) {
          // Single-location update from scraper
          setLocations((prev) => sortByFreeSpots(applyLiveUpdate(prev, msg)));
        }
      } catch (e) {
        console.warn("WS parse error:", e);
      }
    };

    ws.onerror = () => setWsStatus("error");

    ws.onclose = () => {
      setWsStatus("error");
      // Reconnect after 5 s
      setTimeout(connectWs, 5000);
    };
  }, []);

  // ---------- Mount ----------

  useEffect(() => {
    fetchLocations();
    connectWs();
    return () => wsRef.current?.close();
  }, []);

  // ---------- Render ----------

  const onRefresh = useCallback(() => {
    setRefreshing(true);
    fetchLocations();
  }, [fetchLocations]);

  if (loading) {
    return (
      <View style={styles.centered}>
        <ActivityIndicator size="large" color="#1d4ed8" />
        <Text style={styles.loadingText}>Loading parking data…</Text>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      {/* Connection status badge */}
      <View
        style={[
          styles.badge,
          wsStatus === "live" ? styles.badgeLive : styles.badgeError,
        ]}
      >
        <Text style={styles.badgeText}>
          {wsStatus === "live"
            ? "● Live"
            : wsStatus === "connecting"
              ? "● Connecting…"
              : "● Reconnecting…"}
        </Text>
      </View>

      <FlatList
        data={locations}
        keyExtractor={(item) => item.id}
        renderItem={({ item }: { item: Location }) => (
          <LocationRow
            location={item}
            onPress={() =>
              navigation.navigate("GarageDetail", { location: item })
            }
          />
        )}
        refreshControl={
          <RefreshControl
            refreshing={refreshing}
            onRefresh={onRefresh}
            tintColor="#1d4ed8"
          />
        }
        contentContainerStyle={styles.list}
        ItemSeparatorComponent={() => <View style={styles.separator} />}
      />
    </View>
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
  centered: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    gap: 12,
  },
  loadingText: {
    color: "#6b7280",
    fontSize: 15,
  },
  badge: {
    paddingHorizontal: 12,
    paddingVertical: 4,
    alignSelf: "flex-end",
    margin: 8,
    borderRadius: 12,
  },
  badgeLive: {
    backgroundColor: "#dcfce7",
  },
  badgeError: {
    backgroundColor: "#fee2e2",
  },
  badgeText: {
    fontSize: 12,
    fontWeight: "600",
    color: "#374151",
  },
  list: {
    paddingHorizontal: 12,
    paddingBottom: 24,
  },
  separator: {
    height: StyleSheet.hairlineWidth,
    backgroundColor: "#e5e7eb",
  },
  row: {
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: "#ffffff",
    paddingHorizontal: 16,
    paddingVertical: 14,
    borderRadius: 12,
    marginVertical: 4,
    // Shadow
    ...Platform.select({
      ios: {
        shadowColor: "#000",
        shadowOffset: { width: 0, height: 1 },
        shadowOpacity: 0.06,
        shadowRadius: 4,
      },
      android: { elevation: 2 },
    }),
  },
  rowPressed: {
    opacity: 0.75,
  },
  rowLeft: {
    flex: 1,
    gap: 4,
  },
  rowName: {
    fontSize: 15,
    fontWeight: "600",
    color: "#111827",
  },
  rowMeta: {
    fontSize: 12,
    color: "#6b7280",
  },
  rowRight: {
    alignItems: "center",
    marginLeft: 12,
    minWidth: 44,
  },
  freeCount: {
    fontSize: 22,
    fontWeight: "700",
    lineHeight: 26,
  },
  freeLabel: {
    fontSize: 11,
    color: "#9ca3af",
  },
});
