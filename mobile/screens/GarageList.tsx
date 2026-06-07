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
import DestinationSearch from "../components/DestinationSearch";
import OccupancyBar, { occupancyColor } from "../components/OccupancyBar";
import RecommendedCard from "../components/RecommendedCard";
import {
  enrichAndSortLocations,
  formatDistanceLabel,
} from "../utils/geo";
import {
  AnchorPoint,
  Location,
  SortMode,
  WsLocationUpdate,
} from "../types";
import { RootStackParamList } from "../App";

type Props = NativeStackScreenProps<RootStackParamList, "GarageList">;

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

interface RowProps {
  location: Location;
  sortMode: SortMode;
  onPress: () => void;
}

const LocationRow = React.memo(({ location, sortMode, onPress }: RowProps) => {
  const color = occupancyColor(location.occupancy_pct);

  return (
    <Pressable
      style={({ pressed }: { pressed: boolean }) => [
        styles.row,
        location.isRecommended && styles.rowRecommended,
        pressed && styles.rowPressed,
      ]}
      onPress={onPress}
    >
      <View style={styles.rowLeft}>
        <Text style={styles.rowName} numberOfLines={1}>
          {location.name}
        </Text>
        <Text style={styles.rowMeta}>
          {location.neighborhood ?? ""}
          {location.neighborhood && location.distanceKm != null ? "  ·  " : ""}
          {location.distanceKm != null
            ? formatDistanceLabel(location.distanceKm, sortMode)
            : ""}
        </Text>
        <OccupancyBar
          occupancyPct={location.occupancy_pct}
          freeSpots={location.free_spots}
          totalSpots={location.total_spots}
        />
      </View>

      <View style={styles.rowRight}>
        <Text style={[styles.freeCount, { color }]}>
          {location.free_spots ?? "—"}
        </Text>
        <Text style={styles.freeLabel}>free</Text>
      </View>
    </Pressable>
  );
});

export default function GarageList({ navigation }: Props) {
  const [locations, setLocations] = useState<Location[]>([]);
  const [recommended, setRecommended] = useState<Location | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [wsStatus, setWsStatus] = useState<"connecting" | "live" | "error">(
    "connecting",
  );
  const [sortMode, setSortMode] = useState<SortMode>("near_me");
  const [anchor, setAnchor] = useState<AnchorPoint | null>(null);
  const [userCoords, setUserCoords] = useState<{
    lat: number;
    lng: number;
  } | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const anchorRef = useRef<AnchorPoint | null>(null);
  const rawLocationsRef = useRef<Location[]>([]);

  useEffect(() => {
    anchorRef.current = anchor;
  }, [anchor]);

  const processAndSet = useCallback(
    (raw: Location[], currentAnchor: AnchorPoint | null) => {
      rawLocationsRef.current = raw;
      const { locations: sorted, recommended: pick } =
        enrichAndSortLocations(raw, currentAnchor);
      setLocations(sorted);
      setRecommended(pick);
    },
    [],
  );

  const fetchLocations = useCallback(async () => {
    try {
      const resp = await fetch(`${API_URL}/locations`);
      const data: Location[] = await resp.json();
      if (!Array.isArray(data)) {
        console.warn("Unexpected /locations response:", data);
        return;
      }
      processAndSet(data, anchorRef.current);
    } catch (e) {
      console.warn("Failed to fetch locations:", e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [processAndSet]);

  useEffect(() => {
    (async () => {
      const { status } = await ExpoLocation.requestForegroundPermissionsAsync();
      if (status !== "granted") return;
      const pos = await ExpoLocation.getCurrentPositionAsync({});
      const coords = { lat: pos.coords.latitude, lng: pos.coords.longitude };
      setUserCoords(coords);
      setAnchor((prev) =>
        prev && sortMode === "destination"
          ? prev
          : { lat: coords.lat, lng: coords.lng, label: "Your location" },
      );
    })();
  }, []);

  useEffect(() => {
    if (!userCoords || sortMode !== "near_me") return;
    setAnchor({
      lat: userCoords.lat,
      lng: userCoords.lng,
      label: "Your location",
    });
  }, [userCoords, sortMode]);

  useEffect(() => {
    if (!anchor) return;
    processAndSet(rawLocationsRef.current, anchor);
  }, [anchor, processAndSet]);

  const handleDestinationSelected = useCallback((dest: AnchorPoint) => {
    setSortMode("destination");
    setAnchor(dest);
  }, []);

  const handleClearDestination = useCallback(() => {
    setSortMode("near_me");
    if (userCoords) {
      setAnchor({
        lat: userCoords.lat,
        lng: userCoords.lng,
        label: "Your location",
      });
    }
  }, [userCoords]);

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
        const currentAnchor = anchorRef.current;

        if (msg.type === "snapshot") {
          const updates: WsLocationUpdate[] = msg.payload;
          const updateMap: Record<string, WsLocationUpdate> = {};
          updates.forEach((u) => {
            updateMap[u.location_id] = u;
          });

          const merged = rawLocationsRef.current.map((loc) => {
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
          });

          processAndSet(merged, currentAnchor);
        } else if (msg.location_id) {
          const merged = applyLiveUpdate(rawLocationsRef.current, msg);
          processAndSet(merged, currentAnchor);
        }
      } catch (e) {
        console.warn("WS parse error:", e);
      }
    };

    ws.onerror = () => setWsStatus("error");

    ws.onclose = () => {
      setWsStatus("error");
      setTimeout(connectWs, 5000);
    };
  }, [processAndSet]);

  useEffect(() => {
    fetchLocations();
    connectWs();
    return () => wsRef.current?.close();
  }, []);

  const onRefresh = useCallback(() => {
    setRefreshing(true);
    fetchLocations();
  }, [fetchLocations]);

  const navigateToDetail = useCallback(
    (location: Location) => {
      navigation.navigate("GarageDetail", { location });
    },
    [navigation],
  );

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

      <DestinationSearch
        sortMode={sortMode}
        anchorLabel={sortMode === "destination" ? anchor?.label ?? null : null}
        onDestinationSelected={handleDestinationSelected}
        onClearDestination={handleClearDestination}
      />

      {recommended && (
        <RecommendedCard
          location={recommended}
          onPress={() => navigateToDetail(recommended)}
        />
      )}

      <FlatList
        data={locations}
        keyExtractor={(item) => item.id}
        renderItem={({ item }: { item: Location }) => (
          <LocationRow
            location={item}
            sortMode={sortMode}
            onPress={() => navigateToDetail(item)}
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
    marginBottom: 0,
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
  rowRecommended: {
    borderWidth: 1,
    borderColor: "#fcd34d",
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
