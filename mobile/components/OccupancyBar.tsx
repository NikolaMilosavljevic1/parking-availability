import React from "react";
import { View, Text, StyleSheet } from "react-native";

interface Props {
  occupancyPct: number | null;
  freeSpots: number | null;
  totalSpots: number | null;
  /** Show the "X / Y free" label alongside the bar. Default: true */
  showLabel?: boolean;
}

/** Returns a color based on occupancy percentage. */
export function occupancyColor(pct: number | null): string {
  if (pct === null) return "#9ca3af"; // grey — unknown
  if (pct < 50) return "#22c55e"; // green
  if (pct < 80) return "#f59e0b"; // amber
  return "#ef4444"; // red
}

/**
 * OccupancyBar
 * A horizontal progress bar that fills based on occupancy, coloured
 * green → amber → red. Shows free/total counts alongside.
 */
export default function OccupancyBar({
  occupancyPct,
  freeSpots,
  totalSpots,
  showLabel = true,
}: Props) {
  const pct = occupancyPct ?? 0;
  const color = occupancyColor(occupancyPct);

  const label = (() => {
    if (freeSpots === null) return "No data";
    if (totalSpots) return `${freeSpots} / ${totalSpots} free`;
    return `${freeSpots} free`;
  })();

  return (
    <View style={styles.container}>
      {/* Track */}
      <View style={styles.track}>
        <View
          style={[
            styles.fill,
            {
              width: `${Math.min(100, Math.max(0, pct))}%` as any,
              backgroundColor: color,
            },
          ]}
        />
      </View>

      {showLabel && <Text style={[styles.label, { color }]}>{label}</Text>}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  track: {
    flex: 1,
    height: 6,
    borderRadius: 3,
    backgroundColor: "#e5e7eb",
    overflow: "hidden",
  },
  fill: {
    height: "100%",
    borderRadius: 3,
  },
  label: {
    fontSize: 12,
    fontWeight: "600",
    width: 90,
    textAlign: "right",
  },
});
