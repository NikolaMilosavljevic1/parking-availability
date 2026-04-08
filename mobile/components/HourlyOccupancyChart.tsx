import React, { useEffect, useRef } from "react";
import { ScrollView, StyleSheet, Text, View } from "react-native";

import { occupancyColor } from "./OccupancyBar";
import { Snapshot } from "../types";

// ---------------------------------------------------------------------------
// Layout constants
// ---------------------------------------------------------------------------

const BAR_W   = 22;   // bar width in dp
const BAR_GAP = 6;    // gap between bars
const SLOT_W  = BAR_W + BAR_GAP; // total slot width per hour
const CHART_H = 112;  // usable bar height
const LABEL_H = 22;   // hour-label row height
const Y_W     = 34;   // fixed y-axis column width

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface Props {
  snapshots: Snapshot[]; // hourly-averaged, oldest → newest
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function HourlyOccupancyChart({ snapshots }: Props) {
  const scrollRef = useRef<ScrollView>(null);

  // Always start scrolled to the rightmost (most recent) bar
  useEffect(() => {
    if (snapshots.length > 0) {
      scrollRef.current?.scrollToEnd({ animated: false });
    }
  }, [snapshots.length]);

  if (snapshots.length === 0) {
    return (
      <Text style={styles.empty}>
        Not enough data yet — check back after a few scrape cycles.
      </Text>
    );
  }

  const contentWidth = snapshots.length * SLOT_W;

  return (
    <View style={styles.wrapper}>
      {/* ── Y-axis labels ──────────────────────────────────────────── */}
      <View style={[styles.yAxis, { height: CHART_H + LABEL_H }]}>
        {([100, 50, 0] as const).map((pct) => (
          <Text
            key={pct}
            style={[
              styles.yLabel,
              { bottom: (pct / 100) * CHART_H + LABEL_H - 7 },
            ]}
          >
            {pct}%
          </Text>
        ))}
      </View>

      {/* ── Scrollable chart area ───────────────────────────────────── */}
      <ScrollView
        ref={scrollRef}
        horizontal
        showsHorizontalScrollIndicator={false}
        style={styles.scroll}
        contentContainerStyle={{ width: contentWidth }}
      >
        {/* Chart area (bars + grid lines) */}
        <View style={{ height: CHART_H, width: contentWidth }}>
          {/* Grid lines at 50 % and 100 % */}
          {([50, 100] as const).map((pct) => (
            <View
              key={pct}
              style={[
                styles.gridLine,
                { bottom: (pct / 100) * CHART_H, width: contentWidth },
              ]}
            />
          ))}

          {/* Bars — aligned to bottom of chart area */}
          <View style={styles.barsRow}>
            {snapshots.map((s, i) => {
              const pct    = s.occupancy_pct ?? 0;
              const barH   = Math.max(4, (pct / 100) * CHART_H);
              const color  = occupancyColor(s.occupancy_pct);
              const isNow  = i === snapshots.length - 1;

              return (
                <View key={s.scraped_at} style={styles.barSlot}>
                  <View
                    style={[
                      styles.bar,
                      {
                        height: barH,
                        backgroundColor: color,
                        opacity: isNow ? 1 : 0.65,
                      },
                    ]}
                  />
                </View>
              );
            })}
          </View>
        </View>

        {/* Hour labels */}
        <View style={[styles.labelsRow, { width: contentWidth }]}>
          {snapshots.map((s, i) => {
            const hour   = new Date(s.scraped_at).getHours();
            const isNow  = i === snapshots.length - 1;
            // Show label for "Now", and every 3 hours on the rest
            const show   = isNow || hour % 3 === 0;

            return (
              <View key={s.scraped_at} style={styles.labelSlot}>
                {show && (
                  <Text style={[styles.hourLabel, isNow && styles.hourLabelNow]}>
                    {isNow ? "Now" : `${hour}h`}
                  </Text>
                )}
              </View>
            );
          })}
        </View>
      </ScrollView>
    </View>
  );
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles = StyleSheet.create({
  wrapper: {
    flexDirection: "row",
    alignItems: "flex-start",
  },

  // Y-axis
  yAxis: {
    width: Y_W,
    position: "relative",
  },
  yLabel: {
    position: "absolute",
    right: 6,
    fontSize: 10,
    color: "#9ca3af",
    fontVariant: ["tabular-nums"],
  },

  // Scrollable chart
  scroll: {
    flex: 1,
  },

  // Grid lines
  gridLine: {
    position: "absolute",
    left: 0,
    height: StyleSheet.hairlineWidth,
    backgroundColor: "#e5e7eb",
  },

  // Bars
  barsRow: {
    flexDirection: "row",
    alignItems: "flex-end",
    height: "100%",
    position: "absolute",
    bottom: 0,
    left: 0,
  },
  barSlot: {
    width: SLOT_W,
    alignItems: "center",
    justifyContent: "flex-end",
  },
  bar: {
    width: BAR_W,
    borderTopLeftRadius: 4,
    borderTopRightRadius: 4,
  },

  // Hour labels
  labelsRow: {
    flexDirection: "row",
    height: LABEL_H,
  },
  labelSlot: {
    width: SLOT_W,
    alignItems: "center",
    justifyContent: "center",
  },
  hourLabel: {
    fontSize: 10,
    color: "#9ca3af",
  },
  hourLabelNow: {
    color: "#1d4ed8",
    fontWeight: "700",
    fontSize: 11,
  },

  // Empty state
  empty: {
    fontSize: 13,
    color: "#9ca3af",
    textAlign: "center",
    paddingVertical: 16,
  },
});
