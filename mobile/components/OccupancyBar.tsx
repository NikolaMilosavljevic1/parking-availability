import React from 'react';
import { View, Text, StyleSheet } from 'react-native';

import { useLocale } from '../i18n';

interface Props {
  occupancyPct: number | null;
  freeSpots: number | null;
  totalSpots: number | null;
  /** Show the "X / Y free" label alongside the bar. Default: true */
  showLabel?: boolean;
}

/** Returns a color based on occupancy percentage. */
export function occupancyColor(pct: number | null): string {
  if (pct === null) return '#9ca3af';
  if (pct < 50) return '#22c55e';
  if (pct < 80) return '#f59e0b';
  return '#ef4444';
}

export default function OccupancyBar({
  occupancyPct,
  freeSpots,
  totalSpots,
  showLabel = true,
}: Props) {
  const { t } = useLocale();
  const pct = occupancyPct ?? 0;
  const color = occupancyColor(occupancyPct);

  const label = (() => {
    if (freeSpots === null) return t('noData');
    if (totalSpots) {
      return t('freeOfTotal', { free: freeSpots, total: totalSpots });
    }
    return t('freeCount', { free: freeSpots });
  })();

  return (
    <View style={styles.container}>
      <View style={styles.track}>
        <View
          style={[
            styles.fill,
            {
              width: `${Math.min(100, Math.max(0, pct))}%` as `${number}%`,
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
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  track: {
    flex: 1,
    height: 6,
    borderRadius: 3,
    backgroundColor: '#e5e7eb',
    overflow: 'hidden',
  },
  fill: {
    height: '100%',
    borderRadius: 3,
  },
  label: {
    fontSize: 12,
    fontWeight: '600',
    width: 90,
    textAlign: 'right',
  },
});
