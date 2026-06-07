import React from 'react';
import { Platform, Pressable, StyleSheet, Text, View } from 'react-native';

import { occupancyColor } from './OccupancyBar';
import { formatDistance } from '../utils/geo';
import { Location } from '../types';

interface Props {
  location: Location;
  onPress: () => void;
}

export default function RecommendedCard({ location, onPress }: Props) {
  const color = occupancyColor(location.occupancy_pct);
  const occupancyLabel =
    location.occupancy_pct != null
      ? `${Math.round(location.occupancy_pct)}% full`
      : null;

  return (
    <Pressable
      style={({ pressed }) => [styles.card, pressed && styles.pressed]}
      onPress={onPress}
    >
      <Text style={styles.badge}>RECOMMENDED</Text>
      <Text style={styles.name} numberOfLines={2}>
        {location.name}
      </Text>
      <Text style={styles.meta}>
        {location.distanceKm != null ? formatDistance(location.distanceKm) : '—'}
        {'  ·  '}
        <Text style={[styles.free, { color }]}>
          {location.free_spots ?? '—'} free
        </Text>
        {occupancyLabel ? `  ·  ${occupancyLabel}` : ''}
      </Text>
      <Text style={styles.cta}>View details →</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: '#fffbeb',
    borderRadius: 14,
    borderWidth: 1,
    borderColor: '#fcd34d',
    padding: 14,
    marginHorizontal: 12,
    marginBottom: 8,
    ...Platform.select({
      ios: {
        shadowColor: '#000',
        shadowOffset: { width: 0, height: 1 },
        shadowOpacity: 0.06,
        shadowRadius: 4,
      },
      android: { elevation: 2 },
    }),
  },
  pressed: {
    opacity: 0.8,
  },
  badge: {
    fontSize: 11,
    fontWeight: '800',
    color: '#b45309',
    letterSpacing: 0.5,
    marginBottom: 4,
  },
  name: {
    fontSize: 16,
    fontWeight: '700',
    color: '#111827',
    marginBottom: 4,
  },
  meta: {
    fontSize: 13,
    color: '#6b7280',
  },
  free: {
    fontWeight: '700',
  },
  cta: {
    marginTop: 8,
    fontSize: 13,
    fontWeight: '600',
    color: '#1d4ed8',
  },
});
