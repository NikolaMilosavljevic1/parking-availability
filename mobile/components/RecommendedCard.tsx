import React from 'react';
import { Platform, Pressable, StyleSheet, Text, View } from 'react-native';

import { occupancyColor } from './OccupancyBar';
import { formatDistance } from '../utils/geo';
import { formatRateShort, useLocale } from '../i18n';
import { Location } from '../types';

interface Props {
  location: Location;
  onPress: () => void;
}

export default function RecommendedCard({ location, onPress }: Props) {
  const { t, locale } = useLocale();
  const color = occupancyColor(location.occupancy_pct);
  const rateLabel = formatRateShort(location, locale);

  return (
    <Pressable
      style={({ pressed }) => [styles.card, pressed && styles.pressed]}
      onPress={onPress}
    >
      <Text style={styles.badge}>{t('recommended').toUpperCase()}</Text>
      <Text style={styles.name} numberOfLines={2}>
        {location.name}
      </Text>
      <Text style={styles.meta}>
        {location.distanceKm != null ? formatDistance(location.distanceKm) : '—'}
        {'  ·  '}
        <Text style={[styles.free, { color }]}>
          {location.free_spots ?? '—'} {t('free')}
        </Text>
        {rateLabel ? `  ·  ${rateLabel}` : ''}
      </Text>
      <Text style={styles.cta}>{t('viewDetails')}</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: '#f8fafc',
    borderRadius: 14,
    borderWidth: 1,
    borderColor: '#cbd5e1',
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
    fontWeight: '700',
    color: '#1e3a5f',
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
    color: '#1e3a5f',
  },
});
