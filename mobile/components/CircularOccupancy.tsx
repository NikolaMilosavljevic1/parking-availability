import React from 'react';
import { StyleSheet, Text, View } from 'react-native';
import Svg, { Circle } from 'react-native-svg';

import { occupancyColor } from './OccupancyBar';
import { useLocale } from '../i18n';

const SIZE = 128;
const STROKE = 12;
const RADIUS = (SIZE - STROKE) / 2;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;

interface Props {
  occupancyPct: number | null;
}

export default function CircularOccupancy({ occupancyPct }: Props) {
  const { t } = useLocale();
  const pct =
    occupancyPct == null ? 0 : Math.min(100, Math.max(0, occupancyPct));
  const color = occupancyColor(occupancyPct);
  const dash = (pct / 100) * CIRCUMFERENCE;

  return (
    <View style={styles.wrap} accessibilityRole="image">
      <Svg width={SIZE} height={SIZE}>
        <Circle
          cx={SIZE / 2}
          cy={SIZE / 2}
          r={RADIUS}
          stroke="#e5e7eb"
          strokeWidth={STROKE}
          fill="none"
        />
        <Circle
          cx={SIZE / 2}
          cy={SIZE / 2}
          r={RADIUS}
          stroke={color}
          strokeWidth={STROKE}
          fill="none"
          strokeDasharray={`${dash} ${CIRCUMFERENCE}`}
          strokeLinecap="round"
          transform={`rotate(-90 ${SIZE / 2} ${SIZE / 2})`}
        />
      </Svg>
      <View style={styles.center} pointerEvents="none">
        <Text style={[styles.pct, { color }]}>
          {occupancyPct == null ? '—' : `${Math.round(pct)}%`}
        </Text>
        <Text style={styles.caption}>{t('occupancy')}</Text>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    width: SIZE,
    height: SIZE,
    alignItems: 'center',
    justifyContent: 'center',
  },
  center: {
    ...StyleSheet.absoluteFillObject,
    alignItems: 'center',
    justifyContent: 'center',
  },
  pct: {
    fontSize: 22,
    fontWeight: '700',
    letterSpacing: -0.5,
  },
  caption: {
    fontSize: 11,
    color: '#6b7280',
    marginTop: 2,
  },
});
