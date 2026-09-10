import React, { useEffect, useMemo, useState } from 'react';
import { Platform, StyleSheet, Text, View } from 'react-native';
import MapView, { Marker } from 'react-native-maps';

import { occupancyColor } from './OccupancyBar';
import { useLocale } from '../i18n';
import { Location } from '../types';

const BELGRADE = {
  latitude: 44.8176,
  longitude: 20.4633,
  latitudeDelta: 0.08,
  longitudeDelta: 0.08,
};

interface Props {
  locations: Location[];
  userCoords: { lat: number; lng: number } | null;
  onSelect: (location: Location) => void;
}

function shortGarageName(name: string): string {
  return name
    .replace(/^Garaža\s+"?/, '')
    .replace(/^Parkiralište\s+"?/, '')
    .replace(/"$/, '')
    .trim();
}

export default function GarageMap({ locations, userCoords, onSelect }: Props) {
  const { t } = useLocale();
  const [tracksViewChanges, setTracksViewChanges] = useState(true);

  useEffect(() => {
    const id = setTimeout(() => setTracksViewChanges(false), 800);
    return () => clearTimeout(id);
  }, [locations.length]);

  const mapped = useMemo(
    () =>
      locations.filter(
        (loc) => loc.latitude != null && loc.longitude != null,
      ),
    [locations],
  );

  const region = useMemo(() => {
    if (userCoords) {
      return {
        latitude: userCoords.lat,
        longitude: userCoords.lng,
        latitudeDelta: 0.05,
        longitudeDelta: 0.05,
      };
    }
    return BELGRADE;
  }, [userCoords]);

  return (
    <View style={styles.container}>
      <MapView
        style={styles.map}
        key={userCoords ? `${userCoords.lat.toFixed(4)},${userCoords.lng.toFixed(4)}` : 'belgrade'}
        initialRegion={region}
        showsUserLocation={userCoords != null}
        showsMyLocationButton={Platform.OS === 'android'}
        toolbarEnabled={false}
      >
        {mapped.map((loc) => {
          const label = shortGarageName(loc.name);
          const color = occupancyColor(loc.occupancy_pct);

          return (
            <Marker
              key={loc.id}
              tracksViewChanges={tracksViewChanges}
              anchor={{ x: 0.5, y: 0.28 }}
              coordinate={{
                latitude: loc.latitude!,
                longitude: loc.longitude!,
              }}
              title={label}
              description={
                loc.free_spots != null
                  ? `${loc.free_spots} ${t('free')}`
                  : undefined
              }
              onPress={() => onSelect(loc)}
            >
              <View style={styles.marker}>
                <View
                  style={[
                    styles.pin,
                    { backgroundColor: color },
                    loc.isRecommended && styles.pinRecommended,
                  ]}
                />
                <View style={styles.labelBubble}>
                  <Text style={styles.labelText} numberOfLines={1}>
                    {label}
                  </Text>
                </View>
              </View>
            </Marker>
          );
        })}
      </MapView>

      <View style={styles.legend} pointerEvents="none">
        {userCoords && (
          <Text style={styles.youAreHere}>{t('youAreHere')}</Text>
        )}
        <LegendDot color="#22c55e" label={t('mapLegendLow')} />
        <LegendDot color="#f59e0b" label={t('mapLegendMid')} />
        <LegendDot color="#ef4444" label={t('mapLegendHigh')} />
      </View>
    </View>
  );
}

function LegendDot({ color, label }: { color: string; label: string }) {
  return (
    <View style={styles.legendItem}>
      <View style={[styles.legendDot, { backgroundColor: color }]} />
      <Text style={styles.legendLabel}>{label}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    marginTop: 8,
  },
  map: {
    flex: 1,
  },
  marker: {
    alignItems: 'center',
    maxWidth: 108,
  },
  pin: {
    width: 16,
    height: 16,
    borderRadius: 8,
    borderWidth: 2,
    borderColor: '#ffffff',
    zIndex: 1,
    ...Platform.select({
      ios: {
        shadowColor: '#000',
        shadowOffset: { width: 0, height: 1 },
        shadowOpacity: 0.25,
        shadowRadius: 2,
      },
      android: { elevation: 3 },
    }),
  },
  pinRecommended: {
    width: 20,
    height: 20,
    borderRadius: 10,
    borderColor: '#fcd34d',
    borderWidth: 3,
  },
  labelBubble: {
    marginTop: 3,
    backgroundColor: 'rgba(255,255,255,0.94)',
    borderRadius: 6,
    paddingHorizontal: 6,
    paddingVertical: 2,
    maxWidth: 108,
    ...Platform.select({
      ios: {
        shadowColor: '#000',
        shadowOffset: { width: 0, height: 1 },
        shadowOpacity: 0.12,
        shadowRadius: 2,
      },
      android: { elevation: 2 },
    }),
  },
  labelText: {
    fontSize: 10,
    fontWeight: '600',
    color: '#111827',
    textAlign: 'center',
  },
  legend: {
    position: 'absolute',
    left: 12,
    bottom: 16,
    backgroundColor: 'rgba(255,255,255,0.94)',
    borderRadius: 10,
    paddingHorizontal: 10,
    paddingVertical: 8,
    gap: 4,
  },
  legendItem: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  legendDot: {
    width: 10,
    height: 10,
    borderRadius: 5,
  },
  legendLabel: {
    fontSize: 11,
    color: '#374151',
    fontWeight: '500',
  },
  youAreHere: {
    fontSize: 11,
    fontWeight: '700',
    color: '#1e3a5f',
    marginBottom: 4,
  },
});
