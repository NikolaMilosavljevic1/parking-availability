import React, { useState } from 'react';
import {
  ActivityIndicator,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import * as ExpoLocation from 'expo-location';

import { AnchorPoint, SortMode } from '../types';

interface Props {
  sortMode: SortMode;
  anchorLabel: string | null;
  onDestinationSelected: (anchor: AnchorPoint) => void;
  onClearDestination: () => void;
}

function normalizeQuery(query: string): string {
  const trimmed = query.trim();
  if (!trimmed) return trimmed;
  if (trimmed.toLowerCase().includes('belgrade') || trimmed.toLowerCase().includes('beograd')) {
    return trimmed;
  }
  return `${trimmed}, Belgrade, Serbia`;
}

export default function DestinationSearch({
  sortMode,
  anchorLabel,
  onDestinationSelected,
  onClearDestination,
}: Props) {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async () => {
    const normalized = normalizeQuery(query);
    if (!normalized) return;

    setLoading(true);
    setError(null);

    try {
      const results = await ExpoLocation.geocodeAsync(normalized);
      if (!results.length) {
        setError('Address not found — try a more specific place in Belgrade.');
        return;
      }

      const first = results[0];
      const label =
        query.trim() ||
        first.name ||
        first.street ||
        'Selected destination';

      onDestinationSelected({
        lat: first.latitude,
        lng: first.longitude,
        label,
      });
      setQuery('');
    } catch {
      setError('Could not look up that address. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <View style={styles.container}>
      <View style={styles.searchRow}>
        <TextInput
          style={styles.input}
          placeholder="Where are you going? (optional)"
          placeholderTextColor="#9ca3af"
          value={query}
          onChangeText={(text) => {
            setQuery(text);
            if (error) setError(null);
          }}
          onSubmitEditing={handleSubmit}
          returnKeyType="search"
          editable={!loading}
        />
        {loading && (
          <ActivityIndicator
            size="small"
            color="#1d4ed8"
            style={styles.spinner}
          />
        )}
      </View>

      {error && <Text style={styles.error}>{error}</Text>}

      <Pressable
        style={styles.chip}
        onPress={sortMode === 'destination' ? onClearDestination : undefined}
        disabled={sortMode !== 'destination'}
      >
        <Text style={styles.chipText}>
          {sortMode === 'destination' && anchorLabel
            ? `× Near: ${anchorLabel}`
            : 'Near your location'}
        </Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    paddingHorizontal: 12,
    paddingTop: 4,
    paddingBottom: 8,
    gap: 6,
  },
  searchRow: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: '#ffffff',
    borderRadius: 12,
    borderWidth: 1,
    borderColor: '#e5e7eb',
    paddingHorizontal: 12,
  },
  input: {
    flex: 1,
    fontSize: 15,
    color: '#111827',
    paddingVertical: 10,
  },
  spinner: {
    marginLeft: 8,
  },
  error: {
    fontSize: 12,
    color: '#dc2626',
    paddingHorizontal: 4,
  },
  chip: {
    alignSelf: 'flex-start',
    backgroundColor: '#eff6ff',
    borderRadius: 16,
    paddingHorizontal: 12,
    paddingVertical: 6,
  },
  chipText: {
    fontSize: 12,
    fontWeight: '600',
    color: '#1d4ed8',
  },
});
