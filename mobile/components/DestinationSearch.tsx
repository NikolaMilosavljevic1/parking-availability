import React, { useState } from 'react';
import {
  ActivityIndicator,
  Modal,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';

import { formatDistance } from '../utils/geo';
import { DestinationOption, searchDestinations } from '../utils/geocoding';
import { AnchorPoint, SortMode } from '../types';

interface Props {
  sortMode: SortMode;
  anchorLabel: string | null;
  userCoords: { lat: number; lng: number } | null;
  onDestinationSelected: (anchor: AnchorPoint) => void;
  onClearDestination: () => void;
}

export default function DestinationSearch({
  sortMode,
  anchorLabel,
  userCoords,
  onDestinationSelected,
  onClearDestination,
}: Props) {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pickerVisible, setPickerVisible] = useState(false);
  const [pickerQuery, setPickerQuery] = useState('');
  const [options, setOptions] = useState<DestinationOption[]>([]);

  const selectOption = (option: DestinationOption) => {
    onDestinationSelected({
      lat: option.lat,
      lng: option.lng,
      label: option.label,
    });
    setQuery('');
    setPickerVisible(false);
    setOptions([]);
    setPickerQuery('');
  };

  const handleSubmit = async () => {
    const trimmed = query.trim();
    if (!trimmed) return;

    setLoading(true);
    setError(null);

    try {
      const results = await searchDestinations(trimmed, userCoords);

      if (!results.length) {
        setError('Address not found — try a more specific place in Belgrade.');
        return;
      }

      if (results.length === 1) {
        selectOption(results[0]);
        return;
      }

      setPickerQuery(trimmed);
      setOptions(results);
      setPickerVisible(true);
    } catch {
      setError('Could not look up that address. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const closePicker = () => {
    setPickerVisible(false);
    setOptions([]);
    setPickerQuery('');
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

      <Modal
        visible={pickerVisible}
        transparent
        animationType="slide"
        onRequestClose={closePicker}
      >
        <Pressable style={styles.backdrop} onPress={closePicker}>
          <Pressable style={styles.sheet} onPress={(e) => e.stopPropagation()}>
            <Text style={styles.sheetTitle}>
              Results for "{pickerQuery}"
            </Text>
            <Text style={styles.sheetSubtitle}>
              Tap the correct location — sorted nearest to you
            </Text>

            {options.map((option, index) => (
              <Pressable
                key={`${option.lat}-${option.lng}-${index}`}
                style={({ pressed }) => [
                  styles.option,
                  pressed && styles.optionPressed,
                ]}
                onPress={() => selectOption(option)}
              >
                <Text style={styles.optionBullet}>○</Text>
                <View style={styles.optionText}>
                  <Text style={styles.optionLabel} numberOfLines={2}>
                    {option.label}
                  </Text>
                  {option.distanceKm != null && (
                    <Text style={styles.optionDistance}>
                      {formatDistance(option.distanceKm)} from you
                    </Text>
                  )}
                </View>
              </Pressable>
            ))}

            <Pressable
              style={({ pressed }) => [
                styles.cancel,
                pressed && styles.optionPressed,
              ]}
              onPress={closePicker}
            >
              <Text style={styles.cancelText}>Cancel</Text>
            </Pressable>
          </Pressable>
        </Pressable>
      </Modal>
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
  backdrop: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.4)',
    justifyContent: 'flex-end',
  },
  sheet: {
    backgroundColor: '#ffffff',
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    paddingHorizontal: 20,
    paddingTop: 20,
    paddingBottom: 32,
    maxHeight: '70%',
  },
  sheetTitle: {
    fontSize: 18,
    fontWeight: '700',
    color: '#111827',
    marginBottom: 4,
  },
  sheetSubtitle: {
    fontSize: 13,
    color: '#6b7280',
    marginBottom: 16,
  },
  option: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    paddingVertical: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#e5e7eb',
    gap: 10,
  },
  optionPressed: {
    opacity: 0.6,
  },
  optionBullet: {
    fontSize: 16,
    color: '#1d4ed8',
    lineHeight: 22,
  },
  optionText: {
    flex: 1,
    gap: 2,
  },
  optionLabel: {
    fontSize: 15,
    fontWeight: '600',
    color: '#111827',
  },
  optionDistance: {
    fontSize: 13,
    color: '#6b7280',
  },
  cancel: {
    marginTop: 12,
    paddingVertical: 14,
    alignItems: 'center',
  },
  cancelText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#6b7280',
  },
});
