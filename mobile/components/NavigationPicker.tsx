import React, { useCallback, useEffect, useState } from 'react';
import {
  Linking,
  Modal,
  Platform,
  Pressable,
  StyleSheet,
  Text,
  View,
} from 'react-native';

interface NavApp {
  id: string;
  name: string;
  buildUrl: (lat: number, lng: number, name: string) => string;
  checkScheme: string;
  platforms: ('ios' | 'android')[];
}

const NAV_APPS: NavApp[] = [
  {
    id: 'google-maps',
    name: 'Google Maps',
    buildUrl: (lat, lng) =>
      `comgooglemaps://?daddr=${lat},${lng}&directionsmode=driving`,
    checkScheme: 'comgooglemaps://',
    platforms: ['ios', 'android'],
  },
  {
    id: 'apple-maps',
    name: 'Apple Maps',
    buildUrl: (lat, lng) => `maps://?daddr=${lat},${lng}`,
    checkScheme: 'maps://',
    platforms: ['ios'],
  },
  {
    id: 'waze',
    name: 'Waze',
    buildUrl: (lat, lng) => `waze://?ll=${lat},${lng}&navigate=yes`,
    checkScheme: 'waze://',
    platforms: ['ios', 'android'],
  },
];

const WEB_FALLBACK = (lat: number, lng: number) =>
  `https://www.google.com/maps/dir/?api=1&destination=${lat},${lng}`;

interface Props {
  visible: boolean;
  lat: number;
  lng: number;
  name: string;
  onClose: () => void;
}

export default function NavigationPicker({
  visible,
  lat,
  lng,
  name,
  onClose,
}: Props) {
  const [availableApps, setAvailableApps] = useState<NavApp[]>([]);

  const platform = Platform.OS === 'ios' ? 'ios' : 'android';

  const detectApps = useCallback(async () => {
    const candidates = NAV_APPS.filter((app) =>
      app.platforms.includes(platform),
    );

    const checks = await Promise.all(
      candidates.map(async (app) => {
        try {
          const canOpen = await Linking.canOpenURL(app.checkScheme);
          return canOpen ? app : null;
        } catch {
          return app;
        }
      }),
    );

    const detected = checks.filter((app): app is NavApp => app != null);

    // Expo Go may not detect installed apps — show all platform options as fallback
    setAvailableApps(detected.length > 0 ? detected : candidates);
  }, [platform]);

  useEffect(() => {
    if (visible) {
      detectApps();
    }
  }, [visible, detectApps]);

  const openApp = async (app: NavApp) => {
    const url = app.buildUrl(lat, lng, name);
    try {
      await Linking.openURL(url);
      onClose();
    } catch {
      try {
        await Linking.openURL(WEB_FALLBACK(lat, lng));
        onClose();
      } catch {
        // user cancelled or no handler
      }
    }
  };

  const openWebFallback = async () => {
    try {
      await Linking.openURL(WEB_FALLBACK(lat, lng));
      onClose();
    } catch {
      // ignore
    }
  };

  return (
    <Modal
      visible={visible}
      transparent
      animationType="slide"
      onRequestClose={onClose}
    >
      <Pressable style={styles.backdrop} onPress={onClose}>
        <Pressable style={styles.sheet} onPress={(e) => e.stopPropagation()}>
          <Text style={styles.title}>Navigate with</Text>
          <Text style={styles.subtitle} numberOfLines={2}>
            {name}
          </Text>

          {availableApps.map((app) => (
            <Pressable
              key={app.id}
              style={({ pressed }) => [
                styles.option,
                pressed && styles.optionPressed,
              ]}
              onPress={() => openApp(app)}
            >
              <Text style={styles.optionName}>{app.name}</Text>
            </Pressable>
          ))}

          <Pressable
            style={({ pressed }) => [
              styles.option,
              pressed && styles.optionPressed,
            ]}
            onPress={openWebFallback}
          >
            <Text style={styles.optionName}>Google Maps (browser)</Text>
          </Pressable>

          <Pressable
            style={({ pressed }) => [
              styles.cancel,
              pressed && styles.optionPressed,
            ]}
            onPress={onClose}
          >
            <Text style={styles.cancelText}>Cancel</Text>
          </Pressable>
        </Pressable>
      </Pressable>
    </Modal>
  );
}

const styles = StyleSheet.create({
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
  },
  title: {
    fontSize: 18,
    fontWeight: '700',
    color: '#111827',
    marginBottom: 4,
  },
  subtitle: {
    fontSize: 14,
    color: '#6b7280',
    marginBottom: 16,
  },
  option: {
    paddingVertical: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#e5e7eb',
  },
  optionPressed: {
    opacity: 0.6,
  },
  optionName: {
    fontSize: 16,
    fontWeight: '600',
    color: '#1d4ed8',
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
