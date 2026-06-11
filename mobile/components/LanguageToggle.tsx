import React from 'react';
import { Pressable, StyleSheet, Text, View } from 'react-native';

import { useLocale, type Locale } from '../i18n';

export default function LanguageToggle() {
  const { locale, setLocale } = useLocale();

  return (
    <View style={styles.row}>
      <ToggleButton
        label="SR"
        active={locale === 'sr-Latn'}
        onPress={() => setLocale('sr-Latn')}
      />
      <ToggleButton
        label="EN"
        active={locale === 'en'}
        onPress={() => setLocale('en')}
      />
    </View>
  );
}

function ToggleButton({
  label,
  active,
  onPress,
}: {
  label: string;
  active: boolean;
  onPress: () => void;
}) {
  return (
    <Pressable
      onPress={onPress}
      style={[styles.btn, active && styles.btnActive]}
      accessibilityRole="button"
      accessibilityState={{ selected: active }}
    >
      <Text style={[styles.btnText, active && styles.btnTextActive]}>
        {label}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: 'row',
    borderRadius: 8,
    overflow: 'hidden',
    borderWidth: 1,
    borderColor: '#e5e7eb',
    marginRight: 4,
  },
  btn: {
    paddingHorizontal: 10,
    paddingVertical: 5,
    backgroundColor: '#f9fafb',
  },
  btnActive: {
    backgroundColor: '#1e3a5f',
  },
  btnText: {
    fontSize: 12,
    fontWeight: '600',
    color: '#6b7280',
  },
  btnTextActive: {
    color: '#ffffff',
  },
});
