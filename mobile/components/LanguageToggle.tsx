import React from 'react';
import { Pressable, StyleSheet, Text } from 'react-native';

import { useLocale } from '../i18n';

/** Shown only when the phone language is Serbian: switch to English, or back to Serbian. */
export default function LanguageToggle() {
  const { locale, systemLocale, setLocale } = useLocale();

  if (systemLocale !== 'sr-Latn') {
    return null;
  }

  const showingEnglish = locale === 'en';

  return (
    <Pressable
      onPress={() => setLocale(showingEnglish ? 'sr-Latn' : 'en')}
      style={[styles.btn, showingEnglish && styles.btnActive]}
      accessibilityRole="button"
    >
      <Text style={[styles.btnText, showingEnglish && styles.btnTextActive]}>
        {showingEnglish ? 'SR' : 'EN'}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  btn: {
    marginRight: 4,
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#e5e7eb',
    backgroundColor: '#f9fafb',
  },
  btnActive: {
    backgroundColor: '#1e3a5f',
    borderColor: '#1e3a5f',
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
