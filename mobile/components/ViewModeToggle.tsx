import React from 'react';
import { Pressable, StyleSheet, Text, View } from 'react-native';

import { useLocale } from '../i18n';

export type ViewMode = 'list' | 'map';

interface Props {
  value: ViewMode;
  onChange: (mode: ViewMode) => void;
}

export default function ViewModeToggle({ value, onChange }: Props) {
  const { t } = useLocale();

  return (
    <View style={styles.row}>
      <ModeButton
        label={t('viewList')}
        active={value === 'list'}
        onPress={() => onChange('list')}
      />
      <ModeButton
        label={t('viewMap')}
        active={value === 'map'}
        onPress={() => onChange('map')}
      />
    </View>
  );
}

function ModeButton({
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
    marginHorizontal: 12,
    marginTop: 8,
    borderRadius: 10,
    overflow: 'hidden',
    borderWidth: 1,
    borderColor: '#e5e7eb',
    backgroundColor: '#ffffff',
  },
  btn: {
    flex: 1,
    paddingVertical: 9,
    alignItems: 'center',
    backgroundColor: '#ffffff',
  },
  btnActive: {
    backgroundColor: '#1e3a5f',
  },
  btnText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#6b7280',
  },
  btnTextActive: {
    color: '#ffffff',
  },
});
