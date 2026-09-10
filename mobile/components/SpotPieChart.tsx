import React from 'react';
import { Dimensions, StyleSheet, Text, View } from 'react-native';
import { PieChart } from 'react-native-chart-kit';

import { useLocale } from '../i18n';

interface Props {
  freeSpots: number | null;
  totalSpots: number | null;
}

export default function SpotPieChart({ freeSpots, totalSpots }: Props) {
  const { t } = useLocale();

  if (freeSpots == null || totalSpots == null || totalSpots <= 0) {
    return null;
  }

  const free = Math.max(0, Math.min(freeSpots, totalSpots));
  const occupied = Math.max(0, totalSpots - free);
  const chartWidth = Math.min(Dimensions.get('window').width - 64, 320);

  const data = [
    {
      name: t('free'),
      count: free,
      color: '#22c55e',
      legendFontColor: '#374151',
      legendFontSize: 12,
    },
    {
      name: t('occupied'),
      count: occupied,
      color: '#ef4444',
      legendFontColor: '#374151',
      legendFontSize: 12,
    },
  ];

  return (
    <View style={styles.wrap}>
      <Text style={styles.caption}>{t('spotsBreakdown')}</Text>
      <PieChart
        data={data}
        width={chartWidth}
        height={160}
        accessor="count"
        backgroundColor="transparent"
        paddingLeft="8"
        absolute
        hasLegend
        chartConfig={{
          color: (opacity = 1) => `rgba(17, 24, 39, ${opacity})`,
          labelColor: () => '#374151',
        }}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    alignItems: 'center',
    marginTop: 8,
  },
  caption: {
    fontSize: 12,
    fontWeight: '600',
    color: '#6b7280',
    alignSelf: 'flex-start',
    marginBottom: 4,
  },
});
