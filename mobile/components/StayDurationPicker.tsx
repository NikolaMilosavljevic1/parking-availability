import React, { useCallback, useRef, useState } from 'react';
import {
  PanResponder,
  Pressable,
  StyleSheet,
  Text,
  View,
} from 'react-native';

import { formatHoursLabel, useLocale } from '../i18n';
import { MAX_STAY_HOURS, MIN_STAY_HOURS, StayDuration } from '../utils/pricing';

const THUMB_SIZE = 22;
const SLIDER_TOUCH_HEIGHT = 56;

interface Props {
  value: StayDuration;
  onChange: (value: StayDuration) => void;
  showDaily: boolean;
  showHourly: boolean;
}

function clampHours(hours: number): number {
  return Math.min(MAX_STAY_HOURS, Math.max(MIN_STAY_HOURS, Math.round(hours)));
}

export default function StayDurationPicker({
  value,
  onChange,
  showDaily,
  showHourly,
}: Props) {
  const { t, locale } = useLocale();
  const [trackWidth, setTrackWidth] = useState(0);
  const trackRef = useRef<View>(null);
  const trackWidthRef = useRef(0);
  const trackPageXRef = useRef(0);
  const lastEmittedRef = useRef(
    typeof value === 'number' ? value : MIN_STAY_HOURS,
  );
  const showHourlyRef = useRef(showHourly);
  const isDailyRef = useRef(value === 'daily');

  const isDaily = value === 'daily';
  const hours = typeof value === 'number' ? value : MIN_STAY_HOURS;

  showHourlyRef.current = showHourly;
  isDailyRef.current = isDaily;
  if (typeof value === 'number') {
    lastEmittedRef.current = value;
  }

  const measureTrack = useCallback(() => {
    trackRef.current?.measureInWindow((x, _y, width) => {
      trackPageXRef.current = x;
      trackWidthRef.current = width;
      setTrackWidth(width);
    });
  }, []);

  const hoursFromPageX = useCallback((pageX: number): number => {
    const width = trackWidthRef.current;
    if (width <= THUMB_SIZE) return MIN_STAY_HOURS;

    const usable = width - THUMB_SIZE;
    const localX = pageX - trackPageXRef.current - THUMB_SIZE / 2;
    const ratio = Math.max(0, Math.min(1, localX / usable));

    return clampHours(
      MIN_STAY_HOURS + ratio * (MAX_STAY_HOURS - MIN_STAY_HOURS),
    );
  }, []);

  const updateFromPageX = useCallback(
    (pageX: number) => {
      const next = hoursFromPageX(pageX);
      if (next !== lastEmittedRef.current) {
        lastEmittedRef.current = next;
        onChange(next);
      }
    },
    [hoursFromPageX, onChange],
  );

  const panResponder = useRef(
    PanResponder.create({
      onStartShouldSetPanResponder: () =>
        showHourlyRef.current && !isDailyRef.current,
      onMoveShouldSetPanResponder: () =>
        showHourlyRef.current && !isDailyRef.current,
      onPanResponderTerminationRequest: () => false,
      onPanResponderGrant: (evt) => {
        measureTrack();
        updateFromPageX(evt.nativeEvent.pageX);
      },
      onPanResponderMove: (evt) => {
        updateFromPageX(evt.nativeEvent.pageX);
      },
    }),
  ).current;

  const exitDaily = useCallback(() => {
    onChange(typeof value === 'number' ? value : 3);
  }, [onChange, value]);

  const setHours = useCallback(
    (next: number) => {
      const clamped = clampHours(next);
      lastEmittedRef.current = clamped;
      onChange(clamped);
    },
    [onChange],
  );

  const thumbLeft =
    trackWidth > THUMB_SIZE
      ? ((hours - MIN_STAY_HOURS) / (MAX_STAY_HOURS - MIN_STAY_HOURS)) *
        (trackWidth - THUMB_SIZE)
      : 0;

  const fillWidth =
    trackWidth > 0 ? thumbLeft + THUMB_SIZE / 2 : 0;

  return (
    <View style={styles.wrapper}>
      <Text style={styles.prompt}>{t('howLongStay')}</Text>

      {!showHourly && showDaily && (
        <Text style={styles.hoursLabel}>{t('allDay')}</Text>
      )}

      {showHourly && (
        <>
          <Text style={[styles.hoursLabel, isDaily && styles.hoursLabelMuted]}>
            {isDaily ? t('allDay') : formatHoursLabel(hours, locale)}
          </Text>

          <View style={styles.controlsRow}>
            <Pressable
              style={styles.stepBtn}
              disabled={!isDaily && hours <= MIN_STAY_HOURS}
              onPress={() => {
                if (isDaily) exitDaily();
                else setHours(hours - 1);
              }}
              accessibilityLabel={t('decreaseHours')}
            >
              <Text style={styles.stepBtnText}>−</Text>
            </Pressable>

            <View
              ref={trackRef}
              style={[styles.trackWrap, isDaily && styles.trackDisabled]}
              onLayout={measureTrack}
              {...(showHourly && !isDaily ? panResponder.panHandlers : {})}
              collapsable={false}
            >
              <View style={styles.track} pointerEvents="none">
                <View style={[styles.trackFill, { width: fillWidth }]} />
              </View>
              {!isDaily && (
                <View
                  style={[styles.thumb, { left: thumbLeft }]}
                  pointerEvents="none"
                />
              )}
            </View>

            <Pressable
              style={styles.stepBtn}
              disabled={!isDaily && hours >= MAX_STAY_HOURS}
              onPress={() => {
                if (isDaily) exitDaily();
                else setHours(hours + 1);
              }}
              accessibilityLabel={t('increaseHours')}
            >
              <Text style={styles.stepBtnText}>+</Text>
            </Pressable>
          </View>

          <View style={styles.tickRow}>
            <Text style={styles.tickLabel}>{MIN_STAY_HOURS}h</Text>
            <Text style={styles.tickLabel}>{MAX_STAY_HOURS}h</Text>
          </View>
        </>
      )}

      {showDaily && showHourly && (
        <Pressable
          style={[styles.dailyBtn, isDaily && styles.dailyBtnActive]}
          onPress={() => (isDaily ? exitDaily() : onChange('daily'))}
        >
          <Text
            style={[styles.dailyBtnText, isDaily && styles.dailyBtnTextActive]}
          >
            {t('allDay')}
          </Text>
        </Pressable>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    gap: 4,
  },
  prompt: {
    fontSize: 15,
    fontWeight: '500',
    color: '#374151',
    marginBottom: 4,
  },
  hoursLabel: {
    fontSize: 22,
    fontWeight: '600',
    color: '#111827',
    textAlign: 'center',
    marginVertical: 8,
  },
  hoursLabelMuted: {
    color: '#6b7280',
  },
  controlsRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
  },
  stepBtn: {
    width: 40,
    height: 40,
    borderRadius: 20,
    backgroundColor: '#f3f4f6',
    borderWidth: 1,
    borderColor: '#e5e7eb',
    alignItems: 'center',
    justifyContent: 'center',
  },
  stepBtnText: {
    fontSize: 22,
    fontWeight: '500',
    color: '#1e3a5f',
    lineHeight: 24,
  },
  trackWrap: {
    flex: 1,
    height: SLIDER_TOUCH_HEIGHT,
    justifyContent: 'center',
  },
  trackDisabled: {
    opacity: 0.35,
  },
  track: {
    height: 6,
    borderRadius: 3,
    backgroundColor: '#e5e7eb',
    overflow: 'hidden',
  },
  trackFill: {
    height: '100%',
    backgroundColor: '#1e3a5f',
    borderRadius: 3,
  },
  thumb: {
    position: 'absolute',
    width: THUMB_SIZE,
    height: THUMB_SIZE,
    borderRadius: THUMB_SIZE / 2,
    backgroundColor: '#ffffff',
    borderWidth: 2,
    borderColor: '#1e3a5f',
    top: (SLIDER_TOUCH_HEIGHT - THUMB_SIZE) / 2,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.12,
    shadowRadius: 2,
    elevation: 2,
  },
  tickRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    paddingHorizontal: 50,
    marginTop: -4,
  },
  tickLabel: {
    fontSize: 11,
    color: '#9ca3af',
  },
  dailyBtn: {
    marginTop: 12,
    alignSelf: 'flex-start',
    paddingHorizontal: 16,
    paddingVertical: 9,
    borderRadius: 20,
    backgroundColor: '#f3f4f6',
    borderWidth: 1,
    borderColor: '#e5e7eb',
  },
  dailyBtnActive: {
    backgroundColor: '#1e3a5f',
    borderColor: '#1e3a5f',
  },
  dailyBtnText: {
    fontSize: 14,
    fontWeight: '500',
    color: '#4b5563',
  },
  dailyBtnTextActive: {
    color: '#ffffff',
    fontWeight: '600',
  },
});
