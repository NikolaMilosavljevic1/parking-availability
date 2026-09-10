import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react';
import AsyncStorage from '@react-native-async-storage/async-storage';

import type { StringKey } from './strings';
import { getString } from './strings';
import { localeFromSystem } from './systemLocale';
import { DEFAULT_LOCALE, PREFER_ENGLISH_KEY, type Locale } from './types';

interface LocaleContextValue {
  locale: Locale;
  systemLocale: Locale;
  setLocale: (locale: Locale) => void;
  t: (key: StringKey, params?: Record<string, string | number>) => string;
  ready: boolean;
}

const LocaleContext = createContext<LocaleContextValue | null>(null);

export function LocaleProvider({ children }: { children: React.ReactNode }) {
  const [systemLocale, setSystemLocale] = useState<Locale>(DEFAULT_LOCALE);
  const [locale, setLocaleState] = useState<Locale>(DEFAULT_LOCALE);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const system = localeFromSystem();
    setSystemLocale(system);

    AsyncStorage.getItem(PREFER_ENGLISH_KEY)
      .then((stored) => {
        if (system === 'sr-Latn' && stored === '1') {
          setLocaleState('en');
        } else {
          setLocaleState(system);
        }
      })
      .catch(() => {
        setLocaleState(system);
      })
      .finally(() => setReady(true));
  }, []);

  const setLocale = useCallback(
    (next: Locale) => {
      const system = systemLocale;
      if (system !== 'sr-Latn') {
        setLocaleState(system);
        AsyncStorage.removeItem(PREFER_ENGLISH_KEY).catch(() => {});
        return;
      }

      setLocaleState(next);
      if (next === 'en') {
        AsyncStorage.setItem(PREFER_ENGLISH_KEY, '1').catch(() => {});
      } else {
        AsyncStorage.removeItem(PREFER_ENGLISH_KEY).catch(() => {});
      }
    },
    [systemLocale],
  );

  const t = useCallback(
    (key: StringKey, params?: Record<string, string | number>) =>
      getString(locale, key, params),
    [locale],
  );

  const value = useMemo(
    () => ({ locale, systemLocale, setLocale, t, ready }),
    [locale, systemLocale, setLocale, t, ready],
  );

  return (
    <LocaleContext.Provider value={value}>{children}</LocaleContext.Provider>
  );
}

export function useLocale(): LocaleContextValue {
  const ctx = useContext(LocaleContext);
  if (!ctx) {
    throw new Error('useLocale must be used within LocaleProvider');
  }
  return ctx;
}
