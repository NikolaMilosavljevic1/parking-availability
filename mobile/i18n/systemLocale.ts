import { getLocales } from 'expo-localization';

import { DEFAULT_LOCALE, type Locale } from './types';

/** Map the phone language to app locales. Serbian → sr-Latn, everything else → en. */
export function localeFromSystem(): Locale {
  const locales = getLocales();
  const primary = locales[0];
  const tag = (
    primary?.languageTag ??
    primary?.languageCode ??
    ''
  ).toLowerCase();

  if (tag.startsWith('sr') || tag.includes('serbian')) {
    return 'sr-Latn';
  }

  return tag ? 'en' : DEFAULT_LOCALE;
}
