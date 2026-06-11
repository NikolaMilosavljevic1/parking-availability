export type Locale = 'sr-Latn' | 'en';

export const DEFAULT_LOCALE: Locale = 'sr-Latn';
export const LOCALE_STORAGE_KEY = '@belgrade_parking/locale';

export const BELGRADE_TZ = 'Europe/Belgrade';

export function dateLocaleTag(locale: Locale): string {
  return locale === 'sr-Latn' ? 'sr-Latn-RS' : 'en-GB';
}
