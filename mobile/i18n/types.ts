export type Locale = 'sr-Latn' | 'en';

export const DEFAULT_LOCALE: Locale = 'sr-Latn';
export const PREFER_ENGLISH_KEY = '@belgrade_parking/prefer_english';

export const BELGRADE_TZ = 'Europe/Belgrade';

export function dateLocaleTag(locale: Locale): string {
  return locale === 'sr-Latn' ? 'sr-Latn-RS' : 'en-GB';
}
