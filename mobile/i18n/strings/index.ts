import type { Locale } from '../types';
import { en, type StringKey } from './en';
import { srLatn } from './sr-Latn';

const catalogs: Record<Locale, Record<StringKey, string>> = {
  'sr-Latn': srLatn,
  en,
};

export function getString(
  locale: Locale,
  key: StringKey,
  params?: Record<string, string | number>,
): string {
  let text = catalogs[locale][key] ?? catalogs.en[key] ?? key;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      text = text.replace(new RegExp(`\\{${k}\\}`, 'g'), String(v));
    }
  }
  return text;
}

export type { StringKey };
