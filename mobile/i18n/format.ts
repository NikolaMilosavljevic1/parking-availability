import type { Locale } from './types';
import { BELGRADE_TZ, dateLocaleTag } from './types';
import { getString } from './strings';
import type { StringKey } from './strings';
import type { Location } from '../types';

type PricingFields = Pick<
  Location,
  | 'price_first_hour_rsd'
  | 'price_extra_hour_rsd'
  | 'price_daily_rsd'
  | 'pricing_note'
  | 'hours_note'
>;

const HOURS_NOTE_MAP: Record<string, StringKey> = {
  'Open 24 hours': 'hoursOpen24',
  'Customs terminal': 'hoursCustoms',
};

const PRICING_NOTE_MAP: Record<string, StringKey> = {
  'Daily pass only (no hourly rate listed)': 'noteDailyOnly',
  '1,900 RSD for the first 12 commenced hours, then 700 RSD per additional 12 hours (customs procedure).':
    'noteCustomsPricing',
  '7-day single-entry pass: 3,500 RSD': 'noteVmaWeekly',
};

export function formatRsd(amount: number): string {
  return `${amount.toLocaleString('sr-RS')} RSD`;
}

export function formatTimeBelgrade(iso: string, locale: Locale): string {
  return new Date(iso).toLocaleTimeString(dateLocaleTag(locale), {
    hour: '2-digit',
    minute: '2-digit',
    timeZone: BELGRADE_TZ,
  });
}

export function formatDateBelgrade(iso: string, locale: Locale): string {
  return new Date(iso).toLocaleDateString(dateLocaleTag(locale), {
    weekday: 'short',
    day: 'numeric',
    month: 'short',
    timeZone: BELGRADE_TZ,
  });
}

/** Serbian: 1 sat, 2-4 sata, 5+ sati */
export function formatHoursLabel(hours: number, locale: Locale): string {
  if (locale === 'en') {
    return hours === 1 ? '1 hour' : `${hours} hours`;
  }
  const mod10 = hours % 10;
  const mod100 = hours % 100;
  if (mod10 === 1 && mod100 !== 11) return `${hours} sat`;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) {
    return `${hours} sata`;
  }
  return `${hours} sati`;
}

export function formatDistanceLabel(
  km: number,
  sortMode: 'near_me' | 'destination',
  locale: Locale,
  formatDistance: (km: number) => string,
): string {
  const base = formatDistance(km);
  if (sortMode === 'destination') {
    return getString(locale, 'toDestination', { distance: base });
  }
  return base;
}

export function translateHoursNote(
  note: string | null | undefined,
  locale: Locale,
): string | null {
  if (!note) return null;
  const key = HOURS_NOTE_MAP[note];
  return key ? getString(locale, key) : note;
}

export function translatePricingNote(
  note: string | null | undefined,
  locale: Locale,
): string | null {
  if (!note) return null;
  const key = PRICING_NOTE_MAP[note];
  return key ? getString(locale, key) : note;
}

function hourlyRate(loc: PricingFields): number | null {
  return loc.price_first_hour_rsd ?? loc.price_extra_hour_rsd ?? null;
}

export function formatRateShort(
  loc: PricingFields,
  locale: Locale,
): string | null {
  if (loc.pricing_note && !hourlyRate(loc) && loc.price_daily_rsd == null) {
    return null;
  }

  const first = loc.price_first_hour_rsd;
  const extra = loc.price_extra_hour_rsd;

  if (first != null && extra != null && first !== extra) {
    return getString(locale, 'rateFromHour', { amount: first });
  }

  const flat = first ?? extra;
  if (flat != null) {
    return getString(locale, 'ratePerHour', { amount: flat });
  }

  if (loc.price_daily_rsd != null) {
    return getString(locale, 'ratePerDay', { amount: loc.price_daily_rsd });
  }

  return null;
}

export function formatRateLines(
  loc: PricingFields,
  locale: Locale,
): string[] {
  const lines: string[] = [];
  const first = loc.price_first_hour_rsd;
  const extra = loc.price_extra_hour_rsd;

  if (first != null && extra != null && first !== extra) {
    lines.push(
      getString(locale, 'priceFirstHour', { amount: formatRsd(first) }),
    );
    lines.push(
      getString(locale, 'priceExtraHour', { amount: formatRsd(extra) }),
    );
  } else {
    const flat = first ?? extra;
    if (flat != null) {
      lines.push(
        getString(locale, 'priceFlatHour', { amount: formatRsd(flat) }),
      );
    }
  }

  if (loc.price_daily_rsd != null) {
    lines.push(
      getString(locale, 'priceDailyPass', {
        amount: formatRsd(loc.price_daily_rsd),
      }),
    );
  }

  const note = translatePricingNote(loc.pricing_note, locale);
  if (note) lines.push(note);

  return lines;
}

const DEMAND_TYPE_KEYS: Record<string, StringKey> = {
  sports: 'demandSports',
  concert: 'demandConcert',
  theatre: 'demandTheatre',
  religious: 'demandReligious',
  festival: 'demandFestival',
  other: 'demandEvent',
};

function demandEventTypeLabel(
  eventType: string | null | undefined,
  locale: Locale,
): string {
  const key = DEMAND_TYPE_KEYS[eventType ?? ''] ?? 'demandEvent';
  return getString(locale, key);
}

type DemandFields = Pick<
  Location,
  | 'elevated_demand'
  | 'demand_event_type'
  | 'demand_venue_name'
  | 'demand_event_name'
>;

export function formatDemandHint(
  location: DemandFields,
  locale: Locale,
): string | null {
  if (!location.elevated_demand) return null;

  const eventType = demandEventTypeLabel(location.demand_event_type, locale);
  const venue = location.demand_venue_name?.trim();
  const eventName = location.demand_event_name?.trim();

  if (venue) {
    return getString(locale, 'elevatedDemandWithVenue', { eventType, venue });
  }
  if (eventName) {
    return getString(locale, 'elevatedDemandWithEventName', { eventName });
  }
  return getString(locale, 'elevatedDemandTypeOnly', { eventType });
}

/**
 * Translate Serbian address boilerplate for EN (corner, municipal office, block, etc.).
 * Street and place names are kept as stored in the DB.
 */
export function formatAddress(
  address: string | null | undefined,
  locale: Locale,
): string | null {
  if (!address) return null;
  if (locale !== 'en') return address;

  const corner3 = address.match(/^Ugao ulica (.+), (.+) i (.+)$/i);
  if (corner3) {
    return `Corner of ${corner3[1]}, ${corner3[2]} and ${corner3[3]}`;
  }

  const corner2 = address.match(/^Ugao ulica (.+) i (.+)$/i);
  if (corner2) {
    return `Corner of ${corner2[1]} and ${corner2[2]}`;
  }

  let result = address;

  result = result.replace(
    /, ispred SO (.+)$/i,
    ', in front of $1 municipal office',
  );

  const streetNoNumber = result.match(/^Ulica (.+) bb$/i);
  if (streetNoNumber) {
    return `${streetNoNumber[1]} (no number)`;
  }

  result = result.replace(/Blok (\d+)/gi, 'Block $1');
  result = result.replace(
    /\(Međunarodni terminal\)/gi,
    '(International Terminal)',
  );
  result = result.replace(/ bb$/i, ' (no number)');
  result = result.replace(/(\d+) i (\d+)/g, '$1 and $2');

  return result;
}
