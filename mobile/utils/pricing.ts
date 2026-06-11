import { Location } from '../types';

type PricingFields = Pick<
  Location,
  | 'price_first_hour_rsd'
  | 'price_extra_hour_rsd'
  | 'price_daily_rsd'
  | 'pricing_note'
>;

export type StayDuration = number | 'daily';

export const MIN_STAY_HOURS = 1;
export const MAX_STAY_HOURS = 12;

function extraHourRate(loc: PricingFields): number | null {
  return loc.price_extra_hour_rsd ?? loc.price_first_hour_rsd ?? null;
}

function hourlyTotalForHours(hours: number, loc: PricingFields): number | null {
  const first = loc.price_first_hour_rsd;
  const extra = extraHourRate(loc);

  if (first == null && extra == null) return null;

  const flat = first != null && extra != null && first === extra;
  const rate = first ?? extra!;

  if (flat || first == null) {
    return rate * hours;
  }
  if (hours <= 1) {
    return first;
  }
  return first + (hours - 1) * (extra ?? first);
}

/** Estimate total RSD for a stay (commenced-hour billing). */
export function estimateParkingCost(
  duration: StayDuration,
  loc: PricingFields,
): number | null {
  if (duration === 'daily' && loc.price_daily_rsd != null) {
    return loc.price_daily_rsd;
  }

  if (typeof duration !== 'number') {
    return loc.price_daily_rsd ?? null;
  }

  const hourlyTotal = hourlyTotalForHours(duration, loc);
  if (hourlyTotal == null) {
    return null;
  }

  if (loc.price_daily_rsd != null && duration >= 6) {
    return Math.min(hourlyTotal, loc.price_daily_rsd);
  }

  return hourlyTotal;
}

export function hasHourlyRate(loc: PricingFields): boolean {
  return loc.price_first_hour_rsd != null || loc.price_extra_hour_rsd != null;
}
