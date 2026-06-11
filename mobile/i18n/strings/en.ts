export const en = {
  appTitle: 'Belgrade Parking',

  // List
  loading: 'Loading parking data…',
  wsLive: 'Live',
  wsConnecting: 'Connecting…',
  wsReconnecting: 'Reconnecting…',
  free: 'free',
  yourLocation: 'Your location',

  // Destination search
  searchPlaceholder: 'Where are you going? (optional)',
  nearYourLocation: 'Near your location',
  nearDestination: 'Near: {label}',
  searchNotFound: 'Address not found — try a more specific place in Belgrade.',
  searchError: 'Could not look up that address. Please try again.',
  resultsFor: 'Results for "{query}"',
  resultsHint: 'Tap the correct location — sorted nearest to you',
  fromYou: '{distance} from you',
  cancel: 'Cancel',

  // Recommended
  recommended: 'Recommended',
  viewDetails: 'View details',

  // Detail sections
  availability: 'Availability',
  freeOfSpaces: 'free of {total} spaces',
  freeSpaces: 'free spaces',
  live: 'Live',
  updatedAt: 'Updated {time}',
  estimatedCost: 'Estimated cost',
  rates: 'Rates',
  payment: 'Payment',
  paymentMethods:
    'Parking Servis mobile app, prepaid card, or pay at the booth.',
  location: 'Location',
  coveredGarage: 'Covered garage',
  openParkingLot: 'Open parking lot',
  getDirections: 'Get directions',

  // Stay duration
  howLongStay: 'How long will you stay?',
  allDay: 'All day',
  decreaseHours: 'Decrease hours',
  increaseHours: 'Increase hours',

  // Navigation picker
  navigateWith: 'Navigate with',
  googleMapsBrowser: 'Google Maps (browser)',

  // Occupancy bar
  noData: 'No data',
  freeCount: '{free} free',
  freeOfTotal: '{free} / {total} free',

  // Distance
  toDestination: '{distance} to destination',

  // Pricing
  rateFromHour: 'from {amount} RSD/hr',
  ratePerHour: '{amount} RSD/hr',
  ratePerDay: '{amount} RSD/day',
  priceFirstHour: 'First hour: {amount}',
  priceExtraHour: 'Each additional hour: {amount}',
  priceFlatHour: 'Per hour: {amount}',
  priceDailyPass: 'Daily pass (24 h): {amount}',

  // DB seed notes (hours_note / pricing_note)
  hoursOpen24: 'Open 24 hours',
  hoursCustoms: 'Customs terminal',
  noteDailyOnly: 'Daily pass only (no hourly rate listed)',
  noteCustomsPricing:
    '1,900 RSD for the first 12 commenced hours, then 700 RSD per additional 12 hours (customs procedure).',
  noteVmaWeekly: '7-day single-entry pass: 3,500 RSD',
} as const;

export type StringKey = keyof typeof en;
