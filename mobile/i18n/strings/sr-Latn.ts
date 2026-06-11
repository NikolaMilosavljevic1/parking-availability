import type { StringKey } from './en';

export const srLatn: Record<StringKey, string> = {
  appTitle: 'Parking Beograd',

  loading: 'Učitavanje parking podataka…',
  wsLive: 'Uživo',
  wsConnecting: 'Povezivanje…',
  wsReconnecting: 'Ponovno povezivanje…',
  free: 'slobodno',
  yourLocation: 'Vaša lokacija',

  searchPlaceholder: 'Gde idete? (opciono)',
  nearYourLocation: 'Blizu vaše lokacije',
  nearDestination: 'Blizu: {label}',
  searchNotFound:
    'Adresa nije pronađena — pokušajte preciznije mesto u Beogradu.',
  searchError: 'Pretraga adrese nije uspela. Pokušajte ponovo.',
  resultsFor: 'Rezultati za "{query}"',
  resultsHint: 'Izaberite tačnu lokaciju — sortirano po udaljenosti',
  fromYou: '{distance} od vas',
  cancel: 'Otkaži',

  recommended: 'Preporučeno',
  viewDetails: 'Pogledaj detalje',

  availability: 'Dostupnost',
  freeOfSpaces: 'slobodno od {total} mesta',
  freeSpaces: 'slobodnih mesta',
  live: 'Uživo',
  updatedAt: 'Ažurirano {time}',
  estimatedCost: 'Procenjeni trošak',
  rates: 'Cene',
  payment: 'Plaćanje',
  paymentMethods:
    'Parking Servis mobilna aplikacija, pripejd kartica ili blagajna.',
  location: 'Lokacija',
  coveredGarage: 'Garaža',
  openParkingLot: 'Parkiralište',
  getDirections: 'Pronađi put',

  elevatedDemandWithVenue:
    'Veće zauzeće nego inače — {eventType} u blizini, {venue}',
  elevatedDemandWithEventName:
    'Veće zauzeće nego inače — {eventName} u blizini',
  elevatedDemandTypeOnly: 'Veće zauzeće nego inače — {eventType} u blizini',
  demandSports: 'fudbalska utakmica',
  demandConcert: 'koncert',
  demandTheatre: 'predstava',
  demandReligious: 'verski skup',
  demandFestival: 'festival',
  demandEvent: 'događaj',

  howLongStay: 'Koliko dugo planirate da ostanete?',
  allDay: 'Ceo dan',
  decreaseHours: 'Smanji broj sati',
  increaseHours: 'Povećaj broj sati',

  navigateWith: 'Navigacija preko',
  googleMapsBrowser: 'Google Maps (pregledač)',

  noData: 'Nema podataka',
  freeCount: '{free} slobodno',
  freeOfTotal: '{free} / {total} slobodno',

  toDestination: '{distance} do destinacije',

  rateFromHour: 'od {amount} RSD/h',
  ratePerHour: '{amount} RSD/h',
  ratePerDay: '{amount} RSD/dan',
  priceFirstHour: 'Prvi sat: {amount}',
  priceExtraHour: 'Svaki naredni sat: {amount}',
  priceFlatHour: 'Po satu: {amount}',
  priceDailyPass: 'Dnevna karta (24 h): {amount}',

  hoursOpen24: 'Otvoreno 24 sata',
  hoursCustoms: 'Carinski terminal',
  noteDailyOnly: 'Samo dnevna karta (nema satne tarife)',
  noteCustomsPricing:
    '1.900 RSD za prvih 12 započetih sati, zatim 700 RSD za svakih narednih 12 sati (carinski postupak).',
  noteVmaWeekly: 'Sedmodnevna karta: 3.500 RSD',
};
