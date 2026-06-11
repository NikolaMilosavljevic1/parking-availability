import React, { useCallback, useEffect, useState } from "react";
import {
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { NativeStackScreenProps } from "@react-navigation/native-stack";

import { API_URL } from "../config";
import NavigationPicker from "../components/NavigationPicker";
import StayDurationPicker from "../components/StayDurationPicker";
import { occupancyColor } from "../components/OccupancyBar";
import {
  formatAddress,
  formatDemandHint,
  formatRateLines,
  formatRsd,
  formatTimeBelgrade,
  translateHoursNote,
  useLocale,
} from "../i18n";
import {
  estimateParkingCost,
  hasHourlyRate,
  StayDuration,
} from "../utils/pricing";
import { Location } from "../types";
import { RootStackParamList } from "../App";

type Props = NativeStackScreenProps<RootStackParamList, "GarageDetail">;

export default function GarageDetail({ route }: Props) {
  const { t, locale } = useLocale();
  const [location, setLocation] = useState<Location>(route.params.location);
  const [navPickerVisible, setNavPickerVisible] = useState(false);
  const [stayDuration, setStayDuration] = useState<StayDuration>(3);

  const applyDefaultDuration = useCallback((loc: Location) => {
    const hourly = hasHourlyRate(loc);
    if (!hourly && loc.price_daily_rsd != null) {
      setStayDuration("daily");
    } else {
      setStayDuration(3);
    }
  }, []);

  const loadDetail = useCallback(async () => {
    try {
      const locResp = await fetch(`${API_URL}/locations/${location.id}`);
      if (locResp.ok) {
        const fresh: Location = await locResp.json();
        setLocation(fresh);
        applyDefaultDuration(fresh);
      }
    } catch (e) {
      console.warn("GarageDetail fetch error:", e);
    }
  }, [location.id, applyDefaultDuration]);

  useEffect(() => {
    loadDetail();
  }, []);

  const color = occupancyColor(location.occupancy_pct);
  const estimatedCost = estimateParkingCost(stayDuration, location);
  const rateLines = formatRateLines(location, locale);
  const showDailyOption = location.price_daily_rsd != null;
  const canEstimate =
    hasHourlyRate(location) || location.price_daily_rsd != null;

  const locationTypeLabel =
    location.location_type === "garage"
      ? t("coveredGarage")
      : t("openParkingLot");

  const hoursNote = translateHoursNote(location.hours_note, locale);
  const demandHint = formatDemandHint(location, locale);

  return (
    <ScrollView style={styles.container} contentContainerStyle={styles.content}>
      <View style={styles.card}>
        <Text style={styles.sectionLabel}>{t("availability")}</Text>
        <View style={styles.availRow}>
          <View style={styles.availMain}>
            <Text style={[styles.freeNumber, { color }]}>
              {location.free_spots ?? "—"}
            </Text>
            <Text style={styles.freeCaption}>
              {location.total_spots
                ? t("freeOfSpaces", { total: location.total_spots })
                : t("freeSpaces")}
            </Text>
          </View>
          <View style={styles.availAside}>
            {location.live && (
              <View style={styles.liveBadge}>
                <View style={styles.liveDot} />
                <Text style={styles.liveText}>{t("live")}</Text>
              </View>
            )}
            {location.scraped_at && (
              <Text style={styles.updatedAt}>
                {t("updatedAt", {
                  time: formatTimeBelgrade(location.scraped_at, locale),
                })}
              </Text>
            )}
          </View>
        </View>
        {demandHint && (
          <View style={styles.demandBanner}>
            <Text style={styles.demandText}>{demandHint}</Text>
          </View>
        )}
      </View>

      {canEstimate && (
        <View style={styles.card}>
          <Text style={styles.sectionLabel}>{t("estimatedCost")}</Text>
          <StayDurationPicker
            value={stayDuration}
            onChange={setStayDuration}
            showDaily={showDailyOption}
            showHourly={hasHourlyRate(location)}
          />
          <Text style={styles.estimateValue}>
            {estimatedCost != null ? formatRsd(estimatedCost) : "—"}
          </Text>
        </View>
      )}

      {rateLines.length > 0 && (
        <View style={styles.card}>
          <Text style={styles.sectionLabel}>{t("rates")}</Text>
          {rateLines.map((line) => (
            <Text key={line} style={styles.infoLine}>
              {line}
            </Text>
          ))}
          {hoursNote && (
            <Text style={[styles.infoLine, styles.infoMuted]}>{hoursNote}</Text>
          )}
          <View style={styles.divider} />
          <Text style={styles.subheading}>{t("payment")}</Text>
          <Text style={styles.infoLine}>{t("paymentMethods")}</Text>
        </View>
      )}

      <View style={styles.card}>
        <Text style={styles.sectionLabel}>{t("location")}</Text>
        {location.address && (
          <Text style={styles.infoLine}>
            {formatAddress(location.address, locale)}
          </Text>
        )}
        {location.neighborhood && (
          <Text style={styles.infoMuted}>{location.neighborhood}</Text>
        )}
        <Text style={styles.typeTag}>{locationTypeLabel}</Text>
      </View>

      {location.latitude != null && location.longitude != null && (
        <>
          <Pressable
            style={({ pressed }: { pressed: boolean }) => [
              styles.directionsBtn,
              pressed && { opacity: 0.85 },
            ]}
            onPress={() => setNavPickerVisible(true)}
          >
            <Text style={styles.directionsBtnText}>{t("getDirections")}</Text>
          </Pressable>
          <NavigationPicker
            visible={navPickerVisible}
            lat={location.latitude!}
            lng={location.longitude!}
            name={location.name}
            onClose={() => setNavPickerVisible(false)}
          />
        </>
      )}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#f4f5f7",
  },
  content: {
    padding: 16,
    gap: 12,
    paddingBottom: 40,
  },
  card: {
    backgroundColor: "#ffffff",
    borderRadius: 12,
    padding: 16,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#e5e7eb",
    ...Platform.select({
      ios: {
        shadowColor: "#000",
        shadowOffset: { width: 0, height: 1 },
        shadowOpacity: 0.04,
        shadowRadius: 4,
      },
      android: { elevation: 1 },
    }),
  },
  sectionLabel: {
    fontSize: 12,
    fontWeight: "600",
    color: "#6b7280",
    textTransform: "uppercase",
    letterSpacing: 0.6,
    marginBottom: 12,
  },
  availRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  availMain: {
    flex: 1,
  },
  freeNumber: {
    fontSize: 48,
    fontWeight: "700",
    lineHeight: 52,
    letterSpacing: -1,
  },
  freeCaption: {
    fontSize: 14,
    color: "#6b7280",
    marginTop: 2,
  },
  availAside: {
    alignItems: "flex-end",
    gap: 6,
  },
  liveBadge: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    backgroundColor: "#ecfdf5",
    borderRadius: 6,
    paddingHorizontal: 8,
    paddingVertical: 4,
  },
  liveDot: {
    width: 6,
    height: 6,
    borderRadius: 3,
    backgroundColor: "#16a34a",
  },
  liveText: {
    fontSize: 11,
    fontWeight: "600",
    color: "#15803d",
  },
  updatedAt: {
    fontSize: 11,
    color: "#9ca3af",
  },
  demandBanner: {
    marginTop: 14,
    paddingTop: 12,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#f3f4f6",
  },
  demandText: {
    fontSize: 13,
    color: "#92400e",
    lineHeight: 18,
    backgroundColor: "#fffbeb",
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 8,
    overflow: "hidden",
  },
  estimateValue: {
    fontSize: 28,
    fontWeight: "700",
    color: "#111827",
    marginTop: 14,
    letterSpacing: -0.5,
  },
  infoLine: {
    fontSize: 15,
    color: "#374151",
    lineHeight: 22,
    marginBottom: 4,
  },
  infoMuted: {
    fontSize: 14,
    color: "#6b7280",
    marginBottom: 6,
  },
  subheading: {
    fontSize: 13,
    fontWeight: "600",
    color: "#4b5563",
    marginBottom: 6,
  },
  divider: {
    height: StyleSheet.hairlineWidth,
    backgroundColor: "#e5e7eb",
    marginVertical: 12,
  },
  typeTag: {
    fontSize: 13,
    fontWeight: "500",
    color: "#1e3a5f",
    marginTop: 8,
  },
  directionsBtn: {
    backgroundColor: "#1e3a5f",
    borderRadius: 10,
    paddingVertical: 14,
    alignItems: "center",
  },
  directionsBtnText: {
    color: "#ffffff",
    fontSize: 15,
    fontWeight: "600",
  },
});
