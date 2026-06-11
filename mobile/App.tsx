import React from "react";
import { NavigationContainer } from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { StatusBar } from "expo-status-bar";

import GarageList from "./screens/GarageList";
import GarageDetail from "./screens/GarageDetail";
import LanguageToggle from "./components/LanguageToggle";
import { LocaleProvider, useLocale } from "./i18n";
import { Location } from "./types";

export type RootStackParamList = {
  GarageList: undefined;
  GarageDetail: { location: Location };
};

const Stack = createNativeStackNavigator<RootStackParamList>();

function AppNavigator() {
  const { t } = useLocale();

  return (
    <Stack.Navigator
      screenOptions={{
        headerStyle: { backgroundColor: "#ffffff" },
        headerTintColor: "#111827",
        headerTitleStyle: { fontWeight: "700" },
        contentStyle: { backgroundColor: "#f9fafb" },
        headerRight: () => <LanguageToggle />,
      }}
    >
      <Stack.Screen
        name="GarageList"
        component={GarageList}
        options={{ title: t("appTitle") }}
      />
      <Stack.Screen
        name="GarageDetail"
        component={GarageDetail}
        options={({ route }) => ({
          title: route.params.location.name
            .replace(/^Garaža\s+"?/, "")
            .replace(/^Parkiralište\s+"?/, "")
            .replace(/"$/, ""),
        })}
      />
    </Stack.Navigator>
  );
}

export default function App() {
  return (
    <LocaleProvider>
      <NavigationContainer>
        <StatusBar style="dark" />
        <AppNavigator />
      </NavigationContainer>
    </LocaleProvider>
  );
}
