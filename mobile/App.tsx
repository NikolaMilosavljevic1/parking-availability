import React from "react";
import { NavigationContainer } from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { StatusBar } from "expo-status-bar";

import GarageList from "./screens/GarageList";
import GarageDetail from "./screens/GarageDetail";
import { Location } from "./types";

// ---------------------------------------------------------------------------
// Navigation type map — shared with screens via import
// ---------------------------------------------------------------------------

export type RootStackParamList = {
  GarageList: undefined;
  GarageDetail: { location: Location };
};

const Stack = createNativeStackNavigator<RootStackParamList>();

// ---------------------------------------------------------------------------
// App root
// ---------------------------------------------------------------------------

export default function App() {
  return (
    <NavigationContainer>
      <StatusBar style="dark" />
      <Stack.Navigator
        screenOptions={{
          headerStyle: { backgroundColor: "#ffffff" },
          headerTintColor: "#111827",
          headerTitleStyle: { fontWeight: "700" },
          contentStyle: { backgroundColor: "#f9fafb" },
        }}
      >
        <Stack.Screen
          name="GarageList"
          component={GarageList}
          options={{ title: "Belgrade Parking" }}
        />
        <Stack.Screen
          name="GarageDetail"
          component={GarageDetail}
          options={({ route }) => ({
            // Strip the Serbian "Garaža" / "Parkiralište" prefix for a cleaner header
            title: route.params.location.name
              .replace(/^Garaža\s+"?/, "")
              .replace(/^Parkiralište\s+"?/, "")
              .replace(/"$/, ""),
          })}
        />
      </Stack.Navigator>
    </NavigationContainer>
  );
}
