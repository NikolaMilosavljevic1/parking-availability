# Mobile assets

Expo uses this folder for app icons and splash images.

## Adding a custom icon (optional)

1. Create a **1024×1024** PNG named `icon.png` in this folder
2. Add to `app.json`:
   ```json
   "icon": "./assets/icon.png"
   ```
3. For Android adaptive icon foreground, add:
   ```json
   "android": {
     "adaptiveIcon": {
       "foregroundImage": "./assets/icon.png",
       "backgroundColor": "#1d4ed8"
     }
   }
   ```

## Splash screen

The app currently uses a solid-color splash (`#1d4ed8` in `app.json`). To use an image, add `splash.png` here and set `"image": "./assets/splash.png"` under the `splash` key.

Until custom assets are added, Expo uses its default icon in development.
