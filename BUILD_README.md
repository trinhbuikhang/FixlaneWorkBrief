# Build Instructions - Data Processing Tool

## Quick Build with Splash Screen

The optimized build includes a **splash screen** that makes the app appear instantly when launched, even on slow systems.

### Step 1: Create Splash Screen (One-time)

```powershell
python create_splash.py
```

This creates `splash.png` with rose gold theme matching the app design.

### Step 2: Build the Executable

```powershell
python build_optimized.py
```

The executable will be created at: `dist/DataProcessingTool_Optimized.exe`

## What is the Splash Screen?

When users double-click your `.exe` file:
1. **Splash screen appears immediately** (< 1 second)
2. App extracts and loads in the background (5-10 seconds)
3. Main window appears and splash closes automatically

**Without splash screen:** Users see nothing for 10+ seconds and may think the app is broken.

**With splash screen:** Users see your branded loading screen immediately!

## Build Features

- **Size:** ~150-200MB (optimized from 722MB)
- **Startup:** Splash screen shows instantly
- **Excluded:** Unnecessary ML/plotting libraries
- **Included:** All icons, polars, PyQt6, logging
- **Target:** Windows 10/11 64-bit

## Testing the Build

1. Copy `DataProcessingTool_Optimized.exe` to a clean Windows machine
2. Double-click the exe
3. You should see the rose gold splash screen immediately
4. Main window appears after 5-10 seconds
5. Splash closes automatically

## Troubleshooting

### Splash screen not showing?

Make sure `splash.png` exists before building:
```powershell
python create_splash.py
python build_optimized.py
```

### App still slow to appear?

The splash screen shows **immediately** but the actual app still needs time to:
- Extract from the exe bundle (~5 seconds)
- Import PyQt6 and polars (~2-3 seconds)
- Load the main window (~1-2 seconds)

This is normal for PyInstaller `--onefile` mode. The splash screen lets users know the app is loading.

### Alternative: `--onedir` mode (faster but larger)

If you need faster startup, use `--onedir` instead:
- Startup: 2-3 seconds (no extraction needed)
- Size: ~500MB folder (many files)
- Distribution: Must zip the entire folder

## Build Comparison

| Mode | Startup | Size | Files |
|------|---------|------|-------|
| Original | 10+ sec | 722MB | 1 file |
| Optimized + Splash | <1 sec visible | 150-200MB | 1 file |
| OneDir (optional) | 2-3 sec | 500MB | Many files |

**Recommended:** Optimized + Splash (current setup)

## Distribution

Package for users:
1. `DataProcessingTool_Optimized.exe` - The application
2. Optional: README or shortcut

Users only need the `.exe` file - no Python installation required!

## Version Info

- App Version: 1.0
- PyInstaller: 6.x
- Python: 3.13
- Theme: Rose Gold
- Splash: 600x400 PNG
