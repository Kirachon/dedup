# Offline Beneficiary Tool

Offline Beneficiary Tool is a fully offline Windows desktop app for LGU
beneficiary encoding, deduplication review, CSV import/export, PSGC-backed
location selection, audit history, and local SQLite storage.

The app is built with Go, Fyne, and SQLite. It runs without a network
connection after first setup.

## What You Need

- Windows 10 or Windows 11
- Git
- Go 1.25.x
- PowerShell 7 recommended
- A working Windows C toolchain for Fyne/Go builds if your machine does not
  already have one

The repository already includes the PSGC CSV file used by the app:

- `lib_geo_map_2025_202603251312.csv`

The repository also includes the CSV import template bundled with the app:

- `beneficiary_import_template.csv`

That template uses the operator-facing matching layout:

- `id`
- `last_name`
- `first_name`
- `middle_name`
- `extension_name`
- `region`
- `province`
- `city_municipality`
- `barangay`
- `contact_no`
- `month_mm`
- `day_dd`
- `year_yyyy`
- `sex`

The `id` field can be left blank on import if you want the system to
auto-generate one.

## Beginner Setup

### 1. Clone the repository

```powershell
git clone https://github.com/Kirachon/dedup.git
cd dedup
```

### 2. Check that the PSGC CSV is present

The app loads PSGC data from `lib_geo_map_2025_202603251312.csv`.
Keep that file in the repository root when running from source.

Use `beneficiary_import_template.csv` as the starting point for CSV imports.
The packaged release copies it next to the executable, and the Encoding screen
uses the same split birthdate fields (`month_mm`, `day_dd`, `year_yyyy`).

If you move it, set the environment variable `BENEFICIARY_APP_PSGC_CSV`
to the new path.

### 3. Run the app from source

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run.ps1
```

This starts the desktop app and creates the local database automatically
on first run.

### 4. Use the app

On first launch, the app will:

1. create the local application data folder
2. create the SQLite database
3. run schema migrations
4. load PSGC data from the CSV file
5. open the desktop shell

The default database is stored under your Windows user profile, usually at:

- `%AppData%\beneficiary-app\beneficiary.db`

## Packaging a Release

If you want a portable build folder, use the package script:

```powershell
powershell -ExecutionPolicy Bypass -File build/package.ps1 -Version 0.1.0
```

The release folder is written under `build/releases/`.
It includes:

- `beneficiary-app.exe`
- `lib_geo_map_2025_202603251312.csv`
- `beneficiary_import_template.csv`
- `THIRD_PARTY_NOTICES.md`
- `manifest.json`
- `checksums.sha256`

You can run the packaged app directly from that release folder.

## Validation Commands

Run the standard build:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build.ps1
```

Run tests:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/test.ps1
```

Run the repo validation script:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/validate.ps1
```

Run the full workflow smoke test:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/smoke.ps1
```

If Windows endpoint protection locks temporary test executables, use the
known-lock fallback:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/test.ps1 -AllowKnownWindowsExeLockFallback
```

## Antivirus Note

If ESET NOD32 or another antivirus quarantines Go temp executables during
builds, add exclusions for:

- `build\.gocache`
- `build\.gotmp`
- `%LOCALAPPDATA%\Temp\go-build*`

If a build fails with `Access is denied` on `a.out.exe` or `.test.exe`,
restore the quarantined file, add the exclusion, and run the command again.

## Beginner Workflow

1. Open the app.
2. Go to **Settings** and confirm the LGU profile details.
3. Go to **Encoding** and add beneficiary records.
4. Use **Precheck** before saving new entries.
5. Use **Dedup** to review possible duplicates.
6. Use **Import** to bring in CSV or exchange packages.
7. Use **Export** to generate cleaned CSV output.
8. Use **Backup** before any risky data change.

## Project Layout

- `cmd/beneficiary-app` - application entry point
- `internal/app` - startup and bootstrap flow
- `internal/config` - runtime configuration
- `internal/db` - SQLite bootstrap and migrations
- `internal/psgc` - PSGC CSV ingestion and lookup logic
- `internal/repository` - database access layer
- `internal/service` - business logic and orchestration
- `internal/ui` - Fyne desktop UI
- `scripts` - build, run, test, and validation helpers
- `build` - packaging scripts and build docs
- `docs/release` - release checklist and handoff notes

## More Details

- See `build/README.md` for packaging and artifact layout details.
- See `docs/release/README.md` for release handoff notes.
- See `offline_beneficiary_tool_plan.md` for the implementation plan.
