# Bygga G-ETL som fristående program

G-ETL kan byggas som en fristående executable för Windows, macOS och Linux.

## Automatisk build via GitHub Actions

Vid varje ny tag (`v*`) byggs automatiskt executables för alla plattformar:

```bash
# Skapa en ny release
git tag v1.0.0
git push origin v1.0.0
```

Executables publiceras som GitHub Releases.

### Manuell trigger

Du kan också trigga en build manuellt via GitHub Actions → "Build Executables" → "Run workflow".

---

## Lokal build

### Krav

- Python 3.11+
- UV (pakethanterare)
- Plattformsspecifika dependencies (se nedan)

### macOS

```bash
# Installera system-dependencies
brew install gdal proj geos unixodbc

# Bygg executable
task admin:build
```

### Linux (Ubuntu/Debian)

```bash
# Installera system-dependencies
sudo apt-get update
sudo apt-get install -y \
    libgdal-dev \
    gdal-bin \
    libspatialindex-dev \
    unixodbc-dev

# Bygg executable
task admin:build
```

### Windows

```bash
# Installera ODBC (via Chocolatey)
choco install unixodbc

# Bygg executable
task admin:build
```

---

## Output

Efter build finns executable i `dist/`-mappen:

| Plattform | Fil |
|-----------|-----|
| macOS | `dist/g-etl` |
| Linux | `dist/g-etl` |
| Windows | `dist/g-etl.exe` |

---

## Användning

```bash
# Starta TUI
./dist/g-etl

# Med mock-läge
./dist/g-etl --mock

# Med annan config-fil
./dist/g-etl --config /path/to/datasets.yml
```

---

## Kända begränsningar

### MSSQL-plugin
MSSQL-pluginet kräver att användaren installerar ODBC-drivrutinen separat:
- **Windows**: [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- **macOS**: `brew install microsoft/mssql-release/msodbcsql18`
- **Linux**: Se [Microsoft docs](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)

### DuckDB Spatial Extension
Spatial extension laddas automatiskt vid körning. Om det inte fungerar kan användaren behöva:
```sql
INSTALL spatial;
LOAD spatial;
```

---

## Felsökning

### "Library not loaded" på macOS
Om du får fel om saknade bibliotek (t.ex. GDAL):
```bash
brew reinstall gdal
```

### Stor filstorlek
Executable kan bli ~200-500 MB beroende på inkluderade dependencies. Detta är normalt för Python-applikationer med geopandas/GDAL.

### Windows Defender varning
Windows kan varna för okänd utgivare. Klicka "Mer info" → "Kör ändå".
