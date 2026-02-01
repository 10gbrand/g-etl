# Bygga Linux-executable med Nuitka

Nuitka kompilerar Python till C och skapar en fristående executable. Bygget körs i Docker för att skapa en Linux-kompatibel binary.

## Snabbstart

```bash
task admin:build-nuitka
```

## Manuellt bygge

```bash
# Bygg Docker-image
docker build -f Dockerfile.nuitka -t g-etl-nuitka .

# Kör bygget och kopiera resultat
mkdir -p dist/nuitka
docker run --rm -v $(pwd)/dist/nuitka:/output g-etl-nuitka cp /build/app.bin /output/g_etl

# Resultat finns i dist/nuitka/
ls -la dist/nuitka/
```

## Kör executable

```bash
# I Docker (från macOS)
docker run --rm -it -v $(pwd)/dist/nuitka:/app debian:bookworm /app/g_etl

# Direkt på Linux
./dist/nuitka/g_etl
```

## Byggtid

- Python-analys: ~3 min
- C-kodgenerering: ~1 min
- C-kompilering: ~15-30 min (beroende på maskin)

Total byggtid: **20-35 minuter**

## Felsökning

### Bygget tar för lång tid

Nuitka kompilerar alla dependencies (numpy, pandas, geopandas) till C. Detta är normalt.

### Out of memory

Öka Docker-minnet i Docker Desktop > Settings > Resources.

### Executable startar inte

Kontrollera att alla dependencies finns med. Kör i Docker för att testa:

```bash
docker run --rm -it g-etl-nuitka /build/app.bin --help
```
