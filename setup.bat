@echo off
REM G-ETL Setup Script for Windows
REM Laddar ner alla filer som behovs for att kora med docker-compose

setlocal enabledelayedexpansion

set REPO=10gbrand/g-etl
set BRANCH=main
set BASE_URL=https://raw.githubusercontent.com/%REPO%/%BRANCH%
set API_URL=https://api.github.com/repos/%REPO%/contents

echo === G-ETL Setup ===
echo.

REM Skapa mappar
echo Skapar mappar...
if not exist config mkdir config
if not exist sql\migrations mkdir sql\migrations
if not exist data mkdir data
if not exist input_data mkdir input_data

REM Ladda ner docker-compose.yml
echo Laddar ner docker-compose.yml...
curl -sL "%BASE_URL%/docker-compose.yml" -o docker-compose.yml
if errorlevel 1 (
    echo Fel: Kunde inte ladda ner docker-compose.yml
    exit /b 1
)

REM Ladda ner config-filer
echo Laddar ner config...
curl -sL "%BASE_URL%/config/datasets.yml" -o config\datasets.yml
if errorlevel 1 (
    echo Fel: Kunde inte ladda ner datasets.yml
    exit /b 1
)

REM Ladda ner SQL-filer dynamiskt via GitHub API
echo Laddar ner SQL-templates...

REM Anvand PowerShell for att parsa JSON och ladda ner filer
powershell -Command ^
    "$files = (Invoke-RestMethod -Uri '%API_URL%/sql/migrations?ref=%BRANCH%') | Where-Object { $_.name -like '*.sql' }; ^
    foreach ($file in $files) { ^
        Write-Host ('  - ' + $file.name); ^
        Invoke-WebRequest -Uri $file.download_url -OutFile ('sql\migrations\' + $file.name) ^
    }"

if errorlevel 1 (
    echo Fel: Kunde inte ladda ner SQL-filer
    exit /b 1
)

echo.
echo === Setup klar! ===
echo.
echo Struktur:
echo   config\datasets.yml  - Dataset-konfiguration (redigera for att valja datakallor)
echo   sql\migrations\      - SQL-templates for transformationer
echo   input_data\          - Lagg lokala geodatafiler har (monteras som /app/input_data)
echo   data\                - Resultat sparas har
echo.
echo Starta med:
echo   docker compose run --rm admin
echo.
