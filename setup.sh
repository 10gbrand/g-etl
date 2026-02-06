#!/bin/bash
# G-ETL Setup Script
# Laddar ner alla filer som behövs för att köra med docker-compose

set -e

REPO="10gbrand/g-etl"
BRANCH="main"
BASE_URL="https://raw.githubusercontent.com/${REPO}/${BRANCH}"
API_URL="https://api.github.com/repos/${REPO}/contents"

echo "=== G-ETL Setup ==="
echo ""

# Skapa mappar
echo "Skapar mappar..."
mkdir -p config sql/migrations data input_data docker/huey

# Ladda ner docker-compose.yml
echo "Laddar ner docker-compose.yml..."
curl -sL "${BASE_URL}/docker-compose.yml" -o docker-compose.yml

# Ladda ner config-filer
echo "Laddar ner config..."
curl -sL "${BASE_URL}/config/datasets.yml" -o config/datasets.yml

# Ladda ner SQL-filer dynamiskt via GitHub API
echo "Laddar ner SQL-templates..."
SQL_FILES=$(curl -sL "${API_URL}/sql/migrations?ref=${BRANCH}" | grep '"name":' | sed 's/.*"name": "\([^"]*\)".*/\1/' | grep '\.sql$')

for file in $SQL_FILES; do
    echo "  - ${file}"
    curl -sL "${BASE_URL}/sql/migrations/${file}" -o "sql/migrations/${file}"
done

# Ladda ner Huey Dockerfile (DuckDB data explorer)
echo "Laddar ner Huey Dockerfile..."
curl -sL "${BASE_URL}/docker/huey/Dockerfile" -o docker/huey/Dockerfile

echo ""
echo "=== Setup klar! ==="
echo ""
echo "Struktur:"
echo "  config/datasets.yml  - Dataset-konfiguration (redigera för att välja datakällor)"
echo "  sql/migrations/      - SQL-templates för transformationer"
echo "  input_data/          - Lägg lokala geodatafiler här (monteras som /app/input_data)"
echo "  data/                - Resultat sparas här"
echo ""
echo "Starta med:"
echo "  docker compose run --rm admin"
echo ""
echo "Analysera resultat med Huey (DuckDB data explorer):"
echo "  docker compose up huey"
echo "  Öppna http://localhost:8080"
echo ""
