#!/bin/bash
# G-ETL Setup Script
# Laddar ner alla filer som behövs för att köra med docker-compose

set -e

REPO="10gbrand/g-etl"
BRANCH="main"
BASE_URL="https://raw.githubusercontent.com/${REPO}/${BRANCH}"

echo "=== G-ETL Setup ==="
echo ""

# Skapa mappar
echo "Skapar mappar..."
mkdir -p config sql/migrations data input_data

# Ladda ner docker-compose.yml
echo "Laddar ner docker-compose.yml..."
curl -sL "${BASE_URL}/docker-compose.yml" -o docker-compose.yml

# Ladda ner config-filer
echo "Laddar ner config..."
curl -sL "${BASE_URL}/config/datasets.yml" -o config/datasets.yml

# Ladda ner SQL-filer
echo "Laddar ner SQL-templates..."
for file in \
    001_db_extensions.sql \
    002_db_schemas.sql \
    003_db_makros.sql \
    004_staging_transform_template.sql \
    005_staging2_normalisering_template.sql \
    006_mart_h3_cells_template.sql \
    007_mart_compact_h3_cells_template.sql
do
    curl -sL "${BASE_URL}/sql/migrations/${file}" -o "sql/migrations/${file}"
done

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
