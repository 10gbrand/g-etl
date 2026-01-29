-- DuckDB Scheman
-- Skapa alla scheman som används i ETL-pipelinen

-- Raw: Rå ingesterad data från plugins
CREATE SCHEMA IF NOT EXISTS raw;

-- Staging: Mellanliggande transformationer (validering, MD5, H3)
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging_2: Normaliserade dataset med enhetlig struktur
CREATE SCHEMA IF NOT EXISTS staging_2;

-- Mart: Aggregerade tabeller (t.ex. h3_cells)
CREATE SCHEMA IF NOT EXISTS mart;
