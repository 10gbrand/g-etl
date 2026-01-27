-- DuckDB Scheman
-- Skapa alla scheman som används i ETL-pipelinen

-- Raw: Rå ingesterad data från plugins
CREATE SCHEMA IF NOT EXISTS raw;

-- Staging: Mellanliggande transformationer
CREATE SCHEMA IF NOT EXISTS staging;

-- Mart: Färdiga dataset för konsumtion
CREATE SCHEMA IF NOT EXISTS mart;
