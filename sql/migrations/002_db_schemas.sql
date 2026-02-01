-- Migration: DuckDB Scheman
-- Version: 002
-- Skapar alla scheman som används i ETL-pipelinen

-- migrate:up

-- Raw: Rå ingesterad data från plugins
CREATE SCHEMA IF NOT EXISTS raw;

-- Staging: Mellanliggande transformationer (validering, MD5, H3)
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging_2: Normaliserade dataset med enhetlig struktur
CREATE SCHEMA IF NOT EXISTS staging_2;

-- Mart: Aggregerade tabeller (t.ex. h3_cells)
CREATE SCHEMA IF NOT EXISTS mart;

-- migrate:down

-- OBS: DROP SCHEMA CASCADE tar bort ALL data i schemat
DROP SCHEMA IF EXISTS mart CASCADE;
DROP SCHEMA IF EXISTS staging_2 CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS raw CASCADE;
