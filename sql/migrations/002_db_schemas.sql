-- Migration: DuckDB Scheman
-- Version: 002
-- Skapar bas-scheman som används i ETL-pipelinen
--
-- OBS: Staging-scheman (staging_004, staging_005, etc.) skapas dynamiskt
-- av pipeline_runner.py baserat på SQL-templates nummer.

-- migrate:up

-- Raw: Rå ingesterad data från plugins
CREATE SCHEMA IF NOT EXISTS raw;

-- Mart: Aggregerade tabeller (t.ex. h3_cells)
CREATE SCHEMA IF NOT EXISTS mart;

-- migrate:down

-- OBS: DROP SCHEMA CASCADE tar bort ALL data i schemat
DROP SCHEMA IF EXISTS mart CASCADE;
DROP SCHEMA IF EXISTS raw CASCADE;
