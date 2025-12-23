-- Migration: init
-- Created: 2025-12-23
-- Description: Installera extensions för geodata ETL

-- Spatial extension för geometri och geodata
INSTALL spatial;
LOAD spatial;

-- Parquet för GeoParquet-stöd
INSTALL parquet;
LOAD parquet;

-- HTTP filesystem för WFS och remote-filer
INSTALL httpfs;
LOAD httpfs;

-- JSON för API-responses
INSTALL json;
LOAD json;

-- Skapa schema för rådata och bearbetad data
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- Metadata-tabell för att spåra migrationer
CREATE TABLE IF NOT EXISTS _migrations (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    applied_at TIMESTAMP DEFAULT current_timestamp
);

-- Registrera denna migration
INSERT INTO _migrations (id, name) VALUES (1, '20251223184625_init');
