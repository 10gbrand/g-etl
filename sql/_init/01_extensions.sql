-- DuckDB Extensions
-- Installera och ladda alla nödvändiga extensions

-- Spatial - geometri och geodata
INSTALL spatial;
LOAD spatial;

-- Parquet - GeoParquet-stöd
INSTALL parquet;
LOAD parquet;

-- HTTPFS - WFS och fjärrfiler via HTTP
INSTALL httpfs;
LOAD httpfs;

-- JSON - API-responshantering
INSTALL json;
LOAD json;

-- H3 - Spatial indexering (community extension)
INSTALL h3 FROM community;
LOAD h3;
