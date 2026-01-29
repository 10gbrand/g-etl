-- Common: Output-funktioner för mart
-- Skapar återanvändbara makron för färdiga dataset
--
-- Användning i dataset-specifika SQL-filer:
--   SELECT add_source_metadata('Skogsstyrelsen', 'avverkningsanmalningar') AS _source FROM ...

-- Makro för att skapa source-metadata struct
CREATE OR REPLACE MACRO add_source_metadata(provider, dataset_id) AS
    struct_pack(
        provider := provider,
        dataset := dataset_id,
        loaded_at := CURRENT_TIMESTAMP
    );

-- Makro för att generera en unik hash-baserad ID
CREATE OR REPLACE MACRO generate_id(seed_value) AS
    md5(CAST(seed_value AS VARCHAR) || CAST(CURRENT_TIMESTAMP AS VARCHAR));

-- Makro för att formatera datum till ISO-format
CREATE OR REPLACE MACRO format_date_iso(d) AS
    CASE
        WHEN d IS NULL THEN NULL
        ELSE strftime(d, '%Y-%m-%d')
    END;
