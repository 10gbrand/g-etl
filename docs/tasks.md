# tasks

## Enrad per restriktion eller kolumn per restr

Vilken struktur är effektivast?

den som nu används:

|h3|typ|värde|
|---|---|---|
|aa|reservat|1|
|aa|sump|1|
|ab|sump|1|
|aa|volym|55|
|ab|volym|32|

eller:

|h3|reservat|sump|volym|
|---|---|---|---|
|aa|1|1|55|
|ab|null|1|32|

jag bedömmer att vi kommer ha totalt ett ca hundra typer totalt, men dessa kanske kan delas upp i 4-5 gupper, restriktioner, träddata, ståndortsdata mm. dessa kan delas upp på 4-5 tabeller oavsett metod ovan?

totalt är det ca 4,1 miljoner hektar som skall indexeras i h3

### Svar

Rekommendation: Hybrid-modell (4-5 vida tabeller per kategori)
Motivering:

1. Wide format per kategori (BÄST för ditt case)

-- Restriktioner
CREATE TABLE mart.h3_restrictions AS
SELECT
    h3_cell,
    MAX(CASE WHEN klass='naturreservat' THEN 1 END) AS reservat,
    MAX(CASE WHEN klass='biotopskydd' THEN 1 END) AS biotopskydd,
    MAX(CASE WHEN klass='natura2000' THEN 1 END) AS natura2000,
    COUNT(DISTINCT dataset_id) AS antal_restriktioner
FROM mart.h3_index
WHERE kategori = 'restriktioner'
GROUP BY h3_cell;

-- Träddata
CREATE TABLE mart.h3_tree_data AS
SELECT
    h3_cell,
    SUM(CASE WHEN typ='volym' THEN värde END) AS volym,
    AVG(CASE WHEN typ='höjd' THEN värde END) AS medelhöjd,
    SUM(CASE WHEN typ='grundyta' THEN värde END) AS grundyta
FROM mart.h3_tree_attributes
GROUP BY h3_cell;
Fördelar:

✅ 20-25 kolumner per tabell (hanterbara)
✅ 1 query istället för 100 JOINs för kategori
✅ DuckDB:s columnar storage = effektiv NULL-komprimering
✅ Snabba analytiska queries ("alla restriktioner i område X")
✅ Enkelt lägga till nya kategorier som nya tabeller
2. Long format (nuvarande) - BRA för vissa use cases

-- Nuvarande struktur
SELECT h3_cell, klass, dataset_id FROM mart.h3_index
WHERE h3_cell IN (...)
Fördelar:

✅ Flexibelt - nya typer utan schema-ändringar
✅ Kompakt för sparse data
✅ Bra för "vilka datasets finns här?" (nuvarande användning)
Nackdelar:

❌ Långsamt för "ge mig alla attribut för cell X"
❌ Kräver PIVOT för analys
❌ Många rader = overhead
3. Single wide table (100 kolumner) - UNDVIK

CREATE TABLE mart.h3_all_attributes (
    h3_cell VARCHAR,
    reservat INT, biotopskydd INT, ..., volym FLOAT, ...  -- 100 columns!
)
Nackdelar:

❌ Ohanterlig storlek
❌ Många NULLs (ca 95% om sparse)
❌ Svårt att underhålla
Konkret rekommendation för ditt system
Struktur:


-- 1. Behåll mart.h3_index (VIEW) för "dataset-overlap"
CREATE VIEW mart.h3_index AS ...  -- (din nuvarande lösning)

-- 2. Lägg till kategori-tabeller för attribut-analys
CREATE TABLE mart.h3_restrictions (
    h3_cell VARCHAR PRIMARY KEY,
    naturreservat BOOL,
    biotopskydd BOOL,
    natura2000 BOOL,
    vattenskydd BOOL,
    ... (~20 restriktions-typer)
);

CREATE TABLE mart.h3_forest (
    h3_cell VARCHAR PRIMARY KEY,
    volym_m3 FLOAT,
    grundyta_m2 FLOAT,
    medelhöjd_m FLOAT,
    ålder_år INT,
    ... (~25 skogs-attribut)
);

CREATE TABLE mart.h3_site (
    h3_cell VARCHAR PRIMARY KEY,
    markfuktighet VARCHAR,
    bonitet INT,
    jordart VARCHAR,
    ... (~20 ståndorts-attribut)
);
Query-exempel:


-- Snabbt: Alla attribut för område
SELECT 
    r.*,
    f.volym_m3,
    f.medelhöjd_m,
    s.bonitet
FROM mart.h3_restrictions r
LEFT JOIN mart.h3_forest f USING (h3_cell)
LEFT JOIN mart.h3_site s USING (h3_cell)
WHERE h3_cell IN (SELECT h3_cell FROM g_h3_query_table('POLYGON(...)', 8));

-- Snabbt: Aggregering per kategori
SELECT 
    COUNT(*) FILTER (WHERE naturreservat) AS cells_med_reservat,
    AVG(volym_m3) AS medel_volym
FROM mart.h3_restrictions r
JOIN mart.h3_forest f USING (h3_cell)
WHERE h3_cell = ANY(...);
Prestanda-estimat (4.1M hektar ≈ 50-100M H3-celler vid res 8)
Struktur	Rows	Columns	Storage	Query-tid (typisk)
Long (EAV)	5-10 miljarder	3	~200 GB	5-30s (pivot required)
Wide (100 cols)	50-100M	100	~50 GB (många NULLs)	1-5s
Hybrid (5 tabeller)	50-100M × 5	20 vardera	~80 GB	0.5-2s ✅
Implementation
Skulle du vilja att jag skapar detta? Jag kan:

Definiera kategorierna baserat på dina datasets
Skapa SQL-templates för kategori-tabellerna
Generera post-merge SQL som bygger wide tables från mart.h3_index
Optimera queries för typiska analyser

## Flera olia piplinetyper

Det jag har byggt hittills är ju för att hantera externa restriktioner.
om man med samma grun vill hatera flera olika typer av piplines kan man då gära så här:

Nu:

hela piplinen beskrivs av en serie *.sql:
.
├── 001_db_extensions.sql
├── 002_db_schemas.sql
├── 003_db_makros.sql
├── 004_staging_transform_template.sql
├── 005_staging_normalisering_template.sql
├── 006_mart_h3_cells_template.sql
├── 007_mart_compact_h3_cells_template.sql
└── 100_mart_h3_index_merged.sql

Ändra till:

piplinens beskrivs fortfarande av *.sql.
de som är gemensamma ligger i ./sql sedan har resp pipline en egen mapp som innehåller resp *.sql. namnsättningens prefix nedan är föra att få de ligga i rätt ordning 

.
├── 001_db_extensions.sql
├── 002_db_schemas.sql
├── 003_db_makros.sql
├── 004_staging_transform_template.sql
├── aaa_avdelning
    ├── 001_*_template.sql
    ├── 002_*_template.sql
    ├── 003_*_template.sql
    └── 100_*_merged.sql
└── aab_ext_restr
    ├── 001_staging_normalisering_template.sql
    ├── 002_mart_h3_cells_template.sql
    ├── 003_mart_compact_h3_cells_template.sql
    └── 100_mart_h3_index_merged.sql
├── x01_db_extensions.sql
├── x02_db_schemas.sql
├── x03_db_makros.sql

och att man i resp config anger vilken pipline som skall användas. tex genom att ange tex "aab_ext_restr"

- id: avverkningsanmalningar
    name: Avverkningsanmalningar
    description: Avverkningsanmälda områden från Skogsstyrelsen
    typ: skogsstyrelsen_gpkg
    plugin: zip_geopackage
    url: https://geodpags.skogsstyrelsen.se/geodataport/data/sksAvverkAnm_gpkg.zip
    enabled: true
    pipeline: aab_ext_restr
    field_mapping:
      source_id_column: $beteckn
      klass: avverkningsanmalan
      grupp:
      typ:
      leverantor: sks