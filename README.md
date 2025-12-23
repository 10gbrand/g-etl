# DuckDB ETL-stack med dbmate, dbt och YARP

Detta dokument beskriver en containerbaserad ETL-stack för geodata och HTTP(S)-baserade källor, byggd på:

* DuckDB – analytisk SQL-motor med inbyggt stöd för geodata
* dbmate – minimal schema- och bootstrap-migrering
* dbt – ELT / transformationslager
* YARP – auth- och routing-proxy
* Docker – reproducerbar körmiljö
* .env – hantering av känslig information
* För stödfunktioner och scripting i övrigt föredras python

## Stacken är särskilt anpassad för:

* WFS
* GeoPackage
* GeoParquet
* Skyddade datakällor (t.ex. Lantmäteriets geodataportal)

## Lagringsformat

Om inte annat sägs så vill jag använda Geoparquet som lagringsformat för geodata.

## Som utvecklingsmiljö kör jag:

### Grundkrav

* Om på Windows så används WSL2
* Jetify devbox
* Docker

### I Devbox

* Taskfile (för hantering av komandon som ofta upprepas)
* En master Taskfile som sedan länkar till olika del-Taskfile för olika områden tex Docker, Duckdb, dbt osv
* Python miljön hanteras med UV

Så det enda kravet av någon som vill jobba med detta projekt skall vara att hen har Jetify devbox och docker-desktop.

### Claude

För stöd i kodning anvädns i detta projekt Claude

#### Instruktioner till Claude

* Shift + Enter används för radbryting i promten
* Jag vill att du _alltid_ först precenterar ett förslag på lösning som vi kan förfina innan du genomför ändringen.
* Jag vill att du uppdaterar dokumnetationen löpande
* Jag vill att du lägger till användbara komandon i taskfiles löpande
