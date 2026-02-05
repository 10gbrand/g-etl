# Datasets

Dokumentation av alla konfigurerade dataset i G-ETL.

## Skogsstyrelsen (sks)

| ID | Namn | Status | Beskrivning |
| -- | ---- | ------ | ----------- |
| `avverkningsanmalningar` | Avverkningsanmälningar | Aktiv | Avverkningsanmälda områden |
| `sksbiotopskydd` | Biotopskydd SKS | Aktiv | Biotopskydd med typ och naturtyp |
| `biotopskydd_sks` | Biotopskydd SKS (alt) | Aktiv | Biotopskydd, kod 105 i GISS |
| `naturvardsavtal_sks` | Naturvårdsavtal SKS | Aktiv | Naturvårdsavtal, kod 104 i GISS |
| `nyckelbiotoper` | Nyckelbiotoper | Aktiv | Nyckelbiotoper, kod 112 i GISS |
| `stornyckelbiotoper` | Storskogsbrukets Nyckelbiotoper | Aktiv | Kod 113 i GISS |
| `sumpskogar` | Sumpskogar | Aktiv | Sumpskogar, kod 114 i GISS |
| `skog_o_historia_yta` | Skog & historia - Ytor | Aktiv | Skogshistoriska ytor (polygoner) |
| `skog_o_historia_pkt` | Skog & historia - Punkter | Aktiv | Skogshistoriska punkter |
| `skog_o_historia_linje` | Skog & historia - Linjer | Aktiv | Skogshistoriska linjer |
| `sksdodagranar` | Döda granar AI | Inaktiv | AI-detekterade döda granar |
| `utford_avverkning` | Utförda avverkningar | Inaktiv | Källan finns ej |
| `skog_med_hoga_naturvarden` | SMHN | Inaktiv | Källan finns ej |
| `frotaktsomraden` | Frötäktsområden | Inaktiv | Källan finns ej |

## Naturvårdsverket (nvv)

| ID | Namn | Status | Beskrivning |
| -- | ---- | ------ | ----------- |
| `nationalparker` | Nationalparker | Aktiv | Kod 101 i GISS |
| `naturreservat` | Naturreservat | Aktiv | Kod 102 i GISS |
| `natura2000_sci_ac` | Natura2000 SCI AC | Aktiv | Kod 103 i GISS |
| `natura2000_sci_alvar_bd` | Natura2000 SCI Alvar BD | Aktiv | Kod 103 i GISS |
| `natura2000_sci_ej_alvar_rikstackande` | Natura2000 SCI Rikstäckande | Aktiv | Kod 103 i GISS |
| `natura2000_spa` | Natura2000 SPA | Aktiv | Kod 103 i GISS |
| `naturvardsavtal` | Naturvårdsavtal | Aktiv | Kod 104 i GISS |
| `biotopskydd` | Biotopskydd | Aktiv | Kod 105 i GISS |
| `vso` | VSO | Aktiv | Kod 106 i GISS |
| `Naturvardsomrade` | Naturvårdsområde | Aktiv | Kod 118 i GISS |
| `kulturreservat` | Kulturreservat | Aktiv | - |
| `naturminnen_punkt` | Naturminnen (punkter) | Aktiv | - |
| `naturminnen_ytor` | Naturminnen (ytor) | Aktiv | - |
| `ramsar_vatmarker` | Ramsar-våtmarker | Aktiv | Internationellt skyddade |
| `skyddsvarda_statliga_skogar` | Skyddsvärda statliga skogar | Aktiv | - |
| `skogliga_vardekarnor` | Skogliga värdekärnor | Inaktiv | Stor fil (314 MB) |
| `dikningsforetagskartan` | Dikningsföretagskartan | Inaktiv | Källan finns ej |

## Riksantikvarieämbetet (raa)

| ID | Namn | Status | Beskrivning |
| -- | ---- | ------ | ----------- |
| `fornlamning` | Fornlämningar | Aktiv | Fornlämningar från RAA |

## SGU (sgu)

| ID | Namn | Status | Beskrivning |
| -- | ---- | ------ | ----------- |
| `torvlagerfoljder` | Torvlagerföljder | Aktiv | - |
| `jordarter25k-100k` | Jordarter 25k-100k | Inaktiv | - |

## Havs- och vattenmyndigheten (hav)

| ID | Namn | Status | Beskrivning |
| -- | ---- | ------ | ----------- |
| `Vardefullavatten` | Värdefulla vatten | Aktiv | Levande sjöar och vattendrag |

### Tillgängliga lager för Vardefullavatten

WFS-tjänsten innehåller flera lager. Aktivt lager: `vardefulla-vatten:levande-sjoar-vattendrag`

**Övriga tillgängliga lager (ej konfigurerade):**

| Lager | Beskrivning |
| ----- | ----------- |
| `vardefulla-vatten:levande-sjoar-vattendrag-generaliserad` | Generaliserad version |
| `vardefulla-vatten:svf_FIV` | Särskilt värdefulla vatten - FIV |
| `vardefulla-vatten:svf_RAA` | Särskilt värdefulla vatten - RAA |
| `vardefulla-vatten:svf_NV` | Särskilt värdefulla vatten - NV |
| `vardefulla-vatten:svf_NV_Storalvar` | Särskilt värdefulla vatten - Storalvar |
| `vardefulla-vatten:vf_FIV` | Värdefulla vatten - FIV |

För att lägga till ett lager, skapa ett nytt dataset i `datasets.yml` med samma URL men annat `layer`-värde.

## SLU (slu)

| ID | Namn | Status | Beskrivning |
| -- | ---- | ------ | ----------- |
| `Forsok` | Silvaboreal försök | Inaktiv | - |

## Lokala dataset

| ID | Namn | Status | Beskrivning |
| -- | ---- | ------ | ----------- |
| `lokalt_dataset` | Lokalt dataset | Inaktiv | Mall för lokala GeoPackage-filer |
