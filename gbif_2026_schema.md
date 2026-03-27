# GBIF 2026 Occurrence Data Schema

Full 50-column schema for the GBIF 2026 occurrence data located at `/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet`.

| Column Name | Type |
|-------------|------|
| gbifid | VARCHAR |
| datasetkey | VARCHAR |
| occurrenceid | VARCHAR |
| kingdom | VARCHAR |
| phylum | VARCHAR |
| class | VARCHAR |
| order | VARCHAR |
| family | VARCHAR |
| genus | VARCHAR |
| species | VARCHAR |
| infraspecificepithet | VARCHAR |
| taxonrank | VARCHAR |
| scientificname | VARCHAR |
| verbatimscientificname | VARCHAR |
| verbatimscientificnameauthorship | VARCHAR |
| countrycode | VARCHAR |
| locality | VARCHAR |
| stateprovince | VARCHAR |
| occurrencestatus | VARCHAR |
| individualcount | INTEGER |
| publishingorgkey | VARCHAR |
| decimallatitude | DOUBLE |
| decimallongitude | DOUBLE |
| coordinateuncertaintyinmeters | DOUBLE |
| coordinateprecision | DOUBLE |
| elevation | DOUBLE |
| elevationaccuracy | DOUBLE |
| depth | DOUBLE |
| depthaccuracy | DOUBLE |
| eventdate | TIMESTAMP |
| day | INTEGER |
| month | INTEGER |
| year | INTEGER |
| taxonkey | INTEGER |
| specieskey | INTEGER |
| basisofrecord | VARCHAR |
| institutioncode | VARCHAR |
| collectioncode | VARCHAR |
| catalognumber | VARCHAR |
| recordnumber | VARCHAR |
| identifiedby | VARCHAR[] |
| dateidentified | TIMESTAMP |
| license | VARCHAR |
| rightsholder | VARCHAR |
| recordedby | VARCHAR[] |
| typestatus | VARCHAR[] |
| establishmentmeans | VARCHAR |
| lastinterpreted | TIMESTAMP |
| mediatype | VARCHAR[] |
| issue | VARCHAR[] |
