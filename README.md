# schema-wash
Library for cleaning, processing andd filtering JSON-nl data prior to db ingest


Usage
-----

nohup python3 schemawash/lib.py /path/to/input/ /outpath ./data_sources/config.yaml --file_suffix json.gz --maxworkers 16 > output.log 2>&1 &

python3 schemawash/lib.py ../observatory/openalex-snapshot/data/openaire/relations ../datacite/openaire/products_related ./datasources/openaire_relations_products_related.yaml --file_suffix json.gz > output.log 2>&1