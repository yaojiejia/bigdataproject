# /data_ingest

Data-ingestion code for the NYC Neighborhood Insights pipeline. Each team
member has their own subdirectory.

| Dataset | Module | Source |
|---|---|---|
| NYPD complaints (crime) | `alexj/download.py` | NYC Open Data `5uac-w243` |
| DOHMH restaurant inspections | `alexj/download.py` | NYC Open Data `43nn-pn8j` |
| 311 food-safety complaints | `alexj/download.py` | NYC Open Data `erm2-nwe9` |
| Zillow ZORI rent (ZIP) | `alexj/download.py` | Zillow Research CSV URL |
| 2020 NTA polygons (GeoJSON) | `alexj/geo_download.py` | NYC Open Data `9nt8-h7nd` |
| NTA population template | `alexj/geo_download.py` | Generated from the GeoJSON |

## Run

From the project root:

```bash
make download        # 4 raw CSVs -> data/raw/
make geo-download    # NTA GeoJSON + population template -> data/geo/
```

Or as Python modules:

```bash
python -m data_ingest.alexj.download
python -m data_ingest.alexj.geo_download
```

Both scripts are idempotent — they skip a file if it already exists on disk.
