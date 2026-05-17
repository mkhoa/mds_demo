---
name: managing-mage-pipelines
description: Use to build Mage AI blocks (loaders, transformers, exporters) and to trigger or inspect pipelines in the MDS Demo project. Use when a user asks to add a data source, build an ingestion step, or run a pipeline.
---

# Mage AI Blocks — mds_demo Project

## Project Layout
Organize new blocks in domain subdirectories under `application/mage_ai/mds_demo/`.
- `data_loaders/{domain}/`
- `transformers/{domain}/`
- `data_exporters/` (Generic: `generic_components/write2landing_block.py`)
- `pipelines/{pipeline_name}/` (Contains `metadata.yaml` and `__init__.py`)

**Rule:** Always create `__init__.py` in every new subdirectory.

---

## 1. Data Loader
Use for fetching or reading data from external sources or storage.

### Template
```python
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """Read data using pipeline variables."""
    # Example: param = kwargs.get('param', 'default')
    df = pd.DataFrame() # ... fetch data ...
    print(f"Loaded {len(df):,} rows")
    return df

@test
def test_output(df, *args) -> None:
    assert df is not None, 'Output is None'
    assert len(df) > 0, 'DataFrame is empty'
```

### Common Loaders
- **MinIO/MinIO (S3):** Use `fsspec` and `os.getenv` for credentials.
- **HTTP:** Use `requests` and `io.BytesIO`.

---

## 2. Transformer
Use for cleaning, enriching, or reshaping DataFrames.

### Template
```python
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    # ... transformations ...
    print(f"After transform: {len(df):,} rows, {len(df.columns)} columns")
    return df

@test
def test_output(df, *args) -> None:
    assert df is not None and len(df) > 0, 'Empty output'
```

---

## 3. Data Exporter
Use for saving data to the warehouse or landing area.

### Generic Landing Exporter (Preferred)
Reference `generic_components/write2landing_block.py` in `metadata.yaml`.
**Required Variables:** `ingestion_data`, `bucket_name` (default: `dwhfilesystem`), `file_format` (default: `parquet`).

### Delta Lake Exporter
```python
import os
from deltalake.writer import write_deltalake

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_delta(df, *args, **kwargs):
    path = f"s3://{kwargs.get('bucket', 'dwhfilesystem')}/{kwargs.get('destination_path')}"
    write_deltalake(path, df, mode='append', storage_options={
        'AWS_ACCESS_KEY_ID': os.getenv('MINIO_ROOT_USER', 'admin'),
        'AWS_SECRET_ACCESS_KEY': os.getenv('MINIO_ROOT_PASSWORD', 'admin123'),
        'AWS_ENDPOINT_URL': os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        'AWS_REGION': 'us-east-1',
        'AWS_MinIO (S3)_ALLOW_UNSAFE_RENAME': 'true',
        'AWS_ALLOW_HTTP': 'true',
    })
```

---

## 4. Pipeline Configuration
Pipelines are defined in `pipelines/{pipeline_name}/metadata.yaml`.

### Wiring Rules
- `uuid` must match the relative file path without `.py` extension.
- `upstream_blocks` and `downstream_blocks` use block `uuid`s.
- Use `file_source.path` for shared/generic blocks.

### Variables
Define in `variables` section of `metadata.yaml`:
- `ingestion_data`: Folder name in landing path.
- `bucket_name`: `dwhfilesystem`.

---

## 5. Checklist
- [ ] `__init__.py` created in all new subdirectories.
- [ ] Hardcoded values replaced by `kwargs.get()` or env vars.
- [ ] `@test` blocks included in loaders and transformers.
- [ ] `print` statements added for row counts.
- [ ] `requirements.txt` updated if new libraries are used.

## Triggering pipelines

Run a pipeline from the `magic` container:

    DC="docker compose -p mds_demo -f /workspace/docker-compose.yml"
    $DC exec -T magic mage run mds_demo <pipeline_name>

List pipelines by inspecting `application/mage_ai/mds_demo/pipelines/`.
After a run, verify the loaded data with the `querying-the-warehouse` skill.


## MDS Platform Context
- **Warehouse:** Postgres 17 (`warehouse_db`) with `pg_duckdb` and `pgvector`.
- **Storage:** MinIO (`dwhfilesystem`) for landing area.
- **Orchestration:** Mage AI with dbt-core.
- **Federation:** Trino for cross-source joins.
- **BI:** Metabase dashboards.
