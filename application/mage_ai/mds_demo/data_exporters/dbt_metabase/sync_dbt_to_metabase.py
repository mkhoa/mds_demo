import os
import subprocess
from pathlib import Path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

DBT_PROJECT_DIR = '/home/src/mds_demo/dbt/data_warehouse'


@data_exporter
def sync_to_metabase(*args, **kwargs) -> None:
    """
    Compile dbt to refresh manifest.json, then push model/column descriptions
    into Metabase using an API key.

    Pipeline variables:
        metabase_url      : Metabase base URL (default: http://mds_demo-metabase-1:3000)
        metabase_api_key  : Metabase API key
        metabase_database : Database name as shown in Metabase (default: warehouse)
        include_schemas   : Comma-separated schemas to sync (default: bdh,adl)
    """
    metabase_url    = kwargs.get('metabase_url',     'http://mds_demo-metabase-1:3000')
    metabase_db     = kwargs.get('metabase_database', 'warehouse')
    api_key         = kwargs.get('metabase_api_key',  os.getenv('METABASE_API_KEY', ''))
    include_schemas = kwargs.get('include_schemas',   'bdh,adl')

    if not api_key:
        raise ValueError(
            "Set pipeline variable 'metabase_api_key' or env var METABASE_API_KEY"
        )

    # Step 1 — refresh manifest.json
    print("Compiling dbt project...")
    result = subprocess.run(
        ['dbt', 'compile', '--profiles-dir', '.', '--project-dir', DBT_PROJECT_DIR],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt compile failed:\n{result.stderr}")
    print("Manifest refreshed.")

    manifest_path = str(Path(DBT_PROJECT_DIR) / 'target' / 'manifest.json')

    # Step 2 — sync to Metabase
    print(f"Syncing schemas [{include_schemas}] → Metabase ({metabase_url})...")
    result = subprocess.run(
        [
            'dbt-metabase', 'models',
            '--manifest-path',    manifest_path,
            '--metabase-url',     metabase_url,
            '--metabase-api-key', api_key,
            '--metabase-database', metabase_db,
            '--include-schemas',  include_schemas,
            '--append-tags',
            '--order-fields',
        ],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt-metabase sync failed:\n{result.stderr}")

    print("Sync complete.")


@test
def test_output(*args, **kwargs) -> None:
    pass
