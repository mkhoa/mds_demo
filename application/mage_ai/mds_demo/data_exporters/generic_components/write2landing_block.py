from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from pandas import DataFrame
from datetime import datetime
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_data_to_s3(df: DataFrame, **kwargs) -> None:
    """
    Generic landing-zone exporter to MinIO/S3.

    Pipeline variables:
        ingestion_data : str  — dataset name used in the path (e.g. 'nyc_taxi')
        bucket_name    : str  — target bucket (default: 'dwhfilesystem')
        file_format    : str  — 'csv' or 'parquet' (default: 'parquet')
        config_profile : str  — io_config.yaml profile (default: 'default')
    """
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = kwargs.get('config_profile', 'default')

    # Fall back to now() when running manually (execution_date is None)
    today        = kwargs.get('execution_date') or datetime.utcnow()
    date_id      = today.strftime('%Y%m%d')
    timestamp_id = today.strftime('%H%M%S')

    ingestion_data = kwargs.get('ingestion_data')
    if not ingestion_data:
        raise ValueError("Pipeline variable 'ingestion_data' is required")

    bucket_name  = kwargs.get('bucket_name', 'dwhfilesystem')
    file_format  = kwargs.get('file_format', 'parquet')
    extension    = 'parquet' if file_format == 'parquet' else 'csv'

    # Hive-style partition path — year/month/day enable partition pruning;
    # timestamp in the filename disambiguates multiple runs on the same day.
    object_key = (
        f'landing_area/{ingestion_data}'
        f'/year={today.year}/month={today.month}/day={today.day}'
        f'/{ingestion_data}_{timestamp_id}.{extension}'
    )

    print(f"Exporting {len(df):,} rows → s3://{bucket_name}/{object_key}")

    S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )

    print("Export complete.")


@test
def test_output(*args, **kwargs) -> None:
    pass
