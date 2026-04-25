import os
import pandas as pd
from deltalake.writer import write_deltalake

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

STORAGE_OPTIONS = {
    'AWS_ACCESS_KEY_ID':       os.getenv('MINIO_ROOT_USER', 'admin'),
    'AWS_SECRET_ACCESS_KEY':   os.getenv('MINIO_ROOT_PASSWORD', 'admin123'),
    'AWS_ENDPOINT_URL':        os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
    'AWS_REGION':              'us-east-1',
    'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    'AWS_ALLOW_HTTP':          'true',
}


@data_exporter
def export_nyc_taxi_to_minio(df: pd.DataFrame, *args, **kwargs) -> None:
    """
    Write NYC taxi data as a Delta Lake table to MinIO.

    Path: s3://dwhfilesystem/raw_area/nyc_taxi/
    Partitioned by: taxi_type / year / month
    """
    bucket = kwargs.get('bucket', 'dwhfilesystem')
    base_path = kwargs.get('base_path', 'raw_area/nyc_taxi')
    destination = f"s3://{bucket}/{base_path}"

    taxi_type = kwargs.get('taxi_type', 'yellow')
    year      = kwargs.get('year',  2024)
    month     = kwargs.get('month', 1)

    print(f"Writing {len(df):,} rows to {destination}")
    print(f"Partitions: taxi_type={taxi_type}, year={year}, month={month:02d}")

    write_deltalake(
        destination,
        df,
        mode='append',
        partition_by=['taxi_type', 'year', 'month'],
        storage_options=STORAGE_OPTIONS,
    )

    print(f"Done — Delta table written to {destination}")


@test
def test_output(*args, **kwargs) -> None:
    pass
