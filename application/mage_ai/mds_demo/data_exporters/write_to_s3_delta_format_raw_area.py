import pandas as pd
import os

from deltalake.writer import write_deltalake
from mage_ai.io.config import ConfigKey, EnvironmentVariableLoader
from datetime import datetime
from zoneinfo import ZoneInfo

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(df, *args, **kwargs):
    """
    Export data to a Delta Table

    Docs: https://delta-io.github.io/delta-rs/python/usage.html#writing-delta-tables
    """
    config = EnvironmentVariableLoader()
    MINIO_USER = config.get('min_io_admin')
    MINIO_KEY = config.get('min_io_pwd')
    
    storage_options = {
        'AWS_ACCESS_KEY_ID': MINIO_USER,
        'AWS_SECRET_ACCESS_KEY': MINIO_KEY,
        'AWS_ENDPOINT_URL': 'http://minio:9000',
        'AWS_REGION': 'us-east-1',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
        'AWS_ALLOW_HTTP': 'true',
        'AWS_STORAGE_ALLOW_HTTP': 'true'
    }

    bucket = kwargs.get('bucket')
    cointainer = kwargs.get('tgt_cointainer')
    folder_path = kwargs.get('folder_path')
    landing_path = f"s3://{bucket}/{cointainer}/{folder_path}"

    write_deltalake(
        landing_path,
        df,
        mode='overwrite',          # append or overwrite
        overwrite_schema=False, # set True to alter the schema when overwriting
        partition_by=['year_month_id', 'date_id', 'timestamp_id'],
        storage_options=storage_options,
    )