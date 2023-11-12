import pandas as pd
import os

from deltalake.writer import write_deltalake

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(df, *args, **kwargs):
    """
    Export data to a Delta Table

    Docs: https://delta-io.github.io/delta-rs/python/usage.html#writing-delta-tables
    """
    MINIO_USER = 'admin'
    MINIO_KEY = 'admin123'

    storage_options = {
        'AWS_ACCESS_KEY_ID': MINIO_USER,
        'AWS_SECRET_ACCESS_KEY': MINIO_KEY,
        'AWS_ENDPOINT_URL': 'http://minio:9000',
        'AWS_REGION': 'us-east-1',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
        'AWS_ALLOW_HTTP': 'true',
        'AWS_STORAGE_ALLOW_HTTP': 'true'
    }

    uri = 's3://dwhfilesystem/raw_area/vn_stock_income_statement'

    write_deltalake(
        uri,
        df,
        mode='append',          # append or overwrite
        overwrite_schema=False, # set True to alter the schema when overwriting
        partition_by=['date_id'],
        storage_options=storage_options,
    )