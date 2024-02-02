import pandas as pd
import s3fs
import fsspec
import os

from mage_ai.io.config import ConfigKey, EnvironmentVariableLoader
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(df, *args, **kwargs):
    """
    Export data to csv format in blob storage

    """

    config = EnvironmentVariableLoader()
    MINIO_URL = config.get('min_io_url')
    MINIO_USER = config.get('min_io_admin')
    MINIO_KEY = config.get('min_io_pwd')

    fsspec.config.conf = {
      "s3":
      {
        "key": MINIO_USER,
        "secret": MINIO_KEY,
        "client_kwargs": {
          "endpoint_url": os.getenv("S3_ENDPOINT", MINIO_URL)
        }
      }
    }
    
    today = kwargs.get('execution_date') + timedelta(hours=7)
    year_month_id = today.strftime("%Y%m")
    date_id = today.strftime("%Y%m%d")
    timestamp_id = today.strftime("%H%M%S")

    bucket = kwargs.get('bucket')
    container = kwargs.get('container')
    folder_path = kwargs.get('folder_path')
    landing_path = f"s3://{bucket}/{container}/{folder_path}/{year_month_id}/{date_id}/{timestamp_id}.csv"

    # Add partition column to DataFrame
    df['year_month_id'] = year_month_id
    df['date_id'] = date_id
    df['timestamp_id'] = timestamp_id

    df.to_csv(landing_path, index=False)