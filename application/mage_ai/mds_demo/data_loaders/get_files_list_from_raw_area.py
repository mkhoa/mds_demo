import pandas as pd
import s3fs
import fsspec
import os

from mage_ai.io.config import ConfigKey, EnvironmentVariableLoader
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_from_s3_bucket(*args, **kwargs) -> List:
    """
    Template for loading data from a S3 bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#s3
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

    fs = s3fs.S3FileSystem()

    bucket = kwargs.get('bucket')
    container = kwargs.get('src_container')
    execution_date = kwargs.get('execution_date')
    # convert to local timezone
    grass_date = execution_date + timedelta(hours=7)
    folder_path = kwargs.get('folder_path')
    date_id = grass_date.strftime("%Y%m%d")
    year_month_id = grass_date.strftime("%Y%m")


    source_path = f"s3://{bucket}/{container}/{folder_path}/{year_month_id}/{date_id}/"
    files = fs.ls(source_path)
    
    return [files]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'