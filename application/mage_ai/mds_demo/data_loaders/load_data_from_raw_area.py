import pandas as pd
import s3fs
import fsspec
import os

from mage_ai.io.config import ConfigKey, EnvironmentVariableLoader
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(files: List, *args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
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

    s3_file = f"s3://{files}"
    df = pd.read_csv(s3_file, dtype={'year_month_id': str, 'date_id': str, 'timestamp_id': str})

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'