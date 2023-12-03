import pandas as pd
import boto3
import s3fs
import fsspec
import os

from mage_ai.io.config import ConfigKey, EnvironmentVariableLoader
from datetime import datetime
from zoneinfo import ZoneInfo
from io import StringIO

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_s3_bucket(*args, **kwargs):
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

    session = boto3.session.Session()
    s3 = session.resource(
        service_name='s3',
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_KEY,
        endpoint_url=MINIO_URL,
    )

    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    year_month_id = today.strftime("%Y%m")
    date_id = today.strftime("%Y%m%d")
    timestamp_id = today.strftime("%H%M%S")

    bucket = kwargs.get('bucket')
    source_path = kwargs.get('source_path')
    
    prefix = f"{source_path}/{year_month_id}/{date_id}"

    bucket = s3.Bucket(bucket)
    prefix_objs = bucket.objects.filter(Prefix=prefix)
    prefix_df = []

    for obj in prefix_objs:
        key = obj.key
        body = obj.get()['Body'].read()
        result = str(body, 'utf-8')
        df = pd.read_csv(StringIO(result))
        prefix_df.append(df)
    
    df = pd.concat(prefix_df)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'