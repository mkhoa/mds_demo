import pandas as pd

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from os import path
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
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    file_lists = kwargs.get('s3')

    # file_lists = ['s3://dataforgood-fb-data/demographic_csvs/population/population_lat_0_lon_100.csv']

    results = []
    metadata = []

    for file in file_lists:
        bucket = file.split('/')[2]
        folder_path = file.split('/')[3:]
        folder_path = '/'.join(folder_path)
        filename = file.split('/')[-1]
        df = S3.with_config(ConfigFileLoader(config_path, config_profile)).load(
            bucket,
            folder_path,
        )

        results.append(df) 
        metadata.append(dict(block_uuid=filename))

    return [results, metadata]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """

    
    assert output is not None, 'The output is undefined'