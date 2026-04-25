from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_s3(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a S3 bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#s3
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    today = kwargs.get('execution_date')
    year_month_id = today.strftime("%Y%m")
    date_id = today.strftime("%Y%m%d")
    timestamp_id = today.strftime("%H%M%S")
    ingestion_date = kwargs.get('ingestion_data')

    bucket_name = 'dwhfilesystem'
    object_key = f'landing_area/{ingestion_data}/{year_month_id}/{date_id}/{ingestion_data}_{timestamp_id}.csv'

    S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )