from pandas import DataFrame
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(data, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.DROP_DUPLICATE

    Docs: https://docs.mage.ai/guides/transformer-blocks#drop-duplicates
    """

    df = DataFrame(data)
    filename = kwargs.get('block_uuid')
    
    today = kwargs.get('execution_date') + timedelta(hours=7)
    year_month_id = today.strftime("%Y%m")
    date_id = today.strftime("%Y%m%d")
    timestamp_id = today.strftime("%H%M%S")

    # Add partition column to DataFrame
    df['year_month_id'] = year_month_id
    df['date_id'] = date_id
    df['timestamp_id'] = timestamp_id

    return df, filename


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'