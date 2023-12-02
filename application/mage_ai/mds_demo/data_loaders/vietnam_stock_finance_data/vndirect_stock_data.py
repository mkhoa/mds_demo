import io
import pandas as pd

from pandas import DataFrame
from vnstock import *

def unpivot_table(df: DataFrame) -> DataFrame:
    df = pd.melt(df, id_vars=['symbol', 'Metrics'])

    return df

@data_loader
def load_fs_data(**kwargs) -> DataFrame:
    """
    Template for loading Financial Statement data

    """
    symbol = ['MWG', 'SSI', 'VNM', 'FPT', 'CEO', 'HAG']
    report_type = 'BalanceSheet'
    frequency = 'Yearly'

    df = pd.DataFrame()

    for i in symbol:
        try:
            r = financial_report (symbol=i, report_type=report_type, frequency=frequency)
            r['symbol'] = i
            df = pd.concat([df, r], sort=False)
        except:
            pass

    df = df.rename(columns={"CHỈ TIÊU": "Metrics"})
    df = unpivot_table(df)

    return df

@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
