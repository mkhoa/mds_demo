import io
import pandas as pd
import requests
import pyspark

from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

def data_from_internet():
    url = 'https://raw.githubusercontent.com/mage-ai/datasets/master/restaurant_user_transactions.csv'

    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text), sep=',')


@data_loader
def load_data(**kwargs):
    
    df = data_from_internet()
    # df_spark = kwargs['spark'].createDataFrame(df)
    # df_spark.show()

    return df