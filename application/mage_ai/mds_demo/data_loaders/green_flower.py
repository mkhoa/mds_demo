from pandas import DataFrame
import io
import pandas as pd
import requests
import pyspark

@data_loader
def load_data(**kwargs) -> DataFrame:
    spark = kwargs.get('spark')
    df = spark.sql('select 1 as test')
    
    return df