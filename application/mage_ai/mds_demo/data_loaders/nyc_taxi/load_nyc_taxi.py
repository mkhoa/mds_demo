import io
import os
import requests
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

NYC_TLC_BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

TAXI_TYPES = {
    'yellow': 'yellow_tripdata',
    'green':  'green_tripdata',
    'fhv':    'fhv_tripdata',
    'fhvhv':  'fhvhv_tripdata',
}


@data_loader
def load_nyc_taxi(*args, **kwargs) -> pd.DataFrame:
    """
    Load NYC TLC taxi trip data for a given taxi type, year, and month.

    Pipeline variables (set in Mage UI → Edit Pipeline → Variables):
        taxi_type : str   — yellow | green | fhv | fhvhv  (default: yellow)
        year      : int   — e.g. 2024
        month     : int   — 1-12
    """
    taxi_type = kwargs.get('taxi_type', 'yellow')
    year      = int(kwargs.get('year',  2024))
    month     = int(kwargs.get('month', 1))

    if taxi_type not in TAXI_TYPES:
        raise ValueError(f"Unknown taxi_type '{taxi_type}'. Choose from: {list(TAXI_TYPES)}")

    filename = f"{TAXI_TYPES[taxi_type]}_{year}-{month:02d}.parquet"
    url = f"{NYC_TLC_BASE_URL}/{filename}"

    print(f"Downloading: {url}")
    response = requests.get(url, timeout=300)

    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url} — HTTP {response.status_code}")

    df = pd.read_parquet(io.BytesIO(response.content))

    df['taxi_type'] = taxi_type
    df['year']      = year
    df['month']     = month

    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    print(df.dtypes)

    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'Output is None'
    assert isinstance(df, pd.DataFrame), 'Output is not a DataFrame'
    assert len(df) > 0, 'DataFrame is empty'
    assert 'taxi_type' in df.columns, 'Missing taxi_type column'
