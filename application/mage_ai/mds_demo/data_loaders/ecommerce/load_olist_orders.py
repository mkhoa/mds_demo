import io
import requests
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

OLIST_ORDERS_URL = (
    'https://github.com/olist/work-at-olist-data/raw/master/datasets/olist_orders_dataset.csv'
)


@data_loader
def load_olist_orders(*args, **kwargs) -> pd.DataFrame:
    url = kwargs.get('source_url', OLIST_ORDERS_URL)
    print(f"Downloading Olist orders from: {url}")

    response = requests.get(url, timeout=300, allow_redirects=True)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url} — HTTP {response.status_code}")

    df = pd.read_csv(io.BytesIO(response.content))
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    print(df.dtypes)
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'Output is None'
    assert isinstance(df, pd.DataFrame), 'Output is not a DataFrame'
    assert len(df) > 0, 'DataFrame is empty'
    assert 'order_id' in df.columns, 'Missing order_id column'
