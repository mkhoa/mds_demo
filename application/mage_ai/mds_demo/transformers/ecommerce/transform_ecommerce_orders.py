import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

DATETIME_COLS = [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date',
]


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip().str.strip('"')

    for col in DATETIME_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    before = len(df)
    df = df.drop_duplicates(subset=['order_id'])
    dropped = before - len(df)
    if dropped:
        print(f"Dropped {dropped:,} duplicate order_ids")

    print(f"After transform: {len(df):,} rows, {len(df.columns)} columns")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'Output is None'
    assert len(df) > 0, 'DataFrame is empty after transform'
    assert 'order_id' in df.columns, 'Missing order_id column'
    assert df['order_id'].notna().all(), 'Null order_ids found'
