import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# Rename map: source column → standard name per taxi type
YELLOW_RENAME = {
    'tpep_pickup_datetime':  'pickup_datetime',
    'tpep_dropoff_datetime': 'dropoff_datetime',
}
GREEN_RENAME = {
    'lpep_pickup_datetime':  'pickup_datetime',
    'lpep_dropoff_datetime': 'dropoff_datetime',
}


@transformer
def transform_nyc_taxi(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Clean and standardise NYC taxi trip data.
    - Rename taxi-type-specific datetime columns to unified names
    - Drop rows missing pickup/dropoff or location IDs
    - Remove zero/negative trip distances and fares
    - Cast columns to correct types
    - Add partition helper columns
    """
    taxi_type = kwargs.get('taxi_type', 'yellow')

    # Normalise datetime column names
    rename_map = YELLOW_RENAME if taxi_type == 'yellow' else GREEN_RENAME
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Drop rows missing critical fields
    required = [c for c in ['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID'] if c in df.columns]
    df = df.dropna(subset=required)

    # Remove nonsensical trips
    if 'trip_distance' in df.columns:
        df = df[df['trip_distance'] > 0]
    if 'fare_amount' in df.columns:
        df = df[df['fare_amount'] > 0]
    if 'pickup_datetime' in df.columns and 'dropoff_datetime' in df.columns:
        df = df[df['dropoff_datetime'] > df['pickup_datetime']]

    # Ensure correct types
    for col in ['PULocationID', 'DOLocationID']:
        if col in df.columns:
            df[col] = df[col].astype('int32')

    # year/month are encoded in the landing path as hive partitions (year=YYYY/month=M/).
    # Drop them from the parquet data to avoid column conflicts when reading with
    # hive_partitioning=true in the raw dbt model.
    df = df.drop(columns=['year', 'month'], errors='ignore')

    print(f"After transform: {len(df):,} rows, {len(df.columns)} columns")

    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'Output is None'
    assert len(df) > 0, 'DataFrame is empty after transform'
    assert 'taxi_type' in df.columns, 'Missing taxi_type column'
    assert 'year' not in df.columns, 'year should be dropped (encoded in landing path)'
    assert 'month' not in df.columns, 'month should be dropped (encoded in landing path)'
