import pandas as pd
from google.cloud import bigquery

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Load Overture Maps division_area polygons for Vietnam from BigQuery public dataset.
    These polygons are the spatial bridge between era5 location_id and Vietnamese
    ma_xa (used downstream in dim_division_area to assign each division to a ward).
    Geometry is serialised as WKT for parquet storage.
    Auth via GOOGLE_APPLICATION_CREDENTIALS env var.
    """
    query = """
SELECT
    id,
    subtype,
    names.primary                 AS location_name,
    country,
    region,
    ST_Y(ST_CENTROID(geometry))   AS latitude,
    ST_X(ST_CENTROID(geometry))   AS longitude,
    ST_ASTEXT(geometry)           AS geometry_wkt
FROM `bigquery-public-data.overture_maps.division_area`
WHERE country = 'VN'
ORDER BY subtype
"""

    client = bigquery.Client()
    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df):,} division_area rows for Vietnam.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'DataFrame is None'
    assert len(df) > 0, 'No rows returned — check BigQuery credentials and query'
    required = {'id', 'subtype', 'location_name', 'country', 'latitude', 'longitude', 'geometry_wkt'}
    assert required.issubset(df.columns), f'Missing columns: {required - set(df.columns)}'
    assert df['id'].notna().all(), 'Null ids found'
    assert df['geometry_wkt'].notna().all(), 'Null geometry_wkt values found'
    assert (df['country'] == 'VN').all(), 'Non-VN rows leaked into output'
