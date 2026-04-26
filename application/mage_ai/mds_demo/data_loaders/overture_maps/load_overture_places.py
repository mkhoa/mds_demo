import pandas as pd
from google.cloud import bigquery

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Load Overture Maps places (POIs) for Vietnam from BigQuery public dataset.
    Uses ST_INTERSECTS with the Vietnam country boundary for spatial filtering.
    Geometry is serialised as WKT string for parquet storage.
    Auth via GOOGLE_APPLICATION_CREDENTIALS env var (service account JSON mounted in container).
    """
    query = """
WITH Vietnam_Boundary AS (
    SELECT geometry AS geom
    FROM `bigquery-public-data.overture_maps.division_area`
    WHERE country = 'VN' AND subtype = 'country'
)
SELECT
    p.id,
    p.names.primary                                     AS place_name,
    p.categories.primary                                AS primary_category,
    p.addresses.list[SAFE_OFFSET(0)].element.freeform   AS address,
    p.phones.list[SAFE_OFFSET(0)].element               AS phone,
    p.socials.list[SAFE_OFFSET(0)].element              AS social_link,
    ST_Y(p.geometry)                                    AS latitude,
    ST_X(p.geometry)                                    AS longitude,
    ST_ASTEXT(p.geometry)                               AS geometry_wkt
FROM `bigquery-public-data.overture_maps.place` AS p
INNER JOIN Vietnam_Boundary AS vn ON ST_INTERSECTS(p.geometry, vn.geom)
WHERE p.names.primary IS NOT NULL
"""

    client = bigquery.Client()
    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df):,} places for Vietnam.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'DataFrame is None'
    assert len(df) > 0, 'No places returned — check BigQuery credentials and query'
    required = {'id', 'place_name', 'latitude', 'longitude', 'geometry_wkt'}
    assert required.issubset(df.columns), f'Missing columns: {required - set(df.columns)}'
    assert df['id'].notna().all(), 'Null ids found in places'
    assert df['latitude'].notna().all(), 'Null latitude values found'
    assert df['longitude'].notna().all(), 'Null longitude values found'
