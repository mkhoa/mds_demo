import pandas as pd
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Load Overture Maps base (land features) for Vietnam from BigQuery public dataset.
    Uses ST_INTERSECTS with the Vietnam country boundary for spatial filtering.
    Geometry is serialised as WKT string for parquet storage.
    """
    from mage_ai.io.bigquery import BigQuery
    from mage_ai.io.config import ConfigFileLoader
    from mage_ai.data_preparation.repo_manager import get_repo_path

    query = """
WITH Vietnam_Boundary AS (
    SELECT geometry AS geom
    FROM `bigquery-public-data.overture_maps.division_area`
    WHERE country = 'VN' AND subtype = 'country'
)
SELECT
    l.id,
    l.subtype,
    l.class,
    l.names.primary                     AS land_name,
    ST_Y(ST_CENTROID(l.geometry))       AS latitude,
    ST_X(ST_CENTROID(l.geometry))       AS longitude,
    ST_ASTEXT(l.geometry)               AS geometry_wkt
FROM `bigquery-public-data.overture_maps.land` AS l
INNER JOIN Vietnam_Boundary AS vn ON ST_INTERSECTS(l.geometry, vn.geom)
"""

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    df = BigQuery.with_config(ConfigFileLoader(config_path, 'default')).load(query)
    print(f"Loaded {len(df):,} base land features for Vietnam.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'DataFrame is None'
    assert len(df) > 0, 'No features returned — check BigQuery credentials and query'
    required = {'id', 'subtype', 'latitude', 'longitude', 'geometry_wkt'}
    assert required.issubset(df.columns), f'Missing columns: {required - set(df.columns)}'
    assert df['id'].notna().all(), 'Null ids found in base features'
    assert df['geometry_wkt'].notna().all(), 'Null geometry_wkt values found'
