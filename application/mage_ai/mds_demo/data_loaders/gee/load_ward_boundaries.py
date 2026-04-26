import json
import os

import pandas as pd
import psycopg2

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_ward_boundaries(*args, **kwargs) -> pd.DataFrame:
    """
    Load ward boundaries (GeoJSON polygons) from stg.vn_gis_new_ward.

    Returns one row per ward with columns:
        ma_xa, ten_xa, ma_tinh, ten_tinh, geometry_json
    """
    conn = psycopg2.connect(
        host=os.getenv('WAREHOUSE_DB_HOST', 'warehouse_db'),
        port=int(os.getenv('WAREHOUSE_DB_PORT', 5432)),
        dbname=os.getenv('WAREHOUSE_DB_DBNAME', 'warehouse'),
        user=os.getenv('WAREHOUSE_DB_USER', 'warehouse'),
        password=os.getenv('WAREHOUSE_DB_PASS', 'warehouse'),
    )
    try:
        df = pd.read_sql(
            """
            SELECT
                ma_xa,
                ten_xa,
                ma_tinh,
                ten_tinh,
                geometry_json
            FROM stg.vn_gis_new_ward
            WHERE geometry_json IS NOT NULL
              AND ma_xa IS NOT NULL
            ORDER BY ma_tinh, ma_xa
            """,
            conn,
        )
    finally:
        conn.close()

    print(f"Loaded {len(df):,} ward boundaries across {df['ma_tinh'].nunique()} provinces.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'Output is None'
    assert len(df) > 0, 'No wards returned'
    assert {'ma_xa', 'ma_tinh', 'geometry_json'}.issubset(df.columns), 'Missing required columns'
