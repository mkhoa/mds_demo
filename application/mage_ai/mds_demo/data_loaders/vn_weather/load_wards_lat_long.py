import os
import pandas as pd
import psycopg2

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_wards(*args, **kwargs) -> pd.DataFrame:
    """
    Load ward identifiers and centroid coordinates from stg.vn_gis_new_ward.
    Returns ma_xa, ten_xa, ma_tinh, ten_tinh, lat, long.
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
                lat,
                long
            FROM stg.vn_gis_new_ward
            WHERE lat IS NOT NULL AND long IS NOT NULL
            ORDER BY ma_xa
            """,
            conn,
        )
    finally:
        conn.close()

    print(f"Loaded {len(df):,} wards with coordinates.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'Output is None'
    assert len(df) > 0, 'No wards returned'
    assert {'ma_xa', 'lat', 'long'}.issubset(df.columns), 'Missing required columns'
