"""
Fetch NDVI zonal statistics per Vietnamese ward from Google Earth Engine.

Source:  MODIS/061/MOD13Q1 — 16-day composites, 250 m resolution.
         NDVI band range: [-2000, 10000] (divide by 10000 → [-0.2, 1.0])
         SummaryQA: 0 = good quality pixels only.
Output:  Tabular only — zonal statistics (mean, min, max, p25, p75, count)
         per ward per composite date. No image or TIFF data is downloaded;
         all pixel computation happens server-side via reduceRegions().getInfo().
Auth:    GOOGLE_APPLICATION_CREDENTIALS (Application Default Credentials).
Batching: one reduceRegions call per province per image date.
"""

import os
import time
from datetime import date, timedelta

import ee
import google.auth
import pandas as pd
import psycopg2

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


MODIS_COLLECTION  = 'MODIS/061/MOD13Q1'
MODIS_SCALE_M     = 250          # native resolution
NDVI_SCALE_FACTOR = 0.0001       # raw int → float NDVI
RATE_LIMIT_S      = 0.5          # seconds between getInfo calls


def _init_ee() -> None:
    project = os.getenv('GEE_PROJECT_ID', 'powerbi-1498789217796')
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/earthengine',
                'https://www.googleapis.com/auth/cloud-platform'],
    )
    ee.Initialize(credentials, project=project)


def _get_watermark() -> date:
    """Return the latest image_date already in stg.gee__ndvi, or None for full load."""
    conn = psycopg2.connect(
        host=os.getenv('WAREHOUSE_DB_HOST', 'warehouse_db'),
        port=int(os.getenv('WAREHOUSE_DB_PORT', 5432)),
        dbname=os.getenv('WAREHOUSE_DB_DBNAME', 'warehouse'),
        user=os.getenv('WAREHOUSE_DB_USER', 'warehouse'),
        password=os.getenv('WAREHOUSE_DB_PASS', 'warehouse'),
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(image_date) FROM stg.gee__ndvi")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except psycopg2.errors.UndefinedTable:
        return None
    finally:
        conn.close()


def _wards_to_fc(ward_rows: list[dict]) -> ee.FeatureCollection:
    features = []
    for w in ward_rows:
        geom_dict = json.loads(w['geometry_json'])
        features.append(
            ee.Feature(
                ee.Geometry(geom_dict),
                {'ma_xa': w['ma_xa'], 'ten_xa': w['ten_xa'],
                 'ma_tinh': w['ma_tinh'], 'ten_tinh': w['ten_tinh']},
            )
        )
    return ee.FeatureCollection(features)


def _reduce_image_over_province(
    ndvi_image: ee.Image,
    image_date: str,
    province_fc: ee.FeatureCollection,
) -> list[dict]:
    reducer = (
        ee.Reducer.mean()
        .combine(ee.Reducer.min(),                  sharedInputs=True)
        .combine(ee.Reducer.max(),                  sharedInputs=True)
        .combine(ee.Reducer.percentile([25, 75]),   sharedInputs=True)
        .combine(ee.Reducer.count(),                sharedInputs=True)
    )

    stats = ndvi_image.reduceRegions(
        collection=province_fc,
        reducer=reducer,
        scale=MODIS_SCALE_M,
        crs='EPSG:4326',
    )

    rows = []
    for feat in stats.getInfo()['features']:
        p = feat['properties']
        ndvi_mean = p.get('mean')
        if ndvi_mean is None:           # ward outside image footprint
            continue
        rows.append({
            'image_date': image_date,
            'ma_xa':       p.get('ma_xa'),
            'ten_xa':      p.get('ten_xa'),
            'ma_tinh':     p.get('ma_tinh'),
            'ten_tinh':    p.get('ten_tinh'),
            'ndvi_mean':   round(ndvi_mean * NDVI_SCALE_FACTOR, 6),
            'ndvi_min':    round((p.get('min') or 0) * NDVI_SCALE_FACTOR, 6),
            'ndvi_max':    round((p.get('max') or 0) * NDVI_SCALE_FACTOR, 6),
            'ndvi_p25':    round((p.get('p25') or 0) * NDVI_SCALE_FACTOR, 6),
            'ndvi_p75':    round((p.get('p75') or 0) * NDVI_SCALE_FACTOR, 6),
            'pixel_count': int(p.get('count') or 0),
            'source':      MODIS_COLLECTION,
        })
    return rows


@transformer
def fetch_ndvi(ward_df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Pipeline variables:
        start_date : str  'YYYY-MM-DD'  — used only on full-refresh (no stg watermark)
        end_date   : str  'YYYY-MM-DD'  — inclusive end; defaults to today
    """
    _init_ee()

    end_str   = kwargs.get('end_date')   or date.today().isoformat()
    start_str = kwargs.get('start_date') or '2020-01-01'

    watermark = _get_watermark()
    if watermark is not None:
        # Re-read from the last composite date so late tiles are caught.
        # +1 day so MODIS filter is exclusive of the already-loaded date.
        effective_start = (watermark + timedelta(days=1)).isoformat()
        print(f"Incremental load from {effective_start} to {end_str}  (watermark: {watermark})")
    else:
        effective_start = start_str
        print(f"Full load from {effective_start} to {end_str}")

    # Fetch image list
    collection = (
        ee.ImageCollection(MODIS_COLLECTION)
        .filterDate(effective_start, end_str)
        .select(['NDVI', 'SummaryQA'])
    )
    image_list = collection.toList(collection.size()).getInfo()
    if not image_list:
        print("No new MODIS images in the requested date range.")
        return pd.DataFrame()

    print(f"Found {len(image_list)} MODIS composites to process.")

    # Group ward rows by province
    provinces: dict[str, list[dict]] = {}
    for _, row in ward_df.iterrows():
        provinces.setdefault(row['ma_tinh'], []).append(row.to_dict())

    all_rows = []
    for img_info in image_list:
        image_id   = img_info['id']
        image_date = ee.Date(img_info['properties']['system:time_start']).format('YYYY-MM-dd').getInfo()
        print(f"  Processing {image_date}  ({image_id})")

        image    = ee.Image(image_id)
        qa_mask  = image.select('SummaryQA').lte(1)   # 0=good, 1=marginal
        ndvi_img = image.select('NDVI').updateMask(qa_mask)

        for prov_code, prov_wards in provinces.items():
            fc = _wards_to_fc(prov_wards)
            try:
                rows = _reduce_image_over_province(ndvi_img, image_date, fc)
                all_rows.extend(rows)
            except Exception as exc:
                print(f"    WARNING: province {prov_code} failed for {image_date}: {exc}")
            time.sleep(RATE_LIMIT_S)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df['image_date'] = pd.to_datetime(df['image_date']).dt.date
    df['year']  = pd.to_datetime(df['image_date']).dt.year
    df['month'] = pd.to_datetime(df['image_date']).dt.month
    df['day']   = pd.to_datetime(df['image_date']).dt.day

    print(f"Fetched {len(df):,} ward×date rows across {df['image_date'].nunique()} dates.")
    return df


@test
def test_output(df: pd.DataFrame, *args) -> None:
    if df is None or df.empty:
        return     # empty is valid when no new dates exist
    required = {'image_date', 'ma_xa', 'ndvi_mean', 'year', 'month', 'day'}
    assert required.issubset(df.columns), f'Missing columns: {required - set(df.columns)}'
    assert df['ndvi_mean'].between(-0.3, 1.01).all(), 'NDVI values out of expected range'
