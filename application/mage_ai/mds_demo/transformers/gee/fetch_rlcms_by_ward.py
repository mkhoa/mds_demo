"""
Fetch SERVIR-Mekong RLCMS land-cover class area per Vietnamese ward.

Source:  projects/servir-mekong/rlcms/landcover — annual ImageCollection, 30 m.
Reducer: frequencyHistogram — pixel counts per land-cover class per ward polygon.
Output:  Tabular only — one row per (year, ward, class_code). No image or TIFF
         data is downloaded; all pixel computation happens server-side via
         reduceRegions().getInfo().
         area_km2 = pixel_count × (30 × 30 / 1_000_000).

RLCMS class codes for mainland SEA:
  1 = Forest          5 = Cropland
  2 = Shrubland       6 = Urban / Built-up
  3 = Grassland       7 = Barren
  4 = Wetland         8 = Open Water
  9 = Mangrove       10 = Plantation / Tree crop

Auth:    GOOGLE_APPLICATION_CREDENTIALS (Application Default Credentials).
         The service account must be granted Reader access on the asset.
Batching: one reduceRegions call per province per year.
"""

import os
import time
from datetime import date

import ee
import google.auth
import pandas as pd
import psycopg2

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


LC_COLLECTION  = 'projects/servir-mekong/rlcms/landcover'
LC_BAND        = None                    # auto-detected from first image
PIXEL_AREA_KM2 = 30 * 30 / 1_000_000   # 900 m² → 0.0009 km²
SCALE_M        = 30
RATE_LIMIT_S     = 0.5


def _init_ee() -> None:
    project = os.getenv('GEE_PROJECT_ID', 'powerbi-1498789217796')
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/earthengine',
                'https://www.googleapis.com/auth/cloud-platform'],
    )
    ee.Initialize(credentials, project=project)


def _get_watermark_year() -> int | None:
    """Return the latest year already in stg.gee__rlcms, or None for full load."""
    conn = psycopg2.connect(
        host=os.getenv('WAREHOUSE_DB_HOST', 'warehouse_db'),
        port=int(os.getenv('WAREHOUSE_DB_PORT', 5432)),
        dbname=os.getenv('WAREHOUSE_DB_DBNAME', 'warehouse'),
        user=os.getenv('WAREHOUSE_DB_USER', 'warehouse'),
        password=os.getenv('WAREHOUSE_DB_PASS', 'warehouse'),
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(year) FROM stg.gee__rlcms")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] else None
    except psycopg2.errors.UndefinedTable:
        return None
    finally:
        conn.close()


def _wards_to_fc(ward_rows: list[dict]) -> ee.FeatureCollection:
    features = [
        ee.Feature(
            ee.Geometry(json.loads(w['geometry_json'])),
            {'ma_xa': w['ma_xa'], 'ten_xa': w['ten_xa'],
             'ma_tinh': w['ma_tinh'], 'ten_tinh': w['ten_tinh']},
        )
        for w in ward_rows
    ]
    return ee.FeatureCollection(features)


def _histogram_to_rows(
    stats_info: dict,
    year: int,
    band: str,
) -> list[dict]:
    rows = []
    for feat in stats_info['features']:
        p = feat['properties']
        histogram: dict = p.get(band) or {}
        if not histogram:
            continue
        total_pixels = sum(histogram.values())
        for class_str, pixel_count in histogram.items():
            rows.append({
                'year':        year,
                'ma_xa':       p.get('ma_xa'),
                'ten_xa':      p.get('ten_xa'),
                'ma_tinh':     p.get('ma_tinh'),
                'ten_tinh':    p.get('ten_tinh'),
                'class_code':  int(class_str),
                'pixel_count': int(pixel_count),
                'area_km2':    round(int(pixel_count) * PIXEL_AREA_KM2, 6),
                'pct_area':    round(int(pixel_count) / total_pixels * 100, 4) if total_pixels else 0.0,
                'source':      LC_COLLECTION,

            })
    return rows


@transformer
def fetch_rlcms(ward_df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Pipeline variables:
        start_year : int  — used only on full load (e.g. 2015)
        end_year   : int  — inclusive; defaults to current year
    """
    _init_ee()

    current_year = date.today().year
    end_year   = int(kwargs.get('end_year')   or current_year)
    start_year = int(kwargs.get('start_year') or 2015)

    watermark = _get_watermark_year()
    if watermark is not None:
        effective_start = watermark   # re-process last year in case of re-runs
        print(f"Incremental load years {effective_start}–{end_year}  (watermark: {watermark})")
    else:
        effective_start = start_year
        print(f"Full load years {effective_start}–{end_year}")

    years = list(range(effective_start, end_year + 1))

    # Resolve available images from the ImageCollection
    collection = ee.ImageCollection(LC_COLLECTION)
    image_meta = collection.filter(ee.Filter.calendarRange(effective_start, end_year, 'year')).toList(50).getInfo()

    if not image_meta:
        print("No RLCMS images found for the requested year range.")
        return pd.DataFrame()

    sample_bands = image_meta[0].get('bands', [])
    band = LC_BAND or (sample_bands[0]['id'] if sample_bands else 'b1')
    print(f"Using classification band: '{band}'  ({len(image_meta)} images)")

    # Group wards by province for batched getInfo calls
    provinces: dict[str, list[dict]] = {}
    for _, row in ward_df.iterrows():
        provinces.setdefault(row['ma_tinh'], []).append(row.to_dict())

    all_rows = []
    for img_info in image_meta:
        image_id   = img_info['id']
        image_year = int(
            ee.Date(img_info['properties']['system:time_start'])
            .get('year').getInfo()
        )
        if image_year not in years:
            continue
        print(f"  Processing year {image_year}  ({image_id})")

        lc_image = ee.Image(image_id).select(band)

        for prov_code, prov_wards in provinces.items():
            fc = _wards_to_fc(prov_wards)
            try:
                stats = lc_image.reduceRegions(
                    collection=fc,
                    reducer=ee.Reducer.frequencyHistogram(),
                    scale=SCALE_M,
                    crs='EPSG:4326',
                )
                rows = _histogram_to_rows(stats.getInfo(), image_year, band)
                all_rows.extend(rows)
            except Exception as exc:
                print(f"    WARNING: province {prov_code} / year {image_year} failed: {exc}")
            time.sleep(RATE_LIMIT_S)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    print(f"Fetched {len(df):,} ward×class rows across {df['year'].nunique()} years.")
    return df


@test
def test_output(df: pd.DataFrame, *args) -> None:
    if df is None or df.empty:
        return
    required = {'year', 'ma_xa', 'class_code', 'pixel_count', 'area_km2', 'pct_area'}
    assert required.issubset(df.columns), f'Missing columns: {required - set(df.columns)}'
    assert (df['pct_area'] >= 0).all() and (df['pct_area'] <= 100).all(), 'pct_area out of range'
