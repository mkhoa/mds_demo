"""
Load ERA5-Land daily weather for Vietnamese localities via BigQuery + Earth Engine.

Source datasets:
  - bigquery-public-data.overture_maps.division_area  (VN locality polygons)
  - ECMWF/ERA5_LAND/DAILY_AGGR via BigQuery ST_REGIONSTATS  (weather stats)

Output: one row per (locality, observation_date) with temperature, precipitation,
        wind speed, and soil moisture aggregated over each locality polygon.

Auth: GOOGLE_APPLICATION_CREDENTIALS — picked up automatically by the BigQuery client
      via Application Default Credentials. The service account must have BigQuery
      Job User + Earth Engine access on project GEE_PROJECT_ID.

Incremental: checks stg.era5_weather__locality for the latest observation_date
             and resumes from the next day. Falls back to start_date on first run.
"""

import os
from datetime import date, timedelta

import pandas as pd
import psycopg2
from google.cloud import bigquery

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


BQ_PROJECT = os.getenv('GEE_PROJECT_ID', 'powerbi-1498789217796')
ERA5_LAG   = 5   # ERA5-Land daily data available with ~5-day lag


ERA5_QUERY = """\
WITH DateSeries AS (
  SELECT date_val
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('{start_date}'), DATE('{end_date}'))) AS date_val
),
TargetGeometry AS (
  SELECT
    id                     AS location_id,
    names.primary          AS location_name,
    subtype,
    ST_SIMPLIFY(geometry, 1000) AS geom
  FROM `bigquery-public-data.overture_maps.division_area`
  WHERE country = 'VN'
    AND subtype = 'locality'
),
AggregatedStats AS (
  SELECT
    tg.location_id,
    tg.location_name,
    tg.subtype,
    ds.date_val AS observation_date,
    CONCAT('ee://ECMWF/ERA5_LAND/DAILY_AGGR/', FORMAT_DATE('%Y%m%d', ds.date_val)) AS asset_path,
    tg.geom
  FROM TargetGeometry tg
  CROSS JOIN DateSeries ds
)

SELECT
  location_id,
  location_name,
  subtype,
  observation_date,
  -- Temperature (K → °C)
  ST_REGIONSTATS(geom, asset_path, 'temperature_2m').mean     - 273.15 AS avg_temp_c,
  ST_REGIONSTATS(geom, asset_path, 'temperature_2m_max').mean - 273.15 AS max_temp_c,
  -- Precipitation (m → mm)
  ST_REGIONSTATS(geom, asset_path, 'total_precipitation_sum').mean * 1000 AS precip_mm,
  -- Wind speed (m/s magnitude from u/v components)
  SQRT(
    POW(ST_REGIONSTATS(geom, asset_path, 'u_component_of_wind_10m').mean, 2) +
    POW(ST_REGIONSTATS(geom, asset_path, 'v_component_of_wind_10m').mean, 2)
  ) AS wind_speed_ms,
  -- Soil moisture (volumetric, m³/m³)
  ST_REGIONSTATS(geom, asset_path, 'volumetric_soil_water_layer_1').mean AS soil_moisture
FROM AggregatedStats
WHERE observation_date <= CURRENT_DATE()
ORDER BY observation_date DESC, location_name ASC
"""


def _get_watermark() -> date | None:
    """Return the latest observation_date already in stg.era5_weather__locality, or None."""
    try:
        conn = psycopg2.connect(
            host    = os.getenv('WAREHOUSE_DB_HOST',   'warehouse_db'),
            port    = int(os.getenv('WAREHOUSE_DB_PORT', 5432)),
            dbname  = os.getenv('WAREHOUSE_DB_DBNAME', 'warehouse'),
            user    = os.getenv('WAREHOUSE_DB_USER',   'warehouse'),
            password= os.getenv('WAREHOUSE_DB_PASS',   'warehouse'),
        )
        cur = conn.cursor()
        cur.execute("SELECT MAX(observation_date) FROM stg.era5_weather__locality")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except psycopg2.errors.UndefinedTable:
        return None
    finally:
        conn.close()


@data_loader
def load_era5_from_bq(*args, **kwargs) -> pd.DataFrame:
    """
    Pipeline variables:
        start_date : YYYY-MM-DD  full-load start (default '2020-01-01')
        end_date   : YYYY-MM-DD  inclusive end   (default: today − 5 days)
        max_days   : int         safety cap per run (default 30)
    """
    default_end = (date.today() - timedelta(days=ERA5_LAG)).isoformat()

    start_date = kwargs.get('start_date') or '2020-01-01'
    end_date   = kwargs.get('end_date')   or default_end
    max_days   = int(kwargs.get('max_days') or 30)

    watermark = _get_watermark()
    if watermark is not None:
        effective_start = (watermark + timedelta(days=1)).isoformat()
        print(f"Incremental load from {effective_start} to {end_date}  (watermark: {watermark})")
    else:
        effective_start = start_date
        print(f"Full load from {effective_start} to {end_date}")

    start_d = date.fromisoformat(effective_start)
    end_d   = date.fromisoformat(end_date)

    if start_d > end_d:
        print("No new dates to process.")
        return pd.DataFrame()

    # Cap per-run range to avoid BigQuery timeout on large date spans
    capped_end = min(end_d, start_d + timedelta(days=max_days - 1))
    if capped_end < end_d:
        print(f"Capped to {max_days} days: {effective_start} → {capped_end.isoformat()}")

    query = ERA5_QUERY.format(start_date=effective_start, end_date=capped_end.isoformat())

    client = bigquery.Client(project=BQ_PROJECT)
    print(f"Running BigQuery ERA5 query [{effective_start} → {capped_end.isoformat()}] …")

    df = client.query(query).to_dataframe()

    if not df.empty:
        # BQ DATE columns come back as db-dtypes `dbdate`, which pyarrow can't
        # serialize. Cast to standard datetime64 so Mage can cache the output.
        df['observation_date'] = pd.to_datetime(df['observation_date'])

    print(f"Loaded {len(df):,} rows  ({df['observation_date'].nunique() if not df.empty else 0} dates).")
    return df

