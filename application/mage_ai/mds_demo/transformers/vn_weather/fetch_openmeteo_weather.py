import time
from datetime import datetime, timedelta, timezone

import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

DAILY_VARIABLES = [
    # weather classification
    "weather_code",
    # temperature — growing degree days, heat/frost stress
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    # precipitation — irrigation scheduling
    "precipitation_sum",
    "rain_sum",
    # solar radiation — photosynthesis, crop growth
    "sunshine_duration",
    "shortwave_radiation_sum",
    # wind — surface evaporation
    "wind_speed_10m_max",
    # evapotranspiration — core input for irrigation models (FAO-56)
    "et0_fao_evapotranspiration",
]

BATCH_SIZE = 20       # wards per API request
MIN_REQUEST_INTERVAL = 30  # seconds between requests


def _build_client() -> openmeteo_requests.Client:
    cache_session = requests_cache.CachedSession('.openmeteo_cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.5)
    return openmeteo_requests.Client(session=retry_session)


def _parse_batch(responses: list, ward_batch: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for response, (_, ward) in zip(responses, ward_batch.iterrows()):
        daily = response.Daily()
        dates = pd.date_range(
            start=pd.Timestamp(daily.Time(), unit='s', tz='UTC'),
            end=pd.Timestamp(daily.TimeEnd(), unit='s', tz='UTC'),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive='left',
        ).date

        data = {'date': dates, 'ma_xa': ward['ma_xa'], 'ten_xa': ward['ten_xa'],
                'ma_tinh': ward['ma_tinh'], 'ten_tinh': ward['ten_tinh'],
                'lat': ward['lat'], 'long': ward['long']}

        for i, var in enumerate(DAILY_VARIABLES):
            values = daily.Variables(i).ValuesAsNumpy()
            data[var] = values

        frames.append(pd.DataFrame(data))

    return pd.concat(frames, ignore_index=True)


@transformer
def fetch_weather(wards: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Fetch historical daily weather for every ward from Open-Meteo Archive API.

    Pipeline variables:
        historical_days : int — number of past days to extract (default 365)
    """
    historical_days = int(kwargs.get('historical_days', 365))

    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = end_date - timedelta(days=historical_days - 1)

    print(f"Date range: {start_date} → {end_date} ({historical_days} days)")
    print(f"Fetching weather for {len(wards):,} wards in batches of {BATCH_SIZE}…")

    client = _build_client()
    all_frames = []
    last_request_time = 0.0

    for batch_start in range(0, len(wards), BATCH_SIZE):
        batch = wards.iloc[batch_start: batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(wards) + BATCH_SIZE - 1) // BATCH_SIZE

        elapsed = time.monotonic() - last_request_time
        wait = MIN_REQUEST_INTERVAL - elapsed
        if wait > 0:
            print(f"  Rate limit: waiting {wait:.1f}s…")
            time.sleep(wait)

        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} wards)…")

        params = {
            "latitude": batch['lat'].tolist(),
            "longitude": batch['long'].tolist(),
            "start_date": str(start_date),
            "end_date": str(end_date),
            "daily": DAILY_VARIABLES,
            "timezone": "Asia/Ho_Chi_Minh",
        }

        last_request_time = time.monotonic()
        responses = client.weather_api(ARCHIVE_URL, params=params)
        all_frames.append(_parse_batch(responses, batch))

    df = pd.concat(all_frames, ignore_index=True)
    print(f"Fetched {len(df):,} daily rows across {len(wards):,} wards.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'Output is None'
    assert len(df) > 0, 'No weather rows returned'
    assert 'ma_xa' in df.columns, 'Missing ma_xa column'
    assert 'date' in df.columns, 'Missing date column'
    assert 'temperature_2m_max' in df.columns, 'Missing temperature column'
