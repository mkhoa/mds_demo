import os
import pandas as pd
from sqlalchemy import create_engine, text

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_to_postgres(df: pd.DataFrame, *args, **kwargs) -> None:
    # POSTGRES_HOST in the magic container env is set to 'localhost' (incorrect for
    # inter-container communication); use the Compose service name directly.
    host = 'warehouse_db'
    port = int(os.getenv('POSTGRES_PORT', '5432'))
    dbname = os.getenv('POSTGRES_DATABASE', 'warehouse')
    user = os.getenv('POSTGRES_USER', 'warehouse')
    password = os.getenv('POSTGRES_PASSWORD', 'warehouse')

    schema = kwargs.get('target_schema', 'raw')
    table = kwargs.get('target_table', 'ecommerce_orders')

    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(conn_str)

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))

    print(f"Writing {len(df):,} rows to {schema}.{table}")
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists='replace',
        index=False,
        chunksize=5000,
    )
    print(f"Done — {schema}.{table} loaded.")


@test
def test_output(*args, **kwargs) -> None:
    pass
