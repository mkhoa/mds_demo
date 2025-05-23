from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(df, *args, **kwargs):
    (
        df.write
        .option('delimiter', ',')
        .option('header', 'True')
        .mode('overwrite')
        .csv('s3a://landing_area/test_data/202503/')
    )