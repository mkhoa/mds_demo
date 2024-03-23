import pandas as pd
import geopandas
import json
import h3
import h3pandas

from urllib.request import urlopen, urlretrieve
from pandas import DataFrame
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


resolution = 8
def polyfill_geo_json(geojson, res):
    """


    """
    out = h3.polyfill(geojson, res, True)
    
    return out

def create_country_h3():
    """


    """
    url = 'https://data.opendevelopmentmekong.net/dataset/55bdad36-c476-4be9-a52d-aa839534200a/resource/b8f60493-7564-4707-aa72-a0172ba795d8/download/vn_iso_province.geojson'
    jsonData = urlopen(url).read().decode('utf-8')
    data = geopandas.read_file(jsonData)
    data = data.explode(index_parts=True)
    gjson = data.to_json()
    jsonObject = json.loads(gjson)
    country_h3 = pd.DataFrame()
    for i in jsonObject['features']:
        r = pd.DataFrame(polyfill_geo_json(i['geometry'], resolution), columns=['h3_hex_id'])
        r['province'] = i['properties']['Name_EN']
        r['country'] = 'Vietnam'
        country_h3 = pd.concat([country_h3, r])
    country_h3['h3_resolution'] = resolution
    country_h3['h3_hex_centroid'] = country_h3.apply(lambda x: h3.h3_to_geo(x['h3_hex_id']), axis = 1)
    country_h3 = country_h3.set_index('h3_hex_id')

    return country_h3
    
@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """

    """
    country_h3 = create_country_h3()
    df = df.h3.geo_to_h3(resolution, lat_col = 'latitude', lng_col = 'longitude')
    df.index.names = ['h3_hex_id']
    df = df.merge(country_h3, how='inner', left_index=True, right_index=True)
    df = df.reset_index()

    return df, filename


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'