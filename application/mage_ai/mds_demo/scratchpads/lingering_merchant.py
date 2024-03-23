"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""
import duckdb
import boto3
from botocore import UNSIGNED
from botocore.config import Config

#Make sure you provide / in the end
client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket = 'dataforgood-fb-data'
prefix = 'csv/month=2019-06/country=VNM/total_population'  
result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
for o in result.get('CommonPrefixes'):
    print(o.get('Prefix'))

