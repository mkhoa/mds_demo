"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""
from lakehouse_engine.engine import load_data

acon = {
  "input_specs": [
    {
      "spec_id": "sales_source",
      "read_type": "batch",
      "data_format": "csv",
      "options": {
        "header": False,
        "delimiter": "|",
        "mode": "FAILFAST"
      },
      "location": "s3a://dwhfilesystem/test/"
    }],
  "transform_specs": [],
  "output_specs": [
    {
      "spec_id": "sales_bronze",
      "input_id": "sales_source",
      "write_type": "append",
      "data_format": "delta",
      "location": "s3a://dwhfilesystem/delta/"
    }
  ]
}

load_data(acon=acon)
