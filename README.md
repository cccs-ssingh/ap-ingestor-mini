# ap-datafeed-ingestor-mini

## Ingestor Mini

The next step in data ingestion at APA4b

## Development

Prior to development, please run the following:

```bash
pip3 install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

To execute a datafeed (with an example):
```bash
python3 main.py \
  --azure_container_input_dir <azure container input directory>\
  --timeperiod_to_process <time period to process> \
  --file_type <file type> \
  --iceberg_partition_format <iceberg partition format> \
  --iceberg_catalog <destination catalog> \
  --iceberg_namespace <destination namespace>\
  --iceberg_table <destination iceberg table> \
```
```bash
python3 main.py \
  --azure_container_input_dir shadowserver \
  --timeperiod_to_process "2024/11/23" \
  --file_type json \
  --iceberg_partition_format yyyy/MM/dd \
  --iceberg_catalog hogwarts_u \
  --iceberg_namespace threat_feeds \
  --iceberg_table shadowserver \
    # the following are exclusive to shadowserver:   
  --json_multiline \
  --iceberg_partition_field timeperiod_loaded_by
```