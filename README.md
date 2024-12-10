# ap-datafeed-ingestor-mini

## Ingestor Mini

The next step in data ingestion at APA4B

## DAGs

Our ingestors are run in airflow, as seen [here](https://airflow.hogwarts.u.azure.chimera.cyber.gc.ca/home?search=APA4B). You'll notice our dags are named `ap_df_<feed>`, please keep to this standard.

### Testing DAGs

There's no need to write a separate DAG for testing out a feed. Just make sure to have emails turned off, or turned on for just your email. In doing this we maintain the ingestion history in one task, and we don't have the potential to leave test DAGs just floating around unused.

An example ingestor task:

```python
from daggers.operators import SpellbookSparkLabsOperator

SpellbookSparkLabsOperator(
    verbose=True,
    task_id="ingestor",
    execution_timeout=timedelta(hours=24),
    dag=dag,
    namespace="spark-datafeeds",
    application="local:///home/prime/main.py",
    image="uchimera.azurecr.io/cccs/ap/ingestion/datafeeds-mini",
    tag="<your feed git branch>", # CHANGE
    name="apa4b-ingest-mini-<feed>", # k8s pod name: (no underscores permitted) # CHANGE
    application_args=[
        "--azure_container_input_dir", "<feed>", # CHANGE
        "--timeperiod_to_process", "{{ macros.datetime.strptime(params.beg_time, '%Y-%m-%d').strftime('%Y/%m/%d') if params.beg_time is not none else logical_date.strftime('%Y/%m/%d') }}",
        "--file_type", "<file type>",  # CHANGE
        
        # Iceberg
        "--iceberg_catalog", "hogwarts_u",
        "--iceberg_namespace", "test", 
        "--iceberg_table", "<feed>'", # CHANGE
        "--iceberg_partition_format", "yyyy/MM/dd", # CHANGE

        # Add more based on the datafeed
    ],
    conf={}, # CHANGE (optional)
    group_secrets={ # env var
        'APA4B-sg': [
            'apdatalakeudatafeeds', 
        ]
    },
)
```

## Development

Prior to development, please run the following:

```bash
python -m pip install -r requirements.txt 
python -m pip install -r requirements-dev.txt
```

After, execute the datafeed (with an example):
```bash
python3 main.py \
  --azure_container_input_dir <azure container input directory>\
  --timeperiod_to_process "<time period to process>" \
  --file_type <file type> \
  --iceberg_partition_format <iceberg partition format> \
  --iceberg_catalog <destination catalog> \
  --iceberg_namespace <destination namespace>\
  --iceberg_table <destination iceberg table>
```
```bash
python3 main.py \
  --azure_container_input_dir shadowserver \
  --timeperiod_to_process "2024/11/23" \
  --file_type json \
  --iceberg_partition_format yyyy/MM/dd \
  --iceberg_catalog hogwarts_u \
  --iceberg_namespace test \
  --iceberg_table shadowserver \
    # the following are exclusive to shadowserver:   
  --json_multiline \
  --iceberg_partition_field timeperiod_loaded_by
```