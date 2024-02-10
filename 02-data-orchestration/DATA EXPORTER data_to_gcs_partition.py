import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/cedar-style-412618-a8e6a69b58b2.json'

bucket_name = 'de-zoomcamp-xiangivyli'
project_id = 'cedar-style-412618'

table_name = 'green_taxi_data_quarter_2020'

root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data(data, *args, **kwargs):

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
