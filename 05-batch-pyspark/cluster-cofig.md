python 07.spark_cluster.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020


URL="spark://de-zoomcamp.europe-west2-c.c.cedar-style-412618.internal:7077"

spark-submit \
    --master="${URL}" \
    07.spark_cluster.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021


--input_green=gs://de-zoomcamp-xiangivyli/pq/green/2021/*/ \
--input_yellow=gs://de-zoomcamp-xiangivyli/pq/yellow/2021/*/ \
--output=gs://de-zoomcamp-xiangivyli/report-2021


gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west1 \
    gs://de-zoomcamp-xiangivyli/code/07.spark_cluster.py \
    -- \
    --input_green=gs://de-zoomcamp-xiangivyli/pq/green/2021/*/ \
    --input_yellow=gs://de-zoomcamp-xiangivyli/pq/yellow/2021/*/ \
    --output=gs://de-zoomcamp-xiangivyli/report-2021


5.6.4 Connecting Spark to Big Query

trips_data_all.reports-2020

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west1 \
    gs://de-zoomcamp-xiangivyli/code/07.spark_cluster_bigquery.py \
    -- \
    --input_green=gs://de-zoomcamp-xiangivyli/pq/green/2020/*/ \
    --input_yellow=gs://de-zoomcamp-xiangivyli/pq/yellow/2020/*/ \
    --output=trips_data_all.reports-2020