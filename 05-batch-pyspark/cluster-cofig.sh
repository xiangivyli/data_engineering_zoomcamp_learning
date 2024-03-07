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