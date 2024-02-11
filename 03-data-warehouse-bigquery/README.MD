The 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data 
Dataset: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Step 1 Import 2022 Green Taxi data into Google Cloud Storage

DATA LOADER and DATA EXPORTER are in the green_taxi_2022_to_gcs.py file, adding pickup_date and dropoff_date columns

### Step 2 Create external table in BigQuery
tip: unclick Use Cached Result option for a accurate result

```
CREATE OR REPLACE EXTERNAL TABLE cedar-style-412618.ny_taxi_green.external_table_2022
OPTIONS (
  FORMAT = 'Parquet',
  uris = ['gs://de-zoomcamp-xiangivyli/ny_green_taxi_data_2022.parquet']
)

-- create a materialised table
CREATE OR REPLACE TABLE cedar-style-412618.ny_taxi_green.non_partitioned_2022_taxi AS
SELECT * FROM `cedar-style-412618.ny_taxi_green.external_table_2022`;
```
Q1 Count
```
SELECT COUNT(*)
FROM cedar-style-412618.ny_taxi_green.external_table_2022;

-- 840402
```

Q2 Count the distinct number of PULocationID
```
--0 MB data
SELECT COUNT(DISTINCT PULocationID)
FROM cedar-style-412618.ny_taxi_green.external_table_2022;

--6.41MB data
SELECT COUNT(DISTINCT PULocationID)
FROM cedar-style-412618.ny_taxi_green.non_partitioned_2022_taxi;
```

Q3 Count fare_amount of 0
```
SELECT COUNT(fare_amount)
FROM cedar-style-412618.ny_taxi_green.non_partitioned_2022_taxi
WHERE fare_amount = 0;

--1622
```

Q4 Filter lpep_pickup_datetime first then order the results by PULocationID
```
-- Create a partitioned and clustered table
CREATE OR REPLACE TABLE cedar-style-412618.ny_taxi_green.partitioned_clustered_2022_taxi
PARTITION BY lpep_pickup_date
CLUSTER BY PULocationID AS 
SELECT * FROM cedar-style-412618.ny_taxi_green.external_table_2022

-- Filter pickup datetime between 01-01 and 03-31
-- Order by PULocationID

--126.93 MB data to process for non partitioned and clustered table
SELECT *
FROM cedar-style-412618.ny_taxi_green.non_partitioned_2022_taxi
WHERE lpep_pickup_date BETWEEN "2022-01-01" and "2022-03-31"
ORDER BY PULocationID DESC;
--31.82 MB data to process for partitioned and clustered table
SELECT *
FROM cedar-style-412618.ny_taxi_green.partitioned_clustered_2022_taxi
WHERE lpep_pickup_date BETWEEN "2022-01-01" and "2022-03-31"
ORDER BY PULocationID DESC;

Q5 

```
--12.82 MB data to process for non partitioned and clustered table
SELECT DISTINCT PULocationID
FROM cedar-style-412618.ny_taxi_green.non_partitioned_2022_taxi
WHERE lpep_pickup_date BETWEEN "2022-06-01" and "2022-06-30"
ORDER BY PULocationID DESC;

--1.12 MB
SELECT DISTINCT PULocationID
FROM cedar-style-412618.ny_taxi_green.partitioned_clustered_2022_taxi
WHERE lpep_pickup_date BETWEEN "2022-06-01" and "2022-06-30"
ORDER BY PULocationID DESC;
```

Q6 GCP Bucket
Q7 Depends?

