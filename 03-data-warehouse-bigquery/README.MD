The 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data 
Dataset: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Step 1 Import 2022 Green Taxi data into Google Cloud Storage

DATA LOADER and DATA EXPORTER are in the green_taxi_2022_to_gcs.py file, adding pickup_date and dropoff_date columns

### Step 2 Create external table in BigQuery
tip: unclick Use Cached Result option for a accurate result
