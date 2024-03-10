## The purpose: prepare **for-hire vehicles (FHV)** dataset in the BigQuery.

## The requriements are:
1. keep records with pickup time in year 2019
2. join with dim_zones and keep records with known pickup and dropoff locations entries

## Steps
Step1: Use **Mage** to load dataset (it is prepared in the GitHub) in the Google Cloud Storage

Step2: Create `fhv_tripdata_2019` table in the BigQuery

Step3: stage models and core models to transform, aggregate data then import cleansed data in the BigQuery 


