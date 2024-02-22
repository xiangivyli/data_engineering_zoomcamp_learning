with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata_2019') }}
    where pickup_locationid IS NOT NULL
    and dropoff_locationid IS NOT NULL
),
dim_zones as (
    select * from {{ ref('dim_zone') }}
    where borough != 'Unknown'
)
select fhv_tripdata.dispatching_base_num, 
    fhv_tripdata.affiliated_base_num, 
    fhv_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.pickup_datetime as pickup_datetime,
    fhv_tripdata.dropoff_datetime as dropOff_datetime
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
