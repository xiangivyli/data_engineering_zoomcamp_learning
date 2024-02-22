with stg_fhv_tripdata as 
(
    select *
    from {{ source('staging','fhv_tripdata_2019')}}
)

select 
    -- indentifiers
    {{ dbt.safe_cast('dispatching_base_num', api.Column.translate_type("string")) }} as dispatching_base_num,
    {{ dbt.safe_cast('Affiliated_base_number', api.Column.translate_type("string")) }} as affiliated_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime

from stg_fhv_tripdata
where extract(year from pickup_datetime) = 2019


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}



