SELECT 
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') AS service_zone
FROM {{ ref('taxi_zone_lookup') }}

