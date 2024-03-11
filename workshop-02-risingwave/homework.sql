-- q1, highest average trip time
CREATE MATERIALIZED VIEW mv_taxi_trip_time_stats AS
SELECT
    taxi_zone_pu.Zone as pickup_zone,
    taxi_zone_do.Zone as dropoff_zone,
    AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60) AS avg_trip_time_minutes,
    MIN(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60) AS min_trip_time_minutes,
    MAX(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60) AS max_trip_time_minutes
FROM
    trip_data
        JOIN taxi_zone as taxi_zone_pu
             ON trip_data.PULocationID = taxi_zone_pu.location_id
        JOIN taxi_zone as taxi_zone_do
             ON trip_data.DOLocationID = taxi_zone_do.location_id
GROUP BY taxi_zone_pu.Zone, taxi_zone_do.Zone 
ORDER BY
    avg_trip_time_minutes DESC;

-- the answer is Yorkville East to Steinway

--q2, find the number of trips for the pair of taxi zones with the highest average trip time
SELECT COUNT(*)
FROM mv_taxi_trip_time_stats
WHERE pickup_zone = 'Yorkville East'
AND dropoff_zone = 'Steinway';

-- the answer is 1


--q3, 17 hours before, top 3 busiest zones
WITH latest AS (
    SELECT MAX(tpep_pickup_datetime) AS latest_pickup_datetime
    FROM trip_data
)
SELECT 
    taxi_zone.Zone AS pickup_zone,
    COUNT(*) AS num_pickups
FROM 
    trip_data
JOIN 
    taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
JOIN 
    latest ON trip_data.tpep_pickup_datetime BETWEEN (latest.latest_pickup_datetime - INTERVAL '17 hour') AND latest.latest_pickup_datetime
GROUP BY 
    taxi_zone.Zone
ORDER BY 
    num_pickups DESC
LIMIT 3;
-- the answer is LaGuardia Airport|JFK Airport|Lincoln Square East







