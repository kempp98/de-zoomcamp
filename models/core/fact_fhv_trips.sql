{{ config(materialized='table') }}

WITH dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)
SELECT
    tripid,
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pickup_location,
    dropoff_location,
    SR_Flag
FROM {{ ref('staging_fhv_data') }} AS fhv
INNER JOIN dim_zones AS pickup
ON fhv.pickup_location = pickup.locationid
INNER JOIN dim_zones AS dropoff
ON fhv.dropoff_location = dropoff.locationid