{{ config(materialized='view') }}

SELECT
     {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropoff_datetime AS timestamp) AS dropoff_datetime,
    CAST(PULocationID AS numeric) AS pickup_location,
    CAST(DOLocationID AS numeric) AS dropoff_location,
    SR_Flag
FROM {{ source('staging', 'fhv_data_v2')}}

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}