{{ config(materialized='table') }}

select
    -- Identificador incremental sin ordenar las 7,5 M filas en memoria.
    row_number() over () as trip_id,
    cast(strftime(pickup_date, '%Y%m%d') as integer) as date_key,
    period_month,
    pickup_date,
    pickup_hour,
    pu_location_id as pickup_zone_id,
    do_location_id as dropoff_zone_id,
    payment_type,
    passenger_count,
    trip_distance,
    duration_minutes,
    average_speed_mph,
    fare_amount,
    tip_amount,
    tip_pct_fare,
    total_amount,
    cbd_congestion_fee,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    source_file,
    warning_flags
from {{ ref('int_trips_enriched') }}
