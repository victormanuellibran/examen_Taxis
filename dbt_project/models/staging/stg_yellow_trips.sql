{{ config(materialized='view') }}

with unioned as (
    select
        cast(vendor_id as bigint) as vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        cast(passenger_count as bigint) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        cast(ratecode_id as bigint) as ratecode_id,
        store_and_fwd_flag,
        cast(pu_location_id as bigint) as pu_location_id,
        cast(do_location_id as bigint) as do_location_id,
        cast(payment_type as bigint) as payment_type,
        cast(fare_amount as double) as fare_amount,
        cast(extra as double) as extra,
        cast(mta_tax as double) as mta_tax,
        cast(tip_amount as double) as tip_amount,
        cast(tolls_amount as double) as tolls_amount,
        cast(improvement_surcharge as double) as improvement_surcharge,
        cast(total_amount as double) as total_amount,
        cast(congestion_surcharge as double) as congestion_surcharge,
        cast(airport_fee as double) as airport_fee,
        cast(cbd_congestion_fee as double) as cbd_congestion_fee,
        source_file,
        warning_flags,
        '2024-06' as period_month
    from {{ source('taxi_external', 'accepted_2024') }}

    union all

    select
        cast(vendor_id as bigint),
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        cast(passenger_count as bigint),
        cast(trip_distance as double),
        cast(ratecode_id as bigint),
        store_and_fwd_flag,
        cast(pu_location_id as bigint),
        cast(do_location_id as bigint),
        cast(payment_type as bigint),
        cast(fare_amount as double),
        cast(extra as double),
        cast(mta_tax as double),
        cast(tip_amount as double),
        cast(tolls_amount as double),
        cast(improvement_surcharge as double),
        cast(total_amount as double),
        cast(congestion_surcharge as double),
        cast(airport_fee as double),
        cast(cbd_congestion_fee as double),
        source_file,
        warning_flags,
        '2025-06' as period_month
    from {{ source('taxi_external', 'accepted_2025') }}
)

select * from unioned
