{{ config(materialized='view') }}

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
), zones as (
    select * from {{ ref('stg_taxi_zones') }}
)

select
    a.*,
    date_diff('second', a.tpep_pickup_datetime, a.tpep_dropoff_datetime) / 60.0 as duration_minutes,
    case
        when a.trip_distance > 0
         and date_diff('second', a.tpep_pickup_datetime, a.tpep_dropoff_datetime) > 0
        then a.trip_distance / (date_diff('second', a.tpep_pickup_datetime, a.tpep_dropoff_datetime) / 3600.0)
    end as average_speed_mph,
    cast(a.tpep_pickup_datetime as date) as pickup_date,
    hour(a.tpep_pickup_datetime) as pickup_hour,
    dayofweek(a.tpep_pickup_datetime) as pickup_day_of_week,
    case when a.fare_amount > 0 then 100.0 * a.tip_amount / a.fare_amount end as tip_pct_fare,
    pu.borough as pickup_borough,
    pu.zone_name as pickup_zone,
    pu.service_zone as pickup_service_zone,
    doo.borough as dropoff_borough,
    doo.zone_name as dropoff_zone,
    doo.service_zone as dropoff_service_zone
from trips a
left join zones pu on pu.zone_id = a.pu_location_id
left join zones doo on doo.zone_id = a.do_location_id
