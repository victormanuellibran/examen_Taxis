{{ config(materialized='table') }}

select
    pickup_zone_id,
    pickup_date,
    pickup_hour,
    count(*) as trips,
    sum(total_amount) as total_revenue,
    avg(duration_minutes) as average_duration_minutes
from {{ ref('fact_trips') }}
group by pickup_zone_id, pickup_date, pickup_hour
