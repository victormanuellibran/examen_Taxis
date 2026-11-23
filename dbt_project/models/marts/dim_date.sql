{{ config(materialized='table') }}

select distinct
    date_key,
    pickup_date as date,
    year(pickup_date) as year,
    month(pickup_date) as month,
    day(pickup_date) as day,
    dayofweek(pickup_date) as day_of_week
from {{ ref('fact_trips') }}
where pickup_date is not null
