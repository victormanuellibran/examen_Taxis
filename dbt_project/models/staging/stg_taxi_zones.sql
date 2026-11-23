{{ config(materialized='view') }}

select
    cast("LocationID" as bigint) as zone_id,
    cast("Borough" as varchar) as borough,
    cast("Zone" as varchar) as zone_name,
    cast(service_zone as varchar) as service_zone
from {{ source('taxi_external', 'taxi_zone_lookup') }}
