with fact_total as (
    select count(*) as total from {{ ref('fact_trips') }}
), aggregate_total as (
    select sum(trips) as total from {{ ref('agg_zone_hour') }}
)
select f.total as fact_total, a.total as aggregate_total
from fact_total f cross join aggregate_total a
where f.total <> a.total
