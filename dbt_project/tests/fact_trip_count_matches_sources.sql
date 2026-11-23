with expected as (
    select count(*) as row_count from {{ source('taxi_external', 'accepted_comparison') }}
    union all
    select count(*) as row_count from {{ source('taxi_external', 'accepted_target') }}
), totals as (
    select sum(row_count) as expected_count from expected
), actual as (
    select count(*) as actual_count from {{ ref('fact_trips') }}
)
select *
from actual cross join totals
where actual_count <> expected_count
