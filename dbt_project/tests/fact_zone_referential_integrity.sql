select f.pickup_zone_id, f.dropoff_zone_id
from {{ ref('fact_trips') }} f
left join {{ ref('dim_zone') }} pu on pu.zone_id = f.pickup_zone_id
left join {{ ref('dim_zone') }} doo on doo.zone_id = f.dropoff_zone_id
where pu.zone_id is null or doo.zone_id is null
