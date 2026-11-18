WITH p AS (SELECT target_month FROM analysis_parameters), target AS (
    SELECT * FROM fact_trips, p WHERE strftime(pickup_date, '%Y-%m') = p.target_month
), zone_rates AS (
    SELECT pickup_zone,
           count(*) FILTER (WHERE coalesce(cbd_congestion_fee, 0) > 0) AS trips_with_fee,
           count(*) AS trips_in_zone,
           100.0 * count(*) FILTER (WHERE coalesce(cbd_congestion_fee, 0) > 0) / nullif(count(*), 0) AS pct_zone_with_cbd_fee
    FROM target
    GROUP BY pickup_zone ORDER BY pct_zone_with_cbd_fee DESC LIMIT 5
)
SELECT (SELECT count(*) FROM target) AS total_trips,
       (SELECT count(*) FROM target WHERE coalesce(cbd_congestion_fee, 0) > 0) AS trips_with_cbd_fee,
       100.0 * (SELECT count(*) FROM target WHERE coalesce(cbd_congestion_fee, 0) > 0) / nullif((SELECT count(*) FROM target), 0) AS pct_with_cbd_fee,
       z.pickup_zone, z.trips_with_fee, z.trips_in_zone, z.pct_zone_with_cbd_fee
FROM zone_rates z;
