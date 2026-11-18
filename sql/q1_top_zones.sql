WITH p AS (SELECT * FROM analysis_parameters),
target AS (
    SELECT pickup_zone, count(*) AS target_trips
    FROM fact_trips, p WHERE strftime(pickup_date, '%Y-%m') = p.target_month
    GROUP BY pickup_zone
), comparison AS (
    SELECT pickup_zone, count(*) AS comparison_trips
    FROM fact_trips, p WHERE strftime(pickup_date, '%Y-%m') = p.comparison_month
    GROUP BY pickup_zone
)
SELECT t.pickup_zone, t.target_trips, coalesce(c.comparison_trips, 0) AS comparison_trips,
       round(100.0 * (t.target_trips - coalesce(c.comparison_trips, 0)) / nullif(c.comparison_trips, 0), 2) AS variation_pct
FROM target t LEFT JOIN comparison c USING (pickup_zone)
ORDER BY t.target_trips DESC LIMIT 10;
