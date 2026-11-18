WITH p AS (SELECT target_month, comparison_month FROM analysis_parameters)
SELECT strftime(f.pickup_date, '%Y-%m') AS month, count(*) AS trips,
       avg(f.fare_amount) AS average_fare
FROM fact_trips f, p
WHERE strftime(f.pickup_date, '%Y-%m') IN (p.target_month, p.comparison_month)
  AND (lower(coalesce(f.pickup_zone, '')) LIKE '%jfk%'
       OR lower(coalesce(f.dropoff_zone, '')) LIKE '%jfk%'
       OR lower(coalesce(f.pickup_zone, '')) LIKE '%la guardia%'
       OR lower(coalesce(f.dropoff_zone, '')) LIKE '%la guardia%')
GROUP BY month ORDER BY month;
