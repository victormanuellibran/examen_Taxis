WITH p AS (SELECT target_month FROM analysis_parameters)
SELECT pickup_hour, count(*) AS trips, sum(total_amount) AS total_revenue,
       avg(total_amount) AS average_revenue_per_trip
FROM fact_trips, p
WHERE strftime(pickup_date, '%Y-%m') = p.target_month
GROUP BY pickup_hour
ORDER BY average_revenue_per_trip DESC
LIMIT 1;
