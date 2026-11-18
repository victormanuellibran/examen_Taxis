WITH p AS (SELECT target_month FROM analysis_parameters)
SELECT payment_type, count(*) AS trips,
       avg(CASE WHEN fare_amount > 0 THEN 100.0 * tip_amount / fare_amount END) AS average_tip_pct_fare
FROM fact_trips, p
WHERE strftime(pickup_date, '%Y-%m') = p.target_month
GROUP BY payment_type ORDER BY payment_type;
