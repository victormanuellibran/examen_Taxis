WITH p AS (SELECT * FROM analysis_parameters), summary AS (
    SELECT strftime(pickup_date, '%Y-%m') AS month, count(*) AS trips, avg(fare_amount) AS average_fare
    FROM fact_trips GROUP BY month
)
SELECT p.comparison_month, p.target_month,
       c.trips AS comparison_trips, t.trips AS target_trips,
       t.trips - c.trips AS absolute_trip_variation,
       round(100.0 * (t.trips - c.trips) / nullif(c.trips, 0), 2) AS trip_variation_pct,
       c.average_fare AS comparison_average_fare, t.average_fare AS target_average_fare,
       t.average_fare - c.average_fare AS average_fare_variation
FROM p JOIN summary c ON c.month = p.comparison_month JOIN summary t ON t.month = p.target_month;
