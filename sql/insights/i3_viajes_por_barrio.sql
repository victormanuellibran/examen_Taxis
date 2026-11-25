-- I3. ¿Dónde creció la demanda? Viajes e ingreso por barrio (borough) de origen.
WITH p AS (SELECT * FROM analysis_parameters), agg AS (
    SELECT pickup_borough,
           count(*) FILTER (WHERE strftime(pickup_date, '%Y-%m') = p.comparison_month) AS comparison_trips,
           count(*) FILTER (WHERE strftime(pickup_date, '%Y-%m') = p.target_month) AS target_trips,
           sum(total_amount) FILTER (WHERE strftime(pickup_date, '%Y-%m') = p.comparison_month) AS comparison_revenue,
           sum(total_amount) FILTER (WHERE strftime(pickup_date, '%Y-%m') = p.target_month) AS target_revenue
    FROM fact_trips, p
    GROUP BY pickup_borough
)
SELECT pickup_borough, comparison_trips, target_trips,
       round(100.0 * (target_trips - comparison_trips) / nullif(comparison_trips, 0), 2) AS trip_variation_pct,
       round(100.0 * comparison_trips / sum(comparison_trips) OVER (), 2) AS comparison_share_pct,
       round(100.0 * target_trips / sum(target_trips) OVER (), 2) AS target_share_pct,
       round(comparison_revenue, 2) AS comparison_revenue,
       round(target_revenue, 2) AS target_revenue
FROM agg
ORDER BY target_trips DESC;
