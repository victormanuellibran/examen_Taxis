-- I1. ¿El peaje de congestión (CBD, desde enero 2025) aceleró el tránsito en Manhattan?
-- Velocidad media (mph) de viajes con origen y destino en Manhattan, por hora de subida.
-- Se excluyen velocidades < 0,5 o > 80 mph (GPS o relojes defectuosos) sólo para este promedio.
WITH p AS (SELECT * FROM analysis_parameters), base AS (
    SELECT pickup_hour, strftime(pickup_date, '%Y-%m') AS month, average_speed_mph
    FROM fact_trips
    WHERE pickup_borough = 'Manhattan' AND dropoff_borough = 'Manhattan'
      AND average_speed_mph BETWEEN 0.5 AND 80
)
SELECT pickup_hour,
       round(avg(average_speed_mph) FILTER (WHERE month = p.comparison_month), 2) AS comparison_mph,
       round(avg(average_speed_mph) FILTER (WHERE month = p.target_month), 2) AS target_mph,
       round(100.0 * (avg(average_speed_mph) FILTER (WHERE month = p.target_month)
             / avg(average_speed_mph) FILTER (WHERE month = p.comparison_month) - 1), 2) AS variation_pct
FROM base, p
GROUP BY pickup_hour
ORDER BY pickup_hour;
