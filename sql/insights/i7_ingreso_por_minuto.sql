-- I7. ¿Cuándo rinde más manejar? Ingreso por minuto de viaje según la hora, en el mes asignado.
-- Complementa la Q2: la Q2 mide ingreso por viaje; esta mide ingreso por minuto con pasajero.
-- Se excluyen duraciones < 1 o > 180 minutos para que relojes defectuosos no distorsionen el cociente.
WITH p AS (SELECT * FROM analysis_parameters)
SELECT pickup_hour,
       count(*) AS trips,
       round(avg(total_amount), 2) AS average_revenue_per_trip,
       round(avg(duration_minutes), 2) AS average_duration_minutes,
       round(sum(total_amount) / sum(duration_minutes), 3) AS revenue_per_minute
FROM fact_trips, p
WHERE strftime(pickup_date, '%Y-%m') = p.target_month
  AND duration_minutes BETWEEN 1 AND 180
GROUP BY pickup_hour
ORDER BY pickup_hour;
