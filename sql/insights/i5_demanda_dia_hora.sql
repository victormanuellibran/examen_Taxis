-- I5. Mapa de calor de demanda del mes asignado: viajes promedio por día de la semana y hora.
-- Se promedia por fecha, porque un mes puede tener 4 o 5 lunes y si se sumara se sesgaría el día que se repite.
WITH p AS (SELECT * FROM analysis_parameters), hourly AS (
    SELECT pickup_date, pickup_hour, count(*) AS trips
    FROM fact_trips, p
    WHERE strftime(pickup_date, '%Y-%m') = p.target_month
    GROUP BY pickup_date, pickup_hour
)
SELECT isodow(pickup_date) AS iso_day,
       CASE isodow(pickup_date) WHEN 1 THEN 'Lunes' WHEN 2 THEN 'Martes' WHEN 3 THEN 'Miércoles'
            WHEN 4 THEN 'Jueves' WHEN 5 THEN 'Viernes' WHEN 6 THEN 'Sábado' ELSE 'Domingo' END AS day_name,
       pickup_hour,
       round(avg(trips), 1) AS average_trips
FROM hourly
GROUP BY iso_day, day_name, pickup_hour
ORDER BY iso_day, pickup_hour;
