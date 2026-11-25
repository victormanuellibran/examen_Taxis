-- I2. ¿Cómo cambió la forma de pago? Respalda la hipótesis de la Q6 (crecimiento de Flex Fare, tipo 0).
-- payment_type según el diccionario TLC: 0 Flex Fare, 1 tarjeta, 2 efectivo, 3 sin cargo, 4 disputa, 5 desconocido.
WITH p AS (SELECT * FROM analysis_parameters), counts AS (
    SELECT payment_type,
           count(*) FILTER (WHERE strftime(pickup_date, '%Y-%m') = p.comparison_month) AS comparison_trips,
           count(*) FILTER (WHERE strftime(pickup_date, '%Y-%m') = p.target_month) AS target_trips
    FROM fact_trips, p
    GROUP BY payment_type
)
SELECT payment_type,
       CASE payment_type WHEN 0 THEN 'Flex Fare' WHEN 1 THEN 'Tarjeta' WHEN 2 THEN 'Efectivo'
            WHEN 3 THEN 'Sin cargo' WHEN 4 THEN 'Disputa' WHEN 5 THEN 'Desconocido' ELSE 'Otro' END AS payment_name,
       comparison_trips, target_trips,
       round(100.0 * comparison_trips / sum(comparison_trips) OVER (), 2) AS comparison_share_pct,
       round(100.0 * target_trips / sum(target_trips) OVER (), 2) AS target_share_pct,
       round(100.0 * target_trips / sum(target_trips) OVER ()
             - 100.0 * comparison_trips / sum(comparison_trips) OVER (), 2) AS share_change_pp
FROM counts
ORDER BY payment_type;
