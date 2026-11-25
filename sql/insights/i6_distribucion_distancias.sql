-- I6. ¿Los viajes se hicieron más largos o más cortos? Proporción de viajes por tramo de distancia (millas).
WITH p AS (SELECT * FROM analysis_parameters), banded AS (
    SELECT CASE WHEN trip_distance < 1 THEN '1. menos de 1'
                WHEN trip_distance < 3 THEN '2. 1 a 3'
                WHEN trip_distance < 7 THEN '3. 3 a 7'
                WHEN trip_distance < 15 THEN '4. 7 a 15'
                ELSE '5. 15 o más' END AS distance_band,
           strftime(pickup_date, '%Y-%m') AS month, fare_amount
    FROM fact_trips
)
SELECT distance_band,
       round(100.0 * count(*) FILTER (WHERE month = p.comparison_month)
             / sum(count(*) FILTER (WHERE month = p.comparison_month)) OVER (), 2) AS comparison_share_pct,
       round(100.0 * count(*) FILTER (WHERE month = p.target_month)
             / sum(count(*) FILTER (WHERE month = p.target_month)) OVER (), 2) AS target_share_pct,
       round(avg(fare_amount) FILTER (WHERE month = p.comparison_month), 2) AS comparison_average_fare,
       round(avg(fare_amount) FILTER (WHERE month = p.target_month), 2) AS target_average_fare
FROM banded, p
GROUP BY distance_band
ORDER BY distance_band;
