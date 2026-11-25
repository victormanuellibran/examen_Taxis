-- I4. Las 5 zonas de origen que más viajes ganaron y las que más perdieron (hasta 5, sólo si bajaron).
-- Mínimo de 1.000 viajes en el mes de comparación, para que el ranking no lo dominen zonas chicas.
-- Se excluyen las zonas 264/265 (borough Unknown / N/A): no son lugares reales sino códigos de "desconocido".
WITH p AS (SELECT * FROM analysis_parameters), z AS (
    SELECT pickup_zone, pickup_borough,
           count(*) FILTER (WHERE strftime(pickup_date, '%Y-%m') = p.comparison_month) AS comparison_trips,
           count(*) FILTER (WHERE strftime(pickup_date, '%Y-%m') = p.target_month) AS target_trips
    FROM fact_trips, p
    WHERE pickup_borough NOT IN ('Unknown', 'N/A')
    GROUP BY pickup_zone, pickup_borough
), ranked AS (
    SELECT *, target_trips - comparison_trips AS trip_change,
           round(100.0 * (target_trips - comparison_trips) / nullif(comparison_trips, 0), 2) AS variation_pct,
           row_number() OVER (ORDER BY target_trips - comparison_trips DESC) AS up_rank,
           row_number() OVER (ORDER BY target_trips - comparison_trips ASC) AS down_rank,
           count(*) FILTER (WHERE target_trips < comparison_trips) OVER () AS zones_that_fell,
           count(*) OVER () AS zones_compared
    FROM z
    WHERE comparison_trips >= 1000
)
SELECT CASE WHEN trip_change >= 0 THEN 'ganadora' ELSE 'perdedora' END AS grupo,
       pickup_zone, pickup_borough, comparison_trips, target_trips, trip_change, variation_pct,
       zones_that_fell, zones_compared
FROM ranked
WHERE up_rank <= 5 OR (down_rank <= 5 AND trip_change < 0)
ORDER BY trip_change DESC;
