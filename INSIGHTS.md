# Insights adicionales NYC Taxi

Motor: `python`; mes comparado: `2024-06`; mes asignado: `2025-06`.

Consultas extra sobre el mismo modelo estrella (no reemplazan a las seis de `RESULTADOS.md`).
La interpretación de cada una está en [`docs/ANALISIS_RESULTADOS.md`](docs/ANALISIS_RESULTADOS.md).

## I1. ¿El peaje de congestión (CBD, desde enero 2025) aceleró el tránsito en Manhattan?

Archivo: [`sql/insights/i1_velocidad_manhattan_por_hora.sql`](sql/insights/i1_velocidad_manhattan_por_hora.sql)

<details><summary>Consulta SQL</summary>

```sql
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
```

</details>

| pickup_hour | comparison_mph | target_mph | variation_pct |
| --- | --- | --- | --- |
| 0 | 11.3900 | 12.1700 | 6.8700 |
| 1 | 12.0100 | 12.6700 | 5.5000 |
| 2 | 12.6300 | 13.2700 | 5.0500 |
| 3 | 13.4200 | 14.0600 | 4.7800 |
| 4 | 14.5600 | 15.1100 | 3.7800 |
| 5 | 14.8700 | 15.6100 | 4.9800 |
| 6 | 13.7800 | 14.2500 | 3.4000 |
| 7 | 11.6300 | 11.9400 | 2.7000 |
| 8 | 9.7200 | 9.9900 | 2.7100 |
| 9 | 9.1800 | 9.2300 | 0.5500 |
| 10 | 8.8000 | 8.7800 | -0.1500 |
| 11 | 8.3200 | 8.3300 | 0.0900 |
| 12 | 8.2200 | 8.2100 | -0.1200 |
| 13 | 8.2000 | 8.1900 | -0.2000 |
| 14 | 8.1400 | 8.1800 | 0.5300 |
| 15 | 8.1200 | 8.1000 | -0.2400 |
| 16 | 8.2000 | 8.2500 | 0.6300 |
| 17 | 8.2900 | 8.3800 | 1.0600 |
| 18 | 8.6600 | 8.8000 | 1.6200 |
| 19 | 9.1900 | 9.4400 | 2.8000 |
| 20 | 9.8800 | 10.3400 | 4.7000 |
| 21 | 10.1800 | 10.7100 | 5.1900 |
| 22 | 10.5500 | 11.0800 | 5.0100 |
| 23 | 10.9700 | 11.7100 | 6.7200 |

## I2. ¿Cómo cambió la forma de pago? Respalda la hipótesis de la Q6 (crecimiento de Flex Fare, tipo 0).

Archivo: [`sql/insights/i2_mezcla_tipos_de_pago.sql`](sql/insights/i2_mezcla_tipos_de_pago.sql)

<details><summary>Consulta SQL</summary>

```sql
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
```

</details>

| payment_type | payment_name | comparison_trips | target_trips | comparison_share_pct | target_share_pct | share_change_pp |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | Flex Fare | 400318 | 1118536 | 11.6300 | 27.6000 | 15.9700 |
| 1 | Tarjeta | 2547999 | 2518105 | 74.0400 | 62.1400 | -11.9000 |
| 2 | Efectivo | 448842 | 352359 | 13.0400 | 8.7000 | -4.3500 |
| 3 | Sin cargo | 13090 | 14333 | 0.3800 | 0.3500 | -0.0300 |
| 4 | Disputa | 31216 | 48899 | 0.9100 | 1.2100 | 0.3000 |
| 5 | Desconocido | 0 | 1 | 0.0000 | 0.0000 | 0.0000 |

## I3. ¿Dónde creció la demanda? Viajes e ingreso por barrio (borough) de origen.

Archivo: [`sql/insights/i3_viajes_por_barrio.sql`](sql/insights/i3_viajes_por_barrio.sql)

<details><summary>Consulta SQL</summary>

```sql
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
```

</details>

| pickup_borough | comparison_trips | target_trips | trip_variation_pct | comparison_share_pct | target_share_pct | comparison_revenue | target_revenue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Manhattan | 3054777 | 3429127 | 12.2500 | 88.7600 | 84.6200 | 73225719.4700 | 82610591.0400 |
| Queens | 310422 | 404268 | 30.2300 | 9.0200 | 9.9800 | 22667647.2200 | 26227385.0400 |
| Brooklyn | 54309 | 177507 | 226.8500 | 1.5800 | 4.3800 | 1736100.1600 | 5025175.7500 |
| Bronx | 10343 | 33931 | 228.0600 | 0.3000 | 0.8400 | 362891.0800 | 1025203.9100 |
| Unknown | 10692 | 6054 | -43.3800 | 0.3100 | 0.1500 | 322758.2200 | 204251.7600 |
| N/A | 694 | 816 | 17.5800 | 0.0200 | 0.0200 | 61978.2500 | 69178.4500 |
| Staten Island | 129 | 441 | 241.8600 | 0.0000 | 0.0100 | 6027.2500 | 18836.5600 |
| EWR | 99 | 89 | -10.1000 | 0.0000 | 0.0000 | 10672.9500 | 8679.0400 |

## I4. Las 5 zonas de origen que más viajes ganaron y las que más perdieron (hasta 5, sólo si bajaron).

Archivo: [`sql/insights/i4_zonas_ganadoras_y_perdedoras.sql`](sql/insights/i4_zonas_ganadoras_y_perdedoras.sql)

<details><summary>Consulta SQL</summary>

```sql
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
```

</details>

| grupo | pickup_zone | pickup_borough | comparison_trips | target_trips | trip_change | variation_pct | zones_that_fell | zones_compared |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ganadora | East Village | Manhattan | 82549 | 105696 | 23147 | 28.0400 | 3 | 90 |
| ganadora | JFK Airport | Queens | 152171 | 170662 | 18491 | 12.1500 | 3 | 90 |
| ganadora | Upper East Side South | Manhattan | 156503 | 169820 | 13317 | 8.5100 | 3 | 90 |
| ganadora | Penn Station/Madison Sq West | Manhattan | 108741 | 121996 | 13255 | 12.1900 | 3 | 90 |
| ganadora | Lower East Side | Manhattan | 40155 | 52638 | 12483 | 31.0900 | 3 | 90 |
| perdedora | East Elmhurst | Queens | 13276 | 12111 | -1165 | -8.7800 | 3 | 90 |
| perdedora | Central Park | Manhattan | 55718 | 52600 | -3118 | -5.6000 | 3 | 90 |
| perdedora | LaGuardia Airport | Queens | 112984 | 108971 | -4013 | -3.5500 | 3 | 90 |

## I5. Mapa de calor de demanda del mes asignado: viajes promedio por día de la semana y hora.

Archivo: [`sql/insights/i5_demanda_dia_hora.sql`](sql/insights/i5_demanda_dia_hora.sql)

<details><summary>Consulta SQL</summary>

```sql
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
```

</details>

| iso_day | day_name | pickup_hour | average_trips |
| --- | --- | --- | --- |
| 1 | Lunes | 0 | 2510.6000 |
| 1 | Lunes | 1 | 1296.8000 |
| 1 | Lunes | 2 | 704.2000 |
| 1 | Lunes | 3 | 521.8000 |
| 1 | Lunes | 4 | 709.2000 |
| 1 | Lunes | 5 | 1164.6000 |
| 1 | Lunes | 6 | 2423.8000 |
| 1 | Lunes | 7 | 4350.0000 |
| 1 | Lunes | 8 | 5922.4000 |
| 1 | Lunes | 9 | 5923.0000 |
| 1 | Lunes | 10 | 5532.0000 |
| 1 | Lunes | 11 | 5690.8000 |
| 1 | Lunes | 12 | 6191.0000 |
| 1 | Lunes | 13 | 6393.0000 |
| 1 | Lunes | 14 | 7068.8000 |
| 1 | Lunes | 15 | 7402.8000 |
| 1 | Lunes | 16 | 6826.0000 |
| 1 | Lunes | 17 | 7616.2000 |
| 1 | Lunes | 18 | 7810.6000 |
| 1 | Lunes | 19 | 6380.4000 |
| 1 | Lunes | 20 | 6342.8000 |
| 1 | Lunes | 21 | 6914.6000 |
| 1 | Lunes | 22 | 5839.2000 |
| 1 | Lunes | 23 | 3686.2000 |
| 2 | Martes | 0 | 2137.3000 |
| 2 | Martes | 1 | 1015.5000 |
| 2 | Martes | 2 | 478.5000 |
| 2 | Martes | 3 | 288.0000 |
| 2 | Martes | 4 | 387.8000 |
| 2 | Martes | 5 | 908.5000 |
| 2 | Martes | 6 | 2489.5000 |
| 2 | Martes | 7 | 4784.0000 |
| 2 | Martes | 8 | 6610.5000 |
| 2 | Martes | 9 | 6441.5000 |
| 2 | Martes | 10 | 6436.3000 |
| 2 | Martes | 11 | 6573.8000 |
| 2 | Martes | 12 | 6843.8000 |
| 2 | Martes | 13 | 7041.8000 |
| 2 | Martes | 14 | 7858.3000 |
| 2 | Martes | 15 | 8373.3000 |
| 2 | Martes | 16 | 7703.0000 |
| 2 | Martes | 17 | 8740.3000 |
| 2 | Martes | 18 | 9325.3000 |
| 2 | Martes | 19 | 7962.3000 |
| 2 | Martes | 20 | 8570.8000 |
| 2 | Martes | 21 | 9795.3000 |
| 2 | Martes | 22 | 8492.8000 |
| 2 | Martes | 23 | 5242.0000 |
| 3 | Miércoles | 0 | 2616.3000 |
| 3 | Miércoles | 1 | 1208.5000 |
| 3 | Miércoles | 2 | 600.0000 |
| 3 | Miércoles | 3 | 405.0000 |
| 3 | Miércoles | 4 | 466.5000 |
| 3 | Miércoles | 5 | 1049.8000 |
| 3 | Miércoles | 6 | 2511.0000 |
| 3 | Miércoles | 7 | 4985.8000 |
| 3 | Miércoles | 8 | 6902.0000 |
| 3 | Miércoles | 9 | 6759.5000 |
| 3 | Miércoles | 10 | 6485.5000 |
| 3 | Miércoles | 11 | 6698.8000 |
| 3 | Miércoles | 12 | 7219.0000 |
| 3 | Miércoles | 13 | 7617.3000 |
| 3 | Miércoles | 14 | 8312.5000 |
| 3 | Miércoles | 15 | 8728.5000 |
| 3 | Miércoles | 16 | 8218.5000 |
| 3 | Miércoles | 17 | 9379.3000 |
| 3 | Miércoles | 18 | 9954.8000 |
| 3 | Miércoles | 19 | 8999.8000 |
| 3 | Miércoles | 20 | 9012.3000 |
| 3 | Miércoles | 21 | 10235.3000 |
| 3 | Miércoles | 22 | 9979.5000 |
| 3 | Miércoles | 23 | 7182.5000 |
| 4 | Jueves | 0 | 4505.3000 |
| 4 | Jueves | 1 | 2229.5000 |
| 4 | Jueves | 2 | 1173.0000 |
| 4 | Jueves | 3 | 765.8000 |
| 4 | Jueves | 4 | 784.8000 |
| 4 | Jueves | 5 | 1184.8000 |
| 4 | Jueves | 6 | 2406.3000 |
| 4 | Jueves | 7 | 4311.3000 |
| 4 | Jueves | 8 | 5999.3000 |
| 4 | Jueves | 9 | 6259.5000 |
| 4 | Jueves | 10 | 6455.0000 |
| 4 | Jueves | 11 | 6867.8000 |
| 4 | Jueves | 12 | 7546.8000 |
| 4 | Jueves | 13 | 8275.3000 |
| 4 | Jueves | 14 | 8533.8000 |
| 4 | Jueves | 15 | 8978.0000 |
| 4 | Jueves | 16 | 8621.3000 |
| 4 | Jueves | 17 | 9166.8000 |
| 4 | Jueves | 18 | 9750.5000 |
| 4 | Jueves | 19 | 9249.0000 |
| 4 | Jueves | 20 | 9181.0000 |
| 4 | Jueves | 21 | 9693.8000 |
| 4 | Jueves | 22 | 9323.3000 |
| 4 | Jueves | 23 | 7472.5000 |
| 5 | Viernes | 0 | 5302.5000 |
| 5 | Viernes | 1 | 3169.5000 |
| 5 | Viernes | 2 | 1782.3000 |
| 5 | Viernes | 3 | 1097.3000 |
| 5 | Viernes | 4 | 990.5000 |
| 5 | Viernes | 5 | 1218.5000 |
| 5 | Viernes | 6 | 2322.3000 |
| 5 | Viernes | 7 | 3805.5000 |
| 5 | Viernes | 8 | 4993.3000 |
| 5 | Viernes | 9 | 5258.8000 |
| 5 | Viernes | 10 | 5492.3000 |
| 5 | Viernes | 11 | 6029.3000 |
| 5 | Viernes | 12 | 6646.0000 |
| 5 | Viernes | 13 | 7081.5000 |
| 5 | Viernes | 14 | 7907.5000 |
| 5 | Viernes | 15 | 8175.3000 |
| 5 | Viernes | 16 | 7352.5000 |
| 5 | Viernes | 17 | 8050.3000 |
| 5 | Viernes | 18 | 8413.0000 |
| 5 | Viernes | 19 | 7790.5000 |
| 5 | Viernes | 20 | 7223.8000 |
| 5 | Viernes | 21 | 7876.5000 |
| 5 | Viernes | 22 | 8981.5000 |
| 5 | Viernes | 23 | 9092.0000 |
| 6 | Sábado | 0 | 8123.8000 |
| 6 | Sábado | 1 | 6528.5000 |
| 6 | Sábado | 2 | 4892.8000 |
| 6 | Sábado | 3 | 3306.0000 |
| 6 | Sábado | 4 | 2034.8000 |
| 6 | Sábado | 5 | 1037.0000 |
| 6 | Sábado | 6 | 1399.5000 |
| 6 | Sábado | 7 | 2036.8000 |
| 6 | Sábado | 8 | 2913.8000 |
| 6 | Sábado | 9 | 4093.5000 |
| 6 | Sábado | 10 | 5470.3000 |
| 6 | Sábado | 11 | 6357.0000 |
| 6 | Sábado | 12 | 7323.5000 |
| 6 | Sábado | 13 | 7956.8000 |
| 6 | Sábado | 14 | 7920.8000 |
| 6 | Sábado | 15 | 7751.3000 |
| 6 | Sábado | 16 | 8288.0000 |
| 6 | Sábado | 17 | 8888.0000 |
| 6 | Sábado | 18 | 9404.5000 |
| 6 | Sábado | 19 | 8231.3000 |
| 6 | Sábado | 20 | 6810.3000 |
| 6 | Sábado | 21 | 7359.5000 |
| 6 | Sábado | 22 | 8645.0000 |
| 6 | Sábado | 23 | 9209.8000 |
| 7 | Domingo | 0 | 8540.8000 |
| 7 | Domingo | 1 | 6672.2000 |
| 7 | Domingo | 2 | 5133.6000 |
| 7 | Domingo | 3 | 3553.2000 |
| 7 | Domingo | 4 | 2333.8000 |
| 7 | Domingo | 5 | 1179.4000 |
| 7 | Domingo | 6 | 1348.8000 |
| 7 | Domingo | 7 | 1837.6000 |
| 7 | Domingo | 8 | 2618.8000 |
| 7 | Domingo | 9 | 4053.4000 |
| 7 | Domingo | 10 | 5322.8000 |
| 7 | Domingo | 11 | 5875.4000 |
| 7 | Domingo | 12 | 6685.0000 |
| 7 | Domingo | 13 | 6942.0000 |
| 7 | Domingo | 14 | 7087.4000 |
| 7 | Domingo | 15 | 7041.8000 |
| 7 | Domingo | 16 | 7311.2000 |
| 7 | Domingo | 17 | 7562.8000 |
| 7 | Domingo | 18 | 7502.2000 |
| 7 | Domingo | 19 | 6483.8000 |
| 7 | Domingo | 20 | 6065.8000 |
| 7 | Domingo | 21 | 6069.2000 |
| 7 | Domingo | 22 | 5322.0000 |
| 7 | Domingo | 23 | 4047.0000 |

## I6. ¿Los viajes se hicieron más largos o más cortos? Proporción de viajes por tramo de distancia (millas).

Archivo: [`sql/insights/i6_distribucion_distancias.sql`](sql/insights/i6_distribucion_distancias.sql)

<details><summary>Consulta SQL</summary>

```sql
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
```

</details>

| distance_band | comparison_share_pct | target_share_pct | comparison_average_fare | target_average_fare |
| --- | --- | --- | --- | --- |
| 1. menos de 1 | 21.2000 | 19.2900 | 8.2800 | 8.6700 |
| 2. 1 a 3 | 49.2800 | 46.7100 | 13.7400 | 13.2700 |
| 3. 3 a 7 | 16.8200 | 20.0700 | 25.1200 | 23.0800 |
| 4. 7 a 15 | 8.0600 | 9.6300 | 44.6100 | 40.6600 |
| 5. 15 o más | 4.6400 | 4.3100 | 75.8600 | 76.4000 |

## I7. ¿Cuándo rinde más manejar? Ingreso por minuto de viaje según la hora, en el mes asignado.

Archivo: [`sql/insights/i7_ingreso_por_minuto.sql`](sql/insights/i7_ingreso_por_minuto.sql)

<details><summary>Consulta SQL</summary>

```sql
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
```

</details>

| pickup_hour | trips | average_revenue_per_trip | average_duration_minutes | revenue_per_minute |
| --- | --- | --- | --- | --- |
| 0 | 145451 | 29.4200 | 15.7700 | 1.8650 |
| 1 | 96061 | 27.7200 | 14.6800 | 1.8880 |
| 2 | 64626 | 25.6200 | 13.9000 | 1.8440 |
| 3 | 43597 | 25.8500 | 13.7800 | 1.8760 |
| 4 | 33598 | 30.1700 | 15.0000 | 2.0120 |
| 5 | 33021 | 32.3800 | 16.1500 | 2.0050 |
| 6 | 63053 | 28.6400 | 16.4300 | 1.7430 |
| 7 | 110228 | 27.1500 | 16.9800 | 1.5990 |
| 8 | 151892 | 26.4700 | 17.2900 | 1.5310 |
| 9 | 164628 | 26.3800 | 17.1900 | 1.5350 |
| 10 | 175069 | 26.7600 | 17.5200 | 1.5270 |
| 11 | 187250 | 27.2300 | 18.2500 | 1.4920 |
| 12 | 205956 | 27.7600 | 18.7400 | 1.4810 |
| 13 | 217728 | 28.2100 | 19.1600 | 1.4720 |
| 14 | 232006 | 30.7400 | 20.1900 | 1.5230 |
| 15 | 239278 | 29.2100 | 20.4700 | 1.4270 |
| 16 | 230480 | 31.3000 | 20.3300 | 1.5400 |
| 17 | 251881 | 30.1100 | 19.1000 | 1.5760 |
| 18 | 263050 | 28.4100 | 16.8400 | 1.6870 |
| 19 | 232529 | 28.2300 | 15.8700 | 1.7800 |
| 20 | 224633 | 27.3200 | 15.5800 | 1.7530 |
| 21 | 244225 | 27.3200 | 15.6200 | 1.7500 |
| 22 | 236940 | 28.0500 | 16.0400 | 1.7480 |
| 23 | 190952 | 29.0300 | 16.2900 | 1.7820 |
