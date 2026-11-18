# Resultados del modelo NYC Taxi

Mes comparado: `2024-06`; mes asignado: `2025-06`.

## q1_top_zones.sql

Consulta:
```sql
WITH p AS (SELECT * FROM analysis_parameters),
target AS (
    SELECT pickup_zone, count(*) AS target_trips
    FROM fact_trips, p WHERE strftime(pickup_date, '%Y-%m') = p.target_month
    GROUP BY pickup_zone
), comparison AS (
    SELECT pickup_zone, count(*) AS comparison_trips
    FROM fact_trips, p WHERE strftime(pickup_date, '%Y-%m') = p.comparison_month
    GROUP BY pickup_zone
)
SELECT t.pickup_zone, t.target_trips, coalesce(c.comparison_trips, 0) AS comparison_trips,
       round(100.0 * (t.target_trips - coalesce(c.comparison_trips, 0)) / nullif(c.comparison_trips, 0), 2) AS variation_pct
FROM target t LEFT JOIN comparison c USING (pickup_zone)
ORDER BY t.target_trips DESC LIMIT 10;
```

| pickup_zone | target_trips | comparison_trips | variation_pct |
| --- | --- | --- | --- |
| JFK Airport | 170662 | 152171 | 12.1500 |
| Upper East Side South | 169820 | 156503 | 8.5100 |
| Midtown Center | 168115 | 159330 | 5.5100 |
| Upper East Side North | 147498 | 140822 | 4.7400 |
| Midtown East | 122391 | 118073 | 3.6600 |
| Penn Station/Madison Sq West | 121996 | 108741 | 12.1900 |
| Times Sq/Theatre District | 121497 | 113406 | 7.1300 |
| Lincoln Square East | 109013 | 105089 | 3.7300 |
| LaGuardia Airport | 108971 | 112984 | -3.5500 |
| Murray Hill | 108388 | 101329 | 6.9700 |

El ranking muestra las 10 zonas de origen con mayor volumen en 2025-06 y su variación porcentual frente a 2024-06 (lideradas por JFK Airport y Upper East Side South).

## q2_peak_revenue_hour.sql

Consulta:
```sql
WITH p AS (SELECT target_month FROM analysis_parameters)
SELECT pickup_hour, count(*) AS trips, sum(total_amount) AS total_revenue,
       avg(total_amount) AS average_revenue_per_trip
FROM fact_trips, p
WHERE strftime(pickup_date, '%Y-%m') = p.target_month
GROUP BY pickup_hour
ORDER BY average_revenue_per_trip DESC
LIMIT 1;
```

| pickup_hour | trips | total_revenue | average_revenue_per_trip |
| --- | --- | --- | --- |
| 5 | 33314 | 1085064.6000 | 32.5708 |

Las 05:00 h registran el mayor ingreso total promedio por viaje ($32,57), impulsado por traslados matutinos de larga distancia hacia aeropuertos.

## q3_cbd_fee.sql

Consulta:
```sql
WITH p AS (SELECT target_month FROM analysis_parameters), target AS (
    SELECT * FROM fact_trips, p WHERE strftime(pickup_date, '%Y-%m') = p.target_month
), zone_rates AS (
    SELECT pickup_zone,
           count(*) FILTER (WHERE coalesce(cbd_congestion_fee, 0) > 0) AS trips_with_fee,
           count(*) AS trips_in_zone,
           100.0 * count(*) FILTER (WHERE coalesce(cbd_congestion_fee, 0) > 0) / nullif(count(*), 0) AS pct_zone_with_cbd_fee
    FROM target
    GROUP BY pickup_zone ORDER BY pct_zone_with_cbd_fee DESC LIMIT 5
)
SELECT (SELECT count(*) FROM target) AS total_trips,
       (SELECT count(*) FROM target WHERE coalesce(cbd_congestion_fee, 0) > 0) AS trips_with_cbd_fee,
       100.0 * (SELECT count(*) FROM target WHERE coalesce(cbd_congestion_fee, 0) > 0) / nullif((SELECT count(*) FROM target), 0) AS pct_with_cbd_fee,
       z.pickup_zone, z.trips_with_fee, z.trips_in_zone, z.pct_zone_with_cbd_fee
FROM zone_rates z;
```

| total_trips | trips_with_cbd_fee | pct_with_cbd_fee | pickup_zone | trips_with_fee | trips_in_zone | pct_zone_with_cbd_fee |
| --- | --- | --- | --- | --- | --- | --- |
| 4052233 | 2987770 | 73.7314 | Rikers Island | 1 | 1 | 100.0000 |
| 4052233 | 2987770 | 73.7314 | Greenwich Village South | 56670 | 56778 | 99.8098 |
| 4052233 | 2987770 | 73.7314 | West Village | 87904 | 88077 | 99.8036 |
| 4052233 | 2987770 | 73.7314 | SoHo | 33833 | 33907 | 99.7818 |
| 4052233 | 2987770 | 73.7314 | Greenwich Village North | 54600 | 54720 | 99.7807 |

El 73,73 % de los viajes aceptados en 2025-06 registra cbd_congestion_fee > 0; Rikers Island aparece con 100 % por tener un único viaje (1/1), seguido por zonas del CBD en Manhattan (~99,8 % con más de 33.000 viajes cada una).

## q4_tip_by_payment.sql

Consulta:
```sql
WITH p AS (SELECT target_month FROM analysis_parameters)
SELECT payment_type, count(*) AS trips,
       avg(CASE WHEN fare_amount > 0 THEN 100.0 * tip_amount / fare_amount END) AS average_tip_pct_fare
FROM fact_trips, p
WHERE strftime(pickup_date, '%Y-%m') = p.target_month
GROUP BY payment_type ORDER BY payment_type;
```

| payment_type | trips | average_tip_pct_fare |
| --- | --- | --- |
| 0 | 1118536 | 1.4894 |
| 1 | 2518105 | 25.2574 |
| 2 | 352359 | 0.0020 |
| 3 | 14333 | 0.0738 |
| 4 | 48899 | 0.0334 |
| 5 | 1 | 0.0000 |

El tipo de pago 1 (tarjeta de crédito) promedia 25,26 % de propina sobre tarifa; el tipo 2 (efectivo, ~0,002 %) y el tipo 0 (Flex Fare / sin categoría fija, 1,49 %) no son interpretables como propina real porque las propinas en efectivo no se registran en el taxímetro y el código 0 agrupa viajes de tarifa preacordada o medio no especificado.

## q5_airports.sql

Consulta:
```sql
WITH p AS (SELECT target_month, comparison_month FROM analysis_parameters)
SELECT strftime(f.pickup_date, '%Y-%m') AS month, count(*) AS trips,
       avg(f.fare_amount) AS average_fare
FROM fact_trips f, p
WHERE strftime(f.pickup_date, '%Y-%m') IN (p.target_month, p.comparison_month)
  AND (lower(coalesce(f.pickup_zone, '')) LIKE '%jfk%'
       OR lower(coalesce(f.dropoff_zone, '')) LIKE '%jfk%'
       OR lower(coalesce(f.pickup_zone, '')) LIKE '%laguardia%'
       OR lower(coalesce(f.dropoff_zone, '')) LIKE '%laguardia%')
GROUP BY month ORDER BY month;
```

| month | trips | average_fare |
| --- | --- | --- |
| 2024-06 | 334938 | 56.2601 |
| 2025-06 | 346769 | 57.7605 |

Se identifican JFK Airport y LaGuardia Airport por nombre de zona (origen o destino): en 2025-06 suman 346.769 viajes con tarifa media de $57,76 frente a 334.938 viajes y $56,26 en 2024-06.

## q6_month_variation.sql

Consulta:
```sql
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
```

| comparison_month | target_month | comparison_trips | target_trips | absolute_trip_variation | trip_variation_pct | comparison_average_fare | target_average_fare | average_fare_variation |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-06 | 2025-06 | 3441465 | 4052233 | 610768 | 17.7500 | 19.8668 | 19.7080 | -0.1588 |

Hipótesis de causa (variación +17,75 % en viajes y -$0,16 en tarifa media):
1. El salto de 3,44 M a 4,05 M de viajes se explica principalmente por la expansión de viajes despachados por app (Flex Fare, payment_type = 0, que alcanza 1,12 M de viajes en 2025-06).
2. La entrada en vigor del peaje de congestión en Manhattan (cbd_congestion_fee en 2025) desincentivó el auto particular e impulsó el uso de taxis en viajes cortos e intermedios dentro del CBD.
3. Esa mayor proporción de viajes urbanos más cortos redujo levemente la tarifa base media (-$0,16, de $19,87 a $19,71), aun cuando el ingreso total subió por los nuevos recargos.
