-- Ningún viaje del hecho puede violar una regla de RECHAZO de la etapa de calidad.
-- Es una segunda línea de defensa: si alguien saltea la cuarentena, este test falla.
-- (dbt considera fallido el test si la consulta devuelve alguna fila.)
SELECT trip_id, duration_minutes, trip_distance, total_amount
FROM {{ ref('fact_trips') }}
WHERE duration_minutes <= 0                         -- R02: bajada no posterior a la subida
   OR trip_distance <= 0 OR trip_distance > 200     -- R03: distancia fuera de rango
   OR total_amount < 0                              -- R04: total negativo
   OR pickup_zone_id IS NULL OR dropoff_zone_id IS NULL  -- R06: zonas faltantes
