# Auditoría técnica, SQL y rendimiento

**Fecha:** 2026-11-23  · **Rama:** `integration/dbt-prep`  · **Commit base:** `ed3f254`

## Veredicto

**CON OBSERVACIONES.** El pipeline, los dos warehouses y las seis consultas pasan la regresión. Se aplicó una corrección SQL mínima en Q1 para respetar la clave de zona. Queda como riesgo documentado la estabilidad de `trip_id` y como advertencia analítica el ranking de porcentajes de Q3 con denominadores pequeños.

## Estado de las seis consultas

| Consulta | Estado | Observación |
|---|---|---|
| Q1 zonas | ✅ | Agrupa por `pickup_zone_id` + nombre; `LEFT JOIN` no multiplica filas. |
| Q2 hora de mayor ingreso | ✅ | Agrupa por hora; `avg` ignora NULL y ordena el ingreso medio, no el total. |
| Q3 cargo CBD | ✅ con advertencia | `coalesce` y `nullif` son seguros; el ranking permite 1/1. |
| Q4 propinas | ✅ | Excluye tarifas no positivas del porcentaje y conserva la cautela sobre efectivo. |
| Q5 aeropuertos | ✅ | Filtro null-safe para JFK/LaGuardia en origen o destino. |
| Q6 variación mensual | ✅ | Usa viajes aceptados y los períodos parametrizados; no atribuye causalidad. |

No hay interpretaciones hardcodeadas en `src/model.py` ni en `src/dbt_runner.py`; `RESULTADOS.md` se genera desde las filas SQL y los meses seleccionados.

## Corrección realizada

`sql/q1_top_zones.sql` agrupaba sólo por `pickup_zone`. La dimensión contiene nombres repetidos para distintos IDs, por lo que podía fusionar entidades. Ahora las CTE `target` y `comparison` agrupan por `pickup_zone_id, pickup_zone` y el join usa ambas columnas. La salida de las diez zonas principales de junio no cambió; la comparación anterior/nueva fue exacta.

## Modelo estrella

Verificación sobre ambos warehouses de junio:

- `fact_trips`: 7.493.698 filas.
- `dim_zone`: 265 filas.
- `dim_date`: 60 filas.
- `agg_zone_hour`: 210.698 filas; suma de `trips` igual a 7.493.698.
- `trip_id` único: sí.
- Huérfanos de pickup/dropoff contra `dim_zone`: 0/0.
- `analysis_parameters`: `2024-06` y `2025-06`.
- Las seis consultas coinciden entre Python y dbt dentro de `1e-5`.

### Riesgo de estabilidad de claves

`fact_trips.trip_id` se genera con `row_number() over ()` tanto en Python como en dbt. Esto garantiza unicidad dentro de una reconstrucción, pero no una identidad estable entre reconstrucciones si cambia el orden físico de lectura. No se cambió porque ordenar 7,5 millones de filas sólo para estabilizar una clave artificial aumentaría memoria y tiempo. Para el examen, las relaciones y conteos son correctos; para una evolución productiva convendría definir una clave natural o hash documentado.

## Rendimiento y recursos

El detalle de `EXPLAIN ANALYZE`, tiempos y tamaños está en [RENDIMIENTO.md](RENDIMIENTO.md). En resumen: Python 31,1 s, dbt 37,9 s; memoria observada previamente ~2.023 MB y ~2.001 MB respectivamente. Es evidencia de la configuración del proceso, no una medición de una notebook física de 8 GB.

## Regresión ejecutada

- `pytest`: 28 passed.
- Pipeline Python junio: correcto.
- Pipeline dbt junio: correcto; `dbt build` 32/32.
- `dbt test`: 24/24.
- Reconciliación junio: 3.539.193 = 3.441.465 + 97.728 y 4.322.960 = 4.052.233 + 270.727.
- Mes dinámico adicional de mayo ya verificado en la iteración anterior.

## Riesgos pendientes y recomendaciones para la defensa

1. Explicar que Q1 usa IDs para no fusionar nombres repetidos.
2. Aclarar que Q3 muestra porcentajes descriptivos y que un denominador pequeño no equivale a evidencia representativa.
3. Diferenciar viajes originales de aceptados: Q6 usa los aceptados después de calidad.
4. Presentar `trip_id` como identificador técnico de la corrida, no como clave histórica estable.
5. No afirmar que se midió una notebook de 8 GB: sólo se observó el consumo del proceso con el límite DuckDB configurado.

No se modificaron `README.md`, `docs/ARQUITECTURA.md` ni `docs/DEFENSA.md`; no se hizo push ni merge.
