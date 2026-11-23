# Auditoría de rendimiento

**Fecha:** 2026-11-23  · **Rama:** `integration/dbt-prep`  · **Datos:** junio 2024/2025

## Configuración observada

- Python/DuckDB: `memory_limit=2GB`, `threads=2`, spill en `data/tmp`.
- dbt/DuckDB: `memory_limit=2GB`, `threads=1`, spill en `data/tmp`.
- No se ejecutaron dos constructores sobre el mismo warehouse.
- `data/tmp` quedó sin archivos al terminar las mediciones.

Las mediciones de memoria disponibles corresponden a ejecuciones completas recientes con esta misma configuración: aproximadamente 2.023 MB para el modo Python y 2.001 MB para el modo dbt (árbol de procesos). No permiten afirmar que se haya probado una notebook física de 8 GB; sólo verifican que el presupuesto configurado de DuckDB se respeta.

## Pipeline completo

| Modo | Períodos | Tiempo | Resultado |
|---|---|---:|---|
| Python | 2024-06 + 2025-06 | 31,1 s | correcto |
| dbt | 2024-06 + 2025-06 | 37,9 s | `dbt build` 32/32 |

Tamaños finales observados:

| Warehouse | Tamaño |
|---|---:|
| `data/warehouse/nyc_taxi.duckdb` | 527,51 MiB |
| `data/warehouse/nyc_taxi_dbt.duckdb` | 546,01 MiB |

## Consultas auditadas

### Q1 — zonas con más viajes

El plan contiene `TABLE_SCAN`, dos `HASH_GROUP_BY`, `HASH_JOIN` y `TOP_N`. La consulta anterior agrupaba solamente por nombre; el catálogo contiene nombres repetidos (`Corona` y `Governor's Island/Ellis Island/Liberty Island`). Se corrigió para agrupar y unir por `pickup_zone_id` y `pickup_zone`.

| Versión | Mediana (3 ejecuciones) | `EXPLAIN ANALYZE` |
|---|---:|---:|
| Antes | 121,387 ms | 0,120 s |
| Después | 138,923 ms | 0,139 s |

El cambio implica +14,4% en esta consulta, pero las diez filas devueltas son exactamente iguales en junio. La ralentización es el costo medido de preservar la granularidad correcta; no se agregó un índice artificial.

### Q6 — variación interanual

El plan contiene `TABLE_SCAN`, `HASH_GROUP_BY`, `CTE_SCAN` y `HASH_JOIN`. La mediana fue 54,858 ms y `EXPLAIN ANALYZE` informó 0,056 s. Agrupa los viajes aceptados por mes y une únicamente los dos períodos de `analysis_parameters`; usa `nullif` para evitar división por cero.

## Conclusión

Las consultas están dominadas por scans y agregaciones sobre `fact_trips`, que es esperable para DuckDB y el volumen del examen. No se aplicaron optimizaciones adicionales porque no había una mejora segura medida. Q3 conserva una advertencia: ordena por porcentaje de CBD y puede poner primero una zona con un solo viaje; el resultado debe leerse junto con su denominador. Cambiar el umbral alteraría la pregunta de negocio, por lo que queda como decisión documental.
