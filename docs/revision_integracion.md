# Informe de Auditoría e Integración — Pipeline NYC Taxi

**Fecha:** 18/11/2026  
**Rama / Worktree de QA:** `qa/integracion` (`EXAMEN_QA`)  
**Estado:** **INTEGRADO, CORREGIDO Y VERIFICADO CON DATOS SINTÉTICOS Y REALES**

---

## 1. Resumen Ejecutivo

Se auditó el proyecto completo frente a la consigna original (`Examen Pipeline NYC Taxi.docx`) y al contrato de integración ([AGENTS.md](file:///C:/Users/Usuario/Documents/FACULTAD/4TO%20A%C3%91O/FUNDAMENTOS%20DE%20DATOS/EXAMEN/AGENTS.md)), abarcando las 5 etapas del pipeline batch para `2024-06` y `2025-06`.

- **Interfaces y Módulos (`src/`):** 100 % compatibles (`run_ingest`, `run_schema`, `run_quality`, `run_transform`, `run_model`, `pipeline.main`).
- **Reconciliación exacta (Datos Reales):**
  - `2024-06`: `3.539.193` leídas = `3.441.465` aceptadas + `97.728` cuarentena (`reconciled: true`).
  - `2025-06`: `4.322.960` leídas = `4.052.233` aceptadas + `270.727` cuarentena (`reconciled: true`).
  - Total en `fact_trips` y `agg_zone_hour`: `7.493.698` viajes aceptados, sin duplicados tras múltiples ejecuciones.
- **Desempeño y Memoria (Restricción 8 GB RAM):**
  - Corrida 1 completa (datos reales): `50,08 s`.
  - Corrida 2 completa (idempotencia datos reales): `50,92 s`.
  - Pico de RAM medido (Working Set / RSS): `~2.008 MB` (`~2,01 GB`), dentro del límite configurado en DuckDB (`memory_limit='2GB'`, `threads='2'`).
- **Tests Automatizados (`pytest -q`):** **17 passed** (14 unitarios + 3 de integración extremo a extremo).

---

## 2. Registro de Errores Encontrados, Causa y Solución Aplicada

### Error #1 (CRÍTICO — `OutOfRangeException` en `src/model.py` al crear `fact_trips` con 7,49 M de filas y límite de 2 GB)
- **Causa:** `src/model.py` utilizaba `row_number() OVER (ORDER BY pickup_date, pickup_hour, pu_location_id, do_location_id, source_file) AS trip_id` junto con la columna `warning_flags` (`LIST<VARCHAR>`). En DuckDB 1.4.3, ordenar globalmente 7,49 millones de filas con tipos anidados (`LIST`) bajo un límite estricto de 2 GB provoca un error de desborde en la paginación del sort externo (`_duckdb.OutOfRangeException: Cannot resize vector...`). Además, `CREATE OR REPLACE TABLE _curated` persistía una copia duplicada de los 7,49 M de registros en el archivo `.duckdb`.
- **Solución aplicada:**
  1. Se reemplazó `CREATE OR REPLACE TABLE _curated` por `DROP TABLE IF EXISTS main._curated` + `CREATE OR REPLACE TEMP VIEW _curated`, evitando duplicar datos en disco (el tamaño de `nyc_taxi.duckdb` bajó de ~950 MB a `531 MB`).
  2. Se reemplazó la ventana ordenada por `row_number() OVER () AS trip_id`, que asigna identificadores únicos secuenciales (`1..7.493.698`) en streaming sin requerir un ordenamiento global en memoria.

### Error #2 (CRÍTICO — Omisión de `LaGuardia Airport` en `sql/q5_airports.sql` y `RESULTADOS.md`)
- **Causa:** En `taxi_zone_lookup.csv` (LocationID `138`), el nombre oficial de la zona es `"LaGuardia Airport"` (sin espacio). La consulta `sql/q5_airports.sql` filtraba con `LIKE '%la guardia%'` (con espacio), excluyendo todos los viajes de LaGuardia.
- **Impacto corregido:**
  - `2024-06`: Pasó de `185.212` viajes (`$65,3918`, sólo JFK) al valor correcto de **`334.938` viajes** y tarifa media **`$56,2601`** (JFK + LaGuardia).
  - `2025-06`: Pasó de `202.184` viajes (`$66,6059`, sólo JFK) al valor correcto de **`346.769` viajes** y tarifa media **`$57,7605`** (JFK + LaGuardia).
- **Solución aplicada:** Se corrigió el filtro en `sql/q5_airports.sql` a `LIKE '%laguardia%'`, se agregó el test de regresión `tests/integration/test_sql_business_rules.py::test_q5_airports_includes_both_jfk_and_laguardia` y se regeneró `RESULTADOS.md`.

### Error #3 (ALTO — Hipótesis faltante en Pregunta 6 e interpretaciones en `src/model.py` / `RESULTADOS.md`)
- **Causa:** La consigna pide para la consulta 6 *"una hipótesis de causa en no más de 5 líneas"* y para la consulta 4 explicar por qué uno de los tipos de pago no es interpretable. `src/model.py` tenía un texto genérico placeholder en `q6_month_variation.sql`.
- **Solución aplicada:** Se actualizaron las interpretaciones en `src/model.py` y se regeneró `RESULTADOS.md` con una hipótesis sustantiva de 4 líneas para `q6_month_variation.sql` y la explicación detallada de `payment_type` en `q4_tip_by_payment.sql`.

### Error #4 (MEDIO — Evidencia de reprocesamiento por partición y pico de RAM en `README.md`)
- **Causa:** La consigna solicita en el `README.md`: (a) demostrar con conteos antes y después que reprocesar un mes reemplaza su partición sin alterar las demás, (b) documentar la granularidad de cada tabla del modelo estrella, y (c) reportar tiempo de ejecución y pico de RAM medidos.
- **Solución aplicada:** Se completó `README.md` con la tabla de conteos antes/después de la ejecución 1 y 2, la granularidad exacta de `fact_trips`, `dim_zone`, `dim_date` y `agg_zone_hour`, y las métricas medidas (`~50,1 s` y `~2,01 GB` de pico de RAM).
