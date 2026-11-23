# Guion de Defensa Oral y Preguntas Técnicas — Pipeline NYC Taxi

Este documento organiza la defensa oral del examen para los dos integrantes del equipo. Incluye:
1. **Protocolo de demostración en vivo** (los 3 pasos exactos que pide la consigna).
2. **Guion de exposición dividido equitativamente** entre Integrante 1 (Parte A) e Integrante 2 (Parte B y Auditoría QA).
3. **Banco de preguntas técnicas probables de la cátedra** con respuestas fundamentadas en el código real y los datos observados.

---

## 1. Protocolo de Demostración en Vivo (PowerShell)

La consigna original establece tres pasos obligatorios para la demostración práctica durante la defensa:
1. Correr el pipeline completo para el mes elegido.
2. Volver a correrlo y mostrar que los conteos no cambian (idempotencia).
3. Mostrar el reporte de calidad y una fila en cuarentena con su regla violada.

### Paso 0: Ejecutar los 17 tests automatizados (5 segundos)
```powershell
.\.venv\Scripts\python.exe -m pytest -v
```
*Qué señalar al profesor:* pasan los 17 tests (14 unitarios y 3 de integración extremo a extremo) sin necesidad de Internet, validando reglas de calidad, drift de esquema, fallas de red, reconciliación, aislamiento de particiones y el filtro SQL de aeropuertos.

### Paso 1: Primera ejecución completa del pipeline (~35–50 segundos)
```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
```
*Qué señalar en los logs:*
- Procesa `2024-06` y `2025-06` en orden cronológico.
- Si los archivos `raw` ya están en disco, verifica el hash SHA-256 contra `data/control/ingestion_log.csv` y registra `reused` sin volver a descargar 133 MB.
- Informa la reconciliación de ambos meses y genera `data/warehouse/nyc_taxi.duckdb` y `RESULTADOS.md`.

### Paso 2: Segunda ejecución consecutiva y verificación de conteos inalterados
```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
```
Y a continuación ejecutar la consulta de verificación sobre el warehouse:
```powershell
@'
import duckdb
con = duckdb.connect("data/warehouse/nyc_taxi.duckdb", read_only=True)
for t in ["dim_zone", "dim_date", "fact_trips", "agg_zone_hour"]:
    print(f"{t}: {con.execute(f'SELECT count(*) FROM {t}').fetchone()[0]:,}")
print("trip_id únicos:", con.execute("SELECT count(*) = count(DISTINCT trip_id) FROM fact_trips").fetchone()[0])
print("Suma trips en agg_zone_hour:", f"{con.execute('SELECT sum(trips) FROM agg_zone_hour').fetchone()[0]:,}")
con.close()
'@ | .\.venv\Scripts\python.exe -
```
*Qué señalar al profesor:* tras correrlo por segunda vez, `fact_trips` sigue teniendo exactamente **7.493.698** filas (`3.441.465` de junio 2024 + `4.052.233` de junio 2025), todos los `trip_id` son únicos y la suma de `agg_zone_hour.trips` coincide exactamente con `fact_trips`.

### Paso 3: Mostrar el reporte de calidad y una fila real en cuarentena con sus reglas violadas
```powershell
Get-Content data/reports/quality_2025-06.json
```
Y para inspeccionar filas reales en cuarentena desde DuckDB:
```powershell
@'
import duckdb
con = duckdb.connect()
rows = con.execute("""
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, total_amount, tip_amount,
           source_file, violated_rules, rejection_flags, warning_flags
    FROM read_parquet('data/quarantine/yellow/year=2025/month=06/rejected.parquet', hive_partitioning=false)
    WHERE len(rejection_flags) >= 2
    LIMIT 5
""").fetchall()
for r in rows:
    print(r)
'@ | .\.venv\Scripts\python.exe -
```
*Qué señalar al profesor:* mostrar cómo la fila conserva todos sus valores originales normalizados, el archivo de origen (`yellow_tripdata_2025-06.parquet`) y las listas `violated_rules`, `rejection_flags` y `warning_flags`.

---

## 2. Guion de Exposición Oral Dividido entre Dos Integrantes (12 a 15 minutos)

### Bloque 1 — Integrante 1 (Víctor Manuel Libran): Arquitectura General, Ingesta, Esquema y Calidad (6–7 min)

1. **Introducción y manejo de memoria (1 min):**
   - "Construimos un pipeline batch reproducible para comparar junio de 2025 contra junio de 2024 (~7,86 millones de filas crudas). Para cumplir con el límite de 8 GB de RAM, diseñamos el flujo en [`src/config.py`](../src/config.py) para que Python nunca cargue los datasets en memoria: usamos DuckDB limitado a `2 GB` de RAM y `2 threads`, con derrame a disco en `data/tmp` y lectura/escritura directa sobre Apache Parquet."
2. **Etapa 1 — Ingesta idempotente y trazabilidad ([`src/ingest.py`](../src/ingest.py)) (1,5 min):**
   - "La función `run_ingest(month)` descarga el Parquet mensual y `taxi_zone_lookup.csv` en streaming de a 1 MiB hacia un archivo temporal `.part`. Si hay un corte de red, realiza hasta 3 reintentos con espera exponencial de 1, 2 y 4 segundos."
   - "Antes de renombrar `.part` al archivo definitivo, verifica con PyArrow que el Parquet sea válido. Luego calcula el SHA-256 incrementalmente y lo registra en `data/control/ingestion_log.csv`. Si corremos de nuevo el mismo mes y el hash coincide con el último estado `ok`, reutiliza el archivo sin descargarlo. Si alguien corrompe el archivo local, el hash difiere y el pipeline falla protegiendo la capa `raw`."
3. **Etapa 2 — Contrato de esquema y evolución 2024 vs 2025 ([`src/schema.py`](../src/schema.py) y [`contracts/yellow_schema.yaml`](../contracts/yellow_schema.yaml)) (1,5 min):**
   - "En 2025 la TLC agregó la columna `cbd_congestion_fee`, que no existe en 2024. Definimos un contrato YAML versionado con las 20 columnas y sus nombres en `snake_case`."
   - "Antes de transformar, `run_schema` compara el esquema real con `DESCRIBE` y escribe `data/reports/schema_YYYY-MM.json`. Para años anteriores a 2025, permite únicamente la ausencia de `cbd_congestion_fee` y la inyecta como `CAST(NULL AS DOUBLE)`. Elegimos `NULL` y no `0` porque en 2024 el peaje no existía; poner cero confundiría un período sin ley vigente con un viaje exento en 2025."
   - "Si mañana aparece una columna nueva no declarada o un tipo incompatible, la política es `fail_and_report`: guarda el JSON de diagnóstico y detiene el pipeline para obligar a revisar el contrato."
4. **Etapa 3 — Calidad, cuarentena y reconciliación ([`src/quality.py`](../src/quality.py)) (2 min):**
   - "Implementamos 8 reglas (`R01` a `R08`) en una única consulta SQL sobre `staged`. Seis son de rechazo (`R01` fecha fuera del mes, `R02` bajada <= subida, `R03` distancia <= 0 o > 200 millas, `R04` importe total negativo, `R06` zona inexistente en catálogo TLC y `R07` filas duplicadas exactas usando `row_number() OVER (PARTITION BY ...)`)."
   - "Dos reglas son de advertencia (`warning`): `R05` (pasajeros nulos o <= 0) y `R08` (propina negativa, que fue nuestra regla propia descubierta explorando los datos: hallamos 108 casos en 2024-06 y 367 en 2025-06). Decidimos que `R05` sea advertencia porque en junio de 2025 hay 1,23 millones de viajes despachados por app sin cantidad de pasajeros cargada; rechazarlos habría eliminado el 30 % de los viajes válidos."
   - "Finalmente ejecutamos una doble reconciliación: verificamos `leídas == aceptadas + cuarentena` en memoria y volvemos a contar los archivos `accepted.parquet` y `rejected.parquet` escritos en disco (`3.539.193 = 3.441.465 + 97.728` en 2024-06 y `4.322.960 = 4.052.233 + 270.727` en 2025-06)."

---

### Bloque 2 — Integrante 2 (Compañero): Transformación, Modelo Estrella, Negocio y Auditoría QA (6–7 min)

1. **Etapa 4 — Capa curada y aislamiento de particiones Hive ([`src/transform.py`](../src/transform.py)) (1,5 min):**
   - "La función `run_transform` toma `accepted.parquet`, calcula las columnas derivadas (`duration_minutes`, `average_speed_mph`, `pickup_date`, `pickup_hour`, `pickup_day_of_week` y `tip_pct_fare` protegiendo con `CASE WHEN fare_amount > 0`) y enriquece origen y destino con `borough`, `zone_name` y `service_zone`."
   - "Escribe en `data/curated/yellow/year=YYYY/month=MM/part-000.parquet` mediante un archivo temporal `.parquet.tmp`. Así, reprocesar un mes reemplaza sólo su partición y no toca los bytes ni la fecha de modificación de los demás meses, lo cual verificamos con un test específico de integración."
2. **Etapa 5 — Modelo estrella y optimización en DuckDB ([`src/model.py`](../src/model.py)) (2 min):**
   - "Construimos el warehouse `data/warehouse/nyc_taxi.duckdb` con cuatro tablas principales: `dim_zone` (granularidad: 1 fila por zona TLC, 265 filas), `dim_date` (1 fila por fecha calendario de subida, 60 filas), `fact_trips` (1 fila por viaje aceptado, 7.493.698 filas) y `agg_zone_hour` (1 fila por zona de origen, fecha y hora, 210.698 filas)."
   - "Un hallazgo importante de ingeniería ocurrió al integrar los 7,49 millones de viajes reales: hacer un `ORDER BY` global en `row_number() OVER (...)` junto con la columna `warning_flags` (que es una lista `LIST<VARCHAR>`) desbordaba el límite de 2 GB de DuckDB. Lo resolvimos definiendo `_curated` como `TEMP VIEW` sobre los Parquet curados y generando `trip_id` en streaming con `row_number() OVER ()`. Eso bajó el tamaño del warehouse de ~950 MB a 531 MB y redujo el tiempo total del pipeline a ~33–50 segundos manteniendo un pico de RAM de ~2,01 GB."
3. **Auditoría de integración y las 6 respuestas de negocio ([`sql/`](../sql/) y [`RESULTADOS.md`](../RESULTADOS.md)) (2,5 min):**
   - "**Q1 (Top 10 zonas):** lideran `JFK Airport` (170.662 viajes, +12,15 %), `Upper East Side South` (169.820, +8,51 %) y `Midtown Center` (168.115, +5,51 %)."
   - "**Q2 (Hora de mayor ingreso promedio):** las `05:00` h con `$32,57` por viaje (33.314 viajes)."
   - "**Q3 (Cargo CBD):** en junio de 2025 el **73,73 %** de los viajes aceptados (2.987.770 viajes) pagó `cbd_congestion_fee > 0`. En el top 5 por proporción aparece primero `Rikers Island` con 100 % porque tuvo 1 solo viaje (1/1), seguido por zonas del CBD de Manhattan (`Greenwich Village South`, `West Village`, `SoHo`, `Greenwich Village North`) con ~99,8 % sobre decenas de miles de viajes."
   - "**Q4 (Propina por tipo de pago):** tarjeta de crédito (`payment_type = 1`) promedia **25,26 %** de propina sobre tarifa. Explicamos que efectivo (`payment_type = 2`, 0,002 %) no es interpretable porque el taxímetro no registra propinas entregadas en mano, y `payment_type = 0` (1,49 %) corresponde a viajes *Flex Fare* de tarifa preacordada."
   - "**Q5 (Aeropuertos JFK y LaGuardia):** durante la auditoría QA detectamos con un test de integración que buscar `'la guardia'` con espacio omitía los viajes de LaGuardia porque en `taxi_zone_lookup.csv` el nombre oficial es `'LaGuardia Airport'` todo junto. Tras corregirlo a `'%laguardia%'`, obtuvimos los valores reales: **334.938 viajes** (`$56,26` de tarifa media) en 2024-06 y **346.769 viajes** (`$57,76`) en 2025-06."
   - "**Q6 (Variación interanual):** los viajes aceptados subieron un **+17,75 %** (+610.768 viajes) y la tarifa media bajó levemente **-$0,16** (de `$19,87` a `$19,71`), fenómeno compatible con la fuerte expansión de viajes *Flex Fare* (`payment_type = 0`) y trayectos urbanos intermedios."

---

## 3. Preguntas Técnicas Probables de la Cátedra y Respuestas de Defensa

### P1. ¿Cómo garantizan que si el pipeline se corta a la mitad no queden archivos Parquet corruptos o incompletos?
**Respuesta:** Todas las escrituras de archivos en [`src/ingest.py`](../src/ingest.py) y [`src/config.py`](../src/config.py) (`download`, `copy_parquet` y `write_json`) escriben primero en un archivo temporal dentro del mismo sistema de archivos (`.part` o `.parquet.tmp`) y recién cuando la escritura y validación terminan con éxito ejecutan `tmp.replace(target)`, que es una operación atómica de renombrado en el sistema operativo. Además, el bloque `finally: tmp.unlink(missing_ok=True)` garantiza que no queden temporales huérfanos ante una excepción.

### P2. ¿Por qué eligieron `NULL` en lugar de `0` para `cbd_congestion_fee` en junio de 2024? ¿Qué cambiaría en las consultas si hubieran puesto `0`?
**Respuesta:** Elegimos `NULL` (`CAST(NULL AS DOUBLE)`) por semántica de datos: en junio de 2024 el peaje por congestión del CBD aún no había entrado en vigencia. Si hubiéramos puesto `0.0`, no podríamos distinguir entre un período en el que el tributo no existía legalmente y un viaje realizado en 2025 que transitó fuera de la zona de peaje (`0.0`). Además, un `AVG(cbd_congestion_fee)` interanual se vería artificialmente diluido por 3,44 millones de ceros de 2024, mientras que en SQL `AVG()` ignora automáticamente los `NULL`. En la consulta 3 usamos `FILTER (WHERE coalesce(cbd_congestion_fee, 0) > 0)` acotado al `target_month` (`2025-06`).

### P3. ¿Por qué la suma de violaciones de las reglas de rechazo (`R01 + R02 + R03 + R04 + R06 + R07`) en el reporte JSON es mayor que `rows_quarantined`?
**Respuesta:** Porque las reglas no son mutuamente excluyentes. En [`src/quality.py`](../src/quality.py) evaluamos las 8 reglas en paralelo sobre cada fila: por ejemplo, en junio de 2025 hay viajes que tienen simultáneamente `total_amount < 0` (`R04`) y `trip_distance <= 0` (`R03`) o `dropoff <= pickup` (`R02`). Cada regla cuenta cuántos viajes la violaron individualmente (`rule_violations`), pero la fila física se envía **una sola vez** a `rejected.parquet` con la lista completa de reglas infringidas en `violated_rules` y `rejection_flags`. Por eso `rows_read == rows_accepted + rows_quarantined` cierra exactamente al nivel de filas.

### P4. ¿Por qué en `R06` (zonas inexistentes) dio `0` rechazos si hay viajes con zonas `264` y `265` ("Unknown" y "Outside of NYC")?
**Respuesta:** Porque la consigna pide rechazar *"Zona de origen o de destino inexistente en la tabla de zonas"*. En el archivo oficial `taxi_zone_lookup.csv` publicado por la TLC, las filas 264 (`Unknown`) y 265 (`Outside of NYC`) existen explícitamente como claves primarias válidas (`LocationID`). Mantenerlas preserva la integridad referencial con `dim_zone` sin descartar viajes reales fuera de los cinco distritos (como traslados hacia Newark o condados vecinos). Si un registro trajera un `LocationID` que realmente no está en `taxi_zone_lookup.csv` (por ejemplo, `999` o `NULL`), `R06` lo envía a cuarentena, tal como lo comprueba `tests/test_quality.py`.

### P5. ¿Cómo resolvieron la detección de filas duplicadas exactas (`R07`) sin incluir el nombre del archivo o columnas auxiliares?
**Respuesta:** En [`src/quality.py`](../src/quality.py) obtenemos dinámicamente con `DESCRIBE staged` todas las columnas normalizadas del contrato excluyendo `source_file` y calculamos `row_number() OVER (PARTITION BY <columnas_del_viaje>) AS _occurrence`. La primera aparición (`_occurrence == 1`) pasa a `accepted.parquet` (si no viola otra regla de rechazo), y toda repetición idéntica (`_occurrence > 1`) se marca con `R07 = True` y va a `rejected.parquet`. Además, leemos los Parquet con `hive_partitioning=false` para que DuckDB no agregue columnas virtuales `year` o `month` al `SELECT *`.

### P6. ¿Por qué en la Pregunta 3 aparece `Rikers Island` en primer lugar con 100 % de viajes con `cbd_congestion_fee > 0` si Rikers Island no está en Manhattan?
**Respuesta:** Porque en todo junio de 2025 hubo **un único viaje** aceptado que inició en `Rikers Island` (`trips_in_zone = 1`), y ese viaje terminó o atravesó el Distrito Central de Negocios de Manhattan pagando el recargo (`trips_with_fee = 1`, proporción `1/1 = 100 %`). La consulta muestra tanto el porcentaje (`pct_zone_with_cbd_fee`) como el numerador y denominador (`trips_with_fee` y `trips_in_zone`) justamente para transparentar ese efecto de tamaño muestral frente a las cuatro zonas siguientes (`Greenwich Village South`, `West Village`, `SoHo`, `Greenwich Village North`), que superan los 33.000 a 88.000 viajes cada una con ~99,8 %.

### P7. ¿Cómo cumple el pipeline el límite de 8 GB de RAM cuando procesa casi 7,9 millones de filas?
**Respuesta:**
1. Configuramos `duckdb.connect(config={"memory_limit": "2GB", "threads": "2", "temp_directory": str(tmp), "preserve_insertion_order": "false"})`.
2. Nunca convertimos tablas a `pandas` ni a listas de Python.
3. En [`src/model.py`](../src/model.py) usamos una vista temporal (`TEMP VIEW _curated`) sobre los Parquet curados y generamos `trip_id` en streaming con `row_number() OVER ()`, evitando ordenar globalmente 7,49 millones de filas con listas anidadas (`warning_flags`). En nuestras pruebas medidas, el pico de Working Set (RSS) del proceso fue de ~`2.008 MB` (~`2,01 GB`).
