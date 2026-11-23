# Guion de Defensa Oral y Preguntas Técnicas — Pipeline NYC Taxi

Este documento organiza la defensa oral del examen para los dos integrantes del equipo. Incluye:
1. **Protocolo de demostración corta en vivo** (sin volver a descargar los millones de filas desde Internet).
2. **Guion de exposición oral dividido equitativamente** entre Integrante 1 (Parte A, dbt, Docker y CI) e Integrante 2 (Parte B, Modelo Estrella, Negocio, Auditoría SQL/Rendimiento y orquestación con Dagster).
3. **Banco de 23 preguntas técnicas probables de la cátedra** con respuestas claras, precisas y fundamentadas en la implementación real.

---

## 1. Protocolo de Demostración Corta en Vivo (PowerShell)

La demostración está diseñada para ejecutarse en **menos de 2 minutos** reutilizando los archivos Parquet oficiales ya presentes en `data/raw/` (validados por hash SHA-256 sin tráfico de red).

### Paso 1: Ejecutar la suite de 37 pruebas automatizadas (`pytest`, ~2 segundos)
```powershell
.\.venv\Scripts\python.exe -m pytest -v
```
*Qué explicar al tribunal:*
- Pasan los **37 tests** (unitarios, parametrización dinámica de meses, selector excluyente de motor, orquestación Dagster y pruebas de integración extremo a extremo) usando datos sintéticos en directorios temporales sin tocar `data/`.

### Paso 2: Correr el pipeline completo y demostrar idempotencia (~31–38 segundos)
```powershell
# Corrida con motor Python (o agregando --modelo dbt para correr con dbt build 32/32)
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --modelo dbt
```
*Qué señalar en la consola:*
- En la etapa 1 (`src.ingest`), ambos archivos mensuales (`2024-06` y `2025-06`) y `taxi_zone_lookup.csv` muestran `reused` porque su hash SHA-256 coincide con `data/control/ingestion_log.csv`. No se vuelve a descargar nada.
- En la etapa 3 (`src.quality`), se informa la reconciliación exacta de ambos meses.
- En el modo `dbt`, `dbt build` ejecuta **32 tareas (`PASS=32`)**: 5 tablas, 3 vistas y **24 data tests**.

### Paso 3: Comprobar que los conteos de ambos warehouses no se duplican
```powershell
@'
import duckdb
for name, path in [("python", "data/warehouse/nyc_taxi.duckdb"), ("dbt", "data/warehouse/nyc_taxi_dbt.duckdb")]:
    con = duckdb.connect(path, read_only=True)
    counts = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ["dim_zone", "dim_date", "fact_trips", "agg_zone_hour"]}
    uniq = con.execute("SELECT count(*) = count(DISTINCT trip_id) FROM fact_trips").fetchone()[0]
    agg_sum = con.execute("SELECT sum(trips) FROM agg_zone_hour").fetchone()[0]
    print(f"[{name}] {counts} | trip_id_unico={uniq} | sum_agg_trips={agg_sum}")
    con.close()
'@ | .\.venv\Scripts\python.exe -
```
*Salida esperada en ambos motores:* `dim_zone: 265`, `dim_date: 60`, `fact_trips: 7493698`, `agg_zone_hour: 210698`, `trip_id_unico=True`, `sum_agg_trips=7493698`.

### Paso 4: Mostrar el reporte de calidad y una fila real en cuarentena con sus reglas violadas
```powershell
Get-Content data/reports/quality_2025-06.json
```
Y consultar directamente una fila de `rejected.parquet`:
```powershell
@'
import duckdb
con = duckdb.connect()
rows = con.execute("""
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, total_amount, tip_amount,
           source_file, violated_rules, rejection_flags, warning_flags
    FROM read_parquet('data/quarantine/yellow/year=2025/month=06/rejected.parquet', hive_partitioning=false)
    WHERE len(rejection_flags) >= 2
    LIMIT 3
""").fetchall()
for r in rows:
    print(r)
'@ | .\.venv\Scripts\python.exe -
```

### Paso 5: Mostrar Dagster como orquestador declarativo (~1 minuto)
```powershell
.\.venv\Scripts\dagster.exe dev -m src.orchestration
```
*Qué señalar en la interfaz (`http://127.0.0.1:3000`):*
- En **Overview → Jobs → `pipeline_nyc_taxi`**, mostrar el DAG de nueve nodos y señalar que `modelo_estrella` depende de ambas transformaciones.
- En **Launchpad**, lanzar una ejecución con `mes: "2025-06"` y `modelo: "python"`. La ingesta sale como `reused` (archivos cacheados por SHA-256).
- Abrir la etapa `calidad_asignado` y mostrar los metadatos: filas leídas, aceptadas, en cuarentena y violaciones por regla, directamente en la interfaz.
- Para demostrar manejo de errores: lanzar con `mes: "2025-13"`. La ingesta falla con `Mes inválido` y las etapas dependientes quedan sin ejecutar.
- Cerrar con `Ctrl+C` (no queda ningún servicio corriendo de fondo).

*Qué explicar al tribunal:*
- Dagster no procesa datos: DuckDB sigue haciendo todo el cómputo. Dagster solo coordina el orden y registra los resultados.
- El pipeline tradicional (`src/pipeline.py`) sigue funcionando sin cambios; Dagster es una capa adicional.

### Paso 6 (Opcional): Mostrar ejecución en Docker o cambio dinámico a Mayo (`2025-05`)
```powershell
docker compose run --rm pipeline python -m pytest -q
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-05 --modelo dbt
```

---

## 2. Guion de Exposición Oral Dividido entre Dos Integrantes (12 a 15 minutos)

### Bloque 1 — Integrante 1 (Víctor Manuel Libran): Arquitectura, Ingesta, Esquema, Calidad, dbt, Docker y CI (6–7 min)

1. **Problema, arquitectura general y presupuesto de 8 GB de RAM (1 min):**
   - "Desarrollamos un pipeline batch reproducible para comparar cualquier mes publicado por NYC TLC contra el mismo mes del año anterior (por defecto junio de 2025 contra junio de 2024, con 7,86 millones de registros crudos, y también verificado con mayo de 2025 contra mayo de 2024, con 8,31 millones de registros)."
   - "Para respetar la restricción de 8 GB de RAM, en [`src/config.py`](../src/config.py) diseñamos el flujo sin cargar datasets en `pandas`: Python sólo coordina y delega el procesamiento a DuckDB configurado con `memory_limit='2GB'`, `threads='2'` y spill a disco en `data/tmp`."
2. **Etapa 1 — Ingesta idempotente y control SHA-256 ([`src/ingest.py`](../src/ingest.py)) (1,5 min):**
   - "La función `run_ingest(month)` descarga el Parquet mensual y el catálogo de zonas en streaming de a 1 MiB hacia un temporal `.part`, con hasta 3 reintentos exponenciales (1, 2 y 4 segundos) ante fallas de red y manejo explícito de HTTP 404 si se pide un mes aún no publicado."
   - "Valida la estructura con PyArrow antes de renombrar `.part`, calcula el SHA-256 incrementalmente y lo registra en `data/control/ingestion_log.csv`. En corridas sucesivas, si el hash local coincide con el último registro `ok`, reutiliza el archivo (`reused`) sin tráfico de red."
3. **Etapa 2 — Contrato YAML y cambio de esquema 2024 vs 2025 ([`src/schema.py`](../src/schema.py)) (1,5 min):**
   - "El contrato [`contracts/yellow_schema.yaml`](../contracts/yellow_schema.yaml) define las 20 columnas en `snake_case`. En 2024 no existía `cbd_congestion_fee`; nuestra política `fail_except_cbd_before_2025` la incorpora como `CAST(NULL AS DOUBLE)`. Elegimos `NULL` y no `0` porque en 2024 el peaje no existía legalmente. Si mañana aparece una columna nueva no declarada, el pipeline reporta el drift en `schema_YYYY-MM.json` y se detiene (`fail_and_report`)."
4. **Etapa 3 — Ocho reglas de calidad, cuarentena y doble reconciliación ([`src/quality.py`](../src/quality.py)) (1,5 min):**
   - "Clasificamos cada fila en una sola pasada SQL evaluando 8 reglas: 6 de rechazo (`R01` fecha fuera del mes, `R02` bajada <= subida, `R03` distancia <= 0 o > 200 millas, `R04` total negativo, `R06` zona inexistente en catálogo y `R07` duplicados exactos) y 2 de advertencia (`R05` pasajeros nulos o <= 0 y `R08` propina negativa, descubierta explorando los datos reales)."
   - "Verificamos `leídas == aceptadas + cuarentena` tanto en memoria como re-contando los Parquet escritos: en junio de 2024, `3.539.193 = 3.441.465 + 97.728`; en junio de 2025, `4.322.960 = 4.052.233 + 270.727`."
5. **Integración de dbt, Docker Compose y GitHub Actions (1,5 min):**
   - "Además del modo Python, integramos **dbt Core + `dbt-duckdb`** (`--modelo dbt`), que construye `staging` e `intermediate` como vistas sobre la capa curada y materializa los `marts` como tablas en un warehouse separado (`nyc_taxi_dbt.duckdb`), validando **24 pruebas de datos** (`dbt build 32/32`)."
   - "Empaquetamos todo en un contenedor batch con **Docker Compose** (`mem_limit: 4g`, montando `./data:/app/data`) y configuramos **GitHub Actions** ([`.github/workflows/tests.yml`](../.github/workflows/tests.yml)) para correr `pip check`, los 37 tests de `pytest`, `dbt parse` y `dbt compile`."

---

### Bloque 2 — Integrante 2 (Carlos Federico Lix Klett): Transformación, Modelo Estrella, Consultas SQL, Auditoría de Rendimiento y Dagster (7–8 min)

1. **Etapa 4 — Capa curada y aislamiento de particiones Hive ([`src/transform.py`](../src/transform.py)) (1,5 min):**
   - "En `run_transform` tomamos `accepted.parquet`, calculamos duración en minutos, velocidad en mph, fecha, hora, día de semana y porcentaje de propina sobre tarifa (cuando `fare_amount > 0`), y enriquecemos origen y destino con el catálogo de zonas."
   - "Escribimos en `data/curated/yellow/year=YYYY/month=MM/part-000.parquet` con reemplazo atómico. Reprocesar un mes actualiza únicamente su partición y deja intactas las demás, como lo comprueba nuestro test de aislamiento."
2. **Etapa 5 — Modelo estrella y decisiones de memoria/claves ([`src/model.py`](../src/model.py)) (2 min):**
   - "El modelo estrella tiene `dim_zone` (265 zonas), `dim_date` (60 fechas en junio / 62 en mayo), `fact_trips` (7.493.698 viajes aceptados en junio / 7.914.540 en mayo) y la tabla agregada `agg_zone_hour` (210.698 grupos horarios en junio cuya suma de `trips` reconstituye exactamente los 7.493.698 viajes)."
   - "Para no superar los 2 GB de DuckDB al procesar 7,5 millones de filas con la lista `warning_flags`, proyectamos `fact_trips` desde una vista sobre los Parquet curados y asignamos `trip_id` en streaming con `row_number() OVER ()`. Documentamos en la auditoría técnica que ese `trip_id` es 100 % único en cada corrida, aunque no es una clave histórica persistente si cambiara el orden físico de lectura."
3. **Resultados de las 6 preguntas de negocio y hallazgos de auditoría SQL ([`sql/`](../sql/) y [`RESULTADOS.md`](../RESULTADOS.md)) (2,5 min):**
   - "**Q1 (Top 10 zonas):** en la auditoría técnica detectamos que en el catálogo TLC hay zonas con distinto `LocationID` pero igual nombre (`Corona` y `Governor's Island...`). Corregimos `q1_top_zones.sql` para agrupar y unir por `(pickup_zone_id, pickup_zone)`. Con `EXPLAIN ANALYZE` medimos que la consulta pasó de `121,4 ms` a `138,9 ms` (+14,4 %), manteniendo la semántica correcta. Lidera `JFK Airport` con 170.662 viajes (+12,15 %)."
   - "**Q2 (Hora de mayor ingreso medio):** las `05:00` h con `$32,5708` por viaje (33.314 viajes)."
   - "**Q3 (Cargo CBD):** en junio de 2025 el **73,73 %** de los viajes aceptados (2.987.770 de 4.052.233) tiene `cbd_congestion_fee > 0`. En el top 5 por proporción aparece primero `Rikers Island` con 100 % porque tuvo 1 solo viaje (`1/1`), seguido por zonas del CBD de Manhattan (`Greenwich Village South`, `West Village`, `SoHo`, `Greenwich Village North`) con ~99,8 % sobre más de 33.000 a 88.000 viajes. Por eso destacamos la importancia de leer el porcentaje junto con su denominador."
   - "**Q4 (Propina por tipo de pago):** tarjeta de crédito (`payment_type = 1`) promedia **25,26 %**; efectivo (`payment_type = 2`, 0,002 %) no es interpretable porque las propinas en efectivo no se registran en el taxímetro."
   - "**Q5 (Aeropuertos JFK y LaGuardia):** tras corregir en QA el filtro a `'%laguardia%'` (sin espacio), obtuvimos **334.938 viajes** (`$56,2601` de tarifa media) en 2024-06 y **346.769 viajes** (`$57,7605`) en 2025-06."
   - "**Q6 (Variación interanual):** los viajes aceptados subieron **+17,75 %** (+610.768) y la tarifa media varió **-$0,1588** (`$19,8668` a `$19,7080`). Como pide la consigna, planteamos una hipótesis de causa: más viajes *Flex Fare* (`payment_type = 0`), más trayectos urbanos cortos y desvío de viajes hacia el taxi tras el peaje CBD de 2025. La presentamos como hipótesis, no como conclusión: dos cortes mensuales no alcanzan para confirmarla sin controlar estacionalidad, oferta de vehículos y otros modos de transporte (mediana en `EXPLAIN ANALYZE` de `54,86 ms`)."

4. **Orquestación declarativa con Dagster ([`src/orchestration.py`](../src/orchestration.py)) (1,5 min):**
   - "Además del pipeline en Python, integramos **Dagster 1.13.31** como orquestador declarativo. El grafo tiene nueve nodos: dos cadenas de cuatro etapas (ingesta → esquema → calidad → transformación, una por período) que convergen en `modelo_estrella`, que necesita ambas capas curadas para comparar."
   - "Cada `@op` es un envoltorio de tres líneas sobre las funciones que ya existían (`run_ingest`, `run_schema`, etc.). No hay lógica de negocio nueva: Dagster solo coordina el orden y registra metadatos (filas leídas, aceptadas, en cuarentena, violaciones por regla)."
   - "Usamos `in_process_executor` deliberadamente: DuckDB toma un lock exclusivo por archivo, y dos cadenas en paralelo duplicarían el pico de memoria. No usamos `dagster-dbt` porque duplicaría el grafo que dbt ya resuelve internamente."
   - "No reemplaza al pipeline tradicional: hay un test que verifica que `src.pipeline` sigue devolviendo 0."

---

## 3. Banco de 23 Preguntas Técnicas Probables y Respuestas de Defensa

### 1. ¿El pipeline sigue un enfoque ETL o ELT?
**Respuesta:** Combina ambos de forma pragmática (un patrón *EtLT* sobre Data Lakehouse local). Las etapas 1 a 4 actúan como un **ETL de limpieza y curación física** hacia Parquet (`raw` $\rightarrow$ `staging` $\rightarrow$ `validated`/`quarantine` $\rightarrow$ `curated`) para garantizar que ningún dato corrupto ingrese al modelo analítico. Luego, la etapa 5 (especialmente en `--modelo dbt`) aplica **ELT dentro de DuckDB**: expone los Parquet curados como fuentes externas (`source()`) y ejecuta las transformaciones dimensionales (`staging` $\rightarrow$ `intermediate` $\rightarrow$ `marts`) directamente en SQL dentro del motor DuckDB.

### 2. ¿Por qué eligieron Apache Parquet en lugar de CSV para las capas intermedias?
**Respuesta:** Por tres razones técnicas:
1. **Almacenamiento columnar y compresión (`ZSTD`):** permite leer sólo las columnas necesarias en cada consulta y reduce drásticamente el espacio en disco y el I/O.
2. **Tipado fuerte nativo:** conserva tipos exactos (`TIMESTAMP`, `BIGINT`, `DOUBLE` y listas anidadas `LIST<VARCHAR>` como `warning_flags` y `violated_rules`), mientras que CSV obligaría a re-parsear texto en cada etapa.
3. **Metadatos en el footer:** permite verificar la cantidad de filas con `pq.ParquetFile(path).metadata.num_rows` en milisegundos sin escanear el archivo completo.

### 3. ¿Qué es DuckDB y por qué es ideal para una restricción de 8 GB de RAM?
**Respuesta:** DuckDB es un motor de base de datos SQL analítico columnar embebido (*in-process OLAP*). A diferencia de PostgreSQL, no requiere un servidor separado; corre dentro del mismo proceso y ejecuta consultas vectorizadas en C++ directamente sobre archivos Parquet. Además, permite fijar un techo duro de memoria (`memory_limit='2GB'`), limitar hilos (`threads='2'`) y volcar bloques intermedios a disco (`temp_directory='data/tmp'`), evitando errores de *Out-Of-Memory*.

### 4. ¿Cuál es la granularidad de cada tabla del modelo estrella y por qué no deben confundirse las filas de `agg_zone_hour` con el total de viajes?
**Respuesta:**
- `fact_trips`: 1 fila por viaje aceptado (`7.493.698` en junio).
- `dim_zone`: 1 fila por zona oficial TLC (`265` zonas).
- `dim_date`: 1 fila por día calendario de subida (`60` días en junio; `62` en mayo).
- `agg_zone_hour`: 1 fila por combinación única `(pickup_zone_id, pickup_date, pickup_hour)` en la que hubo al menos un viaje. En junio tiene **`210.698` filas** (grupos horarios), y la suma de su columna `trips` (`SUM(trips)`) da exactamente **`7.493.698`**, que reconcilia con `COUNT(*)` de `fact_trips`.

### 5. ¿Cómo funcionan las reglas de calidad y por qué una fila rechazada no se borra simplemente con un `WHERE`?
**Respuesta:** Borrar filas inválidas con un `WHERE` silencioso impide auditar qué datos se descartaron y por qué. En [`src/quality.py`](../src/quality.py) evaluamos las 8 reglas (`R01`–`R08`) sobre cada fila: las que sólo tienen advertencias (`R05` pasajeros `<= 0` o `R08` propina negativa) pasan a `accepted.parquet` conservando sus banderas en `warning_flags`; las que violan al menos una regla de rechazo van a `rejected.parquet` con la fila original, `source_file`, `violated_rules` y `rejection_flags`.

### 6. ¿Cómo funciona la reconciliación y por qué se verifica dos veces?
**Respuesta:** La función `reconcile(read, accepted, rejected)` exige que `read == accepted + rejected`. Se verifica primero sobre la tabla temporal `classified` en DuckDB y luego se vuelve a verificar ejecutando `SELECT count(*)` directamente sobre los archivos físicos `accepted.parquet` y `rejected.parquet` recién escritos en disco. Así garantizamos que ninguna fila se haya perdido ni duplicado durante la exportación a Parquet.

### 7. ¿Qué es dbt y qué diferencia tiene con DuckDB?
**Respuesta:** **DuckDB** es el motor SQL que ejecuta las consultas y almacena las tablas en `.duckdb`. **dbt (*data build tool*)** no es un motor SQL ni una base de datos: es un framework declarativo que compila plantillas SQL/Jinja, resuelve el grafo de dependencias (DAG) entre modelos, envía las sentencias DDL/DML a DuckDB a través del adaptador `dbt-duckdb` y ejecuta pruebas automatizadas de integridad sobre las tablas resultantes.

### 8. ¿Qué diferencia hay entre `source()` y `ref()` en dbt?
**Respuesta:**
- `{{ source('taxi_external', '...') }}` apunta a datos externos al proyecto dbt (en nuestro caso, los archivos Parquet `accepted`, `curated` y el CSV `taxi_zone_lookup.csv` definidos en [`dbt_project/models/sources.yml`](../dbt_project/models/sources.yml)).
- `{{ ref('nombre_modelo') }}` referencia a otro modelo dentro de `dbt_project/models/`. Le permite a dbt construir automáticamente el grafo de linaje (DAG) y ejecutar siempre `staging` antes que `intermediate` y `intermediate` antes que `marts`.

### 9. ¿Qué diferencia hay entre `dbt build` y `dbt test`, y qué significan los resultados `32/32` y `24/24`?
**Respuesta:**
- `dbt test` ejecuta únicamente las **24 pruebas de datos** (21 tests genéricos de `schema.yml` como `not_null`, `unique` y `relationships`, más los 3 tests SQL singulares de `dbt_project/tests/`) contra las tablas ya existentes (`24/24 PASS`).
- `dbt build` ejecuta en orden topológico del DAG tanto la creación de los **8 modelos** (3 vistas y 5 tablas) como las **24 pruebas** (`8 + 24 = 32` tareas en total, `32/32 PASS`). Si un test de `staging` o de `fact_trips` fallara, `dbt build` detendría la construcción de los modelos aguas abajo.

### 10. ¿Por qué `Python` y `dbt` usan archivos DuckDB separados (`nyc_taxi.duckdb` y `nyc_taxi_dbt.duckdb`)?
**Respuesta:** Por aislamiento y auditabilidad. Mantener dos archivos independientes evita conflictos entre las vistas creadas por dbt (`stg_yellow_trips`, `int_trips_enriched`) y las tablas del modo Python, previene bloqueos de archivo y nos permitió demostrar empíricamente que ambos motores producen exactamente los mismos conteos y los mismos resultados en las 6 consultas SQL.

### 11. ¿Para qué usamos Docker y Docker Compose si el proyecto ya corre con `.venv`?
**Respuesta:** El entorno virtual `.venv` aísla paquetes de Python pero sigue dependiendo del sistema operativo anfitrión y de la versión de Python instalada en la máquina. **Docker** empaqueta el sistema base Linux (`python:3.12-slim`), el binario `git`, las versiones exactas de `requirements.txt` y el código del proyecto dentro de una imagen reproducible (`examen-nyc-taxi:latest`). **Docker Compose** ([`docker-compose.yml`](../docker-compose.yml)) simplifica la ejecución configurando variables de entorno, límites de memoria (`mem_limit: 4g`) y volúmenes con un solo comando. Es una alternativa reproducible, no una dependencia obligatoria para correr en local.

### 12. ¿Cómo funcionan los volúmenes en Docker y por qué la imagen no incluye la carpeta `data/`?
**Respuesta:** En [`.dockerignore`](../.dockerignore) excluimos `data/` para que la imagen sea liviana y no contenga cientos de megabytes de datos crudos. En [`docker-compose.yml`](../docker-compose.yml) montamos `./data:/app/data` y `./RESULTADOS.md:/app/RESULTADOS.md` como *bind mounts*. Así, el contenedor lee y reutiliza los archivos `raw` del host (verificando su SHA-256 sin re-descargarlos) y escribe los Parquet curados y los warehouses `.duckdb` directamente en la carpeta `data/` de Windows, por lo que al eliminar el contenedor (`--rm` o `docker compose down`) no se pierde ningún dato.

### 13. ¿Qué valida GitHub Actions y por qué no ejecuta `dbt build` ni descarga los Parquet de NYC TLC en la nube?
**Respuesta:** El workflow [`.github/workflows/tests.yml`](../.github/workflows/tests.yml) se dispara en cada `push` o `pull_request` sobre Ubuntu con Python 3.12 y ejecuta `pip check`, `pytest -q` (los 37 tests con datos sintéticos), `dbt parse` y `dbt compile`. No descarga los Parquet reales (~133 MB por par de meses) ni corre `dbt build` en los runners de GitHub porque eso demoraría varios minutos y consumiría ancho de banda externo en cada commit; las pruebas sobre datos reales se ejecutan localmente o en el contenedor Docker con `./data` persistente.

### 14. ¿Qué mostró el análisis con `EXPLAIN ANALYZE` sobre las consultas SQL?
**Respuesta:** En [`docs/RENDIMIENTO.md`](../docs/RENDIMIENTO.md) inspeccionamos los planes físicos de DuckDB sobre los 7,49 millones de viajes de `fact_trips`:
- En **Q1**, el plan realiza un `TABLE_SCAN` de `fact_trips`, dos `HASH_GROUP_BY` (para el mes asignado y el mes de comparación), un `HASH_JOIN` y un `TOP_N` (`LIMIT 10`), tardando **`138,9 ms`** (mediana).
- En **Q6**, realiza un `TABLE_SCAN`, `HASH_GROUP_BY` por mes y `HASH_JOIN` contra `analysis_parameters`, tardando **`54,9 ms`**.
Esto confirma que para el volumen del examen los escaneos columnares vectorizados de DuckDB resuelven las agregaciones en ~0,05–0,14 segundos sin necesidad de índices secundarios que encarecerían la escritura de `fact_trips`.

### 15. ¿Cómo distinguen entre límites de memoria configurados y memoria realmente observada, y qué implica haber medido en una PC de 16 GB?
**Respuesta:**
- **Límites configurados:** DuckDB tiene un tope estricto de `memory_limit='2GB'` (con spill en `data/tmp`) y el contenedor Docker tiene `mem_limit: 4g`.
- **Consumos observados:** en ejecución local (`.venv`), el pico de *Working Set* del árbol de procesos fue de **`~2.023 MB`** (modo Python, `31,1 s`) y **`~2.001 MB`** (modo dbt, `37,9 s`). En Docker (`docker stats`), el contenedor alcanzó un pico de **`1,851 GiB`** (Python, `45,16 s`) y **`1,872 GiB`** (dbt, `53,15 s`).
- **Transparencia sobre el hardware:** las pruebas se corrieron en una máquina con 16 GB de RAM física. Los números comprueban que el proceso del pipeline consume ~2 GB, pero en una notebook física de 8 GB debe sumarse la RAM que usan Windows y la VM de WSL 2 si se abre Docker Desktop; por eso mantenemos siempre disponible la ejecución local nativa con `.venv`.

### 16. ¿Cómo funciona la selección dinámica de meses (`--mes YYYY-MM`)?
**Respuesta:** `parse_month(args.mes)` valida el formato `YYYY-MM` y calcula automáticamente la lista `[f"{year - 1:04d}-{month:02d}", args.mes]`. Esos dos períodos se pasan a cada etapa, se guardan en la tabla `analysis_parameters` (que todas las consultas `q1..q6.sql` leen mediante `WITH p AS (SELECT * FROM analysis_parameters)`), se inyectan como `--vars` JSON en `dbt build` y se usan en [`src/results.py`](../src/results.py) para redactar las interpretaciones sin ningún mes fijo en el código. Lo comprobamos corriendo tanto junio (`2025-06` vs `2024-06`) como mayo (`2025-05` vs `2024-05`).

### 17. ¿Por qué fue necesario corregir `q1_top_zones.sql` para agrupar por `pickup_zone_id` y `pickup_zone`?
**Respuesta:** Porque en `taxi_zone_lookup.csv` el nombre de zona (`Zone`) no es una clave única: por ejemplo, `"Corona"` aparece dos veces (`LocationID` 56 y 57) y `"Governor's Island/Ellis Island/Liberty Island"` aparece tres veces (`LocationID` 103, 104 y 105). Agrupar únicamente por el texto `pickup_zone` fusionaba zonas distintas bajo un mismo grupo. Al agrupar y hacer el `LEFT JOIN` por `(pickup_zone_id, pickup_zone)`, respetamos la clave primaria de `dim_zone`. En junio el Top 10 devuelto es idéntico, con un costo medido de apenas `+17,5 ms` (`121,4 ms` $\rightarrow$ `138,9 ms`).

### 18. ¿Cómo debe interpretarse el resultado de `q3_cbd_fee.sql` donde `Rikers Island` aparece con 100 %?
**Respuesta:** En `q3_cbd_fee.sql` se pide ordenar las 5 zonas de origen con mayor proporción de viajes con `cbd_congestion_fee > 0`. En junio de 2025, `Rikers Island` tuvo **un solo viaje aceptado** en todo el mes (`trips_in_zone = 1`) y ese viaje pagó el cargo (`trips_with_fee = 1`), dando `100 %`. Por eso la consulta incluye las columnas `trips_with_fee` y `trips_in_zone`: permite ver de inmediato que `Rikers Island` es un caso de denominador `1`, mientras que las cuatro zonas siguientes (`Greenwich Village South`, `West Village`, `SoHo` y `Greenwich Village North`) tienen entre `33.907` y `88.077` viajes cada una con ~`99,8 %` de incidencia real dentro del CBD.

### 19. ¿Qué mide exactamente `q6_month_variation.sql` y por qué no demuestra causalidad?
**Respuesta:** `q6_month_variation.sql` mide la variación interanual sobre los **viajes aceptados** en `fact_trips` (los que superaron las reglas de calidad `R01`–`R08`): entre junio de 2024 (`3.441.465` viajes, tarifa media `$19,8668`) y junio de 2025 (`4.052.233` viajes, tarifa media `$19,7080`), el volumen aceptado subió `+17,75 %` (`+610.768` viajes) y la tarifa media bajó `-$0,1588`. Aunque formulamos una hipótesis plausible (expansión de viajes *Flex Fare* `payment_type = 0` y mayor proporción de viajes cortos tras el peaje CBD), enfatizamos que una comparación descriptiva de dos meses no demuestra causalidad estadística sin aislar tendencias plurianuales, clima, oferta de licencias y otros medios de transporte.

### 20. ¿Qué es Dagster y qué aporta sobre el pipeline en Python?
**Respuesta:** Dagster es un orquestador declarativo de pipelines de datos. El pipeline ya corría de punta a punta con [`src/pipeline.py`](../src/pipeline.py), que llama a las cinco etapas en secuencia. Dagster aporta tres cosas: (1) **el grafo de dependencias es explícito y verificable** — es un DAG de nueve nodos que se puede inspeccionar, testear y dibujar, en lugar de un orden implícito en las líneas de un `for`; (2) **observabilidad por etapa** — cada op registra metadatos (filas leídas, aceptadas, en cuarentena, violaciones por regla) visibles en la interfaz web; (3) **reejecución parcial** — si falla la etapa 4, se puede relanzar desde ahí sin repetir la ingesta. Dagster no procesa datos: DuckDB y dbt siguen haciendo todo el cómputo.

### 21. ¿Por qué usaron `in_process_executor` y no ejecución paralela?
**Respuesta:** Por dos razones técnicas: (1) DuckDB toma un lock exclusivo por archivo `.duckdb` durante la escritura, por lo que dos escritores simultáneos fallarían; (2) aunque las dos cadenas del grafo son independientes, correrlas en paralelo duplicaría el pico de memoria (dos instancias de DuckDB con `memory_limit=2GB` cada una), excediendo potencialmente la restricción de 8 GB. Con ejecución secuencial, el perfil de memoria es el mismo que el del pipeline tradicional.

### 22. ¿Por qué no usaron `dagster-dbt` para integrar dbt?
**Respuesta:** `dagster-dbt` expone cada modelo dbt como un asset de Dagster, lo que duplicaría el grafo: los modelos dbt ya tienen su propio DAG declarado en `dbt_project/` resuelto por `source()` y `ref()`. Como [`src/dbt_runner.py`](../src/dbt_runner.py) ya encapsula `dbt build` correctamente y propaga el código de salida, Dagster lo invoca como una etapa más. Si en el futuro se quisiera ver los modelos dbt nodo por nodo en la interfaz de Dagster, `dagster-dbt` sería el camino, con el costo de complejidad que implica.

### 23. ¿Es `fact_trips.trip_id` único y estable entre reconstrucciones?
**Respuesta:**
- **Unicidad:** Sí, `trip_id` es **100 % único** dentro de cada construcción del warehouse (`COUNT(*) == COUNT(DISTINCT trip_id) == 7.493.698`), verificado tanto por script como por el test `unique_fact_trips_trip_id` de dbt.
- **Estabilidad entre reconstrucciones:** Como se genera mediante `row_number() OVER ()` sin un `ORDER BY` global (y con `preserve_insertion_order=false` para no desbordar los 2 GB de RAM ordenando 7,5 millones de filas con listas anidadas), actúa como una **clave subrogada técnica de la corrida**. Si en una reconstrucción futura DuckDB lee los bloques Parquet en distinto orden de hilos, el número asignado a un viaje individual puede cambiar sin alterar ningún conteo, relación con dimensiones ni resultado de las 6 consultas. Si en un sistema productivo se requiriera una clave persistente entre corridas, se podría calcular un hash determinista (`md5` o `xxhash64`) sobre las columnas naturales del viaje.
