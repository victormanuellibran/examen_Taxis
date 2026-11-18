# Pipeline NYC Taxi

Pipeline batch reproducible para los viajes Yellow Taxi de NYC TLC. Procesa el mes indicado y el mismo mes de 2024, desde la descarga original hasta la calidad, la capa curada, el modelo estrella y las preguntas de negocio.

## Instalación y ejecución

Requiere Python 3.10 o superior. Desde PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
```

También se puede usar `make all MES=2025-06`. Para validar sólo las partes de ingesta, esquema y calidad: `python -m src.pipeline --mes 2025-06 --hasta-calidad`.
Los datos se descargan en `data/` y están excluidos de Git. `NYC_TAXI_DATA_DIR` permite elegir otra ubicación para datos.

## Arquitectura y granularidad del modelo

El orquestador ejecuta ingesta, contrato de esquema, calidad y cuarentena para `2024-06` y `2025-06`; luego escribe `data/curated/yellow/year=YYYY/month=MM/part-000.parquet` y construye `data/warehouse/nyc_taxi.duckdb`. DuckDB consulta Parquet directamente, usa hasta 2 GB de memoria y 2 threads, y derrama temporales en `data/tmp`.

La capa curada conserva una fila por viaje aceptado. Agrega duración en minutos (`duration_minutes`), velocidad promedio en mph (`average_speed_mph`), fecha, hora y día de semana de subida (`pickup_date`, `pickup_hour`, `pickup_day_of_week`), porcentaje de propina sobre tarifa (`tip_pct_fare`) y los atributos de zona de origen y destino (`pickup_borough`, `pickup_zone`, `dropoff_borough`, `dropoff_zone`).

Granularidad de las tablas del modelo estrella en DuckDB:
- `fact_trips`: 1 fila por viaje aceptado (`7.493.698` filas entre `2024-06` y `2025-06`).
- `dim_zone`: 1 fila por zona del catálogo oficial TLC (`265` filas, clave `zone_id`).
- `dim_date`: 1 fila por fecha calendario de subida (`60` filas para junio 2024 y junio 2025, clave `date_key` `YYYYMMDD`).
- `agg_zone_hour`: 1 fila por combinación de zona de origen (`pickup_zone_id`), fecha (`pickup_date`) y hora de subida (`pickup_hour`) (`210.698` filas; suma de `trips` = `7.493.698`).

## Idempotencia y aislamiento de particiones (conteos antes y después)

Reprocesar un mes en la capa curada o ejecutar nuevamente el pipeline completo reemplaza atómicamente su `part-000.parquet` y recrea las tablas del modelo sin duplicar registros ni alterar otras particiones:

| Partición / Tabla | Filas Leídas (Raw) | Aceptadas / Curadas (Ejecución 1) | Aceptadas / Curadas (Ejecución 2) | Cuarentena (Ejecución 1 y 2) |
|---|---:|---:|---:|---:|
| `year=2024/month=06` | 3.539.193 | 3.441.465 | 3.441.465 | 97.728 |
| `year=2025/month=06` | 4.322.960 | 4.052.233 | 4.052.233 | 270.727 |
| `fact_trips` (total) | 7.862.153 | 7.493.698 | 7.493.698 | 368.455 |

Además, el test automatizado `tests/integration/test_integration_pipeline.py::test_partition_isolation_on_reprocess` verifica que al reprocesar `2025-06`, el archivo `data/curated/yellow/year=2024/month=06/part-000.parquet` conserva intactos sus bytes y su timestamp de modificación (`st_mtime_ns`).

## Decisiones, recursos medidos y limitaciones

El contrato versionado está en `contracts/yellow_schema.yaml`. `cbd_congestion_fee` se completa como `NULL` (`DOUBLE`) para 2024 porque el cargo no existía en ese año y usar `0` distorsionaría la distinción entre ausencia del tributo y un viaje exento en vigencia del régimen; las columnas nuevas no contempladas hacen fallar el esquema y se reportan en `data/reports/schema_YYYY-MM.json`. Las ocho reglas (`R01` a `R08`), umbrales, severidades, resultados observados y políticas de nulos están documentados en `docs/decisiones_parte_a.md`.

La exploración real encontró propinas negativas (`R08`, 108 en 2024-06 y 367 en 2025-06): se marcan como advertencia en `warning_flags`. Los viajes con pasajeros nulos o cero (`R05`) también se advierten. La reconciliación exige `rows_read == rows_accepted + rows_quarantined`. La ingesta registra SHA-256, bytes, filas, URL, timestamps UTC y estado en `data/control/ingestion_log.csv`.

En la ejecución local del pipeline completo para junio de 2024 y junio de 2025 (`7.862.153` filas leídas), reutilizando los archivos raw descargados:
- **Medición de QA de referencia:** ~`50,1`–`50,9` segundos por corrida completa de las 5 etapas y ~`2,01 GB` de Working Set/RSS (`2008 MB`), medidos en el worktree de QA antes de integrar sus correcciones.
- **Ejecución posterior a la optimización:** la corrida actual terminó en aproximadamente `33,2` segundos según sus timestamps. No se presenta un nuevo pico de RAM para esta corrida porque no se obtuvo una medición equivalente; DuckDB conserva `memory_limit='2GB'`, `threads='2'` y `preserve_insertion_order='false'`, con temporales en `data/tmp`.

## Resultados, tests y declaración de uso de IA

`RESULTADOS.md` se genera automáticamente al ejecutar `src.model` e incluye las seis consultas SQL de `sql/`, sus tablas de resultados y su interpretación. La suite de `pytest` cuenta con **17 tests** (unitarios e integración extremo a extremo en `tests/` y `tests/integration/`) que cubren reglas de calidad sobre datos sintéticos, idempotencia de ingesta y modelo, aislamiento de particiones, reintentos de red, drift de esquema, reconciliación de conteos y validación de consultas de negocio.

**Declaración de uso de IA:** Durante el desarrollo y la auditoría de integración se utilizaron asistentes de IA (Codex y Gemini / Antigravity) para estructurar el contrato YAML, asistir en la redacción de consultas SQL/DuckDB, diseñar pruebas automatizadas de integración y auditar la reconciliación de datos; todas las decisiones de diseño, reglas y resultados fueron verificados sobre los datos reales para su defensa oral.
