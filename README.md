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

## Arquitectura

El orquestador ejecuta ingesta, contrato de esquema, calidad y cuarentena para 2024-06 y 2025-06; luego escribe `data/curated/yellow/year=YYYY/month=MM/part-000.parquet` y construye `data/warehouse/nyc_taxi.duckdb`. DuckDB consulta Parquet directamente, usa hasta 2 GB y 2 threads, y derrama temporales en `data/tmp`.

La capa curada conserva una fila por viaje aceptado. Agrega duración en minutos, velocidad promedio en mph, fecha, hora y día de semana de subida, porcentaje de propina sobre tarifa y los atributos de zona de origen y destino. Reprocesar un mes reemplaza su único `part-000.parquet`; no modifica las particiones de otros meses.

El modelo estrella tiene `fact_trips` con granularidad de un viaje aceptado, `dim_zone` con una fila por zona TLC, `dim_date` con una fila por fecha de subida y `agg_zone_hour` con una fila por zona de origen, fecha y hora.

## Decisiones y limitaciones

El contrato versionado está en `contracts/yellow_schema.yaml`. `cbd_congestion_fee` es NULL para 2024; las columnas nuevas no contempladas hacen fallar el esquema y se reportan. Las ocho reglas, umbrales, severidades, resultados observados y políticas de nulos están documentados en `docs/decisiones_parte_a.md`.

La exploración real encontró propinas negativas: se marcan como advertencia. Los viajes con pasajeros nulos o cero también se advierten. La reconciliación exige filas leídas = aceptadas + cuarentena. La ingesta registra SHA-256, bytes, filas, URL, timestamps UTC y estado en `data/control/ingestion_log.csv`.

En la ejecución local de junio de 2024 y junio de 2025, la parte A tardó aproximadamente 15 segundos después de la descarga inicial y procesó más de 7,8 millones de filas sin cargar el dataset completo en pandas. No se midió el RSS máximo del proceso; DuckDB tiene un límite configurado de 2 GB.

## Resultados y tests

`RESULTADOS.md` se genera al ejecutar el modelo e incluye las seis consultas SQL de `sql/`, sus resultados y una interpretación breve. Los tests cubren calidad sintética, idempotencia, reintentos, drift de esquema, reconciliación y contratos del orquestador.

El asistente utilizado durante el desarrollo fue Codex. Se utilizó para inspeccionar el contrato, escribir y revisar código, preparar pruebas y ejecutar verificaciones; la implementación debe poder explicarse en la defensa oral.
