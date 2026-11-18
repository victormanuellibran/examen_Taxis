# Informe de pruebas final

Guía de verificación del repositorio `examen_Taxis` para PowerShell y los períodos `2024-06` y `2025-06`.

## Obtener el código

```powershell
git clone https://github.com/victormanuellibran/examen_Taxis.git
Set-Location examen_Taxis
git branch --show-current
git log -1 --oneline
```

La rama debe ser `main` y el commit debe ser `0e63429` o posterior.

## Preparar Python

Ejecutar desde la raíz del repositorio, no desde `C:\Windows\System32`:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Si `python` no está disponible:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Resultado esperado: `17 passed`.

## Pipeline completo

No ejecutar dos procesos simultáneamente sobre el mismo `data/warehouse/nyc_taxi.duckdb`.

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
```

Debe terminar con `Finalizó el pipeline completo para ['2024-06', '2025-06']`.

Conteos reales de referencia:

| Mes | Leídas | Aceptadas | Cuarentena |
|---|---:|---:|---:|
| 2024-06 | 3.539.193 | 3.441.465 | 97.728 |
| 2025-06 | 4.322.960 | 4.052.233 | 270.727 |

## Sólo Parte A

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --hasta-calidad
```

Debe terminar con `Finalizó la ejecución parcial hasta calidad para ['2024-06', '2025-06']`.

## Archivos esperados

Después de la ejecución completa deben existir:

```text
data/control/ingestion_log.csv
data/reference/taxi_zone_lookup.csv
data/reports/schema_2024-06.json
data/reports/schema_2025-06.json
data/reports/quality_2024-06.json
data/reports/quality_2025-06.json
data/validated/yellow/year=2024/month=06/accepted.parquet
data/validated/yellow/year=2025/month=06/accepted.parquet
data/quarantine/yellow/year=2024/month=06/rejected.parquet
data/quarantine/yellow/year=2025/month=06/rejected.parquet
data/curated/yellow/year=2024/month=06/part-000.parquet
data/curated/yellow/year=2025/month=06/part-000.parquet
data/warehouse/nyc_taxi.duckdb
RESULTADOS.md
```

`data/` está excluido de Git y se descarga o genera localmente.

## Comprobación del warehouse

```powershell
@'
import duckdb
con = duckdb.connect("data/warehouse/nyc_taxi.duckdb", read_only=True)
for table in ["dim_zone", "dim_date", "fact_trips", "agg_zone_hour"]:
    print(table, con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
print("trip_id_unicos", con.execute("SELECT count(*) = count(DISTINCT trip_id) FROM fact_trips").fetchone()[0])
print("viajes_agregados", con.execute("SELECT sum(trips) FROM agg_zone_hour").fetchone()[0])
con.close()
'@ | .\.venv\Scripts\python.exe -
```

Resultados esperados: `dim_zone 265`, `dim_date 60`, `fact_trips 7493698`, `agg_zone_hour 210698`, `trip_id_unicos True` y `viajes_agregados 7493698`.

## Repetición e idempotencia

Volver a ejecutar el pipeline completo. Los logs deben mostrar `reused` para los raw, sin nuevas descargas ni cambios en los conteos. El reprocesamiento de `2025-06` no modifica la partición curada de `2024-06`.

## Errores frecuentes

- `\.\.venv` no existe: ubicarse en la raíz y crear el entorno virtual.
- `No module named pip`: recrear `.venv` con `py -3.12 -m venv .venv`.
- DuckDB ocupado: cerrar otras ejecuciones y correr una sola instancia.
- Primera ejecución lenta: descarga los Parquet; las siguientes reutilizan los hashes.
- No borrar `data/raw/`: esos archivos son la fuente original inmutable.

Con `17 passed`, pipeline completo con código 0, reconciliación exacta, `trip_id` único y suma agregada igual a `fact_trips`, la prueba queda aprobada para la defensa.
