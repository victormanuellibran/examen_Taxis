# Guía de ejecución para la defensa

Usar PowerShell desde la raíz del repositorio. Los comandos se pueden ejecutar con los archivos de `data/` ya descargados; la ingesta mostrará `reused` cuando el hash sea correcto.

## 1. Preparar el entorno

```powershell
Set-Location "C:\ruta\al\proyecto\examen_Taxis"
.\.venv\Scripts\python.exe -m pip check
```

Si no existe `.venv`:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 2. Ejecutar las pruebas

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Resultado de referencia: `37 passed`.

Estas pruebas usan datos sintéticos o temporales y no reemplazan la ejecución con los Parquet oficiales.

## 3. Ejecutar el pipeline

### Modo Python

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
```

Procesa `2024-06` y `2025-06`. El warehouse es `data/warehouse/nyc_taxi.duckdb`.

### Modo dbt

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --modelo dbt
```

Usa las mismas capas de datos y construye `data/warehouse/nyc_taxi_dbt.duckdb`.

### Sólo hasta calidad

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --hasta-calidad
```

La ejecución termina después de ingesta, esquema, calidad y cuarentena.

### Cambiar el período

```powershell
# Procesa marzo de 2024 y marzo de 2025
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-03
```

`--mes` siempre debe tener formato `YYYY-MM`.

## 4. Revisar los resultados

Conteos de referencia para junio:

| Período | Leídas | Aceptadas | Cuarentena |
|---|---:|---:|---:|
| 2024-06 | 3.539.193 | 3.441.465 | 97.728 |
| 2025-06 | 4.322.960 | 4.052.233 | 270.727 |

Archivos principales:

- `data/reports/quality_YYYY-MM.json`
- `data/validated/yellow/year=YYYY/month=MM/accepted.parquet`
- `data/quarantine/yellow/year=YYYY/month=MM/rejected.parquet`
- `data/curated/yellow/year=YYYY/month=MM/part-000.parquet`
- `data/warehouse/nyc_taxi.duckdb`
- `data/warehouse/nyc_taxi_dbt.duckdb`
- `RESULTADOS.md`

Para comprobar las tablas:

```powershell
@'
import duckdb
con = duckdb.connect("data/warehouse/nyc_taxi.duckdb", read_only=True)
for table in ["dim_zone", "dim_date", "fact_trips", "agg_zone_hour"]:
    print(table, con.execute("SELECT count(*) FROM " + table).fetchone()[0])
print("trip_id_unicos", con.execute("SELECT count(*) = count(DISTINCT trip_id) FROM fact_trips").fetchone()[0])
print("viajes_agregados", con.execute("SELECT sum(trips) FROM agg_zone_hour").fetchone()[0])
con.close()
'@ | .\.venv\Scripts\python.exe -
```

Valores esperados: `dim_zone=265`, `dim_date=60`, `fact_trips=7.493.698`, `trip_id_unicos=True` y `viajes_agregados=7.493.698`.

## 5. Ejecutar dbt directamente

```powershell
.\.venv\Scripts\dbt.exe debug --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe parse --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe compile --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe build --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe test --project-dir dbt_project --profiles-dir dbt_project
```

`build` construye modelos y ejecuta tests. La ejecución de referencia fue `32/32` tareas y `24/24` tests dbt.

## 6. Ejecutar Dagster

```powershell
.\.venv\Scripts\dagster.exe dev -m src.orchestration
```

Abrir <http://127.0.0.1:3000>:

1. entrar en **Overview → Jobs**;
2. seleccionar `pipeline_nyc_taxi`;
3. abrir **Launchpad**;
4. indicar `mes: "2025-06"` y `modelo: "python"` o ` "dbt"`;
5. lanzar la ejecución y revisar **Runs**.

El grafo tiene dos cadenas de cuatro etapas que terminan en `modelo_estrella`. Dagster coordina las funciones existentes; DuckDB ejecuta el procesamiento.

También se puede lanzar sin navegador:

```powershell
.\.venv\Scripts\python.exe -c "from src.orchestration import run_job; print(run_job(mes='2025-06', modelo='python'))"
```

Cerrar Dagster con `Ctrl+C`.

## 7. Ejecutar con Docker

```powershell
docker compose config
docker compose build
docker compose run --rm pipeline python -m pytest -q
docker compose run --rm pipeline python -m src.pipeline --mes 2025-06 --modelo python
```

Docker es una alternativa al entorno virtual. Los datos se conservan en `./data` mediante un volumen y no se incluyen en la imagen.

## 8. Respuestas breves

- **¿Qué hace la ingesta?** Descarga o reutiliza los archivos originales y registra su hash.
- **¿Qué hace calidad?** Separa filas aceptadas y rechazadas, conserva las banderas y comprueba la reconciliación.
- **¿Por qué Parquet?** Es columnar, tipado y eficiente para este volumen.
- **¿Qué hace DuckDB?** Ejecuta SQL localmente sobre Parquet y limita memoria con spill a disco.
- **¿Qué hace dbt?** Organiza modelos SQL, dependencias y tests; DuckDB sigue siendo el motor.
- **¿Qué hace Dagster?** Ordena las etapas y registra el estado de cada una.
- **¿Por qué hay dos warehouses?** Para ejecutar Python y dbt sin bloquear el mismo archivo.
- **¿Por qué no se descargan datos en CI?** El workflow usa tests sintéticos y validaciones de dbt; los Parquet reales se prueban localmente.
- **¿Qué significa idempotencia?** Repetir el comando reutiliza los raw y no duplica las filas curadas.
- **¿Qué limitación queda?** `trip_id` es único por corrida, pero no es una clave estable entre reconstrucciones.

No ejecutar dos procesos simultáneos sobre la misma carpeta `data/`; DuckDB bloquea el warehouse durante la escritura.
