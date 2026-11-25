# Informe de pruebas final

Guía rápida para verificar el repositorio en Windows PowerShell.

## 1. Obtener el código

```powershell
git clone https://github.com/victormanuellibran/examen_Taxis.git
Set-Location examen_Taxis
git switch main
git pull --ff-only origin main
git log -1 --oneline
```

No ejecutar los comandos desde `C:\Windows\System32`. Todos los pasos siguientes parten de la raíz del proyecto.

## 2. Crear el entorno

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
```

Si `.venv` ya existe, sólo ejecutar `pip check`.

## 3. Ejecutar tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Resultado esperado: `40 passed`.

## 4. Ejecutar el pipeline completo

### Python

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
```

Procesa `2024-06` y `2025-06` y escribe `data/warehouse/nyc_taxi.duckdb`.

### dbt

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --modelo dbt
```

Escribe `data/warehouse/nyc_taxi_dbt.duckdb`.

### Sólo hasta calidad

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --hasta-calidad
```

## 5. Conteos de referencia

| Mes | Leídas | Aceptadas | Cuarentena |
|---|---:|---:|---:|
| 2024-06 | 3.539.193 | 3.441.465 | 97.728 |
| 2025-06 | 4.322.960 | 4.052.233 | 270.727 |

La reconciliación debe cumplir `leídas = aceptadas + cuarentena`.

## 6. Comprobar el warehouse

```powershell
@'
import duckdb
for path in ["data/warehouse/nyc_taxi.duckdb", "data/warehouse/nyc_taxi_dbt.duckdb"]:
    con = duckdb.connect(path, read_only=True)
    print(path)
    for table in ["dim_zone", "dim_date", "fact_trips", "agg_zone_hour"]:
        print(table, con.execute("SELECT count(*) FROM " + table).fetchone()[0])
    print("trip_id_unicos", con.execute("SELECT count(*) = count(DISTINCT trip_id) FROM fact_trips").fetchone()[0])
    print("viajes_agregados", con.execute("SELECT sum(trips) FROM agg_zone_hour").fetchone()[0])
    con.close()
'@ | .\.venv\Scripts\python.exe -
```

Para junio, ambos warehouses deben tener `dim_zone=265`, `fact_trips=7.493.698`, `trip_id_unicos=True` y `viajes_agregados=7.493.698`.

## 7. Ejecutar dbt y Dagster

```powershell
.\.venv\Scripts\dbt.exe debug --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe parse --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe compile --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe build --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe test --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dagster.exe dev -m src.orchestration
```

Para Dagster, abrir `http://127.0.0.1:3000`, seleccionar el job `pipeline_nyc_taxi`, definir `mes` y `modelo`, y lanzar la ejecución. Cerrar con `Ctrl+C`.

## 8. Ejecutar Docker (opcional)

```powershell
docker compose config
docker compose build
docker compose run --rm pipeline python -m pytest -q
docker compose run --rm pipeline python -m src.pipeline --mes 2025-06 --modelo python
docker compose down --remove-orphans
```

## 9. Repetir la ejecución

Volver a ejecutar el mismo comando. Los archivos raw deben mostrar `reused` y los conteos no deben duplicarse. No correr dos procesos sobre la misma carpeta `data/`.

## 10. Errores frecuentes

| Mensaje | Solución |
|---|---|
| `.venv\\Scripts\\python.exe` no se reconoce | Volver a la raíz del repositorio. |
| `No module named pip` | Crear de nuevo `.venv` con `py -3.12 -m venv .venv`. |
| `DuckDB file is locked` | Detener la ejecución anterior. |
| `Mes inválido` | Usar el formato `YYYY-MM`. |
| Falta un Parquet | Verificar que el período esté publicado por NYC TLC. |

## 11. Resultado de referencia

La última verificación local de la rama de documentación produjo:

- `40 passed` en pytest.
- `pip check` sin errores.
- `dbt parse` y `dbt compile` correctos.
- Importación de Dagster correcta.
