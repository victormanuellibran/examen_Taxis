# Comandos para la defensa — NYC Taxi

Hoja rápida para tener abierta durante la demostración. Ejecutar desde la raíz del repositorio en PowerShell.

## Entorno

```powershell
Get-Location
Get-ChildItem
.\.venv\Scripts\python.exe -m pip check
```

Salida esperada:

```text
No broken requirements found.
```

Si no existe `.venv`:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Referencia: `40 passed`.

## Pipeline Python

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
```

Procesa `2024-06` y `2025-06`. Warehouse: `data/warehouse/nyc_taxi.duckdb`.

## Pipeline dbt

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --modelo dbt
```

Warehouse: `data/warehouse/nyc_taxi_dbt.duckdb`.

## Sólo hasta calidad

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --hasta-calidad
```

## Otro mes

```powershell
# Procesa marzo de 2024 y marzo de 2025
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-03
```

El valor de `--mes` debe tener formato `YYYY-MM`.

## dbt directo

```powershell
.\.venv\Scripts\dbt.exe debug --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe parse --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe compile --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe build --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe test --project-dir dbt_project --profiles-dir dbt_project
```

Referencias: `dbt build 35/35` y `dbt test 27/27`.

## Dagster

```powershell
.\.venv\Scripts\dagster.exe dev -m src.orchestration
```

Abrir <http://127.0.0.1:3000> y seleccionar:

```text
Overview → Jobs → pipeline_nyc_taxi → Launchpad
```

Python:

```yaml
resources:
  params:
    config:
      mes: "2025-06"
      modelo: "python"
```

dbt:

```yaml
resources:
  params:
    config:
      mes: "2025-06"
      modelo: "dbt"
```

Detener con `Ctrl+C`.

Sin interfaz:

```powershell
.\.venv\Scripts\python.exe -c "from src.orchestration import run_job; print(run_job(mes='2025-06', modelo='python'))"
.\.venv\Scripts\python.exe -c "from src.orchestration import run_job; print(run_job(mes='2025-06', modelo='dbt'))"
```

## Comprobación de warehouses

```powershell
@'
import duckdb

for path in [
    "data/warehouse/nyc_taxi.duckdb",
    "data/warehouse/nyc_taxi_dbt.duckdb",
]:
    con = duckdb.connect(path, read_only=True)
    print(path)
    for table in ["dim_zone", "dim_date", "fact_trips", "agg_zone_hour"]:
        count = con.execute(
            "SELECT count(*) FROM " + table
        ).fetchone()[0]
        print(table, count)
    print(
        "trip_id_unicos",
        con.execute(
            "SELECT count(*) = count(DISTINCT trip_id) FROM fact_trips"
        ).fetchone()[0],
    )
    print(
        "viajes_agregados",
        con.execute("SELECT sum(trips) FROM agg_zone_hour").fetchone()[0],
    )
    con.close()
'@ | .\.venv\Scripts\python.exe -
```

Valores de junio:

```text
dim_zone       265
dim_date       60
fact_trips     7.493.698
agg_zone_hour  210.698
trip_id_unicos True
viajes_agregados 7.493.698
```

## Conteos de calidad

```powershell
Get-Content data/reports/quality_2025-06.json
Get-Content data/reports/quality_2024-06.json
```

Referencia 2025-06:

```text
leídas       4.322.960
aceptadas    4.052.233
cuarentena     270.727
```

Referencia 2024-06:

```text
leídas       3.539.193
aceptadas    3.441.465
cuarentena      97.728
```

Reconciliación: `leídas = aceptadas + cuarentena`.

Fila de cuarentena:

```powershell
@'
import duckdb
con = duckdb.connect()
rows = con.execute("""
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime,
           trip_distance, total_amount, tip_amount,
           source_file, violated_rules,
           rejection_flags, warning_flags
    FROM read_parquet(
        'data/quarantine/yellow/year=2025/month=06/rejected.parquet',
        hive_partitioning=false
    )
    WHERE len(rejection_flags) >= 2
    LIMIT 3
""").fetchall()
for row in rows:
    print(row)
con.close()
'@ | .\.venv\Scripts\python.exe -
```

## Resultados

```powershell
Get-Content RESULTADOS.md
```

Consultas disponibles: Q1 zonas, Q2 hora, Q3 cargo CBD, Q4 propinas, Q5 aeropuertos y Q6 variación mensual.

Valores de referencia:

```text
Q1 — JFK Airport: 170.662 (2025) y 152.171 (2024)
Q2 — 05:00: USD 32,57 promedio
Q3 — 2.987.770 / 4.052.233 = 73,73 %
Q4 — tarjeta: aproximadamente 25,26 %
Q5 — 334.938 (2024) y 346.769 (2025)
Q6 — viajes +17,75 %; tarifa media -0,1588
```

## Archivos para abrir

```text
README.md
RESULTADOS.md
src/pipeline.py
src/ingest.py
src/config.py
src/orchestration.py
src/dbt_runner.py
dbt_project/models/
contracts/yellow_schema.yaml
data/reports/quality_2025-06.json
data/warehouse/nyc_taxi.duckdb
docs/RENDIMIENTO.md
.github/workflows/tests.yml
Dockerfile
docker-compose.yml
```

## Docker

```powershell
docker compose config
docker compose build
docker compose run --rm pipeline python -m pytest -q
docker compose run --rm pipeline python -m src.pipeline --mes 2025-06 --modelo python
docker compose run --rm pipeline python -m src.pipeline --mes 2025-06 --modelo dbt
docker stats
docker compose down --remove-orphans
```

El volumen `./data:/app/data` conserva Parquet, reportes y warehouses. Límites configurados: DuckDB 2 GB, Compose 4 GB y 2 CPU.

No ejecutar dos procesos que escriban sobre `data/` al mismo tiempo.

## Memoria y rendimiento

Docker, terminal 1:

```powershell
docker compose run --rm pipeline python -m src.pipeline --mes 2025-06 --modelo python
```

Docker, terminal 2:

```powershell
docker stats
```

Mediciones de referencia:

```text
Python local — 31,1–31,36 s; RAM ~2.023 MB
dbt local    — 37,9–39,0 s; RAM ~2.001 MB
Docker Python — 45,16 s; RAM 1,851 GiB
Docker dbt   — 48,30–53,15 s; RAM 1,872 GiB
```

Las mediciones se realizaron en una máquina con 16 GB de RAM. No afirmar que se probó físicamente una notebook de exactamente 8 GB.

## EXPLAIN ANALYZE

```powershell
@'
from pathlib import Path
import duckdb

sql = Path("sql/q1_top_zones.sql").read_text(encoding="utf-8")
con = duckdb.connect("data/warehouse/nyc_taxi.duckdb", read_only=True)
for row in con.execute("EXPLAIN ANALYZE " + sql).fetchall():
    print(*row)
con.close()
'@ | .\.venv\Scripts\python.exe -
```

Referencia documentada:

```text
Q1 ≈ 0,139 s
Q6 ≈ 0,056 s
```

## CI

Archivo:

```text
.github/workflows/tests.yml
```

Flujo:

```text
push / pull_request
        ↓
pip check
        ↓
pytest
        ↓
dbt parse
        ↓
dbt compile
```

La CI no descarga los Parquet grandes ni ejecuta `dbt build`.

## Monitoreo en vivo, dashboard e insights

Orden recomendado para la demo, con dos terminales abiertas en la raíz del repo.

**Terminal 1.** Levanta Prometheus y Grafana, y abre Grafana en el navegador:

```powershell
.\run.ps1 monitoreo
```

**Terminal 2.** Corre el pipeline; en Grafana se ve la etapa actual, la RAM y la CPU en vivo:

```powershell
.\run.ps1 pipeline
```

Al terminar, en la misma terminal 2, generá los insights y abrí el dashboard:

```powershell
.\run.ps1 insights
.\run.ps1 dashboard
```

Para la documentación dbt con el grafo de linaje (requiere una corrida previa con `-Modelo dbt`):

```powershell
.\run.ps1 dbt-docs
```

**Qué decir:**
- Grafana: *"el pipeline mide su propia memoria; las líneas punteadas son 2 GB de DuckDB, 4 GB del contenedor y 8 GB de la consigna; el pico es ~2 GB"*.
- Dashboard, pestaña **Calidad y cuarentena**: elegir la regla R02 y mostrar una fila rechazada. Cubre el punto de la consigna *"mostrar una fila en cuarentena con su regla violada"*.
- Insights: *"la tarifa media estable esconde dos efectos opuestos: hay más viajes largos, pero cada tramo es más barato (I6)"*.
- Justificación de herramientas: `docs/TECNOLOGIAS.md`. Análisis completo: `docs/ANALISIS_RESULTADOS.md`.

Apagar el monitoreo al final:

```powershell
.\run.ps1 apagar
```

## Qué mostrar con cada herramienta

| Evidencia | Comando o archivo |
|---|---|
| Tests Python | `python -m pytest -q` |
| Grafo y estados | Dagster |
| Resultados Q1–Q6 | `RESULTADOS.md` |
| Insights I1–I7 | `INSIGHTS.md` y `docs/ANALISIS_RESULTADOS.md` |
| Conteos y relaciones | script DuckDB |
| Calidad | `quality_YYYY-MM.json` y el dashboard (pestaña Calidad) |
| Fila en cuarentena | Dashboard → Calidad y cuarentena → regla R02 |
| Warehouse | `data/warehouse/*.duckdb` |
| RAM y CPU en vivo | Grafana (`.\run.ps1 monitoreo`) o `docker stats` |
| Tiempo y RAM por etapa | Dashboard → Rendimiento (`data/control/run_metrics.csv`) |
| Plan SQL | `EXPLAIN ANALYZE` |
| Modelos dbt y linaje | `dbt_project/models/` y `.\run.ps1 dbt-docs` |
| Justificación de tecnologías | `docs/TECNOLOGIAS.md` |
| CI | `.github/workflows/tests.yml` |

## Reglas durante la demostración

```text
1. Ejecutar desde la raíz del repositorio.
2. Usar un solo proceso escritor sobre data/.
3. No borrar data/raw/.
4. Esperar el final de una ejecución antes de iniciar otra.
5. Cerrar Dagster con Ctrl+C.
6. Revisar RESULTADOS.md y los conteos después de una corrida.
```
