# Dagster: ejecución del pipeline

Esta guía resume cómo levantar Dagster y ejecutar el pipeline desde Windows PowerShell. El código está en [`src/orchestration.py`](../src/orchestration.py).

## Requisitos

Ejecutar los comandos desde la raíz del repositorio y tener creado `.venv`:

```powershell
Set-Location "C:\ruta\al\proyecto\examen_Taxis"
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -c "from src.orchestration import defs; print('Dagster OK')"
```

Los datos deben estar en `data/` o en la ruta indicada por `NYC_TAXI_DATA_DIR`. La ingesta reutiliza los Parquet existentes cuando el hash coincide; no descarga una copia nueva en ese caso.

## Iniciar la interfaz web

```powershell
.\.venv\Scripts\dagster.exe dev -m src.orchestration
```

Abrir <http://127.0.0.1:3000>. Para detener el proceso, usar `Ctrl+C` en la misma terminal.

En Dagster:

1. Ir a **Overview → Jobs**.
2. Seleccionar `pipeline_nyc_taxi`.
3. Abrir **Launchpad**.
4. Pegar una de las configuraciones siguientes.
5. Pulsar **Launch Run** y revisar el resultado en **Runs**.

### Ejecución con el modelo Python

```yaml
resources:
  params:
    config:
      mes: "2025-06"
      modelo: "python"
```

### Ejecución con dbt

```yaml
resources:
  params:
    config:
      mes: "2025-06"
      modelo: "dbt"
```

`mes` debe tener formato `YYYY-MM`. El job usa ese mes y el mismo mes del año anterior; por ejemplo, `2025-06` procesa `2024-06` y `2025-06`.

## Ejecución sin interfaz

Para lanzar el mismo job desde PowerShell:

```powershell
.\.venv\Scripts\python.exe -c "from src.orchestration import run_job; print(run_job(mes='2025-06', modelo='python'))"
.\.venv\Scripts\python.exe -c "from src.orchestration import run_job; print(run_job(mes='2025-06', modelo='dbt'))"
```

El modo `python` escribe `data/warehouse/nyc_taxi.duckdb`. El modo `dbt` escribe `data/warehouse/nyc_taxi_dbt.duckdb`. No ejecutar ambos modos al mismo tiempo sobre el mismo directorio `data/`.

## Pipeline tradicional

El comando sin Dagster sigue disponible:

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --modelo dbt
```

Para ejecutar sólo ingesta, esquema y calidad:

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --hasta-calidad
```

## Etapas del job

El grafo tiene nueve ops:

```text
ingesta_comparacion → esquema_comparacion → calidad_comparacion → transformacion_comparacion ┐
                                                                                              ├→ modelo_estrella
ingesta_asignado    → esquema_asignado    → calidad_asignado    → transformacion_asignado    ┘
```

El job reutiliza las funciones de `src.ingest`, `src.schema`, `src.quality`, `src.transform` y `src.model`/`src.dbt_runner`. Dagster sólo coordina el orden y registra logs y metadatos.

## Comandos de comprobación

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\dbt.exe parse --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe compile --project-dir dbt_project --profiles-dir dbt_project
```

Una ejecución correcta termina sin excepciones. El pipeline Python informa `Finalizó el pipeline completo`; el modo dbt muestra el resumen de `dbt build`.

## Errores frecuentes

| Mensaje | Acción |
|---|---|
| `.venv\Scripts\dagster.exe` no se reconoce | Volver a la raíz del repositorio y crear `.venv`. |
| `Mes inválido` | Usar el formato `YYYY-MM`, por ejemplo `2025-06`. |
| `DuckDB file is locked` | Detener el job anterior y ejecutar una sola corrida por directorio `data/`. |
| Puerto `3000` ocupado | Cerrar la interfaz anterior con `Ctrl+C`. |
| Error de dbt por ruta | Revisar `NYC_TAXI_DATA_DIR`, `NYC_TAXI_DBT_WAREHOUSE` y `NYC_TAXI_DBT_TEMP`. |

Más detalles de arquitectura y decisiones: [`docs/ARQUITECTURA.md`](ARQUITECTURA.md). Para el guion de la exposición: [`docs/DEFENSA.md`](DEFENSA.md).
