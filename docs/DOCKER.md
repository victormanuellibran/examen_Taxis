# Ejecutar el pipeline con Docker

Todos los comandos se ejecutan desde la raíz del repositorio, en PowerShell. Docker Desktop debe estar iniciado.

## Qué aporta Docker

- `Dockerfile`: define la imagen con Python 3.12 y las dependencias del proyecto.
- `docker-compose.yml`: define el servicio `pipeline`, sus variables y los límites de recursos.
- `data/`: se monta desde Windows en `/app/data`; no se copia dentro de la imagen.
- `docker compose run --rm`: crea un contenedor para una tarea y lo elimina al terminar.

Los archivos generados quedan en la carpeta local `data/`. La capa `raw` se conserva y la ingesta reutiliza los archivos cuando el hash coincide.

## Preparar y validar

```powershell
Set-Location "C:\ruta\al\proyecto\examen_Taxis"
docker compose config
docker compose build
```

Si `docker compose config` muestra errores, revisar que Docker Desktop esté activo y que los archivos `Dockerfile` y `docker-compose.yml` existan.

## Comandos de ejecución

### Tests

```powershell
docker compose run --rm pipeline python -m pytest -q
```

Resultado esperado: `40 passed`.

### Pipeline con Python

```powershell
docker compose run --rm pipeline python -m src.pipeline --mes 2025-06 --modelo python
```

Escribe `data/warehouse/nyc_taxi.duckdb`.

### Pipeline con dbt

```powershell
docker compose run --rm pipeline python -m src.pipeline --mes 2025-06 --modelo dbt
```

Escribe `data/warehouse/nyc_taxi_dbt.duckdb` y ejecuta `dbt build`.

### Sólo ingesta, esquema y calidad

```powershell
docker compose run --rm pipeline python -m src.pipeline --mes 2025-06 --hasta-calidad
```

### Cambiar el mes

El argumento `--mes` procesa ese mes y el mismo mes del año anterior:

```powershell
# Mayo de 2024 y mayo de 2025
docker compose run --rm pipeline python -m src.pipeline --mes 2025-05 --modelo python
```

### Comandos directos de dbt

```powershell
docker compose run --rm pipeline dbt debug --project-dir /app/dbt_project --profiles-dir /app/dbt_project
docker compose run --rm pipeline dbt parse --project-dir /app/dbt_project --profiles-dir /app/dbt_project
docker compose run --rm pipeline dbt compile --project-dir /app/dbt_project --profiles-dir /app/dbt_project
docker compose run --rm pipeline dbt build --project-dir /app/dbt_project --profiles-dir /app/dbt_project
```

`debug` revisa la conexión; `parse` y `compile` validan el proyecto; `build` materializa modelos y ejecuta sus tests.

## Ver y detener la ejecución

Los logs aparecen directamente en la terminal. Para consultar los últimos logs del servicio:

```powershell
docker compose logs --tail 100 pipeline
```

Para quitar contenedores y la red de Compose sin borrar datos:

```powershell
docker compose down --remove-orphans
```

No ejecutar dos pipelines simultáneos sobre la misma carpeta `data/`: DuckDB bloquea el warehouse durante la escritura.

## Memoria

El proyecto limita DuckDB a 2 GB y dos hilos. Compose fija un máximo de 4 GB y dos CPU para el contenedor. En una notebook de 8 GB conviene cerrar aplicaciones pesadas y ejecutar una sola corrida.

## Problemas frecuentes

| Mensaje | Solución |
|---|---|
| `docker is not running` | Iniciar Docker Desktop. |
| `file is locked` | Detener la corrida anterior y volver a ejecutar. |
| `Mes inválido` | Usar `YYYY-MM`, por ejemplo `2025-06`. |
| El contenedor no encuentra datos | Comprobar que existe `data/` en la raíz y que el volumen está montado. |
