# Integración continua

La integración continua (CI) ejecuta automáticamente verificaciones reproducibles cada vez que se sube un cambio (`push`) o se abre/actualiza un pull request (`pull_request`). En este repositorio la implementa GitHub Actions mediante el workflow `.github/workflows/tests.yml`.

## Qué ejecuta

El job usa Ubuntu y Python 3.12, instala exactamente `requirements.txt`, verifica las dependencias con `python -m pip check` y ejecuta `python -m pytest -q`. También valida la configuración de dbt con `dbt parse` y `dbt compile`, usando directorios temporales del runner y variables de período explícitas.

## Qué no ejecuta

CI no descarga Parquet de NYC TLC, no ejecuta `dbt build`, no construye los warehouses completos y no levanta Docker, PostgreSQL, Grafana, Prometheus ni Dozzle. Esas tareas requieren los datasets reales y un consumo de recursos que no corresponde a cada push. Las pruebas locales o de entrega deben ejecutar el pipeline completo cuando se necesite validar datos reales.

## Interpretación del resultado

- Un workflow aprobado indica que las dependencias, los 37 tests pytest y la compilación estática de dbt pasaron en un checkout limpio.
- Un workflow fallido debe revisarse desde el job que aparece en la pestaña **Actions** del repositorio. El primer paso rojo contiene el error que debe corregirse.
- Que CI esté aprobado no reemplaza la validación de conteos, reconciliación, relaciones y consultas sobre los Parquet oficiales.

## Cómo comprobarlo en GitHub

1. Abrir la pestaña **Actions** del repositorio `victormanuellibran/examen_Taxis`.
2. Seleccionar el workflow **Tests** y una ejecución asociada al commit o pull request.
3. Revisar los pasos `pytest`, `pip check` y `Validar proyecto dbt sin datasets reales`.

La ejecución real en GitHub debe comprobarse después de publicar la rama. Mientras no se autorice el push, sólo puede afirmarse que el workflow fue validado localmente.
