# GitHub Actions: pruebas automáticas

El workflow está en [../.github/workflows/tests.yml](../.github/workflows/tests.yml). Se ejecuta en cada `push` y `pull_request`.

## Qué hace

En un checkout limpio de Ubuntu y Python 3.12:

1. instala `requirements.txt`;
2. ejecuta `python -m pip check`;
3. ejecuta `python -m pytest -q`;
4. ejecuta `dbt parse` y `dbt compile` con directorios temporales.

No descarga los Parquet reales ni construye los warehouses completos. Esas pruebas se ejecutan localmente o con Docker.

## Comprobarlo localmente

Desde la raíz del proyecto:

```powershell
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\dbt.exe parse --project-dir dbt_project --profiles-dir dbt_project
.\.venv\Scripts\dbt.exe compile --project-dir dbt_project --profiles-dir dbt_project
```

Resultado esperado:

- `No broken requirements found`.
- `40 passed`.
- dbt termina con código `0`.

## Revisar una ejecución en GitHub

1. Abrir la pestaña **Actions** del repositorio.
2. Seleccionar el workflow **Tests**.
3. Abrir la ejecución asociada al commit o pull request.
4. Revisar los pasos `pip check`, `pytest` y `dbt parse/compile`.

Una ejecución verde confirma que el código y las validaciones livianas funcionan en un entorno limpio. No reemplaza la corrida con los datos oficiales.

## Si falla

Abrir el paso marcado en rojo y leer el primer error. Los casos más comunes son:

| Error | Revisión |
|---|---|
| Fallo de pytest | Ejecutar el mismo test localmente con `-v`. |
| Dependencias incompatibles | Comparar `requirements.txt` y repetir `pip check`. |
| dbt no encuentra el proyecto | Comprobar `--project-dir` y `--profiles-dir`. |
| Falta un archivo de datos | No es un problema del workflow: CI no usa datasets grandes. |
