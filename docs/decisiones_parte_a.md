# Parte A — implementación y evidencia

Implementado y ejecutado el 18/11/2026 en la rama `feature/ingesta-calidad`.
El repositorio inicial estaba vacío, sin commits ni archivo del examen. Se siguió el pedido adjunto.
No se crearon ni modificaron `src/transform.py`, `src/model.py`, `sql/`, `README.md` ni `RESULTADOS.md`.

## Ejecutar

Requiere Python >= 3.10. Dependencias directas fijadas en `requirements.txt`.
En esta computadora se preparó Python 3.12.16 y un entorno `.venv` local porque `python` apuntaba al alias de Microsoft Store.

PowerShell, desde la raíz del repositorio:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --hasta-calidad
```

En otra computadora con Python instalado:

```text
python -m venv .venv
python -m pip install -r requirements.txt
python -m pytest -q
python -m src.pipeline --mes 2025-06 --hasta-calidad
```

Antes del segundo comando, activar el entorno (`.venv\Scripts\Activate.ps1` en PowerShell o `source .venv/bin/activate` en Linux/macOS), o usar directamente su ejecutable.
Con GNU Make: `make calidad MES=2025-06` y `make test`.
Con ambas ramas integradas: `python -m src.pipeline --mes 2025-06` o `make all MES=2025-06`.
`PYTHON` se puede redefinir en Make, por ejemplo `make calidad PYTHON=.venv/Scripts/python.exe`.

`--mes` indica el mes final; el orquestador incluye el mismo mes del año anterior y ejecuta ambos en orden cronológico. Las funciones individuales aceptan un único mes.
Las rutas se resuelven desde la raíz del proyecto, independientemente del directorio de ejecución. `NYC_TAXI_DATA_DIR` permite aislar datos y tests.

## Ingesta y control

- Descarga streaming en bloques de 1 MiB; timeout de conexión 15 s y lectura 120 s.
- Un intento inicial y hasta tres reintentos de red, con esperas de 1, 2 y 4 s. Los errores de formato/disco no se reintentan como si fueran problemas de red.
- Se escribe `.part`, se valida el formato y se reemplaza la ruta final. Un error elimina el temporal.
- SHA-256 se calcula incrementalmente sobre los bytes originales. Se compara contra el último registro `ok` del archivo.
- Si el archivo existe y coincide, se reutiliza sin GET. Si difiere, se falla y se conserva el archivo para investigar; no se sobrescribe raw.
- Un archivo preexistente sin registro se valida y adopta explícitamente (`adopted`); ese hash acredita el contenido local, no su autenticidad remota.
- Si falta un archivo previamente registrado, se descarga y compara con el hash histórico. Una diferencia detiene el pipeline para revisión manual.
- `data/control/ingestion_log.csv`: nombre, URL, filas, bytes, SHA-256, timestamp UTC, estado, acción y error. Se registra también la tabla de zonas.
- Cada invocación agrega eventos al log; los eventos repetidos no son duplicación de datos. Los conteos Parquet salen de los metadatos PyArrow.

## Contrato de esquema v1

Se inspeccionaron ambos archivos descargados con `pyarrow.parquet.ParquetFile(...).schema_arrow`, además de `DESCRIBE` de DuckDB.

Los dos archivos tienen `VendorID`, `PULocationID`, `DOLocationID` como int32; pasajeros, código de tarifa y pago como int64; timestamps con precisión de microsegundos; `store_and_fwd_flag` como large_string y distancias/importes como double.
2024 tiene 19 columnas, 2025 tiene 20: `cbd_congestion_fee` se agrega en 2025 como double.
La [documentación oficial TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) también describe esta incorporación desde 2025.

- El contrato mapea explícitamente los 20 nombres a snake_case, incluidos `vendor_id`, `ratecode_id`, `pu_location_id`, `do_location_id`, `airport_fee`.
- Los enteros se unifican a BIGINT. Sólo se permiten ampliaciones sin pérdida desde TINYINT/SMALLINT/INTEGER a BIGINT; otras diferencias detienen la ejecución para revisar el contrato.
- No se usan `TRY_CAST` ni conversiones que oculten valores inválidos. No se corrigen importes, tiempos ni otros valores originales.
- En archivos anteriores a 2025 se permite exclusivamente la ausencia de `cbd_congestion_fee`, que se agrega como NULL de tipo DOUBLE. En 2025 es obligatoria.
- Columnas nuevas: política **fallar y reportar**, sin descartarlas silenciosamente. Primero debe revisarse/versionarse el contrato.
- Cualquier otra columna faltante es error. Los nulos de datos se conservan y se evalúan en calidad.
- `data/reports/schema_YYYY-MM.json` informa esquema observado, faltantes, nuevas, diferencias y errores, aun cuando se rechaza el esquema.
- `source_file` conserva el nombre del Parquet original. No se incorporan columnas Hive `year`/`month` accidentalmente: las lecturas desactivan `hive_partitioning`.

## Calidad

Las definiciones ejecutables y sus justificaciones están en `src/quality.py::RULES` y se incluyen en cada reporte.

| Regla | Condición de incumplimiento | Severidad / motivo |
|---|---|---|
| R01 | Subida nula o fuera de `[inicio del mes, inicio siguiente)` | Rechazo: pertenencia al período |
| R02 | Bajada nula, subida nula o bajada <= subida | Rechazo: duración positiva |
| R03 | Distancia nula, no finita, <= 0 o > 200 millas | Rechazo: umbral operativo conservador de viajes urbanos/regionales |
| R04 | Total nulo, no finito o negativo | Rechazo: excluir reversos e importes no interpretables; cero permitido |
| R05 | Pasajeros nulos o <= 0 | Advertencia: no impide analizar el viaje por zona/importe |
| R06 | Alguna zona nula o inexistente en TLC | Rechazo: integridad referencial; 264/265 válidos si existen en catálogo |
| R07 | Ocurrencias adicionales de una fila exactamente duplicada | Rechazo: conservar una copia, comparar todas las columnas del viaje salvo procedencia |
| R08 | Propina negativa | Advertencia: anomalía observada, posiblemente reversos; no eliminar por esta condición sola |

200 millas es una decisión analítica, no una regla oficial de TLC. La exploración observó máximos de 195.916,81 millas (2024) y 261.262,39 (2025), que motivan acotar outliers; no se afirma que todo viaje mayor a 200 sea imposible.
R08 se deriva de una consulta real: 108 propinas negativas en 2024 (mínimo -99,01) y 367 en 2025 (mínimo -70,07).
Consulta reproducible, reemplazando `ruta_raw` por uno de los archivos descargados:

```sql
SELECT count(*) AS filas,
       count(*) FILTER (WHERE tip_amount < 0) AS propinas_negativas,
       min(tip_amount) AS propina_minima,
       max(trip_distance) AS distancia_maxima
FROM read_parquet('ruta_raw', hive_partitioning=false);
```

Todas las reglas se calculan en una clasificación común, antes de separar salidas. Una fila con varios rechazos aparece una sola vez en cuarentena. Los conteos de incumplimientos pueden superponerse y no deben sumarse para obtener el total rechazado.

Accepted contiene todas las columnas del contrato, `source_file` y `warning_flags` (LIST de VARCHAR, vacía si no hay advertencias).
Rejected conserva las mismas columnas originales normalizadas y agrega `violated_rules` (todas las reglas), `rejection_flags` y `warning_flags`.
R07 numera filas idénticas; la copia elegida es indistinguible en valores. No se promete orden físico ni hash idéntico de los Parquet derivados entre ejecuciones.

La reconciliación exige `rows_read == rows_accepted + rows_quarantined` tanto al clasificar como al contar los archivos escritos. Cualquier error propaga código de salida 1 desde el CLI.

## Evidencia de ejecución real

`python -m src.pipeline --mes 2025-06 --hasta-calidad` finalizó con código 0 para ambos períodos.

| Mes | Leídas | Aceptadas | Cuarentena |
|---|---:|---:|---:|
| 2024-06 | 3.539.193 | 3.441.465 | 97.728 |
| 2025-06 | 4.322.960 | 4.052.233 | 270.727 |

| Regla | 2024-06 | 2025-06 |
|---|---:|---:|
| R01 | 51 | 20 |
| R02 | 1.140 | 68.583 |
| R03 | 53.084 | 135.118 |
| R04 | 48.795 | 74.257 |
| R05 | 446.620 | 1.235.737 |
| R06 | 0 | 0 |
| R07 | 0 | 0 |
| R08 | 108 | 367 |

SHA-256 observados:

- 2024-06: `677cf14c8347f745b583f012fbdba072334c6c4efa17bfd6a64369f2ba30329c`
- 2025-06: `1bea7f5b09c744414451fa7fef09fbc5a393ffb1621c859be6ba0df23a14e1b6`
- Zonas (265 filas): `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`

Las siguientes invocaciones reales reutilizaron los hashes y archivos. Los tests no dependen de Internet.
`python -m pytest -q`: **14 tests pasaron**, incluyendo las tres categorías obligatorias, ocho reglas, superposición de rechazos/advertencias, cambios de esquema, error de hash, reintentos y contratos del orquestador.

## Operación e integración pendiente

- DuckDB procesa SQL sobre Parquet, sin pandas ni listas de millones de registros. Memoria del motor limitada a 2 GB, 2 threads y spill en `data/tmp`. Este límite no es una medición del RSS total ni una prueba en hardware de exactamente 8 GB.
- Ejecutar un solo pipeline por directorio de datos a la vez. No se implementa bloqueo entre procesos concurrentes; los compañeros deben usar checkouts/directorios separados.
- Las salidas se reemplazan mediante temporales; nunca se anexan filas a Parquet. Cada archivo se publica atómicamente, pero las dos salidas y el JSON no forman una transacción conjunta. Si se interrumpe el proceso, volver a ejecutarlo y consumir sólo resultados de una ejecución exitosa.
- Datos, entorno, herramientas locales y temporales están excluidos de Git.
- Falta integrar B: `run_transform(month, validated_path) -> Path`, `run_model(months: list[str]) -> None`, SQL, capa curada, warehouse y documentación general. El modelo se invoca una vez después de transformar ambos meses.
- El modo completo falla explícitamente si los módulos B no están; no se implementaron stubs. La prueba de orquestación usa mocks únicamente dentro de tests y no acredita ejecución real de B.
