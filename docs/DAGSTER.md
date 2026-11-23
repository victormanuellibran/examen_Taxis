# Orquestación con Dagster

Guía de la capa de orquestación implementada en [`src/orchestration.py`](../src/orchestration.py).

---

## 1. Qué es Dagster y por qué lo usamos

Dagster es un orquestador de pipelines de datos: declara las etapas de un proceso y las dependencias entre ellas, decide en qué orden corren, registra el resultado de cada una y permite ver y relanzar ejecuciones desde una interfaz web.

El pipeline ya corría de punta a punta con un orquestador propio en Python ([`src/pipeline.py`](../src/pipeline.py)), que llama a las cinco etapas en secuencia. Ese orquestador sigue funcionando sin cambios. Dagster aporta tres cosas que el bucle de Python no daba:

1. **El grafo de dependencias es explícito y verificable.** Ya no está implícito en el orden de las líneas de un `for`: es un DAG que se puede inspeccionar, testear y dibujar.
2. **Observabilidad por etapa.** Cada etapa registra sus propios logs y metadatos (filas leídas, aceptadas, en cuarentena, columnas faltantes, rutas de salida), y la interfaz muestra exactamente cuál falló.
3. **Reejecución parcial.** Si falla la etapa 4, se puede relanzar desde ahí sin repetir la ingesta.

**Lo que Dagster no hace:** no procesa datos. DuckDB sigue haciendo todo el cómputo sobre Parquet y dbt sigue construyendo el modelo analítico. Dagster sólo coordina cuándo y en qué orden se invocan esas piezas.

---

## 2. El grafo: nueve nodos

El examen compara un mes contra el mismo mes del año anterior. Por eso el grafo tiene **dos cadenas de cuatro etapas** (una por período) que **convergen en un único nodo de modelado**, porque el modelo necesita las dos capas curadas para poder comparar:

```
ingesta_comparacion → esquema_comparacion → calidad_comparacion → transformacion_comparacion ┐
                                                                                             ├→ modelo_estrella
ingesta_asignado    → esquema_asignado    → calidad_asignado    → transformacion_asignado    ┘
```

| Nodo | Etapa | Función que reutiliza |
|---|---|---|
| `ingesta_{rol}` | 1. Ingesta idempotente | `src.ingest.run_ingest` |
| `esquema_{rol}` | 2. Contrato y normalización | `src.schema.run_schema` |
| `calidad_{rol}` | 3. Reglas, cuarentena, reconciliación | `src.quality.run_quality` |
| `transformacion_{rol}` | 4. Capa curada particionada | `src.transform.run_transform` |
| `modelo_estrella` | 5. Modelo y seis consultas | `src.model.run_model` o `src.dbt_runner.run_model` |

donde `{rol}` es `comparacion` (el mes del año anterior) o `asignado` (el mes que se pasa por configuración).

**No hay lógica de negocio nueva.** Cada `@op` es un envoltorio de tres líneas: resuelve el mes desde la configuración, llama a la función que ya existía y adjunta metadatos para la interfaz. Las reglas de calidad, el contrato de esquema, las consultas SQL y los modelos dbt son exactamente los mismos archivos que usa el pipeline tradicional.

### Dependencias reales, no decorativas

Las dependencias son de datos, no de orden: cada op recibe como *input* la ruta que produjo el anterior (`raw_path`, `staged_path`, `accepted_path`). Dagster no puede reordenarlas ni saltearlas. `modelo_estrella` declara dos inputs (`curated_comparacion` y `curated_asignado`), así que es imposible que corra con un solo período cargado.

---

## 3. Configuración de los períodos y del motor

La configuración vive en un recurso, `PipelineParams`, con dos campos equivalentes a los flags del CLI:

| Campo | Equivalente en `src.pipeline` | Valores |
|---|---|---|
| `mes` | `--mes` | `YYYY-MM`, por ejemplo `2025-06` |
| `modelo` | `--modelo` | `python` o `dbt` |

De `mes` se derivan los dos períodos: `PipelineParams(mes="2025-06").periods()` devuelve `("2024-06", "2025-06")`. El mes se valida con la misma función `parse_month` que usa el resto del proyecto, así que un `2025-13` falla antes de descargar nada.

---

## 4. Cómo ejecutarlo

### 4.1. Interfaz web (lo que conviene mostrar)

```powershell
.\.venv\Scripts\dagster.exe dev -m src.orchestration
```

Abre `http://127.0.0.1:3000`. Desde ahí:

- **Overview → Jobs → `pipeline_nyc_taxi`** muestra el DAG de nueve nodos con sus dependencias.
- **Launchpad** permite lanzar una ejecución editando la configuración:

```yaml
resources:
  params:
    config:
      mes: "2025-06"
      modelo: "python"
```

- **Runs** lista las ejecuciones con su estado; al abrir una se ven los logs por etapa y los metadatos (filas leídas, aceptadas y en cuarentena, violaciones por regla).
- Si una etapa falla, aparece en rojo con su excepción, y las etapas que dependían de ella quedan en `skipped`.

Cerrar con `Ctrl+C`: no queda ningún servicio corriendo de fondo.

### 4.2. Sin interfaz, desde Python

```powershell
.\.venv\Scripts\python.exe -c "from src.orchestration import run_job; print(run_job(mes='2025-06', modelo='dbt'))"
```

### 4.3. El pipeline tradicional sigue intacto

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06
```

Dagster es una capa adicional, no un reemplazo. Hay un test que lo verifica (`test_el_pipeline_tradicional_sigue_funcionando`).

---

## 5. Integración con dbt

La etapa 5 acepta los dos motores ya existentes, seleccionados por configuración:

- `modelo: "python"` → `src.model.run_model`, warehouse `data/warehouse/nyc_taxi.duckdb`
- `modelo: "dbt"` → `src.dbt_runner.run_model`, que ejecuta `dbt build` y escribe en `data/warehouse/nyc_taxi_dbt.duckdb`

**No usamos `dagster-dbt`** a propósito. Esa integración expone cada modelo dbt como un asset de Dagster, lo que duplicaría el grafo (los modelos dbt ya tienen su propio DAG declarado en `dbt_project/`) y agregaría una dependencia más para mantener. Como `src/dbt_runner.py` ya encapsula `dbt build` correctamente y propaga el código de salida, Dagster lo invoca como una etapa más. Si en el futuro se quisiera ver los modelos dbt nodo por nodo en la interfaz, `dagster-dbt` sería el camino, con el costo de complejidad que eso implica.

Los dos warehouses siguen separados y los modelos SQL no se duplican.

---

## 6. Memoria y la restricción de 8 GB

El job se define con `executor_def=in_process_executor`, es decir **un solo proceso, una etapa a la vez**. Es una decisión deliberada:

- Dos escritores simultáneos sobre el mismo archivo DuckDB fallarían: DuckDB toma un lock exclusivo.
- Aunque las dos cadenas del grafo son independientes, correrlas en paralelo duplicaría el pico de memoria (dos instancias de DuckDB con `memory_limit=2GB` cada una).

Con ejecución secuencial, el perfil de memoria es el mismo que el del pipeline tradicional: DuckDB acotado a 2 GB con derrame a disco en `data/tmp`. Dagster agrega el overhead de su propio proceso, que es de decenas de MB.

---

## 7. Dependencias: el conflicto con dbt

Instalar Dagster rompe dbt si no se fijan versiones. Dagster arrastra `protobuf 7.x` y `grpcio-health-checking 1.85`, mientras que `dbt-common` y `dbt-adapters` exigen `protobuf <7`. [`requirements.txt`](../requirements.txt) fija las dos versiones que permiten la convivencia:

```
protobuf==6.33.6
grpcio-health-checking==1.81.2
```

Con esos pines, `pip check` no reporta conflictos y los dos motores funcionan.

---

## 8. Pruebas

[`tests/test_orchestration.py`](../tests/test_orchestration.py) cubre, con datos sintéticos y sin red:

1. **Estructura del grafo:** los nueve nodos existen y las dependencias son las esperadas, incluido que `modelo_estrella` dependa de **ambas** transformaciones.
2. **Resolución de períodos:** `2025-06 → (2024-06, 2025-06)`, incluido el cruce de año (`2025-01 → 2024-01`).
3. **Validación de configuración:** mes inválido y motor inválido fallan.
4. **Selector de motor:** apunta a `src.model` y a `src.dbt_runner`.
5. **Corrida completa e idempotencia:** el job corre dos veces y no duplica filas curadas ni vuelve a descargar.
6. **Compatibilidad:** `src.pipeline` sigue devolviendo 0.
7. **Propagación de errores:** una falla inyectada en calidad hace fracasar el job, queda identificada la etapa y `modelo_estrella` nunca se ejecuta.

---

## 9. Cómo demostrarlo

1. Abrir la interfaz: `dagster dev -m src.orchestration`.
2. Mostrar el DAG de nueve nodos y señalar que el modelo depende de los dos períodos.
3. Lanzar una ejecución desde el Launchpad con `mes: "2025-06"`. Los archivos ya están cacheados y validados por hash, así que la ingesta sale como `reused` y no hay descarga.
4. Abrir la etapa `calidad_asignado` y mostrar sus metadatos: filas leídas, aceptadas, en cuarentena y violaciones por regla, directamente en la interfaz.
5. Para mostrar un fallo: lanzar con `mes: "2025-13"`. La ingesta falla con `Mes inválido` y todo lo que depende de ella queda sin ejecutar.

---

## 10. Limitaciones

1. **No hay schedules ni sensors.** El examen pide un pipeline batch ejecutable con un comando; agregar ejecuciones programadas obligaría a dejar un `dagster-daemon` corriendo de fondo, sin beneficio para la consigna.
2. **Almacenamiento de metadatos en SQLite local.** Dagster guarda el historial de ejecuciones en `$DAGSTER_HOME` (o en un directorio temporal si no está definido). No se incorporó PostgreSQL: sería infraestructura innecesaria para una notebook.
3. **Ops, no assets.** Modelamos las etapas como ops porque el pipeline es un proceso secuencial sobre archivos, no un catálogo de tablas versionadas. Con assets se ganaría materialización incremental por partición, a costa de reescribir las cinco etapas.
4. **El paralelismo del grafo no se aprovecha.** Las dos cadenas podrían correr a la vez, pero `in_process_executor` las serializa deliberadamente por memoria y por el lock de DuckDB (ver sección 6).
5. **Sin reejecución parcial probada sobre datos reales.** La interfaz ofrece "Re-execute from failure"; no está cubierto por tests automatizados.
