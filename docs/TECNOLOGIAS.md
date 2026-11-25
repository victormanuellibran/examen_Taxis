# Justificación de tecnologías

La consigna evalúa *"elegir la herramienta y cuidar la memoria"* en una notebook de **8 GB de RAM**, y dice que dbt, Airflow, Prefect, Dagster o PySpark *"suman puntos por sí mismos"*. Este documento explica, para cada pieza del stack, **qué alternativas había, qué criterio usamos y por qué ganó la elegida**.

## Criterios de decisión

Los mismos cinco criterios para todas las piezas:

1. **Memoria:** que funcione con 8 GB procesando ~8 millones de filas (dos meses).
2. **Reproducibilidad:** que cualquiera clone el repo y lo corra con un comando, sin servicios externos.
3. **SQL primero:** la consigna pide responder con SQL sobre el propio modelo.
4. **Costo de operación:** cuántos procesos, servidores y configuración agrega.
5. **Explicabilidad:** todo lo que se entrega se tiene que poder defender en el oral.

## Vista general: núcleo vs. capas opcionales

```
NÚCLEO (lo que pide la consigna; un comando)        CAPAS OPCIONALES (suman, no son requisito)
─────────────────────────────────────────────       ──────────────────────────────────────────
Python 3.12 + DuckDB + Parquet + YAML               dbt        → modelado declarativo + tests
src/pipeline.py: 5 etapas                           Dagster    → orquestación visual
pytest                                              Docker     → entorno idéntico en cualquier PC
                                                    GitHub Actions → CI
                                                    Streamlit  → dashboard de resultados
                                                    Prometheus + Grafana → monitoreo en vivo
```

**Regla de diseño:** ninguna capa opcional cambia un resultado. Si se borran, el núcleo sigue funcionando igual. Eso las hace fáciles de defender.

---

## 1. Motor de procesamiento: **DuckDB**

| Alternativa | A favor | En contra para este caso |
| --- | --- | --- |
| **pandas** | Conocido, mucha documentación | Carga todo en memoria. Un mes (~4,3 M filas × 20 columnas) ocupa varios GB como DataFrame, y el control de duplicados exactos (R07) sobre todas las columnas lo multiplica. No tiene derrame a disco. |
| **Polars** | Muy rápido, *lazy*, poco consumo | Buena opción. Pero su API es de DataFrames, no SQL, y no tiene adaptador dbt maduro. Tendríamos dos lenguajes para la misma lógica. |
| **PySpark** | Escala a un clúster; suma puntos | Necesita JVM y un *driver* que solo, sin datos, ya consume ~1 GB. Está pensado para varios nodos, no para una notebook. Arranque lento y más difícil de explicar. |
| **DuckDB** ✅ | SQL analítico columnar que corre **dentro del proceso** de Python. Lee y escribe Parquet directo. Respeta `memory_limit` y **derrama a disco** (`temp_directory`) cuando no le alcanza. | Un solo escritor por archivo `.duckdb` (lo documentamos como limitación). |

**Configuración que defiende la memoria:** `memory_limit='2GB'`, `threads=2`, `temp_directory=data/tmp` y `preserve_insertion_order=false` (en `src/config.py`).

**Evidencia:** el pico de RAM del proceso completo es de **~2 GB**, medido por `src/metrics.py` y visible en Grafana y en el dashboard. Es la cuarta parte de la notebook de la consigna.

## 2. Almacenamiento: **Parquet particionado estilo Hive**

| Alternativa | Por qué no |
| --- | --- |
| CSV | Sin tipos, sin compresión, hay que leerlo entero. Los archivos de la TLC ya vienen en Parquet; convertir a CSV sería perder información. |
| Una base de datos (Postgres) | Requiere un servidor corriendo. La consigna pide "almacenamiento columnar particionado". |
| **Parquet `year=/month=`** ✅ | Columnar y comprimido (ZSTD). DuckDB lee sólo las columnas que usa. La partición permite **reprocesar un mes sin tocar los demás**, que es un requisito de la Parte 4. |

## 3. Contrato de esquema: **YAML versionado**

| Alternativa | Por qué no |
| --- | --- |
| Tipos dentro del código | La consigna pide explícitamente un contrato "en un archivo versionado, no implícito en el código". |
| Great Expectations / Pandera | Potentes, pero agregan un framework entero para algo que un YAML de 30 líneas resuelve. |
| **`contracts/yellow_schema.yaml`** ✅ | Legible por cualquiera, versionado en Git y con políticas declaradas (`fail_and_report`, `fail_except_cbd_before_2025`). |

## 4. Modelado: **SQL desde Python** y **dbt** (los dos)

- **`--modelo python`** (por defecto): `src/model.py` ejecuta el SQL del modelo estrella. Es el camino mínimo y transparente.
- **`--modelo dbt`**: el mismo modelo, organizado en capas `staging → intermediate → marts`, con `ref()`/`source()` para el linaje y **27 data tests**:
  - claves únicas y no nulas;
  - integridad referencial contra `dim_zone` y `dim_date`;
  - valores aceptados de `payment_type` y `pickup_hour`;
  - reconciliación de conteos;
  - un test que verifica que **ningún viaje del hecho viole una regla de rechazo**.

**¿Por qué los dos?**
- dbt es una de las herramientas que la consigna premia. Agrega tests de datos y documentación con grafo de linaje (`.\run.ps1 dbt-docs`).
- Pero no queríamos que el pipeline *dependa* de dbt para funcionar. Las dos variantes producen las mismas tablas y las mismas respuestas.

| Alternativa | Por qué no |
| --- | --- |
| SQLMesh | Similar a dbt pero menos difundido, y no se vio en la materia. |
| Sólo dbt | Obligaría a instalar dbt aun para el camino mínimo, y suma un subproceso que hay que explicar. |

## 5. Orquestación: **Dagster**

| Alternativa | A favor | En contra para este caso |
| --- | --- | --- |
| Script propio (`src/pipeline.py`) | Simple; es el "comando único" de la consigna | Sin interfaz, sin historial, sin vista de qué etapa falló. **Lo mantenemos** como camino principal. |
| **Airflow** | Estándar de la industria | Necesita *scheduler*, *webserver* y una base de metadatos (Postgres). No corre nativo en Windows (requiere WSL o Docker). Es pesado para 8 GB. |
| **Prefect** | Liviano, buena API en Python | Opción válida. Su interfaz se centra en *flows* y no en el grafo de datos. |
| **Dagster** ✅ | Corre con `dagster dev` sin base externa (SQLite local). Muestra el **grafo de 9 nodos**, metadatos por etapa (filas leídas, aceptadas, en cuarentena) y permite relanzar. | Agrega dependencias. Las fijamos con `protobuf` y `grpcio-health-checking` para que conviva con dbt. |

**Decisión clave:** Dagster **no tiene lógica propia**. Cada `@op` llama a la misma función que usa `src/pipeline.py`, así que los dos caminos dan resultados idénticos. Además, el executor es `in_process_executor`, para que nunca haya dos procesos escribiendo DuckDB a la vez.

## 6. Entorno: **Docker + Docker Compose**

- **Por qué:** garantiza el mismo Python, las mismas librerías y el mismo sistema operativo (Linux) en cualquier máquina.
- **`mem_limit: 4g` y `cpus: 2`:** simulan una máquina más chica que la de la consigna. Si el pipeline anda con 4 GB, anda con 8.
- **Alternativa descartada:** sólo un `venv`. Depende de la versión de Python instalada en cada PC, así que lo dejamos como camino local, no como único.

## 7. Integración continua: **GitHub Actions**

En cada *push* corre `pip check`, `pytest`, `dbt parse` y `dbt compile`. Es gratuito para repositorios públicos y viene integrado con GitHub. Alternativas como GitLab CI o Jenkins implicarían otra plataforma u otro servidor.

## 8. Dashboard de resultados: **Streamlit**

| Alternativa | Por qué no |
| --- | --- |
| Power BI / Tableau | Pagos o con licencia, sólo Windows o web, y no se versionan bien en Git (archivos binarios). Además no leen DuckDB directo. |
| Metabase / Superset | Requieren un servidor con su propia base de metadatos y configurar conexiones a mano. |
| Evidence.dev | Interesante (SQL + Markdown), pero agrega Node.js al stack. |
| **Streamlit** ✅ | **Es Python**: un solo archivo (`dashboard/app.py`) versionado. Reutiliza las **mismas consultas** de `sql/` y abre el warehouse en **modo sólo lectura**. Se levanta con un comando. |

**Qué muestra:**
- las 6 preguntas;
- los 7 insights;
- el reporte de calidad, con un **explorador de filas en cuarentena** (punto de la demo oral);
- el historial de rendimiento de cada corrida.

Sus dependencias están en `requirements-dashboard.txt`, separadas. El pipeline y la imagen Docker no las necesitan.

## 9. Monitoreo en tiempo real: **Prometheus + Grafana**

**Problema:** queríamos *ver* que el pipeline respeta la memoria mientras corre. `docker stats` sólo ve contenedores, y el Administrador de tareas no se puede mostrar ni guardar.

**Solución:**
1. `src/metrics.py` mide **desde adentro del proceso**, con `psutil`: RAM (incluye DuckDB, que corre embebido), CPU, etapa actual, duración y pico por etapa, y filas de calidad.
2. Si se define `NYC_TAXI_METRICS_PORT`, publica esas métricas en formato Prometheus.
3. **Prometheus** las recolecta cada segundo.
4. **Grafana** las grafica con líneas de referencia en 2 GB (DuckDB), 4 GB (contenedor) y 8 GB (consigna).

**Funciona igual por terminal, por Dagster o en Docker.**

| Alternativa | Por qué no |
| --- | --- |
| `docker stats` | Sólo contenedores; nada queda registrado. |
| cAdvisor | Mide contenedores desde afuera; en Docker Desktop para Windows es inestable y no ve las corridas locales. |
| Sólo un CSV de tiempos | Lo tenemos igual (`data/control/run_metrics.csv`), pero no es "en vivo". |

**Por qué Prometheus + Grafana:** es la combinación estándar de observabilidad. Ambos corren como contenedores oficiales (`docker compose --profile monitoreo`) y el tablero viene **provisionado como código** (`monitoring/grafana/dashboards/*.json`), sin clics manuales.

**Costo:** dos contenedores de ≤ 512 MB, sólo cuando se pide el perfil `monitoreo`. Si no se levantan, el pipeline no se entera: el exportador no frena nada si falla.

## 10. Pruebas: **pytest + dbt tests**

- **pytest (40 tests):** reglas de calidad sobre datos sintéticos, idempotencia de la ingesta, reconciliación, orquestación (terminal y Dagster), métricas e insights. No necesitan internet ni los datos reales.
- **dbt (27 data tests):** validan el **modelo ya construido** con datos reales.
- Las dos capas se complementan: pytest prueba el **código**, dbt prueba los **datos**.

---

## Resumen para el oral (30 segundos)

> "Elegimos DuckDB porque es SQL, corre dentro del proceso y respeta un tope de memoria derramando a disco; con eso el pico es de 2 GB, la cuarta parte de la notebook de la consigna. Guardamos en Parquet particionado para poder reprocesar un mes sin tocar los otros. Sobre ese núcleo agregamos dbt para tests y linaje, y Dagster para orquestación visual, porque la consigna los premia y ninguno cambia resultados. Para mostrar los resultados usamos Streamlit, que es Python y reutiliza nuestras consultas. Para demostrar en vivo que cuidamos la memoria, el pipeline publica sus métricas, Prometheus las junta y Grafana las grafica. Descartamos Airflow y PySpark porque necesitan servidores o una JVM que no se justifican para 8 millones de filas en una notebook."
