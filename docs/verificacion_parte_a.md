# Verificación de la Parte A

Esta verificación se realizó sobre `main` en la carpeta de trabajo del examen el 18/11/2026. No modifica datos raw ni archivos de la Parte B.

## Suite automatizada

Comando:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Resultado observado:

```text
14 passed in 0.53s
```

La suite cubre calidad sintética, idempotencia, fallas de red y temporales parciales, reconciliación, drift de esquema y contratos del orquestador.

## Ejecución real hasta calidad

Comando:

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --mes 2025-06 --hasta-calidad
```

Resultado observado, con código de salida 0:

| Mes | Filas leídas | Aceptadas | Cuarentena |
|---|---:|---:|---:|
| 2024-06 | 3.539.193 | 3.441.465 | 97.728 |
| 2025-06 | 4.322.960 | 4.052.233 | 270.727 |

En ambos meses se cumple exactamente `leídas = aceptadas + cuarentena`.

## Evidencia de ingesta

Los archivos raw se reutilizaron sin nueva descarga porque sus hashes coincidieron con `data/control/ingestion_log.csv`:

| Archivo | SHA-256 | Filas |
|---|---|---:|
| `yellow_tripdata_2024-06.parquet` | `677cf14c8347f745b583f012fbdba072334c6c4efa17bfd6a64369f2ba30329c` | 3.539.193 |
| `yellow_tripdata_2025-06.parquet` | `1bea7f5b09c744414451fa7fef09fbc5a393ffb1621c859be6ba0df23a14e1b6` | 4.322.960 |
| `taxi_zone_lookup.csv` | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` | 265 |

## Esquema y calidad

El esquema de 2024 informa la ausencia esperada de `cbd_congestion_fee`; el contrato la agrega como `DOUBLE NULL`. Ambos archivos informan `INTEGER` para IDs que se amplían sin pérdida a `BIGINT`. No hubo columnas nuevas ni conversiones inseguras.

Las reglas observadas en los reportes JSON fueron:

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

R08 se justifica por la exploración real de propinas negativas. Las advertencias R05 y R08 conservan las filas y se publican en `warning_flags`; sólo las reglas de rechazo determinan la cuarentena.

## Límites de esta evidencia

La ejecución completa también fue realizada y generó la capa curada, el warehouse y `RESULTADOS.md`. El tiempo observado con los archivos raw ya presentes fue de aproximadamente 70 segundos. El pico de RAM del proceso todavía no se midió con una herramienta estable; DuckDB mantiene configurado un límite de 2 GB y usa `data/tmp` para temporales.
