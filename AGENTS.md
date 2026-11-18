# Acuerdos de integración

- A: rama `feature/ingesta-calidad`; ingesta, esquema, calidad, configuración, orquestador y tests correspondientes.
- B: rama `feature/transformacion-modelo`; `src/transform.py`, `src/model.py`, `sql/`, `README.md`, `RESULTADOS.md`. A no modifica esas rutas.
- Interfaces: `run_ingest(month) -> Path`, `run_schema(month, raw_path) -> Path`, `run_quality(month, staged_path) -> Path`, `run_transform(month, validated_path) -> Path`, `run_model(months: list[str]) -> None`.
- Meses por defecto: 2024-06 y 2025-06; `--mes` indica el período final y se incluye el mismo mes del año anterior.
- Datos relativos a la raíz del proyecto, o `NYC_TAXI_DATA_DIR`. Usar `src.config` para rutas compartidas.
- Accepted conserva columnas del contrato, `source_file` y `warning_flags` (LIST de VARCHAR). Cuarentena agrega `violated_rules` y `rejection_flags`.
- Raw inmutable; sin datos ni credenciales en Git. No crear stubs para transformación/modelo.
- DuckDB limitado a 2 GB y 2 threads; spill en data/tmp. Ejecutar un pipeline por directorio de datos a la vez.
- Verificación: `python -m pytest -q`; ejecución A: `python -m src.pipeline --mes 2025-06 --hasta-calidad`.
