# Pipeline NYC Taxi

**Examen de Fundamentos de Datos**

**Integrantes:** Libran Victor Manuel 57810, Lix KlettCarlos Federico 57809

## Presentación del proyecto

Este proyecto implementa un pipeline de Ingeniería de Datos para procesar y analizar registros de viajes de taxis amarillos de Nueva York, publicados por la New York City Taxi & Limousine Commission (NYC TLC).

El objetivo es construir un proceso reproducible que permita obtener los datos originales, verificar su estructura y calidad, transformarlos y organizarlos en un modelo analítico para responder preguntas de negocio.

Se seleccionaron los períodos **junio de 2024 y junio de 2025**, permitiendo comparar la actividad del mismo mes en dos años consecutivos.

## Solución desarrollada

El sistema implementa un pipeline de procesamiento por lotes dividido en cinco etapas:

1. **Ingesta:** obtención de archivos Parquet y del catálogo oficial de zonas, con controles de integridad e idempotencia.
2. **Normalización del esquema:** aplicación de un contrato YAML para mantener una estructura consistente entre ambos períodos.
3. **Calidad de datos:** validación mediante reglas explícitas, separación de registros aceptados y cuarentena de registros rechazados.
4. **Transformación:** generación de métricas derivadas y archivos Parquet organizados por particiones.
5. **Modelado y análisis:** construcción de un modelo dimensional en DuckDB y ejecución de seis consultas SQL para obtener resultados de negocio.

El proceso completo se ejecuta mediante un único comando.

## Resultados de referencia

En las pruebas realizadas sobre la versión del 18 de noviembre de 2026, se procesaron **7.862.153 registros originales** correspondientes a ambos meses.

De ellos, **7.493.698 fueron aceptados** y **368.455 se enviaron a cuarentena** de acuerdo con las reglas de calidad.

El modelo analítico permite estudiar zonas con mayor actividad, ingresos por horario, cargos de congestión, propinas, viajes relacionados con aeropuertos y variación interanual.

Estas cifras deben actualizarse si las modificaciones finales de las reglas de calidad cambian los resultados.

## Tecnologías utilizadas

El proyecto utiliza Python para automatización, DuckDB y SQL para procesamiento y análisis, Parquet para almacenamiento columnar, PyArrow para inspección de archivos, YAML para el contrato de esquema y pytest para las pruebas automatizadas.

## Organización de la documentación

Las siguientes secciones detallan la arquitectura, la instalación y ejecución, las reglas de calidad, el modelo dimensional, los resultados obtenidos, las pruebas realizadas, las limitaciones del sistema y la declaración del uso de herramientas de inteligencia artificial durante el desarrollo.
