# Big Data y Analítica para la vigilancia del dengue en la GERESA La Libertad

Proyecto integrador del curso Big Data y Analítica de Datos (UPAO, 2026-20).
Pipeline en Apache Spark que integra tres fuentes públicas para medir y adelantar la respuesta
de la GERESA La Libertad a los brotes de dengue en la provincia de Trujillo:

| Fuente | Entidad | Formato | Volumen |
|---|---|---|---|
| Vigilancia epidemiológica de dengue, 2000-2024 | MINSA / CDC Perú (datosabiertos.gob.pe) | CSV `;` con BOM | 1 029 421 filas, 103.5 MiB |
| Clima diario, estación 84501099999 (aeropuerto de Trujillo) | NOAA NCEI, GSOD | CSV | 12 067 días |
| Vigilancia de rumores, 2020-2025 | CDC Perú (epipublic.dge.gob.pe) | PDF (no estructurado) | 1 103 archivos, 342 MiB |

Equipo: Paredes Cabrera Brayan · Lozano Aguilar Erick · Suarez Salazar Drako · Sosa Macuyama Angelo.

## Arquitectura: capas bronce, plata y oro

| Capa | Ruta | Contenido |
|---|---|---|
| Bronce | `bronce/<fuente>/fecha_obtencion=AAAA-MM-DD/` | Archivos tal como llegan (CSV, CSV, PDF) + manifiesto SHA-256 en `bronce/_manifiesto/` |
| Plata | `plata/<tabla>/ano=AAAA/` | Tablas Delta limpias y tipadas: `casos`, `clima_diario`, `notas_cdc`; `_cuarentena/` con las filas rechazadas y `_calidad/resultados` con una fila por regla y corrida |
| Oro | `oro/<tabla>/` | Esquema estrella: `dim_tiempo`, `dim_geografia`, `fact_casos_semana_distrito`, `fact_clima_semana`, `fact_respuesta_semana`, `fact_alerta_semana`; y `serie_semanal_trujillo` (variables del modelo) |

- **Calidad:** el catálogo de reglas está en `src/calidad.py`. Si una regla crítica supera su umbral (1 %),
  la etapa se detiene antes de escribir plata u oro (puerta de calidad).
- **Linaje:** toda tabla de plata y oro lleva `_corrida_id` y `_fecha_carga`.
- **Oro no tiene edad, sexo ni localidad por caso**: el grano mínimo es semana × distrito.
- En Google Cloud, oro se publica en BigQuery (dataset `oro`) y el tablero de Looker Studio lee solo de ahí.

## Estructura

```
repo/
├── run_pipeline.sh          ejecuta las etapas en una sola máquina (pruebas)
├── run_dataproc.sh          ejecuta el pipeline en Google Cloud con un clúster Dataproc temporal
├── requirements.txt
├── src/
│   ├── comun.py             rutas (DATALAKE_URI), leer()/escribir() de tablas, esquemas, semana epidemiológica
│   ├── calidad.py           catálogo de reglas, tabla de resultados y puerta de calidad
│   ├── 01_ingesta.py        datos/ -> bronce/ + manifiesto SHA-256
│   ├── 02_limpieza_dengue.py   bronce -> plata/casos + reglas t1, r1-r6, u1
│   ├── 03_limpieza_clima.py    bronce -> plata/clima_diario (°F->°C, pulgadas->mm) + reglas t2, c1-c5
│   ├── 04_extraccion_rumores.py PDF -> plata/notas_cdc (pypdf en una UDF de Spark) + reglas n1-n3
│   ├── 05_integracion.py    plata -> oro: esquema estrella y serie semanal + reglas i1-i3
│   ├── 06_optimizacion.py   CSV vs tabla particionada; sort-merge vs broadcast join
│   ├── 07_escalabilidad.py  réplicas x1, x10, x50, x100 (en el clúster, con 2 y 4 workers)
│   ├── 08_eda.py            figuras del análisis exploratorio
│   ├── 09_modelo_base.py    regresión logística (Spark MLlib) -> oro/fact_alerta_semana
│   └── 10_publicar_oro.py   comprueba los indicadores K1-K5 y publica oro en BigQuery
├── docs/                    diagrama de arquitectura
├── muestra/                 muestra pequeña de las tres fuentes
└── resultados/              reportes JSON, figuras y logs de la última corrida
```

El data lake local se crea fuera del repositorio (`../datalake/`).

## Requisitos

- Java 17 o 21 (Spark 4.1)
- Python 3.10 o superior
- En Windows: `winutils.exe` y `hadoop.dll` de Hadoop 3.3.x en `%HADOOP_HOME%\bin`, y las variables
  `SPARK_HOME` (carpeta `pyspark` del entorno), `PYSPARK_PYTHON` y `PYSPARK_DRIVER_PYTHON` (el Python del entorno)
- La primera vez, Spark descarga Delta Lake 4.2.0 de Maven (unos 85 MB)
- Espacio libre: ~1 GB para el data lake y ~600 MB temporales para la prueba de escalabilidad

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Datos

Los archivos completos no se suben (el CSV de dengue supera el límite de 100 MiB de GitHub).
Descárguelos en `../../datos/` (o indique otra carpeta con `FUENTES_DIR`):

- Dengue: https://www.datosabiertos.gob.pe/dataset/vigilancia-epidemiol%C3%B3gica-de-dengue
  → `dengue_vigilancia_2000_2024.csv`
- Clima: https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-summary-of-the-day&stations=84501099999&startDate=1990-01-01&endDate=2025-12-31&format=csv
  → `clima_trujillo_aeropuerto_noaa_1990_2025.csv`
- Rumores CDC: listado por año con `POST https://epipublic.dge.gob.pe/publico/listado_ajax`
  (`bq_categoria=15&bq_anio=<año>`) → `rumores_cdc/<año>/*.pdf`

Compruebe que son los mismos archivos con los hash de `resultados/01_ingesta_manifiesto.json`
y `resultados/manifiesto_rumores_sha256.csv`.

Para probar sin descargar nada, use la muestra (2 000 casos de Trujillo sin localidad,
clima 2023-2024 y dos reportes PDF):

```bash
FUENTES_DIR=muestra DATALAKE_URI=file:///tmp/datalake_muestra RESULTADOS_DIR=/tmp/resultados_muestra \
  ./run_pipeline.sh 1 2 3 4 5
```

(`RESULTADOS_DIR` evita pisar los reportes y los logs de la corrida completa en `resultados/`.)
Las etapas 6 a 10 también corren con la muestra, pero el modelo (etapa 9) necesita los datos completos:
entrena con 2000-2014 y la muestra solo trae clima de 2023-2024.

## Ejecución

```bash
./run_pipeline.sh            # etapas 1 a 10
./run_pipeline.sh 2 5        # solo algunas etapas
```

Variables de entorno:

| Variable | Por defecto | Ejemplo en la VM del curso |
|---|---|---|
| `DATALAKE_URI` | `file:///…/proyecto/datalake` | `hdfs://localhost:9000/datalake` |
| `SPARK_MASTER` | `local[*]` | `yarn` |
| `SPARK_DRIVER_MEMORY` | `3g` | `2g` |
| `FUENTES_DIR` | `../../datos` | `~/BigData_UPAO/datos` |

Cada etapa escribe su log en `resultados/logs/` y su reporte en `resultados/NN_*.json`
(o en la carpeta indicada con `RESULTADOS_DIR`).

## Ejecución en Google Cloud (Dataproc)

```bash
gcloud auth login
PROYECTO=<id-del-proyecto> BUCKET=<bucket-en-us-central1> ./run_dataproc.sh
```

El script:
- sube las fuentes a `gs://<bucket>/datalake/bronce/`;
- crea un clúster Dataproc temporal (imagen 3.0.5: Spark 4.1.2 sobre YARN, Delta Lake 4.2.0);
- envía las etapas 2 a 10 y repite la prueba de escalabilidad con 4 workers;
- copia los reportes a `resultados/nube/<corrida>/`;
- **borra el clúster siempre**, aunque algo falle. Como seguro adicional, el clúster se borra solo
  tras 30 minutos sin trabajos o a las 6 horas.

## Resultados de la corrida del 11/10/2026

- 1 029 421 casos válidos (100 %); 14 edades fuera de rango anuladas; 152 091 filas idénticas conservadas.
- 6 660 notas extraídas de 1 103 PDF; 47 notas de dengue en La Libertad (precisión 95.7 % en revisión manual).
- Línea base reproducida: inicio del brote 2023 en la SE09, primera acción de control en la SE19 (10 semanas).
- Optimización: consulta 2.98× más rápida con Parquet particionado; join 1.32× más rápido con broadcast.
- Escalabilidad: x100 (102.9 millones de filas) en 137 s frente a 9 s de x1.
- Modelo base: AUC-PR 0.944, sensibilidad 0.933, precisión 0.741; alerta del brote 2023 dos semanas antes del inicio.
