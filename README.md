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

## Estructura

```
repo/
├── run_pipeline.sh          ejecuta las etapas en orden
├── requirements.txt
├── src/
│   ├── comun.py             rutas (DATALAKE_URI), esquemas, semana epidemiológica
│   ├── 01_ingesta.py        datos/ -> raw/ + manifiesto SHA-256
│   ├── 02_limpieza_dengue.py   raw -> processed/casos (Parquet por año) + reglas de calidad
│   ├── 03_limpieza_clima.py    raw -> processed/clima_diario (°F->°C, pulgadas->mm, centinelas)
│   ├── 04_extraccion_rumores.py PDF -> processed/notas_cdc (pypdf en una UDF de Spark)
│   ├── 05_integracion.py    calendario ⟕ casos ⟕ clima ⟕ notas por año y semana -> curated/
│   ├── 06_optimizacion.py   CSV vs Parquet particionado; sort-merge vs broadcast join
│   ├── 07_escalabilidad.py  réplicas x1, x10, x50, x100
│   ├── 08_eda.py            figuras del análisis exploratorio
│   └── 09_modelo_base.py    regresión logística (Spark MLlib)
├── docs/                    diagrama de arquitectura
├── muestra/                 muestra pequeña de las tres fuentes
└── resultados/              reportes JSON, figuras y logs de la última corrida
```

El data lake se crea fuera del repositorio (`../datalake/`) con tres zonas:
`raw/<fuente>/fecha_obtencion=AAAA-MM-DD/`, `processed/<tabla>/ano=AAAA/` y `curated/<tabla>/`.

## Requisitos

- Java 17 (Spark 3.5.1 también funciona con Java 21)
- Python 3.10 o superior
- En Windows: `winutils.exe` y `hadoop.dll` de Hadoop 3.3.x en `%HADOOP_HOME%\bin`
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
FUENTES_DIR=muestra DATALAKE_URI=file:///tmp/datalake_muestra ./run_pipeline.sh 1 2 3 4 5
```

## Ejecución

```bash
./run_pipeline.sh            # etapas 1 a 9
./run_pipeline.sh 2 5        # solo algunas etapas
```

Variables de entorno:

| Variable | Por defecto | Ejemplo en la VM del curso |
|---|---|---|
| `DATALAKE_URI` | `file:///…/proyecto/datalake` | `hdfs://localhost:9000/datalake` |
| `SPARK_MASTER` | `local[*]` | `yarn` |
| `SPARK_DRIVER_MEMORY` | `3g` | `2g` |
| `FUENTES_DIR` | `../../datos` | `~/BigData_UPAO/datos` |

Cada etapa escribe su log en `resultados/logs/` y su reporte en `resultados/0N_*.json`.

## Resultados de la corrida del 11/10/2026

- 1 029 421 casos válidos (100 %); 14 edades fuera de rango anuladas; 152 091 filas idénticas conservadas.
- 6 660 notas extraídas de 1 103 PDF; 47 notas de dengue en La Libertad (precisión 95.7 % en revisión manual).
- Línea base reproducida: inicio del brote 2023 en la SE09, primera acción de control en la SE19 (10 semanas).
- Optimización: consulta 2.98× más rápida con Parquet particionado; join 1.32× más rápido con broadcast.
- Escalabilidad: x100 (102.9 millones de filas) en 137 s frente a 9 s de x1.
- Modelo base: AUC-PR 0.944, sensibilidad 0.933, precisión 0.741; alerta del brote 2023 dos semanas antes del inicio.
