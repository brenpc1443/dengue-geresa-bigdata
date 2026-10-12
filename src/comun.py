"""Configuración y utilidades compartidas por todas las etapas del pipeline.

Las rutas no se escriben dentro de los scripts: todas salen de DATALAKE_URI
(variable de entorno o valor por defecto de abajo). Para pasar de disco local
a HDFS o S3 solo se cambia esa variable, por ejemplo:
    DATALAKE_URI=hdfs://localhost:9000/datalake
"""
import json
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

RAIZ_REPO = Path(__file__).resolve().parents[1]
RESULTADOS = RAIZ_REPO / "resultados"
RESULTADOS.mkdir(exist_ok=True)

# Carpeta con los archivos tal como se descargaron de las fuentes oficiales.
# Una ruta relativa en FUENTES_DIR se toma desde la raíz del repositorio (p. ej. FUENTES_DIR=muestra).
FUENTES = RAIZ_REPO / os.environ.get("FUENTES_DIR", RAIZ_REPO.parents[1] / "datos")
# Sin codificar espacios (%20): Hadoop toma la ruta tal cual.
DATALAKE_URI = os.environ.get(
    "DATALAKE_URI", "file:///" + (RAIZ_REPO.parent / "datalake").resolve().as_posix().lstrip("/")
).rstrip("/")


def uri_local(p):
    """URI de una ruta local sin codificar espacios (Hadoop no decodifica %20)."""
    return "file:///" + Path(p).resolve().as_posix().lstrip("/")


def ruta(zona, *partes):
    """Ruta dentro del data lake: ruta('processed', 'casos')."""
    return "/".join([DATALAKE_URI, zona, *partes])


# Umbral de brote (sección 1.4): máximo semanal 2018-2022 en la provincia de Trujillo.
UMBRAL_BROTE = 17
UBIGEO_PROV_TRUJILLO = "1301"


def sesion(nombre, **conf):
    b = (SparkSession.builder.appName(nombre)
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
         .config("spark.sql.session.timeZone", "America/Lima")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false"))
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------- esquemas
# Contrato de la zona cruda: el CSV de dengue se lee con este esquema y no con
# inferSchema. Todo entra como texto; los tipos se corrigen en la limpieza.
ESQUEMA_DENGUE_RAW = T.StructType([T.StructField(c, T.StringType()) for c in [
    "departamento", "provincia", "distrito", "localidad", "enfermedad", "ano",
    "semana", "diagnostic", "diresa", "ubigeo", "localcod", "edad", "tipo_edad", "sexo"]])

COLUMNAS_CLIMA_RAW = ["STATION", "DATE", "DEWP", "FRSHTT", "GUST", "MAX", "MIN", "MXSPD",
                      "PRCP", "SLP", "SNDP", "STP", "TEMP", "VISIB", "WDSP"]
ESQUEMA_CLIMA_RAW = T.StructType([T.StructField(c, T.StringType()) for c in COLUMNAS_CLIMA_RAW])


def semana_epidemiologica(col_fecha):
    """(año, semana) epidemiológicos del MINSA/CDC: semanas de domingo a sábado;
    la semana 1 es la que contiene el 4 de enero (la que tiene al menos 4 días del año)."""
    d = F.to_date(col_fecha)
    inicio = F.date_sub(d, F.dayofweek(d) - 1)            # domingo de esa semana
    ano_epi = F.year(F.date_add(inicio, 3))              # el miércoles decide el año
    ene4 = F.make_date(ano_epi, F.lit(1), F.lit(4))
    inicio_s1 = F.date_sub(ene4, F.dayofweek(ene4) - 1)
    semana = (F.floor(F.datediff(inicio, inicio_s1) / 7) + 1).cast("int")
    return ano_epi.cast("int"), semana


# ---------------------------------------------------------------- registro
class Cronometro:
    def __init__(self):
        self.t0 = time.perf_counter()

    def __call__(self):
        return round(time.perf_counter() - self.t0, 2)


def guardar_json(nombre, datos):
    p = RESULTADOS / nombre
    p.write_text(json.dumps(datos, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"-> {p.relative_to(RAIZ_REPO)}")
    return p
