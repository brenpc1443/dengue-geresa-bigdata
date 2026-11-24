"""Configuración y utilidades compartidas por todas las etapas del pipeline.

Arquitectura medallion: el data lake tiene tres capas.
    bronce/  archivos tal como llegan de las fuentes (nunca se modifican)
    plata/   datos limpios, tipados y con control de calidad (tablas Delta)
    oro/     esquema estrella para el tablero y tabla de variables del modelo (tablas Delta)

Las rutas no se escriben dentro de los scripts: todas salen de DATALAKE_URI
(variable de entorno o valor por defecto de abajo). Para pasar de disco local
a HDFS o a Cloud Storage solo se cambia esa variable, por ejemplo:
    DATALAKE_URI=hdfs://localhost:9000/datalake
    DATALAKE_URI=gs://<bucket>/datalake

Toda lectura y escritura de tablas pasa por leer() y escribir(). Si cambia el
formato de tabla (FORMATO_TABLAS=delta | parquet), cambia solo este archivo.
"""
import json
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

RAIZ_REPO = Path(__file__).resolve().parents[1]
# En el clúster el código corre desde un .zip: los reportes se escriben en RESULTADOS_DIR
# y además se copian al data lake (ver guardar_json).
RESULTADOS = Path(os.environ.get("RESULTADOS_DIR", RAIZ_REPO / "resultados"))
RESULTADOS.mkdir(parents=True, exist_ok=True)

# Carpeta con los archivos tal como se descargaron de las fuentes oficiales.
# Una ruta relativa en FUENTES_DIR se toma desde la raíz del repositorio (p. ej. FUENTES_DIR=muestra).
FUENTES = RAIZ_REPO / os.environ.get("FUENTES_DIR", RAIZ_REPO.parents[1] / "datos")
# Sin codificar espacios (%20): Hadoop toma la ruta tal cual.
DATALAKE_URI = os.environ.get(
    "DATALAKE_URI", "file:///" + (RAIZ_REPO.parent / "datalake").resolve().as_posix().lstrip("/")
).rstrip("/")
LAGO_LOCAL = DATALAKE_URI.startswith("file:")

# Identificador de la corrida: el script de ejecución lo fija para todas las etapas;
# si una etapa se ejecuta sola, toma la fecha y hora de inicio.
CORRIDA_ID = os.environ.get("CORRIDA_ID") or time.strftime("%Y%m%dT%H%M%S")

FORMATO_TABLAS = os.environ.get("FORMATO_TABLAS", "delta")
# Fuera del clúster, Spark descarga Delta Lake de Maven. En Dataproc (imagen 3.0) Delta Lake
# 4.2.0 viene como componente opcional y no hace falta descargarlo.
DELTA_PAQUETE = os.environ.get("DELTA_PAQUETE", "io.delta:delta-spark_2.13:4.2.0")
EN_CLUSTER = os.environ.get("EN_CLUSTER") == "1"

ZONAS = ("bronce", "plata", "oro")


def uri_local(p):
    """URI de una ruta local sin codificar espacios (Hadoop no decodifica %20)."""
    return "file:///" + Path(p).resolve().as_posix().lstrip("/")


def ruta(zona, *partes):
    """Ruta dentro del data lake: ruta('plata', 'casos')."""
    return "/".join([DATALAKE_URI, zona, *partes])


# Umbral de brote (sección 1.4): máximo semanal 2018-2022 en la provincia de Trujillo.
UMBRAL_BROTE = 17
UBIGEO_PROV_TRUJILLO = "1301"


def sesion(nombre, **conf):
    b = (SparkSession.builder.appName(nombre)
         .config("spark.sql.session.timeZone", "America/Lima")
         .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
         .config("spark.ui.showConsoleProgress", "false"))
    # Sin SPARK_MASTER, Spark usa local[*] en la laptop y YARN cuando lo lanza Dataproc.
    if os.environ.get("SPARK_MASTER"):
        b = b.master(os.environ["SPARK_MASTER"])
    if not EN_CLUSTER:
        b = b.config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        if os.name == "nt" and os.environ.get("HADOOP_HOME"):
            # en Windows, Hadoop necesita hadoop.dll (de %HADOOP_HOME%\bin) para escribir archivos
            dll = Path(os.environ["HADOOP_HOME"], "bin").as_posix()
            b = b.config("spark.driver.extraJavaOptions", f"-Djava.library.path={dll}")
    if FORMATO_TABLAS == "delta":
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        if not EN_CLUSTER and DELTA_PAQUETE:
            b = b.config("spark.jars.packages", DELTA_PAQUETE)
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------- tablas
def escribir(df, zona, tabla, particion=None, modo="overwrite"):
    """Escribe una tabla de plata u oro con las columnas de linaje (_corrida_id, _fecha_carga)."""
    df = (df.withColumn("_corrida_id", F.lit(CORRIDA_ID))
            .withColumn("_fecha_carga", F.current_timestamp()))
    w = df.write.format(FORMATO_TABLAS).mode(modo)
    if FORMATO_TABLAS == "delta" and modo == "overwrite":
        w = w.option("overwriteSchema", "true")
    if particion:
        w = w.partitionBy(particion)
    w.save(ruta(zona, tabla))


def leer(spark, zona, tabla):
    return spark.read.format(FORMATO_TABLAS).load(ruta(zona, tabla))


SIN_LINAJE = ["_corrida_id", "_fecha_carga"]


# ---------------------------------------------------------------- esquemas
# Contrato de la capa bronce: el CSV de dengue se lee con este esquema y no con
# inferSchema. Todo entra como texto; los tipos se corrigen en la limpieza.
ESQUEMA_DENGUE_RAW = T.StructType([T.StructField(c, T.StringType()) for c in [
    "departamento", "provincia", "distrito", "localidad", "enfermedad", "ano",
    "semana", "diagnostic", "diresa", "ubigeo", "localcod", "edad", "tipo_edad", "sexo"]])

COLUMNAS_CLIMA_RAW = ["STATION", "DATE", "DEWP", "FRSHTT", "GUST", "MAX", "MIN", "MXSPD",
                      "PRCP", "SLP", "SNDP", "STP", "TEMP", "VISIB", "WDSP"]
ESQUEMA_CLIMA_RAW = T.StructType([T.StructField(c, T.StringType()) for c in COLUMNAS_CLIMA_RAW])


def fecha_segura(col):
    """Texto -> fecha; nulo si el texto no es una fecha válida.
    Desde Spark 4.0 el modo ANSI está activo por defecto y to_date fallaría con un valor inválido."""
    return F.try_to_date(F.col(col) if isinstance(col, str) else col)


def semana_epidemiologica(col_fecha):
    """(año, semana) epidemiológicos del MINSA/CDC: semanas de domingo a sábado;
    la semana 1 es la que contiene el 4 de enero (la que tiene al menos 4 días del año)."""
    d = fecha_segura(col_fecha)
    inicio = F.date_sub(d, F.dayofweek(d) - 1)            # domingo de esa semana
    ano_epi = F.year(F.date_add(inicio, 3))              # el miércoles decide el año
    ene4 = F.make_date(ano_epi, F.lit(1), F.lit(4))
    inicio_s1 = F.date_sub(ene4, F.dayofweek(ene4) - 1)
    semana = (F.floor(F.datediff(inicio, inicio_s1) / 7) + 1).cast("int")
    return ano_epi.cast("int"), semana


def semana_key(ano="ano", semana="semana"):
    """Clave de la dimensión tiempo: año x 100 + semana (202309 = SE09-2023)."""
    return (F.col(ano) * 100 + F.col(semana)).cast("int")


# ---------------------------------------------------------------- registro
class Cronometro:
    def __init__(self):
        self.t0 = time.perf_counter()

    def __call__(self):
        return round(time.perf_counter() - self.t0, 2)


def subir_al_lago(local, nombre_destino=None):
    """Copia un archivo local a <lago>/resultados/<corrida>/ cuando el lago no es local
    (en Dataproc los reportes quedan en el nodo maestro, que se borra con el clúster)."""
    if LAGO_LOCAL:
        return
    spark = SparkSession.getActiveSession()
    if spark is None:
        return
    jvm = spark.sparkContext._jvm
    destino = jvm.org.apache.hadoop.fs.Path(ruta("resultados", CORRIDA_ID, nombre_destino or Path(local).name))
    fs = destino.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    fs.copyFromLocalFile(False, True, jvm.org.apache.hadoop.fs.Path(Path(local).resolve().as_uri()), destino)


def guardar_json(nombre, datos):
    p = RESULTADOS / nombre
    p.write_text(json.dumps(datos, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    subir_al_lago(p)
    print(f"-> {p}")
    return p
