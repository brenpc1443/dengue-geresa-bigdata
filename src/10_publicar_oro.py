"""Etapa 10 - Comprobación de los indicadores y publicación de la capa oro en BigQuery.

1. Calcula los indicadores del tablero desde oro y los compara con la línea base del informe
   (sección 1.4): K1 en la SE09-2023 = 24 casos; K5, rezago de respuesta 2023 = 10 semanas
   (SE09 -> SE19). Con la muestra pequeña los valores no coinciden: se informa sin detener nada.
2. Si BQ_DATASET está definido, copia las tablas de oro a BigQuery con el conector Spark-BigQuery.
   Método indirecto: el conector deja Parquet en un bucket temporal y BigQuery lo carga con un
   trabajo de carga por lotes, que no tiene costo. Variables:
       BQ_PROYECTO   proyecto de Google Cloud
       BQ_DATASET    dataset de destino (p. ej. oro)
       BQ_BUCKET_TEMPORAL  bucket de Cloud Storage para la carga (sin gs://)
Looker Studio lee de ese dataset; nunca lee bronce ni plata.
"""
import os

from pyspark.sql import Window, functions as F

from comun import UMBRAL_BROTE, Cronometro, guardar_json, leer, sesion

TABLAS_ORO = ["dim_tiempo", "dim_geografia", "fact_casos_semana_distrito", "fact_clima_semana",
              "fact_respuesta_semana", "fact_alerta_semana"]
LINEA_BASE = {"k1_casos_se09_2023": 24, "k5_rezago_2023_semanas": 10}


def k1_trujillo(spark):
    """K1: casos semanales de la provincia de Trujillo, con todas las semanas del calendario."""
    geo = leer(spark, "oro", "dim_geografia").filter("es_prov_trujillo").select("ubigeo")
    casos = (leer(spark, "oro", "fact_casos_semana_distrito").join(geo, "ubigeo")
             .groupBy("semana_key").agg(F.sum("casos").alias("casos"),
                                        F.sum(F.col("casos_con_alarma") + F.col("casos_graves")).alias("alarma_o_grave")))
    return (leer(spark, "oro", "dim_tiempo").select("semana_key", "ano", "semana", "etiqueta")
            .join(casos, "semana_key", "left").fillna(0, ["casos", "alarma_o_grave"]))


def indicadores(spark):
    k1 = k1_trujillo(spark)
    w = Window.orderBy("semana_key")
    k1 = (k1.withColumn("sobre", F.col("casos") > UMBRAL_BROTE)
            .withColumn("semaforo", F.when(F.col("casos") > UMBRAL_BROTE, "rojo")
                        .when(F.col("casos") > UMBRAL_BROTE // 2, "ambar").otherwise("verde"))
            .withColumn("k3_pct_alarma_grave", F.round(F.try_divide(F.col("alarma_o_grave"), F.col("casos")) * 100, 1)))   # nulo si 0 casos (ANSI)
    # K2: racha de semanas consecutivas sobre el umbral
    k1 = k1.withColumn("grupo", F.sum((~F.col("sobre")).cast("int")).over(w))
    k1 = k1.withColumn("k2_racha", F.sum(F.col("sobre").cast("int")).over(
        Window.partitionBy("grupo").orderBy("semana_key").rowsBetween(Window.unboundedPreceding, 0)))
    filas = {r["semana_key"]: r for r in k1.collect()}
    se09 = filas.get(202309)

    # K5: rezago 2023 = primera semana sobre el umbral -> primera nota de acción de control en La Libertad
    inicio = min((k for k, r in filas.items() if 202301 <= k <= 202352 and r["sobre"]), default=None)
    acciones = (leer(spark, "oro", "fact_respuesta_semana")
                .filter((F.col("region") == "LA LIBERTAD") & (F.col("notas_accion_control") > 0))
                .select("semana_key").collect())
    primera = min((a["semana_key"] for a in acciones if inicio and a["semana_key"] >= inicio), default=None)
    rezago = (primera - inicio) if inicio and primera and primera // 100 == inicio // 100 else None
    valores = {
        "k1_casos_se09_2023": se09["casos"] if se09 else None,
        "k1_semaforo_se09_2023": se09["semaforo"] if se09 else None,
        "k2_racha_se09_2023": se09["k2_racha"] if se09 else None,
        "k3_pct_alarma_grave_2023": None,
        "k5_inicio_brote_2023": filas[inicio]["etiqueta"] if inicio else None,
        "k5_primera_accion_2023": filas[primera]["etiqueta"] if primera in filas else None,
        "k5_rezago_2023_semanas": rezago,
    }
    a2023 = [r for k, r in filas.items() if 202301 <= k <= 202352]
    total = sum(r["casos"] for r in a2023)
    if total:
        valores["k3_pct_alarma_grave_2023"] = round(sum(r["alarma_o_grave"] for r in a2023) / total * 100, 1)
    alerta = leer(spark, "oro", "fact_alerta_semana")
    primera_alerta = (alerta.filter("alerta AND semana_key BETWEEN 202301 AND 202309")
                      .agg(F.min("semana_key")).first()[0])
    valores["k4_primera_alerta_2023"] = f"SE{primera_alerta % 100:02d}-2023" if primera_alerta else None
    comprobacion = {k: {"esperado": v, "obtenido": valores[k], "coincide": valores[k] == v}
                    for k, v in LINEA_BASE.items()}
    return valores, comprobacion


def publicar_bigquery(spark):
    proyecto, dataset = os.environ.get("BQ_PROYECTO"), os.environ["BQ_DATASET"]
    bucket = os.environ["BQ_BUCKET_TEMPORAL"]
    filas = {}
    for t in TABLAS_ORO:
        df = leer(spark, "oro", t)
        destino = f"{proyecto}.{dataset}.{t}" if proyecto else f"{dataset}.{t}"
        (df.write.format("bigquery").option("table", destino)
         .option("writeMethod", "indirect").option("temporaryGcsBucket", bucket)
         .mode("overwrite").save())
        filas[t] = df.count()
    return filas


def main():
    spark = sesion("10_publicar_oro")
    reloj = Cronometro()
    valores, comprobacion = indicadores(spark)
    informe = {"indicadores": valores, "comprobacion_linea_base": comprobacion}
    if os.environ.get("BQ_DATASET"):
        informe["publicado_en_bigquery"] = publicar_bigquery(spark)
    else:
        informe["publicado_en_bigquery"] = "no: BQ_DATASET no está definido (corrida local)"
    informe["segundos"] = reloj()
    guardar_json("10_publicar_oro.json", informe)
    print(informe)
    spark.stop()


if __name__ == "__main__":
    main()
