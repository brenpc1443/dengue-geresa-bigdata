"""Etapa 3 - Limpieza del clima diario NOAA GSOD, estación aeropuerto de Trujillo (raw -> processed).

Entrada : raw/clima/fecha_obtencion=*/clima_trujillo_aeropuerto_noaa_1990_2025.csv
Salida  : processed/clima_diario/ano=AAAA/   Parquet en unidades métricas, con año y semana epidemiológica
          resultados/03_calidad_clima.json

NOAA usa unidades de EE.UU. y marca los faltantes con valores centinela distintos según la columna.
"""
from pyspark.sql import functions as F

from comun import ESQUEMA_CLIMA_RAW, Cronometro, guardar_json, ruta, semana_epidemiologica, sesion

# columna -> (centinela de faltante, conversión a unidad métrica, nombre de salida)
F_A_C = lambda c: (c - 32) * 5 / 9          # °F -> °C
CONVERSIONES = {
    "TEMP": (9999.9, F_A_C, "temp_media_c"),
    "MAX": (9999.9, F_A_C, "temp_max_c"),
    "MIN": (9999.9, F_A_C, "temp_min_c"),
    "DEWP": (9999.9, F_A_C, "punto_rocio_c"),
    "PRCP": (99.99, lambda c: c * 25.4, "precipitacion_mm"),       # pulgadas -> mm
    "WDSP": (999.9, lambda c: c * 1.852, "viento_kmh"),            # nudos -> km/h
    "VISIB": (999.9, lambda c: c * 1.609344, "visibilidad_km"),    # millas -> km
    "SLP": (9999.9, lambda c: c, "presion_nivel_mar_hpa"),
}


def limpiar(raw):
    df = raw.select("DATE", *[F.trim(F.col(c)).cast("double").alias(c) for c in CONVERSIONES])
    for col, (centinela, conv, salida) in CONVERSIONES.items():
        df = df.withColumn(salida, F.round(F.when(F.col(col) != centinela, conv(F.col(col))), 2))
    # Regla de coherencia: la mínima no puede bajar de 8 °C (en la serie 1990-2025 el percentil 0.1
    # es 10.2 °C; solo 3 días quedan por debajo, entre 1.0 y 7.0 °C) ni superar a la media, y la
    # máxima no puede ser menor que la media. Los valores que la incumplen se anulan y se cuentan.
    df = (df.withColumn("atipico_min", (F.col("temp_min_c") < 8) | (F.col("temp_min_c") > F.col("temp_media_c")))
            .withColumn("atipico_max", F.col("temp_max_c") < F.col("temp_media_c"))
            .withColumn("temp_min_c", F.when(~F.coalesce("atipico_min", F.lit(False)), F.col("temp_min_c")))
            .withColumn("temp_max_c", F.when(~F.coalesce("atipico_max", F.lit(False)), F.col("temp_max_c"))))
    ano_epi, sem_epi = semana_epidemiologica("DATE")
    # Humedad relativa a partir de temperatura y punto de rocío (fórmula de Magnus)
    magnus = lambda t: F.exp(17.625 * t / (243.04 + t))
    df = (df.withColumn("fecha", F.to_date("DATE"))
          .withColumn("ano", F.year("fecha"))
          .withColumn("ano_epi", ano_epi).withColumn("semana_epi", sem_epi)
          .withColumn("humedad_rel_pct", F.round(100 * magnus(F.col("punto_rocio_c")) / magnus(F.col("temp_media_c")), 1)))
    return df.select("fecha", "ano", "ano_epi", "semana_epi", *[v[2] for v in CONVERSIONES.values()], "humedad_rel_pct",
                     "atipico_min", "atipico_max")


def main():
    spark = sesion("03_limpieza_clima")
    reloj = Cronometro()
    raw = (spark.read.option("header", True).schema(ESQUEMA_CLIMA_RAW)
           .csv(ruta("raw", "clima", "fecha_obtencion=*", "*.csv")))
    df = limpiar(raw).dropDuplicates(["fecha"]).filter(F.col("fecha").isNotNull())
    df.cache()

    total = df.count()
    medidas = [v[2] for v in CONVERSIONES.values()] + ["humedad_rel_pct"]
    nulos = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in medidas]).first().asDict()
    rangos = df.agg(*[F.min(c).alias(f"{c}_min") for c in medidas[:5]],
                    *[F.max(c).alias(f"{c}_max") for c in medidas[:5]]).first().asDict()
    dias_por_ano = {r["ano"]: r["count"] for r in df.groupBy("ano").count().orderBy("ano").collect()}

    df.repartition("ano").write.mode("overwrite").partitionBy("ano").parquet(ruta("processed", "clima_diario"))

    atipicos = df.agg(F.sum(F.col("atipico_min").cast("int")), F.sum(F.col("atipico_max").cast("int"))).first()
    informe = {"filas_leidas_raw": raw.count(), "dias_unicos": total,
               "atipicos_anulados": {"temp_min_c": atipicos[0], "temp_max_c": atipicos[1]},
               "faltantes_por_medida": nulos,
               "porcentaje_completitud": {c: round((1 - n / total) * 100, 2) for c, n in nulos.items()},
               "rangos_despues_de_convertir": {k: round(v, 2) for k, v in rangos.items()},
               "dias_por_ano": dias_por_ano, "segundos": reloj()}
    guardar_json("03_calidad_clima.json", informe)
    print({k: informe[k] for k in ("filas_leidas_raw", "dias_unicos", "segundos")})
    spark.stop()


if __name__ == "__main__":
    main()
