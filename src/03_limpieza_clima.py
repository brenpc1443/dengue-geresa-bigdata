"""Etapa 3 - Limpieza del clima diario NOAA GSOD, estación aeropuerto de Trujillo (bronce -> plata).

Entrada : bronce/clima/fecha_obtencion=*/clima_trujillo_aeropuerto_noaa_1990_2025.csv
Salida  : plata/clima_diario/ano=AAAA/   tabla Delta en unidades métricas, con año y semana epidemiológica
          plata/_cuarentena/clima_diario/ días cuyo texto no se pudo convertir a número
          plata/_calidad/resultados/      reglas t2 y c1-c5 (se agrega)
          resultados/03_calidad_clima.json

NOAA usa unidades de EE.UU. y marca los faltantes con valores centinela distintos según la columna.
"""
from functools import reduce

from pyspark.sql import functions as F

import calidad
from comun import ESQUEMA_CLIMA_RAW, Cronometro, escribir, fecha_segura, guardar_json, ruta, semana_epidemiologica, sesion

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
MEDIDAS = [v[2] for v in CONVERSIONES.values()] + ["humedad_rel_pct"]


def limpiar(raw):
    txt = {c: F.trim(F.col(c)) for c in CONVERSIONES}
    # t2: el texto no vacío debe convertirse a número (try_cast: con ANSI un cast inválido fallaría)
    falla_t2 = reduce(lambda a, b: a | b, [(txt[c].isNotNull() & (txt[c] != "") & txt[c].try_cast("double").isNull())
                                         for c in CONVERSIONES])
    df = raw.select("DATE", *[txt[c].try_cast("double").alias(c) for c in CONVERSIONES], (~falla_t2).alias("t2"))
    # c1 y c2: cuántos valores vienen con el centinela de faltante (antes de anularlos)
    centinela = lambda cols: reduce(lambda a, b: a + b, [(F.col(c) == CONVERSIONES[c][0]).cast("int") for c in cols])
    df = (df.withColumn("n_centinelas_c1", centinela([c for c in CONVERSIONES if c != "PRCP"]))
            .withColumn("n_centinelas_c2", centinela(["PRCP"])))
    for col, (valor, conv, salida) in CONVERSIONES.items():
        df = df.withColumn(salida, F.round(F.when(F.col(col) != valor, conv(F.col(col))), 2))
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
    df = (df.withColumn("fecha", fecha_segura("DATE"))
          .withColumn("ano", F.year("fecha"))
          .withColumn("ano_epi", ano_epi).withColumn("semana_epi", sem_epi)
          .withColumn("humedad_rel_pct", F.round(100 * magnus(F.col("punto_rocio_c")) / magnus(F.col("temp_media_c")), 1)))
    return df.select("fecha", "ano", "ano_epi", "semana_epi", *MEDIDAS, "atipico_min", "atipico_max",
                     "t2", "n_centinelas_c1", "n_centinelas_c2")


def main():
    spark = sesion("03_limpieza_clima")
    reloj = Cronometro()
    raw = (spark.read.option("header", True).schema(ESQUEMA_CLIMA_RAW)
           .csv(ruta("bronce", "clima", "fecha_obtencion=*", "*.csv")))
    todo = limpiar(raw).filter(F.col("fecha").isNotNull())
    todo.cache()
    filas_leidas = raw.count()
    n_con_fecha = todo.count()
    df = todo.dropDuplicates(["fecha"])
    df.cache()
    total = df.count()

    c = df.agg(F.sum((~F.col("t2")).cast("int")).alias("t2"),
               F.sum("n_centinelas_c1").alias("c1"), F.sum("n_centinelas_c2").alias("c2"),
               F.sum(F.coalesce(F.col("atipico_min"), F.lit(False)).cast("int")).alias("c4"),
               F.sum(F.coalesce(F.col("atipico_max"), F.lit(False)).cast("int")).alias("c5")).first().asDict()
    n_valores_c1 = total * (len(CONVERSIONES) - 1)
    resultados = [
        calidad.resultado("t2", total, c["t2"]),
        calidad.resultado("c1", n_valores_c1, c["c1"]),       # se evalúan valores, no días
        calidad.resultado("c2", total, c["c2"]),
        calidad.resultado("c3", n_con_fecha, n_con_fecha - total),
        calidad.resultado("c4", total, c["c4"]),
        calidad.resultado("c5", total, c["c5"]),
    ]
    calidad.registrar(spark, resultados)
    calidad.puerta(resultados)

    salida = df.filter("t2").drop("t2", "n_centinelas_c1", "n_centinelas_c2")
    escribir(salida.repartition("ano"), "plata", "clima_diario", particion="ano")
    escribir(df.filter(~F.col("t2")).drop("n_centinelas_c1", "n_centinelas_c2"), "plata", "_cuarentena/clima_diario")

    nulos = salida.agg(*[F.sum(F.col(m).isNull().cast("int")).alias(m) for m in MEDIDAS]).first().asDict()
    rangos = salida.agg(*[F.min(m).alias(f"{m}_min") for m in MEDIDAS[:5]],
                        *[F.max(m).alias(f"{m}_max") for m in MEDIDAS[:5]]).first().asDict()
    dias_por_ano = {r["ano"]: r["count"] for r in salida.groupBy("ano").count().orderBy("ano").collect()}
    informe = {"filas_leidas_bronce": filas_leidas, "dias_unicos": total,
               "calidad": calidad.resumen(resultados),
               "atipicos_anulados": {"temp_min_c": c["c4"], "temp_max_c": c["c5"]},
               "faltantes_por_medida": nulos,
               "porcentaje_completitud": {m: round((1 - n / total) * 100, 2) for m, n in nulos.items()},
               "rangos_despues_de_convertir": {k: round(v, 2) for k, v in rangos.items() if v is not None},
               "dias_por_ano": dias_por_ano, "segundos": reloj()}
    guardar_json("03_calidad_clima.json", informe)
    print({k: informe[k] for k in ("filas_leidas_bronce", "dias_unicos", "segundos")})
    spark.stop()


if __name__ == "__main__":
    main()
