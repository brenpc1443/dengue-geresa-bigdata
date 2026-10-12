"""Etapa 2 - Limpieza y control de calidad de los casos de dengue (raw -> processed).

Entrada : raw/dengue/fecha_obtencion=*/dengue_vigilancia_2000_2024.csv (separador ';', con BOM)
Salida  : processed/casos/ano=AAAA/        Parquet, un registro por caso notificado
          processed/rechazos_casos/        filas que no pasan las reglas críticas
          resultados/02_calidad_dengue.json
"""
from pyspark.sql import functions as F

from comun import (ESQUEMA_DENGUE_RAW, UBIGEO_PROV_TRUJILLO, Cronometro, guardar_json, ruta, sesion)

DIAGNOSTICOS = {"A97.0": "DENGUE SIN SIGNOS DE ALARMA",
                "A97.1": "DENGUE CON SIGNOS DE ALARMA",
                "A97.2": "DENGUE GRAVE"}


def leer_raw(spark):
    # Esquema declarado (no inferSchema): si la fuente cambia de columnas, falla aquí y no más adelante.
    return (spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8")
            .schema(ESQUEMA_DENGUE_RAW).csv(ruta("raw", "dengue", "fecha_obtencion=*", "*.csv")))


def limpiar(df):
    """Devuelve el DataFrame con tipos corregidos, columnas derivadas y una marca por regla."""
    txt = [c for c in df.columns]
    df = df.select([F.upper(F.trim(F.col(c))).alias(c) for c in txt])

    df = (df
          .withColumn("ano", F.col("ano").cast("int"))
          .withColumn("semana", F.col("semana").cast("int"))
          .withColumn("edad", F.col("edad").cast("double"))
          # edad en años según la unidad declarada: A = años, M = meses, D = días
          .withColumn("edad_anios", F.when(F.col("tipo_edad") == "A", F.col("edad"))
                      .when(F.col("tipo_edad") == "M", F.col("edad") / 12)
                      .when(F.col("tipo_edad") == "D", F.col("edad") / 365.25)))

    diag_ok = F.create_map(*[F.lit(x) for kv in DIAGNOSTICOS.items() for x in kv])
    reglas = {
        # reglas críticas: si fallan, la fila no sirve para contar casos por semana y lugar
        "r1_ano_valido": F.col("ano").between(2000, 2024),
        "r2_semana_valida": F.col("semana").between(1, 53),
        "r3_ubigeo_6_digitos": F.col("ubigeo").rlike("^[0-9]{6}$"),
        "r4_diagnostico_coherente": diag_ok[F.col("diagnostic")] == F.col("enfermedad"),
        # reglas no críticas: si fallan, el campo queda nulo pero el caso se conserva
        "r5_sexo_valido": F.col("sexo").isin("M", "F"),
        "r6_edad_en_rango": F.col("edad_anios").between(0, 110),
    }
    for nombre, cond in reglas.items():
        df = df.withColumn(nombre, F.coalesce(cond, F.lit(False)))

    df = (df
          .withColumn("edad_anios", F.when(F.col("r6_edad_en_rango"), F.round("edad_anios", 2)))
          .withColumn("sexo", F.when(F.col("r5_sexo_valido"), F.col("sexo")))
          .withColumn("valido", F.col("r1_ano_valido") & F.col("r2_semana_valida")
                      & F.col("r3_ubigeo_6_digitos") & F.col("r4_diagnostico_coherente"))
          # columnas derivadas
          .withColumn("ubigeo_prov", F.substring("ubigeo", 1, 4))
          .withColumn("es_prov_trujillo", F.col("ubigeo_prov") == UBIGEO_PROV_TRUJILLO)
          # Alto Trujillo aparece como distrito propio solo en 2024; antes sus casos se
          # notificaban en El Porvenir. Para comparar series se agrupa en "distrito_serie".
          .withColumn("distrito_serie", F.when(F.col("distrito") == "ALTO TRUJILLO", "EL PORVENIR")
                      .otherwise(F.col("distrito")))
          .withColumn("grupo_edad", F.when(F.col("edad_anios") < 12, "0-11")
                      .when(F.col("edad_anios") < 18, "12-17").when(F.col("edad_anios") < 30, "18-29")
                      .when(F.col("edad_anios") < 60, "30-59").when(F.col("edad_anios").isNotNull(), "60+")))
    return df, list(reglas)


COLUMNAS_PROCESSED = ["ano", "semana", "departamento", "provincia", "distrito", "distrito_serie", "ubigeo",
                      "ubigeo_prov", "es_prov_trujillo", "diresa", "diagnostic", "enfermedad",
                      "edad_anios", "grupo_edad", "sexo"]


def main():
    spark = sesion("02_limpieza_dengue")
    reloj = Cronometro()
    raw = leer_raw(spark)
    df, reglas = limpiar(raw)
    df.cache()

    total = df.count()
    resumen = df.agg(*[F.sum(F.col(r).cast("int")).alias(r) for r in reglas],
                     F.sum(F.col("valido").cast("int")).alias("validos")).first().asDict()
    completitud = raw.agg(*[(F.count(c) / F.count(F.lit(1)) * 100).alias(c) for c in raw.columns]).first().asDict()
    filas_identicas = total - raw.distinct().count()

    validos = df.filter("valido").select(*COLUMNAS_PROCESSED)
    (validos.repartition("ano").write.mode("overwrite").partitionBy("ano")
     .parquet(ruta("processed", "casos")))
    df.filter(~F.col("valido")).write.mode("overwrite").parquet(ruta("processed", "rechazos_casos"))

    informe = {
        "filas_leidas": total,
        "filas_validas": resumen["validos"],
        "filas_rechazadas": total - resumen["validos"],
        "porcentaje_validas": round(resumen["validos"] / total * 100, 4),
        "cumplimiento_por_regla": {r: {"cumplen": resumen[r], "no_cumplen": total - resumen[r],
                                       "porcentaje": round(resumen[r] / total * 100, 4)} for r in reglas},
        "completitud_raw_porcentaje": {k: round(v, 2) for k, v in completitud.items()},
        "filas_identicas_conservadas": filas_identicas,
        "casos_prov_trujillo": validos.filter("es_prov_trujillo").count(),
        "segundos": reloj(),
    }
    guardar_json("02_calidad_dengue.json", informe)
    print({k: v for k, v in informe.items() if not isinstance(v, dict)})
    spark.stop()


if __name__ == "__main__":
    main()
