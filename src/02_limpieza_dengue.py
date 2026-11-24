"""Etapa 2 - Limpieza y control de calidad de los casos de dengue (bronce -> plata).

Entrada : bronce/dengue/fecha_obtencion=*/dengue_vigilancia_2000_2024.csv (separador ';', con BOM)
Salida  : plata/casos/ano=AAAA/              tabla Delta, un registro por caso notificado
          plata/_cuarentena/casos/           filas que no pasan las reglas críticas, con la regla que falló
          plata/_calidad/resultados/         una fila por regla en esta corrida (se agrega)
          resultados/02_calidad_dengue.json

Orden: se evalúan todas las reglas, se registran los resultados y se pasa por la puerta de
calidad. Solo si la puerta se abre se escriben plata/casos y la cuarentena.
"""
from pyspark.sql import functions as F

import calidad
from comun import (ESQUEMA_DENGUE_RAW, UBIGEO_PROV_TRUJILLO, Cronometro, escribir, guardar_json, ruta, sesion)

DIAGNOSTICOS = {"A97.0": "DENGUE SIN SIGNOS DE ALARMA",
                "A97.1": "DENGUE CON SIGNOS DE ALARMA",
                "A97.2": "DENGUE GRAVE"}
CRITICAS = ["r1", "r2", "r3", "r4", "t1"]


def leer_raw(spark):
    # Esquema declarado (no inferSchema): si la fuente cambia de columnas, falla aquí y no más adelante.
    return (spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8")
            .schema(ESQUEMA_DENGUE_RAW).csv(ruta("bronce", "dengue", "fecha_obtencion=*", "*.csv")))


def a_numero(col, tipo):
    """try_cast: con el modo ANSI de Spark 4 un cast de texto inválido falla; try_cast deja nulo."""
    return F.col(col).try_cast(tipo)


def limpiar(df):
    """Devuelve el DataFrame con tipos corregidos, columnas derivadas y una marca por regla."""
    df = df.select([F.upper(F.trim(F.col(c))).alias(c) for c in df.columns])
    no_vacio = lambda c: F.col(c).isNotNull() & (F.col(c) != "")

    df = (df
          # t1: el texto no vacío debe convertirse a número
          .withColumn("t1", ~((no_vacio("ano") & a_numero("ano", "int").isNull())
                              | (no_vacio("semana") & a_numero("semana", "int").isNull())
                              | (no_vacio("edad") & a_numero("edad", "double").isNull())))
          .withColumn("ano", a_numero("ano", "int"))
          .withColumn("semana", a_numero("semana", "int"))
          .withColumn("edad", a_numero("edad", "double"))
          # edad en años según la unidad declarada: A = años, M = meses, D = días
          .withColumn("edad_anios", F.when(F.col("tipo_edad") == "A", F.col("edad"))
                      .when(F.col("tipo_edad") == "M", F.col("edad") / 12)
                      .when(F.col("tipo_edad") == "D", F.col("edad") / 365.25)))

    diag_ok = F.create_map(*[F.lit(x) for kv in DIAGNOSTICOS.items() for x in kv])
    reglas = {
        # reglas críticas: si fallan, la fila no sirve para contar casos por semana y lugar
        "r1": F.col("ano").between(2000, 2024),
        "r2": F.col("semana").between(1, 53),
        "r3": F.col("ubigeo").rlike("^[0-9]{6}$"),
        "r4": diag_ok[F.col("diagnostic")] == F.col("enfermedad"),
        # reglas no críticas: si fallan, el campo queda nulo pero el caso se conserva
        "r5": F.col("sexo").isin("M", "F"),
        "r6": F.col("edad_anios").between(0, 110),
    }
    for nombre, cond in reglas.items():
        df = df.withColumn(nombre, F.coalesce(cond, F.lit(False)))

    df = (df
          .withColumn("edad_anios", F.when(F.col("r6"), F.round("edad_anios", 2)))
          .withColumn("sexo", F.when(F.col("r5"), F.col("sexo")))
          .withColumn("valido", F.col("r1") & F.col("r2") & F.col("r3") & F.col("r4") & F.col("t1"))
          .withColumn("_regla_fallida", F.concat_ws(",", *[F.when(~F.col(r), F.lit(r)) for r in CRITICAS]))
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
    return df, ["t1", *reglas]


COLUMNAS_PLATA = ["ano", "semana", "departamento", "provincia", "distrito", "distrito_serie", "ubigeo",
                  "ubigeo_prov", "es_prov_trujillo", "diresa", "diagnostic", "enfermedad",
                  "edad_anios", "grupo_edad", "sexo"]
COLUMNAS_PROCESSED = COLUMNAS_PLATA  # nombre anterior, lo usa la etapa 7


def main():
    spark = sesion("02_limpieza_dengue")
    reloj = Cronometro()
    raw = leer_raw(spark)
    df, reglas = limpiar(raw)
    df.cache()

    total = df.count()
    cumplen = df.agg(*[F.sum(F.col(r).cast("int")).alias(r) for r in reglas],
                     F.sum(F.col("valido").cast("int")).alias("validos")).first().asDict()
    completitud = raw.agg(*[(F.count(c) / F.count(F.lit(1)) * 100).alias(c) for c in raw.columns]).first().asDict()
    filas_identicas = total - raw.distinct().count()

    resultados = [calidad.resultado(r, total, total - cumplen[r]) for r in reglas]
    resultados.append(calidad.resultado("u1", total, filas_identicas))
    calidad.registrar(spark, resultados)
    calidad.puerta(resultados)          # si una regla crítica supera su umbral, se detiene aquí

    validos = df.filter("valido").select(*COLUMNAS_PLATA)
    escribir(validos.repartition("ano"), "plata", "casos", particion="ano")
    escribir(df.filter(~F.col("valido")).select(*raw.columns, "_regla_fallida"), "plata", "_cuarentena/casos")

    informe = {
        "filas_leidas": total,
        "filas_validas": cumplen["validos"],
        "filas_rechazadas": total - cumplen["validos"],
        "porcentaje_validas": round(cumplen["validos"] / total * 100, 4),
        "calidad": calidad.resumen(resultados),
        "completitud_bronce_porcentaje": {k: round(v, 2) for k, v in completitud.items()},
        "filas_identicas_conservadas": filas_identicas,
        "casos_prov_trujillo": validos.filter("es_prov_trujillo").count(),
        "segundos": reloj(),
    }
    guardar_json("02_calidad_dengue.json", informe)
    print({k: v for k, v in informe.items() if not isinstance(v, dict)})
    spark.stop()


if __name__ == "__main__":
    main()
