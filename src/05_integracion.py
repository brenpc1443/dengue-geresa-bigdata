"""Etapa 5 - Integración de las fuentes por año y semana epidemiológica (processed -> curated).

    calendario 2000-2024 (todas las semanas, aunque no haya casos)
      LEFT JOIN casos de la provincia de Trujillo agregados por semana     (clave: ano, semana)
      LEFT JOIN clima semanal del aeropuerto de Trujillo                     (clave: ano, semana)
      LEFT JOIN notas del CDC sobre dengue en La Libertad, por semana        (clave: ano, semana)

Se parte de un calendario completo porque en la mayoría de las semanas no hay casos: sin él,
esas semanas desaparecerían y el modelo no vería los periodos sin brote.

Salidas (curated):
    serie_semanal_trujillo/      una fila por semana epidemiológica (1 300 filas aprox.)
    casos_semana_distrito/       casos por semana y distrito de la provincia de Trujillo
"""
from pyspark.sql import functions as F

from comun import UMBRAL_BROTE, Cronometro, guardar_json, ruta, semana_epidemiologica, sesion


def calendario(spark):
    fechas = spark.sql("SELECT explode(sequence(to_date('2000-01-02'), to_date('2024-12-28'), interval 1 day)) AS fecha")
    ano, sem = semana_epidemiologica("fecha")
    return (fechas.select(ano.alias("ano"), sem.alias("semana"), "fecha")
            .groupBy("ano", "semana").agg(F.min("fecha").alias("inicio_semana"))
            .filter(F.col("ano").between(2000, 2024)))


def casos_semanales(casos):
    return (casos.filter("es_prov_trujillo").groupBy("ano", "semana")
            .agg(F.count(F.lit(1)).alias("casos"),
                 F.sum((F.col("diagnostic") == "A97.1").cast("int")).alias("casos_con_alarma"),
                 F.sum((F.col("diagnostic") == "A97.2").cast("int")).alias("casos_graves")))


def clima_semanal(clima):
    return (clima.groupBy(F.col("ano_epi").alias("ano"), F.col("semana_epi").alias("semana"))
            .agg(F.round(F.avg("temp_media_c"), 2).alias("temp_media_c"),
                 F.round(F.max("temp_max_c"), 2).alias("temp_max_c"),
                 F.round(F.min("temp_min_c"), 2).alias("temp_min_c"),
                 F.round(F.sum("precipitacion_mm"), 2).alias("precipitacion_mm"),
                 F.round(F.avg("humedad_rel_pct"), 1).alias("humedad_rel_pct"),
                 F.count("temp_media_c").alias("dias_con_clima")))


def notas_semanales(notas):
    ll = notas.filter("es_dengue AND menciona_la_libertad")
    return (ll.groupBy(F.col("ano_epi").alias("ano"), F.col("semana_epi").alias("semana"))
            .agg(F.count(F.lit(1)).alias("notas_dengue_ll"),
                 F.sum((F.col("tipo_evento") == "accion_control").cast("int")).alias("notas_accion_control")))


def integrar(spark, casos, clima, notas, broadcast=True):
    cs, cl, nt = casos_semanales(casos), clima_semanal(clima), notas_semanales(notas)
    if broadcast:  # las tablas semanales son pequeñas: se envían completas a cada tarea
        cl, nt = F.broadcast(cl), F.broadcast(nt)
    serie = (calendario(spark)
             .join(cs, ["ano", "semana"], "left")
             .join(cl, ["ano", "semana"], "left")
             .join(nt, ["ano", "semana"], "left")
             .fillna(0, ["casos", "casos_con_alarma", "casos_graves", "notas_dengue_ll", "notas_accion_control"])
             .withColumn("supera_umbral", F.col("casos") > UMBRAL_BROTE))
    return serie, cs


def main():
    spark = sesion("05_integracion")
    reloj = Cronometro()
    casos = spark.read.parquet(ruta("processed", "casos"))
    clima = spark.read.parquet(ruta("processed", "clima_diario"))
    notas = spark.read.parquet(ruta("processed", "notas_cdc"))

    serie, cs = integrar(spark, casos, clima, notas)
    serie = serie.orderBy("ano", "semana").cache()
    serie.coalesce(1).write.mode("overwrite").parquet(ruta("curated", "serie_semanal_trujillo"))

    distrito = (casos.filter("es_prov_trujillo")
                .groupBy("ano", "semana", "ubigeo", "distrito", "distrito_serie").count()
                .withColumnRenamed("count", "casos"))
    distrito.coalesce(1).write.mode("overwrite").parquet(ruta("curated", "casos_semana_distrito"))

    # registros sin correspondencia en el join
    semanas_casos = cs.count()
    sin_calendario = cs.join(serie.select("ano", "semana"), ["ano", "semana"], "left_anti").count()
    informe = {
        "semanas_calendario": serie.count(),
        "semanas_con_casos": semanas_casos,
        "semanas_con_casos_fuera_de_calendario": sin_calendario,
        "semanas_sin_clima": serie.filter("dias_con_clima IS NULL").count(),
        "semanas_con_clima_incompleto_menos_4_dias": serie.filter("dias_con_clima < 4").count(),
        "semanas_sobre_umbral": serie.filter("supera_umbral").count(),
        "casos_totales_serie": serie.agg(F.sum("casos")).first()[0],
        "segundos": reloj(),
    }
    guardar_json("05_integracion.json", informe)
    print(informe)
    serie.filter("ano = 2023 AND semana BETWEEN 5 AND 20").show(20)
    spark.stop()


if __name__ == "__main__":
    main()
