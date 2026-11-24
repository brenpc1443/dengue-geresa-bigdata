"""Etapa 5 - Integración de las fuentes y construcción de la capa oro (plata -> oro).

Esquema estrella (modelo dimensional, grano declarado en cada tabla):
    dim_tiempo                    una fila = una semana epidemiológica 2000-2024 (clave semana_key = año*100+semana)
    dim_geografia                 una fila = un distrito (ubigeo)
    fact_casos_semana_distrito    una fila = semana x distrito con al menos un caso (Perú completo)
    fact_clima_semana             una fila = semana (estación aeropuerto de Trujillo)
    fact_respuesta_semana         una fila = semana x ámbito (notas de dengue del CDC; LA LIBERTAD o la región de la nota)
La etapa 9 agrega fact_alerta_semana.

Tabla de variables para el análisis y el modelo (no es parte de la estrella):
    serie_semanal_trujillo        una fila = semana: calendario 2000-2024
                                    LEFT JOIN casos de la provincia de Trujillo  (clave: ano, semana)
                                    LEFT JOIN clima semanal                      (clave: ano, semana)
                                    LEFT JOIN notas del CDC sobre La Libertad    (clave: ano, semana)
Se parte de un calendario completo porque en la mayoría de las semanas no hay casos: sin él,
esas semanas desaparecerían y el modelo no vería los periodos sin brote.

Oro no tiene edad, sexo ni localidad por caso: el grano mínimo es semana x distrito.
Reglas de calidad de la integración: i1-i3 (plata/_calidad/resultados).
"""
from pyspark.sql import functions as F

import calidad
from comun import (UMBRAL_BROTE, Cronometro, escribir, guardar_json, leer, semana_epidemiologica,
                   semana_key, sesion)


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


# ---------------------------------------------------------------- estrella
def dim_tiempo(cal):
    return (cal.withColumn("semana_key", semana_key())
            .withColumn("fin_semana", F.date_add("inicio_semana", 6))
            .withColumn("mes_inicio", F.month("inicio_semana"))
            .withColumn("etiqueta", F.format_string("SE%02d-%d", "semana", "ano"))
            .select("semana_key", "ano", "semana", "inicio_semana", "fin_semana", "mes_inicio", "etiqueta"))


def dim_geografia(casos):
    # un ubigeo puede tener el nombre escrito de varias formas en 25 años: se toma el más reciente
    ultimo = lambda c: F.max_by(c, "ano").alias(c)
    return (casos.groupBy("ubigeo")
            .agg(*[ultimo(c) for c in ["distrito", "distrito_serie", "provincia", "ubigeo_prov",
                                       "departamento", "es_prov_trujillo"]]))


def fact_casos(casos):
    diag = lambda d: F.sum((F.col("diagnostic") == d).cast("int"))
    return (casos.groupBy("ano", "semana", "ubigeo")
            .agg(F.count(F.lit(1)).alias("casos"), diag("A97.0").alias("casos_sin_alarma"),
                 diag("A97.1").alias("casos_con_alarma"), diag("A97.2").alias("casos_graves"))
            .withColumn("semana_key", semana_key())
            .select("semana_key", "ubigeo", "casos", "casos_sin_alarma", "casos_con_alarma", "casos_graves"))


def fact_respuesta(notas):
    tipo = lambda t: F.sum((F.col("tipo_evento") == t).cast("int"))
    ambito = F.when(F.col("menciona_la_libertad"), "LA LIBERTAD").otherwise(F.coalesce("region", F.lit("SIN REGIÓN")))
    return (notas.filter("es_dengue").withColumn("region", ambito)
            .groupBy(F.col("ano_epi").alias("ano"), F.col("semana_epi").alias("semana"), "region")
            .agg(F.count(F.lit(1)).alias("notas_dengue"), tipo("accion_control").alias("notas_accion_control"),
                 tipo("fallecidos").alias("notas_fallecidos"), tipo("alerta").alias("notas_alerta"))
            .withColumn("semana_key", semana_key())
            .select("semana_key", "region", "notas_dengue", "notas_accion_control", "notas_fallecidos", "notas_alerta"))


def main():
    spark = sesion("05_integracion")
    reloj = Cronometro()
    casos = leer(spark, "plata", "casos")
    clima = leer(spark, "plata", "clima_diario")
    notas = leer(spark, "plata", "notas_cdc")

    serie, cs = integrar(spark, casos, clima, notas)
    serie = serie.orderBy("ano", "semana").cache()

    # reglas de la integración (antes de escribir oro)
    n_semanas = serie.count()
    semanas_casos = cs.count()
    sin_calendario = cs.join(serie.select("ano", "semana"), ["ano", "semana"], "left_anti").count()
    sin_clima = serie.filter("dias_con_clima IS NULL").count()
    clima_incompleto = serie.filter("dias_con_clima < 4").count()
    resultados = [calidad.resultado("i1", semanas_casos, sin_calendario),
                  calidad.resultado("i2", n_semanas, sin_clima),
                  calidad.resultado("i3", n_semanas, clima_incompleto)]
    calidad.registrar(spark, resultados)
    calidad.puerta(resultados)

    escribir(serie.coalesce(1), "oro", "serie_semanal_trujillo")

    # esquema estrella: los hechos se limitan a las semanas de dim_tiempo (integridad referencial)
    dt = dim_tiempo(calendario(spark)).cache()
    claves = dt.select("semana_key")
    clima_sem = clima_semanal(clima).withColumn("semana_key", semana_key()).drop("ano", "semana")
    hechos = {
        "fact_casos_semana_distrito": fact_casos(casos),
        "fact_clima_semana": clima_sem,
        "fact_respuesta_semana": fact_respuesta(notas),
    }
    filas_oro, fuera_de_calendario = {"dim_tiempo": dt.count()}, {}
    escribir(dt.coalesce(1), "oro", "dim_tiempo")
    geo = dim_geografia(casos)
    escribir(geo.coalesce(1), "oro", "dim_geografia")
    filas_oro["dim_geografia"] = geo.count()
    for nombre, h in hechos.items():
        h = h.cache()
        dentro = h.join(claves, "semana_key", "left_semi")
        fuera_de_calendario[nombre] = h.count() - dentro.count()   # p. ej. notas y clima de 2025
        escribir(dentro.coalesce(1), "oro", nombre)
        filas_oro[nombre] = dentro.count()

    informe = {
        "semanas_calendario": n_semanas,
        "semanas_con_casos": semanas_casos,
        "calidad": calidad.resumen(resultados),
        "semanas_sobre_umbral": serie.filter("supera_umbral").count(),
        "casos_totales_serie": serie.agg(F.sum("casos")).first()[0],
        "filas_oro": filas_oro,
        "filas_fuera_de_dim_tiempo_descartadas": fuera_de_calendario,
        "segundos": reloj(),
    }
    guardar_json("05_integracion.json", informe)
    print(informe)
    serie.filter("ano = 2023 AND semana BETWEEN 5 AND 20").show(20)
    spark.stop()


if __name__ == "__main__":
    main()
