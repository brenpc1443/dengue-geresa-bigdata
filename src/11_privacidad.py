"""Etapa 11 - Riesgo de reidentificación (sección 5.1).

Mide cuántos casos de la provincia de Trujillo quedan solos o en grupos pequeños cuando se
combinan sus cuasi-identificadores (k-anonimato: un caso con k = 1 es único en su grupo).

  Plata (plata/casos): año, semana, distrito, edad y sexo por caso.
    - con la edad exacta en años
    - con el grupo de edad (0-11, 12-17, 18-29, 30-59, 60+), que es lo que se propone publicar
      si alguna vez se agrega un desglose por edad
  Oro (oro/fact_casos_semana_distrito): celdas semana x distrito con 1 a 4 casos, que son
    las que se suprimirían con la regla de "menos de 5 casos".

No escribe tablas ni datos personales: el reporte solo tiene conteos.
"""
from pyspark.sql import functions as F

from comun import Cronometro, guardar_json, leer, sesion

K_MINIMO = 5   # celdas con menos de 5 casos: umbral propuesto por el equipo (sección 5.1)


def grupos_pequenos(df, columnas, total):
    g = df.groupBy(*columnas).agg(F.count(F.lit(1)).alias("k"))
    r = g.agg(F.sum(F.when(F.col("k") == 1, 1).otherwise(0)).alias("unicos"),
              F.sum(F.when(F.col("k") < K_MINIMO, F.col("k")).otherwise(0)).alias("en_grupos_pequenos"),
              F.count(F.lit(1)).alias("grupos")).first()
    unicos, pequenos = int(r["unicos"] or 0), int(r["en_grupos_pequenos"] or 0)
    return {"cuasi_identificadores": columnas, "grupos": int(r["grupos"]),
            "casos_unicos_k1": unicos, "pct_casos_unicos": round(unicos / total * 100, 1) if total else None,
            f"casos_con_k_menor_{K_MINIMO}": pequenos,
            f"pct_casos_con_k_menor_{K_MINIMO}": round(pequenos / total * 100, 1) if total else None}


def main():
    spark = sesion("11_privacidad")
    reloj = Cronometro()
    casos = leer(spark, "plata", "casos").filter("es_prov_trujillo").cache()
    total = casos.count()
    informe = {
        "casos_provincia_trujillo": total,
        "plata_edad_exacta": grupos_pequenos(casos, ["ano", "semana", "distrito", "edad_anios", "sexo"], total),
        "plata_grupo_edad": grupos_pequenos(casos, ["ano", "semana", "distrito", "grupo_edad", "sexo"], total),
    }
    geo = leer(spark, "oro", "dim_geografia").filter("es_prov_trujillo").select("ubigeo")
    oro = leer(spark, "oro", "fact_casos_semana_distrito").join(geo, "ubigeo")
    r = oro.agg(F.count(F.lit(1)).alias("celdas"),
                F.sum(F.when(F.col("casos") < K_MINIMO, 1).otherwise(0)).alias("pequenas"),
                F.sum(F.when(F.col("casos") < K_MINIMO, F.col("casos")).otherwise(0)).alias("casos_pequenas")).first()
    celdas, pequenas = int(r["celdas"]), int(r["pequenas"] or 0)
    informe["oro_semana_distrito_trujillo"] = {
        "celdas": celdas, f"celdas_con_menos_de_{K_MINIMO}_casos": pequenas,
        "pct_celdas": round(pequenas / celdas * 100, 1) if celdas else None,
        "casos_en_esas_celdas": int(r["casos_pequenas"] or 0)}
    informe["segundos"] = reloj()
    guardar_json("11_privacidad.json", informe)
    print(informe)
    spark.stop()


if __name__ == "__main__":
    main()
