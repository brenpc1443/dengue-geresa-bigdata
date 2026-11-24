"""Etapa 6 - Optimización medida antes y después (sección 3.6).

Consulta de negocio: casos semanales de la provincia de Trujillo en 2023 (la que alimenta la alerta).

  A) Formato y particionado
     antes  : leer el CSV crudo completo (1 029 421 filas, 103.5 MiB) y filtrar
     después: leer plata/casos (Delta sobre Parquet, particionado por año) -> Spark solo abre ano=2023
              (poda de particiones) y solo las columnas usadas (poda de columnas)
  B) Join de la integración (etapa 5)
     antes  : sort-merge join (difusión desactivada)
     después: broadcast join de las tablas semanales pequeñas

Cada variante se ejecuta 5 veces en una sesión nueva de Spark y se reporta la mediana.
"""
import statistics
import time
from importlib import import_module

from pyspark.sql import functions as F

from comun import Cronometro, guardar_json, leer, sesion

limpieza = import_module("02_limpieza_dengue")
integracion = import_module("05_integracion")
REPETICIONES = 5


def medir(fn, n=REPETICIONES):
    tiempos = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        tiempos.append(round(time.perf_counter() - t0, 3))
    return {"mediana_s": statistics.median(tiempos), "tiempos_s": tiempos}


def consulta_csv(spark):
    raw = limpieza.leer_raw(spark)
    return (raw.filter((F.col("ano") == "2023") & F.col("ubigeo").startswith("1301"))
            .groupBy("semana").count().collect())


def consulta_parquet(spark):
    casos = leer(spark, "plata", "casos")
    return (casos.filter((F.col("ano") == 2023) & F.col("es_prov_trujillo"))
            .groupBy("semana").count().collect())


def main():
    reloj = Cronometro()
    resultados = {}

    spark = sesion("06_optimizacion")
    # verificación: ambas consultas devuelven lo mismo
    a = sorted((int(r["semana"]), r["count"]) for r in consulta_csv(spark))
    b = sorted((r["semana"], r["count"]) for r in consulta_parquet(spark))
    assert a == b, "las dos variantes no coinciden"
    plan = leer(spark, "plata", "casos").filter("ano = 2023 AND es_prov_trujillo") \
        ._jdf.queryExecution().executedPlan().toString()
    resultados["A_formato_y_particionado"] = {
        "antes_csv_completo": medir(lambda: consulta_csv(spark)),
        "despues_parquet_particionado": medir(lambda: consulta_parquet(spark)),
        "casos_2023_verificados": sum(c for _, c in b),
        "plan_fisico_despues": [l.strip() for l in plan.splitlines() if "PartitionFilters" in l or "ReadSchema" in l],
    }
    spark.stop()

    def correr_integracion(spark, broadcast):
        casos = leer(spark, "plata", "casos")
        clima = leer(spark, "plata", "clima_diario")
        notas = leer(spark, "plata", "notas_cdc")
        serie, _ = integracion.integrar(spark, casos, clima, notas, broadcast=broadcast)
        return serie.write.format("noop").mode("overwrite").save()

    for nombre, bc, conf in [("antes_sort_merge_join", False, {"spark.sql.autoBroadcastJoinThreshold": "-1"}),
                             ("despues_broadcast_join", True, {})]:
        spark = sesion("06_optimizacion_join", **conf)
        correr_integracion(spark, bc)  # calentamiento
        resultados.setdefault("B_join_integracion", {})[nombre] = medir(lambda: correr_integracion(spark, bc))
        spark.stop()

    for k, v in resultados.items():
        antes, despues = [x["mediana_s"] for x in v.values() if isinstance(x, dict) and "mediana_s" in x]
        v["mejora_veces"] = round(antes / despues, 2)
        v["reduccion_pct"] = round((1 - despues / antes) * 100, 1)
    resultados["segundos_totales"] = reloj()
    spark = sesion("06_optimizacion_reporte")   # sesión activa para copiar el reporte al lago
    guardar_json("06_optimizacion.json", resultados)
    spark.stop()
    print(resultados)


if __name__ == "__main__":
    main()
