"""Etapa 7 - Prueba de escalabilidad (sección 3.7).

Para que la lectura también crezca con el volumen, la réplica se escribe en el lago:
  1. el CSV real de dengue (bronce) se convierte una vez a un archivo Parquet con el mismo esquema
     crudo (todas las columnas como texto) y se copia N veces -> <lago>/_tmp/escalabilidad_<corrida>/
  2. el factor N lee N de esas copias (N x 1 029 421 filas) y ejecuta el pipeline de casos:
     limpieza y reglas de calidad -> conteo de validez -> agregación semanal -> join con clima
     y notas -> escritura de la tabla integrada.
Antes de medir se hace una corrida de calentamiento (x1) para no cargar el arranque de la JVM
al primer factor.

La réplica vive en el lago (y no en el disco local del nodo) para que en el clúster la lean todos
los workers. Se copia con el sistema de archivos de Hadoop, que sirve igual para file://, hdfs:// y gs://.
Es temporal: se borra al terminar y no entra al análisis ni al modelo.

Uso: python 07_escalabilidad.py [--workers N] 1 10 50 100
En el clúster, la prueba de escalado horizontal se repite con 2 y con 4 workers (run_dataproc.sh).
--workers solo nombra el reporte (07_escalabilidad_w2.json, _w4.json) para que no se pisen.
"""
import json
import sys
import urllib.request
from importlib import import_module

from pyspark.sql import functions as F

from comun import (CORRIDA_ID, EN_CLUSTER, ESQUEMA_DENGUE_RAW, Cronometro, guardar_json, leer, ruta, sesion)

limpieza = import_module("02_limpieza_dengue")
integracion = import_module("05_integracion")
CASOS_TRUJILLO_REALES = 70564   # casos de la provincia de Trujillo en la corrida completa (control)


def metricas_ui(spark):
    """Lee de la API REST de la Spark UI la memoria pico, los bytes leídos y el tiempo de CPU."""
    sc = spark.sparkContext
    try:
        base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
        get = lambda p: json.load(urllib.request.urlopen(base + p, timeout=10))
        ejecutores = get("/allexecutors")
        etapas = get("/stages")
    except Exception as e:          # en YARN la UI pasa por un proxy; no es indispensable
        return {"metricas_ui": f"no disponibles ({type(e).__name__})"}
    pico = max((e.get("peakMemoryMetrics", {}).get("JVMHeapMemory", 0) for e in ejecutores), default=0)
    return {"executors_activos": len([e for e in ejecutores if e.get("id") != "driver"]) or 1,
            "heap_jvm_pico_mib": round(pico / 2**20, 1),
            "leido_de_disco_mib": round(sum(s.get("inputBytes", 0) for s in etapas) / 2**20, 1),
            "tiempo_cpu_tareas_s": round(sum(s.get("executorCpuTime", 0) for s in etapas) / 1e9, 1)}


def sistema_archivos(spark, uri):
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(uri)
    return jvm, p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), p


def preparar_replica(carpeta, copias):
    spark = sesion("07_preparar_replica")
    base = f"{carpeta}/base"
    limpieza.leer_raw(spark).coalesce(1).write.mode("overwrite").parquet(base)
    jvm, fs, p_base = sistema_archivos(spark, base)
    archivo = [s.getPath() for s in fs.listStatus(p_base) if s.getPath().getName().startswith("part-")][0]
    bytes_copia = fs.getFileStatus(archivo).getLen()
    destinos = []
    for i in range(copias):
        destino = jvm.org.apache.hadoop.fs.Path(f"{carpeta}/replica/copia_{i:03d}.parquet")
        jvm.org.apache.hadoop.fs.FileUtil.copy(fs, archivo, fs, destino, False, spark.sparkContext._jsc.hadoopConfiguration())
        destinos.append(destino.toString())
    spark.stop()
    return destinos, bytes_copia


def ejecutar(archivos, factor, salida):
    spark = sesion(f"07_escalabilidad_x{factor}")
    reloj = Cronometro()
    raw = spark.read.schema(ESQUEMA_DENGUE_RAW).parquet(*archivos[:factor])
    df, _ = limpieza.limpiar(raw)
    validos = df.filter("valido").select(*limpieza.COLUMNAS_PLATA)

    filas, n_validos = df.agg(F.count(F.lit(1)), F.sum(F.col("valido").cast("int"))).first()
    t_calidad = reloj()
    clima = leer(spark, "plata", "clima_diario")
    notas = leer(spark, "plata", "notas_cdc")
    serie, _ = integracion.integrar(spark, validos, clima, notas)
    serie.coalesce(1).write.mode("overwrite").parquet(salida)
    casos_trujillo = spark.read.parquet(salida).agg(F.sum("casos")).first()[0]
    total = reloj()
    r = {"factor": factor, "filas": filas, "filas_validas": n_validos,
         "segundos_calidad": t_calidad, "segundos_total": total,
         "filas_por_segundo": int(filas / total), "casos_trujillo_en_serie": casos_trujillo,
         **metricas_ui(spark)}
    spark.stop()
    return r


def main():
    args = sys.argv[1:]
    workers = None
    if "--workers" in args:
        i = args.index("--workers")
        workers = int(args[i + 1])
        args = args[:i] + args[i + 2:]
    factores = [int(x) for x in args] or [1, 10, 100]
    sufijo = f"_w{workers}" if workers else ""
    carpeta = ruta("_tmp", f"escalabilidad_{CORRIDA_ID}{sufijo}")
    resultados = {"entorno": "clúster" if EN_CLUSTER else "local", "n_workers": workers}
    archivos, bytes_copia = preparar_replica(carpeta, max(factores))
    try:
        resultados["bytes_por_copia_parquet"] = bytes_copia
        casos_x1 = ejecutar(archivos, 1, f"{carpeta}/calentamiento")["casos_trujillo_en_serie"]
        for f in factores:
            r = ejecutar(archivos, f, f"{carpeta}/serie_x{f}")
            # control: la serie integrada debe tener exactamente N veces los casos de x1
            r["control_casos_ok"] = r["casos_trujillo_en_serie"] == casos_x1 * f
            r["control_igual_a_corrida_completa"] = casos_x1 == CASOS_TRUJILLO_REALES
            r["volumen_csv_equivalente_mib"] = round(108_550_394 * f / 2**20, 1)
            print(json.dumps(r, ensure_ascii=False))
            resultados[f"x{f}"] = r
    finally:
        spark = sesion("07_limpieza_replica")
        _, fs, p = sistema_archivos(spark, carpeta)
        fs.delete(p, True)                       # la réplica es temporal
        guardar_json(f"07_escalabilidad{sufijo}.json", resultados)
        spark.stop()


if __name__ == "__main__":
    main()
