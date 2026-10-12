"""Etapa 7 - Prueba de escalabilidad (sección 3.7).

Para que la lectura también crezca con el volumen, la réplica se escribe en disco:
  1. el CSV real de dengue se convierte una vez a un archivo Parquet con el mismo esquema crudo
     (todas las columnas como texto) y se copia 100 veces -> carpeta temporal _replica/
  2. el factor N lee N de esas copias (N x 1 029 421 filas) y ejecuta el pipeline de casos:
     limpieza y reglas de calidad -> conteo de validez -> agregación semanal -> join con clima
     y notas -> escritura de la tabla integrada.
Antes de medir se hace una corrida de calentamiento (x1) para no cargar el arranque de la JVM
al primer factor.

La réplica es temporal: se borra al terminar, no entra al data lake ni al análisis ni al modelo.
Uso: python 07_escalabilidad.py 1 10 50 100
"""
import json
import shutil
import sys
import tempfile
import urllib.request
from importlib import import_module
from pathlib import Path

from pyspark.sql import functions as F

from comun import Cronometro, RESULTADOS, ESQUEMA_DENGUE_RAW, guardar_json, ruta, sesion, uri_local

limpieza = import_module("02_limpieza_dengue")
integracion = import_module("05_integracion")


def metricas_ui(spark):
    """Lee de la API REST de la Spark UI la memoria pico, los bytes leídos y el tiempo de CPU."""
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    get = lambda p: json.load(urllib.request.urlopen(base + p))
    ejecutores = get("/allexecutors")
    etapas = get("/stages")
    pico = max((e.get("peakMemoryMetrics", {}).get("JVMHeapMemory", 0) for e in ejecutores), default=0)
    return {"heap_jvm_pico_mib": round(pico / 2**20, 1),
            "leido_de_disco_mib": round(sum(s.get("inputBytes", 0) for s in etapas) / 2**20, 1),
            "tiempo_cpu_tareas_s": round(sum(s.get("executorCpuTime", 0) for s in etapas) / 1e9, 1)}


def preparar_replica(carpeta, copias):
    spark = sesion("07_preparar_replica")
    base = Path(carpeta, "base")
    limpieza.leer_raw(spark).coalesce(1).write.mode("overwrite").parquet(uri_local(base))
    spark.stop()
    archivo = next(base.glob("part-*.parquet"))
    destino = Path(carpeta, "replica")
    destino.mkdir(exist_ok=True)
    for i in range(copias):
        shutil.copyfile(archivo, destino / f"copia_{i:03d}.parquet")
    return sorted(destino.glob("*.parquet")), archivo.stat().st_size


def ejecutar(archivos, factor, salida):
    spark = sesion(f"07_escalabilidad_x{factor}")
    reloj = Cronometro()
    raw = spark.read.schema(ESQUEMA_DENGUE_RAW).parquet(*[uri_local(a) for a in archivos[:factor]])
    df, _ = limpieza.limpiar(raw)
    validos = df.filter("valido").select(*limpieza.COLUMNAS_PROCESSED)

    filas, n_validos = df.agg(F.count(F.lit(1)), F.sum(F.col("valido").cast("int"))).first()
    t_calidad = reloj()
    clima = spark.read.parquet(ruta("processed", "clima_diario"))
    notas = spark.read.parquet(ruta("processed", "notas_cdc"))
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
    factores = [int(x) for x in sys.argv[1:]] or [1, 10, 100]
    resultados = {}
    with tempfile.TemporaryDirectory(dir=RESULTADOS.parent.parent / "datalake") as tmp:
        archivos, bytes_copia = preparar_replica(tmp, max(factores))
        resultados["bytes_por_copia_parquet"] = bytes_copia
        ejecutar(archivos, 1, uri_local(Path(tmp, "calentamiento")))
        for f in factores:
            r = ejecutar(archivos, f, uri_local(Path(tmp, f"serie_x{f}")))
            # control: la serie integrada debe tener exactamente N veces los casos reales
            r["control_casos_ok"] = r["casos_trujillo_en_serie"] == 70564 * f
            r["volumen_csv_equivalente_mib"] = round(108_550_394 * f / 2**20, 1)
            print(json.dumps(r, ensure_ascii=False))
            resultados[f"x{f}"] = r
            guardar_json("07_escalabilidad.json", resultados)


if __name__ == "__main__":
    main()
