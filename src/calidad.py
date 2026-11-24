"""Control de calidad de la capa plata.

Catálogo de reglas, tabla de resultados por corrida y puerta de calidad.

- Cada regla tiene una dimensión (validez, completitud, consistencia, unicidad,
  integridad referencial), si es crítica, un umbral (% de filas que pueden fallar) y una acción.
- Cada etapa cuenta cuántas filas cumplen y cuántas fallan, y llama a registrar().
- registrar() agrega los resultados a plata/_calidad/resultados (una fila por regla y corrida).
- puerta() detiene la etapa si una regla crítica supera su umbral, ANTES de escribir plata u oro.
  Así una fuente que cambia de formato no llega al tablero.

Umbral de las reglas críticas: 1 %. En la corrida del 11/10/2026 todas dieron 0 %, así que
superar el 1 % indica que la fuente cambió, no ruido normal.
"""
from pyspark.sql import Row

from comun import CORRIDA_ID, escribir

# id -> (tabla, dimensión, descripción, crítica, umbral %, acción)
# umbral None = regla informativa: se registra pero no detiene nada.
CATALOGO = {
    "t1": ("casos", "validez", "ano, semana y edad se convierten a número (texto no vacío)", True, 1.0, "cuarentena"),
    "r1": ("casos", "validez", "ano entre 2000 y 2024", True, 1.0, "cuarentena"),
    "r2": ("casos", "validez", "semana entre 1 y 53", True, 1.0, "cuarentena"),
    "r3": ("casos", "validez", "ubigeo de 6 dígitos", True, 1.0, "cuarentena"),
    "r4": ("casos", "consistencia", "diagnostic coherente con enfermedad", True, 1.0, "cuarentena"),
    "r5": ("casos", "validez", "sexo en {M, F}", False, 5.0, "anular campo"),
    "r6": ("casos", "validez", "edad en años entre 0 y 110", False, 5.0, "anular campo"),
    "u1": ("casos", "unicidad", "filas idénticas (se conservan: no hay ID de caso)", False, None, "registrar"),
    "t2": ("clima_diario", "validez", "las medidas de clima se convierten a número (texto no vacío)", True, 1.0, "cuarentena"),
    "c1": ("clima_diario", "completitud", "centinelas 9999.9 / 999.9 de NOAA pasan a nulo", False, None, "anular campo"),
    "c2": ("clima_diario", "completitud", "centinela 99.99 de precipitación pasa a nulo", False, None, "anular campo"),
    "c3": ("clima_diario", "unicidad", "un registro por fecha", False, None, "quitar duplicados"),
    "c4": ("clima_diario", "consistencia", "temp_min_c >= 8 °C y <= temp_media_c", False, 1.0, "anular campo"),
    "c5": ("clima_diario", "consistencia", "temp_max_c >= temp_media_c", False, 1.0, "anular campo"),
    "n1": ("notas_cdc", "completitud", "cada PDF produce al menos una nota", False, 5.0, "registrar"),
    "n2": ("notas_cdc", "completitud", "fecha del reporte no nula", True, 1.0, "cuarentena"),
    "n3": ("notas_cdc", "completitud", "la nota tiene URL de la fuente", False, 5.0, "registrar"),
    "i1": ("integracion", "integridad referencial", "toda semana con casos existe en el calendario 2000-2024", True, 0.0, "detener"),
    "i2": ("integracion", "completitud", "semanas del calendario sin clima", False, 5.0, "registrar"),
    "i3": ("integracion", "completitud", "semanas con menos de 4 días de clima", False, 5.0, "registrar"),
}


class ErrorCalidad(RuntimeError):
    pass


def resultado(regla, evaluadas, fallidas):
    tabla, dimension, descripcion, critica, umbral, accion = CATALOGO[regla]
    pct = round(fallidas / evaluadas * 100, 4) if evaluadas else 0.0
    return {"regla_id": regla, "tabla": tabla, "dimension": dimension, "descripcion": descripcion,
            "critica": critica, "filas_evaluadas": int(evaluadas), "filas_fallidas": int(fallidas),
            "pct_fallidas": pct, "umbral_pct": umbral,
            "supera_umbral": umbral is not None and pct > umbral, "accion": accion}


def registrar(spark, resultados):
    """Agrega los resultados de esta corrida a plata/_calidad/resultados y los devuelve."""
    filas = [Row(**{**r, "umbral_pct": float(r["umbral_pct"]) if r["umbral_pct"] is not None else None})
             for r in resultados]
    esquema = ("regla_id string, tabla string, dimension string, descripcion string, critica boolean, "
               "filas_evaluadas long, filas_fallidas long, pct_fallidas double, umbral_pct double, "
               "supera_umbral boolean, accion string")
    escribir(spark.createDataFrame(filas, esquema), "plata", "_calidad/resultados", modo="append")
    return resultados


def puerta(resultados):
    """Detiene la etapa si alguna regla crítica supera su umbral."""
    fallas = [r for r in resultados if r["critica"] and r["supera_umbral"]]
    if fallas:
        detalle = "; ".join(f"{r['regla_id']} {r['pct_fallidas']} % > {r['umbral_pct']} %" for r in fallas)
        raise ErrorCalidad(f"Puerta de calidad cerrada (corrida {CORRIDA_ID}): {detalle}")


def resumen(resultados):
    """Versión corta para el reporte JSON de la etapa."""
    return {r["regla_id"]: {"fallidas": r["filas_fallidas"], "pct": r["pct_fallidas"],
                            "umbral_pct": r["umbral_pct"], "supera_umbral": r["supera_umbral"]}
            for r in resultados}
