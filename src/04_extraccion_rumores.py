"""Etapa 4 - Extracción de texto de los PDF "Vigilancia de rumores" del CDC Perú (bronce -> plata).

Fuente no estructurada. Cada PDF es un reporte diario con varias notas de prensa con esta forma:

    <titular>
    <REGIÓN> | <texto de la nota ...>
    Fuente: <url>

Spark lee los PDF como binarios (formato binaryFile), una función de Python con pypdf extrae
el texto de cada archivo y lo separa en notas. Cada nota se marca con:
  - es_dengue            : menciona "dengue"
  - menciona_la_libertad : la etiqueta de región o el texto se refieren a La Libertad / Trujillo
  - tipo_evento          : accion_control | fallecidos | alerta | otro

Salida: plata/notas_cdc/ano=AAAA/ (Delta), plata/_cuarentena/notas_cdc/ (notas sin fecha),
        plata/_calidad/resultados/ (reglas n1-n3) y resultados/04_calidad_rumores.json
"""
import io
import re

from pyspark.sql import functions as F, types as T

import calidad
from comun import Cronometro, escribir, fecha_segura, guardar_json, ruta, semana_epidemiologica, sesion

RE_REGION = re.compile(r"^\s*([A-ZÁÉÍÓÚÑÜ][A-ZÁÉÍÓÚÑÜ .,'\-–/]{1,60}?)\s*\|\s*(.*)$")
RE_FUENTE = re.compile(r"^\s*Fuente\s*:\s*(\S+)")
RE_FECHA_META = re.compile(r"D:(\d{4})(\d{2})(\d{2})")
LUGARES_LL = ("LA LIBERTAD", "TRUJILLO", "LA ESPERANZA", "EL PORVENIR", "FLORENCIA DE MORA", "VÍCTOR LARCO",
              "HUANCHACO", "MOCHE", "LAREDO", "SALAVERRY", "CHEPÉN", "PACASMAYO", "VIRÚ", "ASCOPE",
              "SÁNCHEZ CARRIÓN", "OTUZCO", "GRAN CHIMÚ", "JULCÁN", "PATAZ", "BOLÍVAR", "SANTIAGO DE CHUCO")
# Acción de control ejecutada o anunciada. No basta "fumigar" en infinitivo: en la revisión manual
# de 2023 aparecía en recomendaciones a la población ("permitir fumigar casas"), no en acciones.
RE_ACCION = re.compile(r"fumigaci[oó]n|fumigan\b|fumigaron|fumigador|nebuliza|control larvario|larvicida|abatiz|"
                       r"brigad|cerco epidemiol|control vectorial|intervien|intervenid|viviendas? (inspeccionad|tratad)|"
                       r"recojo de inservibles|capacitaci[oó]n de", re.I)
RE_FALLECIDO = re.compile(r"falleci|muert|deceso", re.I)
RE_ALERTA = re.compile(r"alerta|emergencia sanitaria|declara.{0,20}emergencia", re.I)
ENCABEZADOS = ("BROTES, EPIZOOTIAS", "DESASTRES Y OTRAS", "EVENTOS INTERNACIONALES", "C.T.:", "FAX:", "WWW.")

ESQUEMA_NOTA = T.ArrayType(T.StructType([
    T.StructField("fecha_reporte", T.StringType()),
    T.StructField("n_paginas", T.IntegerType()),
    T.StructField("orden", T.IntegerType()),
    T.StructField("region", T.StringType()),
    T.StructField("titulo", T.StringType()),
    T.StructField("texto", T.StringType()),
    T.StructField("fuente_url", T.StringType()),
]))


def separar_notas(texto):
    """Separa el texto de un reporte en notas (titular, región, texto, url)."""
    lineas = [l.rstrip() for l in texto.splitlines()]
    notas, actual, previa = [], None, ""
    for linea in lineas:
        m_reg, m_fte = RE_REGION.match(linea), RE_FUENTE.match(linea)
        if m_reg and actual is None:
            actual = {"region": " ".join(m_reg.group(1).split()), "titulo": previa,
                      "texto": [m_reg.group(2)], "fuente_url": None}
        elif m_fte and actual is not None:
            actual["fuente_url"] = m_fte.group(1)
            notas.append(actual)
            actual = None
        elif actual is not None:
            actual["texto"].append(linea.strip())
        if linea.strip() and not linea.strip().upper().startswith(ENCABEZADOS):
            previa = linea.strip()
    return notas


def extraer(contenido):
    from pypdf import PdfReader  # import dentro de la función: se ejecuta en cada worker
    lector = PdfReader(io.BytesIO(contenido))
    meta = lector.metadata or {}
    m = RE_FECHA_META.search(str(meta.get("/CreationDate", "")))
    fecha = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None
    texto = "\n".join((p.extract_text() or "") for p in lector.pages)
    salida = []
    for i, n in enumerate(separar_notas(texto), 1):
        salida.append((fecha, len(lector.pages), i, n["region"], n["titulo"][:300],
                       " ".join(" ".join(n["texto"]).split()), n["fuente_url"]))
    if not salida:  # reporte sin notas ("No se han registrado eventos...")
        salida.append((fecha, len(lector.pages), 0, None, None, None, None))
    return salida


def main():
    spark = sesion("04_extraccion_rumores")
    reloj = Cronometro()
    extraer_udf = F.udf(extraer, ESQUEMA_NOTA)

    pdfs = (spark.read.format("binaryFile").option("pathGlobFilter", "*.pdf")
            .option("recursiveFileLookup", True)
            .load(ruta("bronce", "rumores_cdc")))
    notas = (pdfs.repartition(16)
             .select(F.regexp_extract("path", r"([^/]+\.pdf)$", 1).alias("archivo"),
                     F.explode(extraer_udf("content")).alias("n"))
             .select("archivo", "n.*"))
    ano_epi, sem_epi = semana_epidemiologica("fecha_reporte")
    texto_total = F.lower(F.concat_ws(" ", "titulo", "texto"))
    menciona_ll = F.col("region").isin(*LUGARES_LL) | texto_total.rlike("la libertad|trujillo")
    notas = (notas
             .withColumn("fecha_reporte", fecha_segura("fecha_reporte"))
             .withColumn("ano", F.year("fecha_reporte"))
             .withColumn("ano_epi", ano_epi).withColumn("semana_epi", sem_epi)
             .withColumn("es_dengue", texto_total.contains("dengue"))
             .withColumn("menciona_la_libertad", F.coalesce(menciona_ll, F.lit(False)))
             .withColumn("tipo_evento",
                         F.when(texto_total.rlike(RE_ACCION.pattern), "accion_control")
                         .when(texto_total.rlike(RE_FALLECIDO.pattern), "fallecidos")
                         .when(texto_total.rlike(RE_ALERTA.pattern), "alerta").otherwise("otro")))
    notas.cache()

    con_nota = notas.filter("orden > 0")
    informe = {
        "pdf_leidos": notas.select("archivo").distinct().count(),
        "pdf_sin_notas": notas.filter("orden = 0").count(),
        "pdf_sin_fecha": notas.filter("fecha_reporte IS NULL").select("archivo").distinct().count(),
        "notas_extraidas": con_nota.count(),
        "notas_sin_url_fuente": con_nota.filter("fuente_url IS NULL").count(),
        "notas_dengue": con_nota.filter("es_dengue").count(),
        "notas_dengue_la_libertad": con_nota.filter("es_dengue AND menciona_la_libertad").count(),
        "notas_dengue_la_libertad_por_ano": {r["ano"]: r["count"] for r in con_nota.filter(
            "es_dengue AND menciona_la_libertad").groupBy("ano").count().orderBy("ano").collect()},
        "eventos_dengue_la_libertad": {r["tipo_evento"]: r["count"] for r in con_nota.filter(
            "es_dengue AND menciona_la_libertad").groupBy("tipo_evento").count().collect()},
        "notas_por_ano": {r["ano"]: r["count"] for r in con_nota.groupBy("ano").count().orderBy("ano").collect()},
    }
    n_pdf = informe["pdf_leidos"]
    resultados = [
        calidad.resultado("n1", n_pdf, informe["pdf_sin_notas"]),
        calidad.resultado("n2", n_pdf, informe["pdf_sin_fecha"]),
        calidad.resultado("n3", informe["notas_extraidas"], informe["notas_sin_url_fuente"]),
    ]
    calidad.registrar(spark, resultados)
    calidad.puerta(resultados)
    informe["calidad"] = calidad.resumen(resultados)
    # las notas sin fecha no se pueden ubicar en una semana: van a cuarentena
    escribir(con_nota.filter("fecha_reporte IS NOT NULL").repartition("ano"), "plata", "notas_cdc", particion="ano")
    escribir(con_nota.filter("fecha_reporte IS NULL").withColumn("_regla_fallida", F.lit("n2")),
             "plata", "_cuarentena/notas_cdc")
    informe["segundos"] = reloj()
    guardar_json("04_calidad_rumores.json", informe)
    print(informe)
    spark.stop()


if __name__ == "__main__":
    main()
