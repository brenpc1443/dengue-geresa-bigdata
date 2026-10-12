"""Dibuja el diagrama de arquitectura implementada (docs/arquitectura.png)."""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

AZUL, CELESTE, GRIS, NARANJA, VERDE = "#003A70", "#dbe8f5", "#eeeeee", "#fbe3c8", "#dff0d8"
fig, ax = plt.subplots(figsize=(13, 6.6))
ax.set_xlim(0, 130)
ax.set_ylim(0, 66)
ax.axis("off")
plt.rcParams["font.family"] = "Arial"


def caja(x, y, w, h, texto, color, borde=AZUL, tam=8.5, negrita=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.2",
                                fc=color, ec=borde, lw=1.2))
    ax.text(x + w / 2, y + h / 2, texto, ha="center", va="center", fontsize=tam,
            fontweight="bold" if negrita else "normal", wrap=True)


def flecha(x1, y1, x2, y2, texto=None, dy=1.4):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=12, color="#333333", lw=1.1))
    if texto:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + dy, texto, ha="center", fontsize=7, color="#555555", style="italic")


def zona(x, y, w, h, titulo):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.6,rounding_size=2", fc="none",
                                ec="#9aa7b4", lw=1, ls="--"))
    ax.text(x + 1, y + h - 2.2, titulo, fontsize=9, fontweight="bold", color=AZUL)


# Fuentes
zona(0.5, 8, 21, 52, "FUENTES")
caja(2, 45, 18, 10, "Dengue (CSV, 103.5 MiB)\nMINSA / CDC Perú\ndatosabiertos.gob.pe", GRIS)
caja(2, 30, 18, 10, "Clima diario (CSV)\nNOAA GSOD\nestación 84501099999", GRIS)
caja(2, 15, 18, 10, "Rumores CDC (1 103 PDF)\nno estructurada\nepipublic.dge.gob.pe", GRIS)

# Ingesta
caja(25, 30, 13, 10, "01_ingesta\ncopia íntegra\n+ SHA-256", NARANJA)
for y in (50, 35, 20):
    flecha(20.8, y, 24.6, 35)

# Data lake
zona(41, 39, 47, 23, "DATA LAKE  (DATALAKE_URI: file:// hoy, hdfs:// o s3a:// sin cambiar código)")
caja(43, 40.5, 12, 15, "raw/\n<fuente>/\nfecha_obtencion=\nAAAA-MM-DD/\n(archivo original)", CELESTE, tam=8)
caja(60, 40.5, 12, 15, "processed/\ncasos/ano=\nclima_diario/ano=\nnotas_cdc/ano=\n(Parquet, Snappy)", CELESTE, tam=8)
caja(76, 40.5, 11, 15, "curated/\nserie_semanal_\ntrujillo/\ncasos_semana_\ndistrito/", CELESTE, tam=8)
flecha(38.6, 35, 42.6, 49)

# Procesamiento
zona(41, 6, 47, 29, "PROCESAMIENTO - Apache Spark 3.5.1 (PySpark, DataFrames, MLlib)")
caja(43, 17, 13, 12, "02 limpieza dengue\n03 limpieza clima\n04 extracción PDF\n(pypdf en UDF)\n+ reglas de calidad", "white", tam=7.5)
caja(59, 17, 12, 12, "05 integración\ncalendario + casos\n+ clima + notas\n(left join por\naño y semana)", "white", tam=7.5)
caja(74, 17, 12, 12, "08 EDA\n09 modelo base\n(regresión logística\nMLlib)", "white", tam=7.5)
ax.text(64.5, 8.6, "06 optimización y 07 escalabilidad miden esta capa", ha="center", fontsize=7.5, color="#555555")
flecha(49, 40.1, 49, 29.4, "lee", dy=0)
flecha(56.4, 25, 59.6, 46, "escribe", dy=0)
flecha(66, 40.1, 66, 29.4, "lee", dy=0)
flecha(71.4, 25, 76.6, 45, "escribe", dy=0)
flecha(81.5, 40.1, 81.5, 29.4, "lee", dy=0)

# Consumo
zona(92, 8, 37, 52, "CONSUMO")
caja(95, 43, 31, 11, "Resultados del pipeline (repo/resultados/)\nreportes de calidad JSON, figuras del EDA,\nmétricas del modelo, logs de cada etapa", VERDE, tam=8)
caja(95, 26, 31, 11, "Tablero GERESA (Semana 11)\n3-5 indicadores sobre curated/\nalerta semanal de brote", "white", borde="#9aa7b4", tam=8)
caja(95, 11, 31, 10, "Área de Epidemiología GERESA La Libertad\ndecide; Red de Salud Trujillo ejecuta\nel control vectorial", GRIS, tam=8)
flecha(87.6, 50, 94.6, 48)
flecha(87.6, 46, 94.6, 31, "planificado", dy=0)
flecha(110.5, 25.6, 110.5, 21.4)
ax.text(65, 64.5, "Arquitectura implementada - Big Data y Analítica para la vigilancia del dengue en la GERESA La Libertad",
        ha="center", fontsize=11, fontweight="bold", color=AZUL)
fig.tight_layout()
fig.savefig(Path(__file__).with_name("arquitectura.png"), dpi=200)
