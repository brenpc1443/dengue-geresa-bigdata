"""Etapa 8 - Análisis exploratorio sobre los datos limpios (sección 4.1).

Lee solo las zonas processed y curated. Cada gráfico responde a una pregunta:
  P1. ¿Cuándo hubo brotes en la provincia de Trujillo y qué tan excepcional fue 2023-2024?
  P2. ¿Los casos siguen la estación cálida? (estacionalidad de casos y temperatura)
  P3. ¿Con cuántas semanas de adelanto se mueve el clima respecto de los casos? (correlación con rezago)
  P4. ¿Cuánto tardó la respuesta registrada en 2023 frente a la curva de casos? (casos + notas del CDC)
  P5. ¿En qué distritos se concentran los casos de los brotes 2023 y 2024?
Salida: resultados/figuras/*.png y resultados/08_eda.json
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F

from comun import RESULTADOS, UMBRAL_BROTE, guardar_json, ruta, sesion

FIG = RESULTADOS / "figuras"
FIG.mkdir(exist_ok=True)
plt.rcParams.update({"font.family": "Arial", "font.size": 10, "axes.spines.top": False,
                     "axes.spines.right": False, "figure.dpi": 150})
AZUL, ROJO, GRIS, NARANJA = "#1f5a99", "#c0392b", "#7f8c8d", "#e67e22"


def guardar(fig, nombre):
    fig.tight_layout()
    fig.savefig(FIG / nombre, dpi=200)
    plt.close(fig)


def main():
    spark = sesion("08_eda")
    serie = spark.read.parquet(ruta("curated", "serie_semanal_trujillo")).orderBy("ano", "semana").toPandas()
    distrito = spark.read.parquet(ruta("curated", "casos_semana_distrito"))
    notas = spark.read.parquet(ruta("processed", "notas_cdc"))
    hallazgos = {}

    # ---------------- P1: serie completa
    serie["t"] = serie["ano"] + (serie["semana"] - 1) / 52.18
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(serie["t"], serie["casos"], color=AZUL, lw=0.9)
    ax.axhline(UMBRAL_BROTE, color=ROJO, ls="--", lw=1, label=f"Umbral de brote ({UMBRAL_BROTE} casos/semana)")
    ax.set_yscale("symlog", linthresh=10)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Año (semana epidemiológica)")
    ax.set_ylabel("Casos notificados por semana (escala log)")
    ax.set_title("Casos semanales de dengue, provincia de Trujillo, 2000-2024")
    ax.legend(loc="upper left", frameon=False)
    guardar(fig, "p1_serie_semanal.png")
    anual = serie.groupby("ano").agg(casos=("casos", "sum"), semanas_sobre_umbral=("supera_umbral", "sum"),
                                     maximo_semanal=("casos", "max"))
    hallazgos["p1_casos_por_ano"] = anual.reset_index().to_dict("records")
    hallazgos["p1_participacion_2023_2024_pct"] = round(anual.loc[[2023, 2024], "casos"].sum() / anual["casos"].sum() * 100, 1)

    # ---------------- P2: estacionalidad
    est = serie.groupby("semana").agg(casos=("casos", "mean"), temp=("temp_media_c", "mean")).reset_index()
    fig, ax = plt.subplots(figsize=(9, 3.4))
    ax.bar(est["semana"], est["casos"], color=AZUL, alpha=0.8, label="Casos promedio por semana")
    ax.set_xlabel("Semana epidemiológica")
    ax.set_ylabel("Casos promedio (2000-2024)")
    ax2 = ax.twinx()
    ax2.plot(est["semana"], est["temp"], color=NARANJA, lw=2, label="Temperatura media (°C)")
    ax2.set_ylabel("Temperatura media semanal (°C)")
    ax2.spines["top"].set_visible(False)
    ax.set_title("Estacionalidad: casos promedio y temperatura por semana del año")
    h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper right", frameon=False)
    guardar(fig, "p2_estacionalidad.png")
    hallazgos["p2_semana_pico_casos"] = int(est.loc[est["casos"].idxmax(), "semana"])
    hallazgos["p2_semana_pico_temperatura"] = int(est.loc[est["temp"].idxmax(), "semana"])
    hallazgos["p2_pct_casos_semanas_1_a_20"] = round(est.loc[est["semana"] <= 20, "casos"].sum() / est["casos"].sum() * 100, 1)

    # ---------------- P3: correlación con rezago (clima de k semanas antes vs casos de hoy)
    y = np.log1p(serie["casos"])
    rezagos = list(range(0, 13))
    corr = {}
    for var, nombre in [("temp_media_c", "Temperatura media"), ("humedad_rel_pct", "Humedad relativa"),
                        ("precipitacion_mm", "Precipitación")]:
        corr[nombre] = [round(y.corr(serie[var].shift(k)), 3) for k in rezagos]
    fig, ax = plt.subplots(figsize=(9, 3.4))
    for (nombre, vals), c in zip(corr.items(), [NARANJA, AZUL, GRIS]):
        ax.plot(rezagos, vals, marker="o", color=c, label=nombre)
    ax.axhline(0, color="black", lw=0.6)
    ax.set_xlabel("Rezago k (semanas): clima de la semana t-k frente a casos de la semana t")
    ax.set_ylabel("Correlación de Pearson\ncon log(1 + casos)")
    ax.set_title("¿Con cuántas semanas de adelanto se relaciona el clima con los casos?")
    ax.legend(frameon=False)
    guardar(fig, "p3_correlacion_rezago.png")
    hallazgos["p3_correlaciones"] = {n: dict(zip(rezagos, v)) for n, v in corr.items()}
    hallazgos["p3_rezago_max_temp"] = int(np.nanargmax(corr["Temperatura media"]))

    # ---------------- P4: 2023, curva de casos frente a la respuesta registrada por el CDC
    s23 = serie[serie["ano"] == 2023]
    inicio = int(s23.loc[s23["supera_umbral"], "semana"].min())
    acciones = s23.loc[s23["notas_accion_control"] > 0, "semana"].tolist()
    notas_ll = s23.loc[s23["notas_dengue_ll"] > 0, "semana"].tolist()
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.bar(s23["semana"], s23["casos"], color=AZUL, alpha=0.85, label="Casos notificados")
    ax.axhline(UMBRAL_BROTE, color=ROJO, ls="--", lw=1, label="Umbral de brote")
    ax.axvline(inicio, color=ROJO, lw=1.5)
    caja = dict(boxstyle="round,pad=0.25", fc="white", ec="none", alpha=0.9)
    ax.annotate(f"Inicio del brote\nSE{inicio:02d}", (inicio, s23["casos"].max() * 0.9), xytext=(-8, 0),
                textcoords="offset points", color=ROJO, ha="right", bbox=caja)
    if acciones:
        ax.axvline(min(acciones), color=NARANJA, lw=1.5)
        ax.annotate(f"Primera acción de control\nregistrada SE{min(acciones):02d}", (min(acciones), s23["casos"].max() * 0.62),
                    xytext=(-8, 0), textcoords="offset points", color=NARANJA, ha="right", bbox=caja)
    ax.scatter(notas_ll, [s23["casos"].max() * 1.03] * len(notas_ll), marker="v", color=GRIS,
               label="Semana con nota del CDC sobre dengue en La Libertad")
    ax.set_xlabel("Semana epidemiológica 2023")
    ax.set_ylabel("Casos por semana")
    ax.set_title("Brote 2023 en Trujillo: curva de casos frente a la respuesta registrada")
    ax.set_ylim(0, s23["casos"].max() * 1.12)
    ax.legend(loc="center right", frameon=False, fontsize=8)
    guardar(fig, "p4_respuesta_2023.png")
    hallazgos["p4_inicio_brote_2023"] = inicio
    hallazgos["p4_semanas_con_accion_control_2023"] = acciones
    hallazgos["p4_rezago_semanas"] = (min(acciones) - inicio) if acciones else None
    hallazgos["p4_casos_acumulados_antes_de_primera_accion"] = int(s23.loc[s23["semana"] < min(acciones), "casos"].sum()) if acciones else None
    hallazgos["p4_semana_pico_2023"] = int(s23.loc[s23["casos"].idxmax(), "semana"])
    hallazgos["p4_notas_cdc_dengue_ll_2023"] = int(s23["notas_dengue_ll"].sum())

    # ---------------- P5: distritos
    dist = (distrito.filter(F.col("ano").isin(2023, 2024)).groupBy("distrito_serie").pivot("ano", [2023, 2024])
            .agg(F.sum("casos")).fillna(0).toPandas())
    dist["total"] = dist["2023"] + dist["2024"]
    dist = dist.sort_values("total")
    fig, ax = plt.subplots(figsize=(9, 3.8))
    yy = np.arange(len(dist))
    ax.barh(yy - 0.2, dist["2023"], height=0.4, color=GRIS, label="2023")
    ax.barh(yy + 0.2, dist["2024"], height=0.4, color=AZUL, label="2024")
    ax.set_yticks(yy, dist["distrito_serie"].str.title())
    ax.set_xlabel("Casos notificados")
    ax.set_title("Casos por distrito de la provincia de Trujillo, 2023 y 2024\n(Alto Trujillo sumado a El Porvenir)")
    ax.legend(frameon=False)
    guardar(fig, "p5_distritos.png")
    dist["pct"] = (dist["total"] / dist["total"].sum() * 100).round(1)
    hallazgos["p5_distritos"] = dist.sort_values("total", ascending=False).to_dict("records")

    # notas del CDC: contexto nacional
    hallazgos["notas_dengue_por_ano_nacional"] = {r["ano"]: r["count"] for r in
                                                 notas.filter("es_dengue").groupBy("ano").count().orderBy("ano").collect()}
    guardar_json("08_eda.json", hallazgos)
    spark.stop()


if __name__ == "__main__":
    main()
