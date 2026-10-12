"""Etapa 9 - Modelo base de alerta temprana (sección 4.2), con Spark MLlib.

Pregunta: en la semana t, ¿la provincia de Trujillo superará el umbral de brote
(17 casos/semana) en alguna de las 4 semanas siguientes (t+1 ... t+4)?

Variables (todas conocidas en la semana t):
  casos de t, t-1, t-2, t-3 (en log(1+x)) y su variación; temperatura, humedad y precipitación
  de las últimas 4 semanas y de 5 a 8 semanas antes; seno y coseno de la semana del año.
Partición temporal (sin mezclar el futuro en el entrenamiento):
  entrenamiento 2000-2014 (brotes 2000, 2001, 2004-2005, 2007, 2010)
  prueba        2015-2024 (brotes 2015-2017, 2023-2024 y cinco años sin brote)
Comparación: regla actual "alertar cuando la semana t ya supera el umbral" (persistencia).
Métricas: AUC-PR (adecuada al desbalance), AUC-ROC, precisión, sensibilidad, F1 y semanas de
anticipación de la primera alerta al inicio de cada brote del periodo de prueba.
"""
import math

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.sql import Window, functions as F

from comun import UMBRAL_BROTE, guardar_json, ruta, sesion

HORIZONTE = 4
FIN_ENTRENAMIENTO = 2014


def preparar(serie):
    w = Window.orderBy("ano", "semana")
    df = serie.withColumn("log_casos", F.log1p("casos"))
    for k in range(1, 4):
        df = df.withColumn(f"log_casos_l{k}", F.lag("log_casos", k).over(w))
    df = df.withColumn("variacion_1s", F.col("log_casos") - F.col("log_casos_l1"))
    for var in ["temp_media_c", "humedad_rel_pct", "precipitacion_mm"]:
        df = (df.withColumn(f"{var}_4s", F.avg(var).over(w.rowsBetween(-3, 0)))
                .withColumn(f"{var}_5a8s", F.avg(var).over(w.rowsBetween(-7, -4))))
    df = (df.withColumn("sem_sin", F.sin(2 * math.pi * F.col("semana") / 52.18))
            .withColumn("sem_cos", F.cos(2 * math.pi * F.col("semana") / 52.18)))
    futuro = F.max("casos").over(w.rowsBetween(1, HORIZONTE))
    df = (df.withColumn("max_prox_4s", futuro)
            .withColumn("n_futuro", F.count("casos").over(w.rowsBetween(1, HORIZONTE)))
            .withColumn("label", (F.col("max_prox_4s") > UMBRAL_BROTE).cast("double"))
            .filter(F.col("n_futuro") == HORIZONTE)          # las últimas 4 semanas no tienen etiqueta
            .filter(F.col("log_casos_l3").isNotNull()))
    return df


VARIABLES = (["log_casos", "log_casos_l1", "log_casos_l2", "log_casos_l3", "variacion_1s", "sem_sin", "sem_cos"]
             + [f"{v}_{s}" for v in ["temp_media_c", "humedad_rel_pct", "precipitacion_mm"] for s in ["4s", "5a8s"]])
CLIMA = [v for v in VARIABLES if v.startswith(("temp", "humedad", "precip"))]


def metricas(pred, col_pred="prediction", col_score="probability"):
    filas = pred.select("label", col_pred).groupBy("label", col_pred).count().collect()
    m = {(r["label"], r[col_pred]): r["count"] for r in filas}
    vp, fp, fn, vn = m.get((1.0, 1.0), 0), m.get((0.0, 1.0), 0), m.get((1.0, 0.0), 0), m.get((0.0, 0.0), 0)
    prec = vp / (vp + fp) if vp + fp else 0.0
    sens = vp / (vp + fn) if vp + fn else 0.0
    res = {"VP": vp, "FP": fp, "FN": fn, "VN": vn, "precision": round(prec, 3), "sensibilidad": round(sens, 3),
           "f1": round(2 * prec * sens / (prec + sens), 3) if prec + sens else 0.0,
           "exactitud": round((vp + vn) / (vp + fp + fn + vn), 3)}
    for nombre in ["areaUnderPR", "areaUnderROC"]:
        ev = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol=col_score, metricName=nombre)
        res[nombre] = round(ev.evaluate(pred), 3)
    return res


def anticipacion(pred, col_pred):
    """Para cada inicio de brote del periodo de prueba (primera semana sobre el umbral después de
    al menos 8 semanas por debajo), semanas entre la primera alerta y ese inicio."""
    p = pred.orderBy("ano", "semana").select("ano", "semana", "casos", col_pred).toPandas()
    p["sobre"] = p["casos"] > UMBRAL_BROTE
    salida = []
    for i in range(8, len(p)):
        if p.loc[i, "sobre"] and not p.loc[i - 8:i - 1, "sobre"].any():
            previas = p.loc[max(0, i - 12):i]
            alertas = previas[previas[col_pred] == 1.0]
            primera = alertas.index.min() if len(alertas) else None
            salida.append({"inicio_brote": f"SE{int(p.loc[i, 'semana']):02d}-{int(p.loc[i, 'ano'])}",
                           "primera_alerta": (f"SE{int(p.loc[primera, 'semana']):02d}-{int(p.loc[primera, 'ano'])}"
                                              if primera is not None else "sin alerta previa"),
                           "semanas_de_anticipacion": int(i - primera) if primera is not None else 0})
    return salida


def main():
    spark = sesion("09_modelo_base")
    serie = spark.read.parquet(ruta("curated", "serie_semanal_trujillo"))
    df = preparar(serie).cache()
    train = df.filter(F.col("ano") <= FIN_ENTRENAMIENTO)
    test = df.filter(F.col("ano") > FIN_ENTRENAMIENTO)

    pipeline = Pipeline(stages=[
        Imputer(inputCols=CLIMA, outputCols=CLIMA, strategy="median"),   # semanas sin dato de clima
        VectorAssembler(inputCols=VARIABLES, outputCol="x"),
        StandardScaler(inputCol="x", outputCol="features", withMean=True, withStd=True),
        # pesos por clase: las semanas con brote próximo son minoría
        LogisticRegression(featuresCol="features", labelCol="label", weightCol="peso",
                           regParam=0.01, elasticNetParam=0.0, maxIter=100),
    ])
    pos = train.filter("label = 1").count() / train.count()
    pesar = lambda d: d.withColumn("peso", F.when(F.col("label") == 1, 0.5 / pos).otherwise(0.5 / (1 - pos)))
    modelo = pipeline.fit(pesar(train))
    pred = modelo.transform(pesar(test)).cache()

    # regla actual (persistencia): alerta si la semana t ya supera el umbral
    base = (test.withColumn("prediction_regla", (F.col("casos") > UMBRAL_BROTE).cast("double"))
            .withColumn("score_regla", F.col("log_casos")))
    ev_regla = metricas(base, "prediction_regla", "score_regla")

    lr = modelo.stages[-1]
    coef = sorted(zip(VARIABLES, [round(c, 3) for c in lr.coefficients.toArray()]), key=lambda x: -abs(x[1]))
    informe = {
        "filas_entrenamiento": train.count(), "positivos_entrenamiento": train.filter("label = 1").count(),
        "filas_prueba": test.count(), "positivos_prueba": test.filter("label = 1").count(),
        "modelo_regresion_logistica": metricas(pred),
        "regla_actual_persistencia": ev_regla,
        "anticipacion_modelo": anticipacion(pred, "prediction"),
        "anticipacion_regla": anticipacion(base, "prediction_regla"),
        "coeficientes_estandarizados": coef,
        "metricas_entrenamiento": metricas(modelo.transform(pesar(train))),
    }
    # periodo 2023: probabilidad semanal de alerta (para el informe)
    informe["probabilidad_2023_se1_se12"] = [
        {"semana": r["semana"], "casos": r["casos"], "prob": round(float(r["probability"][1]), 3)}
        for r in pred.filter("ano = 2023 AND semana <= 12").orderBy("semana").collect()]
    guardar_json("09_modelo_base.json", informe)
    print({k: v for k, v in informe.items() if k != "probabilidad_2023_se1_se12"})
    spark.stop()


if __name__ == "__main__":
    main()
