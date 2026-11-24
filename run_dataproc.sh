#!/usr/bin/env bash
# Ejecuta el pipeline en Google Cloud con un clúster temporal de Dataproc
# ("Managed Service for Apache Spark"): crear -> etapas 01..10 -> prueba 2 vs 4 workers -> borrar.
#
# El clúster cobra mientras exista, incluso si queda en estado de error. Por eso:
#   - se borra al salir del script, termine bien o mal (trap EXIT);
#   - además se crea con borrado programado: a los 30 min sin trabajos o a las 6 h, lo que pase antes.
#
# Requisitos: Google Cloud CLI (gcloud, bq) con sesión iniciada; un proyecto con las API de
# Dataproc, Compute Engine, Cloud Storage y BigQuery habilitadas; un bucket en us-central1.
#
# Uso:
#   PROYECTO=mi-proyecto BUCKET=mi-bucket ./run_dataproc.sh            # todo
#   PROYECTO=... BUCKET=... ETAPAS="2 3 5" ./run_dataproc.sh           # algunas etapas, sin la prueba 2 vs 4
#
# Variables (con su valor por defecto):
#   REGION=us-central1  IMAGEN=3.0.5-debian13  TIPO_MAQUINA=n2-standard-4
#   N_WORKERS=2  N_WORKERS_ESCALADO=4  BQ_DATASET=oro  FUENTES_DIR=../../datos (para la etapa 1)
set -euo pipefail
cd "$(dirname "$0")"

: "${PROYECTO:?Defina PROYECTO (ID del proyecto de Google Cloud)}"
: "${BUCKET:?Defina BUCKET (nombre del bucket del lago, sin gs://)}"
REGION="${REGION:-us-central1}"
IMAGEN="${IMAGEN:-3.0.5-debian13}"
TIPO_MAQUINA="${TIPO_MAQUINA:-n2-standard-4}"
N_WORKERS="${N_WORKERS:-2}"
N_WORKERS_ESCALADO="${N_WORKERS_ESCALADO:-4}"
BQ_DATASET="${BQ_DATASET:-oro}"
ETAPAS="${ETAPAS:-2 3 4 5 6 7 8 9 10}"
export CORRIDA_ID="${CORRIDA_ID:-$(date '+%Y%m%dT%H%M%S')}"
CLUSTER="dengue-$(echo "$CORRIDA_ID" | tr '[:upper:]' '[:lower:]')"
LAGO="gs://$BUCKET/datalake"
CODIGO="gs://$BUCKET/codigo/$CORRIDA_ID"
LOGS="resultados/nube/$CORRIDA_ID"
mkdir -p "$LOGS"
echo "=== corrida $CORRIDA_ID · clúster $CLUSTER · $REGION · imagen $IMAGEN"

borrar_cluster() {
  echo "=== borrando el clúster $CLUSTER"
  gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" --project="$PROYECTO" --quiet || true
}

# 1. Etapa 1 en esta máquina: las fuentes están aquí y se suben a bronce con gcloud storage
DATALAKE_URI="$LAGO" python src/01_ingesta.py 2>&1 | tee "$LOGS/01_ingesta.log"

# 2. Código del pipeline al bucket (los módulos van en un .zip para --py-files)
( cd src && python -c "import zipfile,glob; z=zipfile.ZipFile('../src.zip','w'); [z.write(f) for f in glob.glob('*.py')]" )
gcloud storage cp src.zip src/*.py "$CODIGO/" --project="$PROYECTO"

# 3. Dataset de BigQuery para oro (si no existe)
bq --project_id="$PROYECTO" --location="$REGION" mk --dataset --force "$PROYECTO:$BQ_DATASET" >/dev/null 2>&1 || true

# 4. Clúster temporal
trap borrar_cluster EXIT
PROPIEDADES="spark-env:DATALAKE_URI=$LAGO,spark-env:EN_CLUSTER=1,spark-env:CORRIDA_ID=$CORRIDA_ID"
PROPIEDADES+=",spark-env:RESULTADOS_DIR=/tmp/resultados_dengue,spark-env:BQ_PROYECTO=$PROYECTO"
PROPIEDADES+=",spark-env:BQ_DATASET=$BQ_DATASET,spark-env:BQ_BUCKET_TEMPORAL=$BUCKET"
PROPIEDADES+=",spark-env:SPARK_SHUFFLE_PARTITIONS=32,dataproc:pip.packages=pypdf==7.1.1"
gcloud dataproc clusters create "$CLUSTER" --project="$PROYECTO" --region="$REGION" \
  --image-version="$IMAGEN" --optional-components=DELTA --enable-component-gateway \
  --master-machine-type="$TIPO_MAQUINA" --worker-machine-type="$TIPO_MAQUINA" --num-workers="$N_WORKERS" \
  --delete-max-idle=30m --delete-max-age=6h --properties="$PROPIEDADES"

enviar() {   # enviar <script> [argumentos...]
  local script="$1"; shift
  echo "=== $(date '+%F %T') $script $*"
  local inicio; inicio=$(date +%s)
  gcloud dataproc jobs submit pyspark "$CODIGO/$script" --project="$PROYECTO" --region="$REGION" \
    --cluster="$CLUSTER" --py-files="$CODIGO/src.zip" -- "$@" 2>&1 | tee "$LOGS/${script%.py}.log"
  echo "=== $script terminado en $(( $(date +%s) - inicio )) s"
}

declare -A SCRIPTS=(
  [2]="02_limpieza_dengue.py" [3]="03_limpieza_clima.py" [4]="04_extraccion_rumores.py"
  [5]="05_integracion.py" [6]="06_optimizacion.py" [8]="08_eda.py" [9]="09_modelo_base.py"
  [10]="10_publicar_oro.py"
)
for n in $ETAPAS; do
  if [ "$n" = 7 ]; then
    enviar 07_escalabilidad.py --workers "$N_WORKERS" 1 10 50 100
  else
    enviar "${SCRIPTS[$n]}"
  fi
done

# 5. Escalado horizontal: la misma prueba con más workers (sección 3.7)
if [[ " $ETAPAS " == *" 7 "* ]] && [ "$N_WORKERS_ESCALADO" != "$N_WORKERS" ]; then
  gcloud dataproc clusters update "$CLUSTER" --project="$PROYECTO" --region="$REGION" --num-workers="$N_WORKERS_ESCALADO"
  enviar 07_escalabilidad.py --workers "$N_WORKERS_ESCALADO" 1 10 50 100
fi

# 6. Reportes de la corrida (JSON y figuras) del lago al repositorio
gcloud storage cp -r "$LAGO/resultados/$CORRIDA_ID/*" "$LOGS/" --project="$PROYECTO" || true
echo "=== corrida $CORRIDA_ID terminada; reportes en $LOGS"
# al salir, trap borra el clúster
