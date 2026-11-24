#!/usr/bin/env bash
# Ejecuta el pipeline en una sola máquina (pruebas y desarrollo). En Google Cloud: run_dataproc.sh.
# Cada etapa deja su registro en resultados/logs/.
#   ./run_pipeline.sh                 -> etapas 1 a 11
#   ./run_pipeline.sh 2 5             -> solo las etapas indicadas
# Variables opcionales: DATALAKE_URI (por defecto ../datalake), FUENTES_DIR (por defecto ../../datos),
#                       SPARK_MASTER (por defecto local[*]), SPARK_DRIVER_MEMORY (por defecto 3g),
#                       FORMATO_TABLAS (delta | parquet; por defecto delta), CORRIDA_ID,
#                       RESULTADOS_DIR (por defecto ../resultados; ahí van también los logs)
set -euo pipefail
cd "$(dirname "$0")/src"
PY="${PYTHON:-python}"
# los logs van junto a los reportes (RESULTADOS_DIR), para que una prueba no pise la corrida oficial
LOGS="${RESULTADOS_DIR:-../resultados}/logs"
mkdir -p "$LOGS"
# un mismo identificador de corrida para todas las etapas (linaje: columna _corrida_id)
export CORRIDA_ID="${CORRIDA_ID:-$(date '+%Y%m%dT%H%M%S')}"
echo "=== corrida $CORRIDA_ID"

declare -A ETAPAS=(
  [1]="01_ingesta.py" [2]="02_limpieza_dengue.py" [3]="03_limpieza_clima.py"
  [4]="04_extraccion_rumores.py" [5]="05_integracion.py" [6]="06_optimizacion.py"
  [7]="07_escalabilidad.py 1 10 50 100" [8]="08_eda.py" [9]="09_modelo_base.py"
  [10]="10_publicar_oro.py" [11]="11_privacidad.py"
)
SELECCION=("$@")
[ ${#SELECCION[@]} -eq 0 ] && SELECCION=(1 2 3 4 5 6 7 8 9 10 11)

for n in "${SELECCION[@]}"; do
  read -r script args <<< "${ETAPAS[$n]}"
  echo "=== $(date '+%F %T') etapa $n: $script $args"
  inicio=$(date +%s)
  "$PY" "$script" ${args:-} 2>&1 | tee "$LOGS/${script%.py}.log"
  echo "=== etapa $n terminada en $(( $(date +%s) - inicio )) s"
done
