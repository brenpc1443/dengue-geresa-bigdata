#!/usr/bin/env bash
# Ejecuta el pipeline completo en orden. Cada etapa deja su registro en resultados/logs/.
#   ./run_pipeline.sh                 -> etapas 1 a 9
#   ./run_pipeline.sh 2 5             -> solo las etapas indicadas
# Variables opcionales: DATALAKE_URI (por defecto ../datalake), FUENTES_DIR (por defecto ../../datos),
#                       SPARK_MASTER (por defecto local[*]), SPARK_DRIVER_MEMORY (por defecto 3g)
set -euo pipefail
cd "$(dirname "$0")/src"
PY="${PYTHON:-python}"
mkdir -p ../resultados/logs

declare -A ETAPAS=(
  [1]="01_ingesta.py" [2]="02_limpieza_dengue.py" [3]="03_limpieza_clima.py"
  [4]="04_extraccion_rumores.py" [5]="05_integracion.py" [6]="06_optimizacion.py"
  [7]="07_escalabilidad.py 1 10 50 100" [8]="08_eda.py" [9]="09_modelo_base.py"
)
SELECCION=("$@")
[ ${#SELECCION[@]} -eq 0 ] && SELECCION=(1 2 3 4 5 6 7 8 9)

for n in "${SELECCION[@]}"; do
  read -r script args <<< "${ETAPAS[$n]}"
  echo "=== $(date '+%F %T') etapa $n: $script $args"
  inicio=$(date +%s)
  "$PY" "$script" ${args:-} 2>&1 | tee "../resultados/logs/${script%.py}.log"
  echo "=== etapa $n terminada en $(( $(date +%s) - inicio )) s"
done
