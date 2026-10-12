"""Etapa 1 - Ingesta a la zona cruda (raw).

Copia cada fuente sin modificarla a
    raw/<fuente>/fecha_obtencion=AAAA-MM-DD/<archivo>
y deja un manifiesto con el tamaño y el hash SHA-256 de cada archivo, que es lo
que se cita en la ficha de procedencia (sección 3.1). Si DATALAKE_URI apunta a
HDFS, la copia se hace con `hdfs dfs -put`.
"""
import hashlib
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

from comun import DATALAKE_URI, FUENTES, Cronometro, guardar_json, ruta

# fuente -> (patrón de archivos dentro de datos/, fecha en que se descargó)
FUENTES_RAW = {
    "dengue": ("dengue_vigilancia_2000_2024.csv", "2026-09-16"),
    "clima": ("clima_trujillo_aeropuerto_noaa_1990_2025.csv", "2026-09-16"),
    "rumores_cdc": ("rumores_cdc/*/*.pdf", "2026-09-30"),
}


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


def copiar(origen, destino_uri):
    if destino_uri.startswith("file:"):
        destino = Path(url2pathname(urlparse(destino_uri).path))
        destino.parent.mkdir(parents=True, exist_ok=True)
        if not destino.exists() or destino.stat().st_size != origen.stat().st_size:
            shutil.copy2(origen, destino)
    else:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", destino_uri.rsplit("/", 1)[0]], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", str(origen), destino_uri], check=True)


def main():
    reloj = Cronometro()
    manifiesto = {"datalake": DATALAKE_URI, "archivos": []}
    for fuente, (patron, fecha) in FUENTES_RAW.items():
        archivos = sorted(FUENTES.glob(patron))
        total = 0
        for a in archivos:
            # los PDF conservan la subcarpeta del año: rumores_cdc/2023/x.pdf
            relativo = a.relative_to(FUENTES / patron.split("/")[0]) if "/" in patron else Path(a.name)
            destino = ruta("raw", fuente, f"fecha_obtencion={fecha}", *relativo.parts)
            copiar(a, destino)
            total += a.stat().st_size
            if fuente != "rumores_cdc":
                manifiesto["archivos"].append({"fuente": fuente, "archivo": a.name, "bytes": a.stat().st_size,
                                               "sha256": sha256(a), "destino": destino})
        if fuente == "rumores_cdc":
            # para los 1 103 PDF se guarda un hash por archivo en un CSV aparte
            lineas = ["archivo,bytes,sha256"] + [f"{a.parent.name}/{a.name},{a.stat().st_size},{sha256(a)}" for a in archivos]
            (Path(__file__).resolve().parents[1] / "resultados" / "manifiesto_rumores_sha256.csv").write_text(
                "\n".join(lineas), encoding="utf-8")
            manifiesto["archivos"].append({"fuente": fuente, "archivo": f"{len(archivos)} PDF", "bytes": total,
                                           "sha256": "ver resultados/manifiesto_rumores_sha256.csv",
                                           "destino": ruta("raw", fuente, f"fecha_obtencion={fecha}")})
        print(f"{fuente:12s} {len(archivos):5d} archivo(s)  {total / 2**20:8.1f} MiB")
    manifiesto["segundos"] = reloj()
    guardar_json("01_ingesta_manifiesto.json", manifiesto)


if __name__ == "__main__":
    main()
