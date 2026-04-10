"""
Lookup de comunas: codigo ↔ nombre ↔ nombre WMS ↔ bbox.

Construye la tabla desde los CSV de datos prediales del SII
(carpeta data/out_sii_2025_2_f/). Cada CSV tiene lat/lon de todos
los predios, lo que permite calcular el bounding box exacto.

Uso:
    from comunas import buscar_comuna

    # Por codigo
    info = buscar_comuna("13101")

    # Por nombre (parcial, sin tildes)
    info = buscar_comuna("nunoa")
    info = buscar_comuna("vina")
    info = buscar_comuna("santiago centro")

    # Retorna dict con: cod, nombre, nombre_wms, bbox
"""

import os
import unicodedata
import pandas as pd
from functools import lru_cache

# Ruta a los CSV prediales: busca en varias ubicaciones posibles
_HERE = os.path.dirname(os.path.abspath(__file__))
_CANDIDATES = [
    os.path.join(_HERE, "data", "out_sii_2025_2_f"),          # ./data/
    os.path.join(_HERE, "..", "..", "data", "out_sii_2025_2_f"),  # ../../data/
    os.path.join(_HERE, "..", "data", "out_sii_2025_2_f"),       # ../data/
]
DATA_DIR = next((d for d in _CANDIDATES if os.path.isdir(d)),
                _CANDIDATES[0])

# Excepciones conocidas: cod → nombre WMS correcto
# (cuando el nombre CSV no coincide con el nombre de la capa WMS)
EXCEPCIONES_WMS = {
    "13101": "SANTIAGO_CENTRO",
}


def _normalizar(texto):
    """Quita tildes y convierte a mayusculas. Ñ → N."""
    texto = texto.replace('Ñ', 'N').replace('ñ', 'n')
    nfkd = unicodedata.normalize('NFKD', texto)
    sin_tildes = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return sin_tildes.upper()


def _nombre_a_wms(nombre_csv):
    """Convierte nombre CSV (con tildes) a nombre WMS del SII."""
    return _normalizar(nombre_csv)


@lru_cache(maxsize=1)
def _cargar_catalogo():
    """Carga catalogo de comunas desde los archivos CSV."""
    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(
            f"No se encuentra directorio de datos: {DATA_DIR}\n"
            f"Se espera la carpeta data/out_sii_2025_2_f/ con CSVs prediales."
        )

    catalogo = {}
    for fname in sorted(os.listdir(DATA_DIR)):
        if not fname.endswith('.csv'):
            continue
        base = fname.replace('.csv', '')
        parts = base.split('_', 1)
        if len(parts) != 2:
            continue

        cod = parts[0]
        nombre_csv = parts[1]

        # Nombre WMS: excepcion o normalizado
        if cod in EXCEPCIONES_WMS:
            nombre_wms = EXCEPCIONES_WMS[cod]
        else:
            nombre_wms = _nombre_a_wms(nombre_csv)

        catalogo[cod] = {
            "cod": cod,
            "nombre": nombre_csv,
            "nombre_wms": nombre_wms,
            "nombre_normalizado": _normalizar(nombre_csv),
            "csv_path": os.path.join(DATA_DIR, fname),
        }

    return catalogo


def _calcular_bbox(csv_path):
    """Calcula bbox desde lat/lon del CSV predial."""
    df = pd.read_csv(csv_path, usecols=['lat', 'lon'])
    df = df.dropna(subset=['lat', 'lon'])
    df = df[(df.lat != 0) & (df.lon != 0)]

    if len(df) == 0:
        return None

    pad = 0.002  # ~200m de padding
    return (
        float(df.lon.min() - pad),
        float(df.lat.min() - pad),
        float(df.lon.max() + pad),
        float(df.lat.max() + pad),
    )


def buscar_comuna(query):
    """
    Busca una comuna por codigo o nombre (parcial, sin tildes).

    Args:
        query: Codigo (ej "13101") o nombre (ej "nunoa", "vina del mar",
               "santiago centro"). Case-insensitive, ignora tildes.

    Returns:
        dict con: cod, nombre, nombre_wms, bbox
        O None si no se encuentra.

    Raises:
        ValueError si la busqueda es ambigua (multiples matches).
    """
    catalogo = _cargar_catalogo()
    query = query.strip()

    # 1. Busqueda exacta por codigo
    if query in catalogo:
        entry = catalogo[query]
        bbox = _calcular_bbox(entry["csv_path"])
        return {
            "cod": entry["cod"],
            "nombre": entry["nombre"],
            "nombre_wms": entry["nombre_wms"],
            "bbox": bbox,
            "csv_path": entry["csv_path"],
        }

    # 2. Busqueda por nombre (normalizado, parcial)
    q_norm = _normalizar(query).replace(' ', '_')
    matches = []

    for cod, entry in catalogo.items():
        nombre_norm = entry["nombre_normalizado"]
        # Match exacto del nombre normalizado
        if nombre_norm == q_norm:
            matches = [(cod, entry)]
            break
        # Match parcial (contiene)
        if q_norm in nombre_norm or nombre_norm in q_norm:
            matches.append((cod, entry))

    if len(matches) == 0:
        # Busqueda mas flexible: cada palabra del query debe estar en el nombre
        words = q_norm.replace('_', ' ').split()
        for cod, entry in catalogo.items():
            nombre_norm = entry["nombre_normalizado"]
            if all(w in nombre_norm for w in words):
                matches.append((cod, entry))

    if len(matches) == 0:
        return None

    if len(matches) == 1:
        cod, entry = matches[0]
        bbox = _calcular_bbox(entry["csv_path"])
        return {
            "cod": entry["cod"],
            "nombre": entry["nombre"],
            "nombre_wms": entry["nombre_wms"],
            "bbox": bbox,
            "csv_path": entry["csv_path"],
        }

    # Multiples matches → mostrar opciones
    opciones = [f"  {cod}: {e['nombre']}" for cod, e in matches[:10]]
    raise ValueError(
        f"Busqueda ambigua '{query}'. Opciones:\n" + "\n".join(opciones)
    )


def listar_comunas(filtro=None):
    """Lista comunas. Opcionalmente filtra por nombre."""
    catalogo = _cargar_catalogo()
    resultado = []

    for cod, entry in sorted(catalogo.items()):
        if filtro:
            f_norm = _normalizar(filtro)
            if f_norm not in entry["nombre_normalizado"]:
                continue
        resultado.append({
            "cod": cod,
            "nombre": entry["nombre"],
            "nombre_wms": entry["nombre_wms"],
        })

    return resultado


# --- CLI rapido ---
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python comunas.py <codigo_o_nombre>")
        print("Ejemplos:")
        print("  python comunas.py 13101")
        print("  python comunas.py nunoa")
        print("  python comunas.py 'vina del mar'")
        print("  python comunas.py --listar")
        print("  python comunas.py --listar santiago")
        exit(0)

    if sys.argv[1] == "--listar":
        filtro = sys.argv[2] if len(sys.argv) > 2 else None
        for c in listar_comunas(filtro):
            print(f"  {c['cod']}: {c['nombre']} (WMS: {c['nombre_wms']})")
        exit(0)

    query = " ".join(sys.argv[1:])
    try:
        info = buscar_comuna(query)
        if info:
            print(f"Codigo:     {info['cod']}")
            print(f"Nombre:     {info['nombre']}")
            print(f"Nombre WMS: {info['nombre_wms']}")
            print(f"BBOX:       {info['bbox']}")
        else:
            print(f"No se encontro comuna: '{query}'")
    except ValueError as e:
        print(e)
