"""
fase0_config.py — Constantes, endpoints, parámetros de servicios.

Parámetros de servicios verificados contra el frontend web del SII (2026-04-17).
NO cambiar eac/eacano sin verificar en DevTools del navegador.
"""

# ─── Endpoints SII ───────────────────────────────────────────────────────────
SII_BASE = "https://www4.sii.cl"
URL_PREDIO_NACIONAL = f"{SII_BASE}/mapasui/services/data/mapasFacadeService/getPredioNacional"
URL_BOOTSTRAP = f"{SII_BASE}/mapasui/internet/"

# ─── Servicios (verificados contra frontend SII 2026-04-17) ──────────────────
# El frontend usa eac=0/eacano=0 para la capa WMS y eac=14/eacano=2022 para AH.
# Usar eac=15/eacano=2025 causa que el SII retorne datos incompletos (sin coords)
# para predios con RAV NO AGRICOLA 2022.
DEFAULT_EAC = 14
DEFAULT_EACANO = 2022
DEFAULT_AH_STYLE = "AH_MUESTRA_EAC_14_2022"

# ─── Infraestructura ─────────────────────────────────────────────────────────
NUM_TUNNELS = 70
VENV_PYTHON = "/root/carto_predios/venv/bin/python3"
BASE_DIR = "/root/carto_predios/sii_vectorizer"

# ─── S3 ──────────────────────────────────────────────────────────────────────
S3_ENDPOINT = "https://nbg1.your-objectstorage.com"
S3_BUCKET = "siipredios"
S3_ACCESS_KEY = "YOUR_ACCESS_KEY"
S3_SECRET_KEY = "YOUR_SECRET_KEY"
S3_OUTPUT_PREFIX = "2025ss_bcn/F0"

# ─── Worker ──────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
SESSION_RENEW_AFTER_NULLS = 20
STALL_THRESHOLD_S = 60  # si un worker no avanza en 60s, rotar IP

# ─── Rotación de IPs ─────────────────────────────────────────────────────────
MULLVAD_RELAYS_PATH = "/tmp/mullvad_relays.json"
MAX_ROTATIONS_PER_WORKER = 3

SPARE_RELAYS = [
    "gb-man", "de-ber", "nl-ams", "fr-par", "se-sto",
    "jp-tyo", "au-syd", "ie-dub", "es-mad", "it-rom",
    "at-vie", "ch-zur", "no-osl", "dk-cph", "fi-hel",
    "pl-war", "ro-buc", "bg-sof", "hr-zag", "cz-prg",
    "hu-bud", "ca-van", "nz-akl", "us-phx", "us-atl",
    "us-slc", "us-den", "us-bos", "br-for", "cl-scl",
]

# ─── Paths ───────────────────────────────────────────────────────────────────
ROLES_SPLIT_DIR = "/root/carto_predios/data/roles_split"
CATASTRO_CSV = "/root/carto_predios/catastro_2025_2.csv"
CATASTRO_S3_KEY = "catastro_historico/output/catastro_2025_2.csv"

# ─── WMS names (para servicios payload) ──────────────────────────────────────
WMS_NAME_OVERRIDES = {
    2303: "SAN_PEDRO_ATACAMA", 3304: "ALTO_DEL_CARMEN",
    5309: "CONCON", 5504: "CALERA", 5606: "LLAILLAY",
    6104: "MOSTAZAL", 6117: "QUINTA_TILCOCO",
    8108: "TREHUACO", 8210: "SAN_PEDRO_DE_LA_PAZ",
    10207: "SAN_JUAN_LA_COSTA", 10410: "CURACO_DE_VELEZ",
    11302: "O'HIGGINS", 12101: "PUERTO_NATALES",
    12103: "TORRES_DEL_PAINE", 13134: "SANTIAGO_OESTE",
    13135: "SANTIAGO_SUR", 14203: "TIL_TIL",
    16162: "PEDRO_AGUIRRE_CERDA", 16303: "SAN_JOSE_MAIPO",
}


def load_wms_names() -> dict[int, str]:
    """Load WMS names from TSV, applying overrides."""
    path = os.path.join(BASE_DIR, "wms_names.txt")
    mapping: dict[int, str] = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("cod"):
                    continue
                parts = line.split("\t")
                if len(parts) >= 2:
                    mapping[int(parts[0])] = parts[1]
    except FileNotFoundError:
        pass
    mapping.update(WMS_NAME_OVERRIDES)
    return mapping


import os  # noqa: E402 (needed by load_wms_names)
