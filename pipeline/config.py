import os

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_REGION = os.environ.get("S3_REGION", "eu-central-1")
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_BUCKET = os.environ.get("S3_BUCKET", "siipredios")
S3_PREFIX = os.environ.get("S3_PREFIX", "catastro_historico/output")

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "5435"))
DB_NAME = os.environ.get("DB_NAME", "roles")
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

STAGING_DIR = "/tmp/roles_pipeline"

PERIODS = [
    "2018_1", "2018_2", "2019_1", "2019_2",
    "2020_1", "2020_2", "2021_1", "2021_2",
    "2022_1", "2022_2", "2023_1", "2023_2",
    "2024_1", "2024_2", "2025_1", "2025_2",
]

LATEST_PERIOD = "2025_2"

# Columns for historical slim table
HIST_COLUMNS = [
    "comuna", "manzana", "predio", "anio", "semestre",
    "rc_avaluo_total", "rc_avaluo_exento", "rc_cuota_trimestral",
    "dc_avaluo_fiscal", "dc_contribucion_semestral",
    "dc_sup_terreno", "sup_construida_total", "dc_cod_destino",
]

# All CSV columns in order
CSV_COLUMNS = [
    "periodo", "anio", "semestre", "comuna", "manzana", "predio",
    "rc_direccion", "rc_serie", "rc_ind_aseo", "rc_cuota_trimestral",
    "rc_avaluo_total", "rc_avaluo_exento", "rc_anio_term_exencion",
    "rc_cod_ubicacion", "rc_cod_destino", "dc_direccion",
    "dc_avaluo_fiscal", "dc_contribucion_semestral", "dc_cod_destino",
    "dc_avaluo_exento", "dc_sup_terreno", "dc_cod_ubicacion",
    "dc_bc1_comuna", "dc_bc1_manzana", "dc_bc1_predio",
    "dc_bc2_comuna", "dc_bc2_manzana", "dc_bc2_predio",
    "dc_padre_comuna", "dc_padre_manzana", "dc_padre_predio",
    "n_lineas_construccion", "sup_construida_total",
    "anio_construccion_min", "anio_construccion_max",
    "materiales", "calidades", "pisos_max", "serie",
]
