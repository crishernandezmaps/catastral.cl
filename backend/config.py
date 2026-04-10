import os
from dotenv import load_dotenv

load_dotenv()

# Database
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# API
API_PORT = int(os.getenv("API_PORT", "8000"))
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://catastral.cl")
BACKEND_URL = os.getenv("BACKEND_URL", "https://catastral.cl")

# Auth
JWT_SECRET = os.environ.get("JWT_SECRET", "")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "cris@tremen.tech")

# Flow.cl
FLOW_API_KEY = os.getenv("FLOW_API_KEY", "")
FLOW_SECRET_KEY = os.getenv("FLOW_SECRET_KEY", "")
FLOW_BASE_URL = os.getenv("FLOW_BASE_URL", "https://www.flow.cl/api")

# S3 (Hetzner Object Storage)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "https://nbg1.your-objectstorage.com")
S3_REGION = os.getenv("S3_REGION", "eu-central-1")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "siipredios")
S3_BASE_PATH = os.getenv("S3_BASE_PATH", "2025ss_bcn")

# HERE Geocoding
HERE_API_KEY = os.environ.get("HERE_API_KEY", "")
STATS_TOKEN = os.environ.get("STATS_TOKEN", "tr3m3n_stats_2026")

# Resend Email
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
