from fastapi import FastAPI
from logging_config import setup_logging

setup_logging()
from fastapi.middleware.cors import CORSMiddleware
from config import FRONTEND_URL
from routers import health, descargas, geocode
from routers import auth, marketplace, admin, sharing

app = FastAPI(
    title="Catastral.cl — Datos Catastrales de Chile",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:5173"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type", "Accept"],
    allow_credentials=True,
)

app.include_router(health.router, prefix="/api")
app.include_router(descargas.router, prefix="/api")
app.include_router(geocode.router, prefix="/api")
app.include_router(auth.router, prefix="/api")
app.include_router(marketplace.router, prefix="/api")
app.include_router(admin.router, prefix="/api")
app.include_router(sharing.router, prefix="/api")
