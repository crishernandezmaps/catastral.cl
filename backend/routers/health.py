from fastapi import APIRouter
from db import pool

router = APIRouter()

@router.get("/health")
def health():
    with pool.connection() as conn:
        row = conn.execute("SELECT 1")
        row.fetchone()
    return {"status": "ok"}
