"""Unified comuna catalog — single source of truth."""
import json
from pathlib import Path

_DATA_DIR = Path(__file__).parent.parent / "data"
_comunas: list[dict] = []
_by_id: dict[str, dict] = {}


def _load():
    global _comunas, _by_id
    if _comunas:
        return
    with open(_DATA_DIR / "comunas.json", "r", encoding="utf-8") as f:
        _comunas = json.load(f)
    _by_id = {c["id"]: c for c in _comunas}


def get_comunas() -> list[dict]:
    _load()
    return _comunas


def find_comuna(comuna_id: str) -> dict | None:
    _load()
    return _by_id.get(comuna_id)
