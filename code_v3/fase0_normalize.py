"""
fase0_normalize.py — Normalización del response de getPredioNacional.

Extrae ~96 campos del JSON del SII incluyendo predioPublicado completo
(Fix 2: rol_base 9xxx + coordenadas UTM del edificio).
"""

import re
import time
import random
import requests

from fase0_config import (
    SII_BASE, URL_PREDIO_NACIONAL, URL_BOOTSTRAP,
    REQUEST_TIMEOUT, MAX_RETRIES,
    DEFAULT_EAC, DEFAULT_EACANO, DEFAULT_AH_STYLE,
)


# ─── Session ─────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    """Crea sesión con cookies bootstrap del SII."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Origin": SII_BASE,
        "Referer": URL_BOOTSTRAP,
    })
    try:
        session.get(URL_BOOTSTRAP, timeout=10)
    except Exception:
        pass
    return session


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe(d, path, default=None):
    cur = d
    for key in path:
        try:
            cur = cur[key]
        except Exception:
            return default
    return cur


def _slug(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"[^\w\s-]+", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s\-]+", "_", s, flags=re.UNICODE)
    return s.strip("_")[:120]


# ─── Payload ─────────────────────────────────────────────────────────────────

def build_payload(comuna: int, manzana: str, predio: str,
                  wms_name: str = "") -> dict:
    """Construye payload getPredioNacional con servicios correctos."""
    c = int(comuna)
    servicios = [
        {"comuna": c, "layer": "sii:BR_CART_AH_MUESTRAS",
         "style": DEFAULT_AH_STYLE, "eac": DEFAULT_EAC, "eacano": DEFAULT_EACANO},
    ]
    if wms_name:
        servicios.insert(0, {
            "comuna": c, "layer": f"sii:BR_CART_{wms_name}_WMS",
            "style": "PREDIOS_WMS_V0", "eac": 0, "eacano": 0,
        })

    return {
        "metaData": {
            "namespace": "cl.sii.sdi.lob.bbrr.mapas.data.api.interfaces."
                         "MapasFacadeService/getPredioNacional",
            "conversationId": "UNAUTHENTICATED-CALL",
            "transactionId": f"tx-{int(time.time()*1000)}-{random.randint(1000,9999)}",
        },
        "data": {
            "predio": {
                "comuna": str(c),
                "manzana": str(int(manzana)),
                "predio": str(int(predio)),
            },
            "servicios": servicios,
        },
    }


# ─── Extracción de capas ─────────────────────────────────────────────────────

def _extract_obs(raw: dict) -> dict:
    """Extrae datos del Observatorio de Mercado de Suelo 2025."""
    capas = _safe(raw, ["data", "datosCapas"], default=[])
    if not isinstance(capas, list):
        return {}
    target = None
    for capa in capas:
        titulo = _safe(capa, ["titulo"], default="")
        if isinstance(titulo, str) and "observatorio" in titulo.lower():
            target = capa
            break
    if not target:
        return {}
    out = {}
    datos = _safe(target, ["datos"], default=[])
    if not isinstance(datos, list):
        return {}
    rename = {
        "region": "obs_region", "región": "obs_region",
        "comuna": "obs_comuna",
        "codigo_area_homogenea": "obs_codigo_area_homogenea",
        "código_área_homogénea": "obs_codigo_area_homogenea",
        "valor_comercial_m2_de_suelo": "obs_valor_comercial_m2_suelo",
        "valor_comercial_m²_de_suelo": "obs_valor_comercial_m2_suelo",
    }
    for item in datos:
        et = _safe(item, ["etiqueta"], default=None)
        val = _safe(item, ["valor"], default=None)
        if et is None:
            continue
        key = _slug(et)
        if key.startswith("transacciones_"):
            out["obs_tx_" + key.replace("transacciones_", "")] = val
        elif key in rename:
            out[rename[key]] = val
        else:
            out["obs_" + key] = val
    return out


def _flatten_capas(raw: dict) -> dict:
    """Aplana todas las datosCapas a columnas prefijadas."""
    capas = _safe(raw, ["data", "datosCapas"], default=[])
    if not isinstance(capas, list):
        return {}
    out = {}
    for capa in capas:
        tslug = _slug(_safe(capa, ["titulo"], default="") or "sin_titulo")
        datos = _safe(capa, ["datos"], default=[])
        if not isinstance(datos, list):
            continue
        for item in datos:
            et = _safe(item, ["etiqueta"], default=None)
            val = _safe(item, ["valor"], default=None)
            if et is None:
                continue
            k = f"cap__{tslug}__{_slug(et)}"
            if k in out and out[k] != val:
                i = 2
                while f"{k}__{i}" in out:
                    i += 1
                out[f"{k}__{i}"] = val
            else:
                out[k] = val
    return out


def _extract_valor_m2(raw: dict) -> str | None:
    capas = _safe(raw, ["data", "datosCapas"], default=[])
    if not isinstance(capas, list):
        return None
    last = None
    for capa in capas:
        datos = _safe(capa, ["datos"], default=[])
        if not isinstance(datos, list):
            continue
        for item in datos:
            et = str(_safe(item, ["etiqueta"], default="")).strip().lower()
            if et in ["valor m² de terreno", "valor m2 de terreno"]:
                val = _safe(item, ["valor"], default=None)
                if isinstance(val, str):
                    last = val.replace("$", "").replace(".", "").replace(",", "").strip()
                elif val is not None:
                    last = str(val)
    return last


# ─── Normalización principal ─────────────────────────────────────────────────

def normalize(comuna: int, manzana: str, predio: str, raw: dict) -> dict:
    """Normaliza el response crudo de getPredioNacional a dict plano."""
    data = _safe(raw, ["data"], default={})
    if not isinstance(data, dict):
        data = {}

    base = {
        "comuna": comuna,
        "manzana": manzana,
        "predio": predio,
        "rol": f"{manzana}-{predio}",
        "existePredio": _safe(data, ["existePredio"]),
        "eacs": _safe(data, ["eacs"]),
        "eacano": _safe(data, ["eacano"]),
        "eacsDescripcion": _safe(data, ["eacsDescripcion"]),
        "direccion_sii": _safe(data, ["direccion"]),
        "nombreComuna": _safe(data, ["nombreComuna"]),
        "destinoDescripcion": _safe(data, ["destinoDescripcion"]),
        "ubicacion": _safe(data, ["ubicacion"]),
        "valorTotal": _safe(data, ["valorTotal"]),
        "valorAfecto": _safe(data, ["valorAfecto"]),
        "valorExento": _safe(data, ["valorExento"]),
        "supTerreno": _safe(data, ["supTerreno"]),
        "supConsMt2": _safe(data, ["supConsMt2"]),
        "supConsMt3": _safe(data, ["supConsMt3"]),
        "medidaSup": _safe(data, ["medidaSup"]),
        "medidaSupConst": _safe(data, ["medidaSupConst"]),
        "ah": _safe(data, ["ah"], default=_safe(data, ["datosAh", "codigoAh"])),
        "sector": _safe(data, ["sector"]),
        "tablaOrigen": _safe(data, ["tablaOrigen"]),
        "periodo": _safe(data, ["periodo"]),
        # Fix: SII invierte convenciones — ubicacionX=lat, ubicacionY=lon
        "lat": _safe(data, ["ubicacionX"]),
        "lon": _safe(data, ["ubicacionY"]),
    }

    # datosAh
    ah = _safe(data, ["datosAh"], default={})
    if isinstance(ah, dict):
        base.update({
            "ah_rangoSuperficie": _safe(ah, ["rangoSuperficie"]),
            "ah_valorUnitario": _safe(ah, ["valorUnitario"]),
            "ah_numeroMuestras": _safe(ah, ["numeroMuestras"]),
            "ah_coefVariacion": _safe(ah, ["coefVariacion"]),
            "ah_mediana": _safe(ah, ["mediana"]),
            "ah_eac": _safe(ah, ["eac"]),
            "ah_eacano": _safe(ah, ["eacano"]),
            "ah_utm_x": _safe(ah, ["ubicacionX"]),
            "ah_utm_y": _safe(ah, ["ubicacionY"]),
        })

    # Fix 2: predioPublicado COMPLETO (rol_base 9xxx + UTM del edificio)
    pp = _safe(data, ["predioPublicado"], default={})
    if isinstance(pp, dict):
        base.update({
            "predioPublicado_id": _safe(pp, ["id"]),
            "predioPublicado_comuna": _safe(pp, ["comuna"]),
            "predioPublicado_manzana": _safe(pp, ["manzana"]),
            "predioPublicado_predio": _safe(pp, ["predio"]),
            "predioPublicado_utm_x": _safe(pp, ["ubicacionX"]),
            "predioPublicado_utm_y": _safe(pp, ["ubicacionY"]),
        })

    # Fix 3: datosCsa — coords UTM de predios agrícolas (CSA = Clasificación Suelos Agrícolas)
    # Los predios agrícolas no tienen datosAh sino datosCsa con eac=16
    # Extraemos las coords UTM del primer entry (todas comparten las mismas coords)
    csa_list = _safe(data, ["datosCsa"], default=[])
    if isinstance(csa_list, list) and len(csa_list) > 0:
        csa = csa_list[0]
        base.update({
            "csa_sector": _safe(csa, ["sector"]),
            "csa_clase": _safe(csa, ["clase"]),
            "csa_utm_x": _safe(csa, ["ubicacionX"]),
            "csa_utm_y": _safe(csa, ["ubicacionY"]),
            "csa_eac": _safe(csa, ["eac"]),
            "csa_eacano": _safe(csa, ["eacano"]),
            "csa_valorUnitario": _safe(csa, ["valorUnitario"]),
        })

    base["valorComercial_clp_m2"] = _extract_valor_m2(raw)
    base.update(_extract_obs(raw))
    base.update(_flatten_capas(raw))
    return base


# ─── Fetch ───────────────────────────────────────────────────────────────────

def fetch_predio(session: requests.Session, comuna: int, manzana: str,
                 predio: str, wms_name: str = "") -> dict:
    """Fetch + normalize un predio. Retry 3x con backoff."""
    payload = build_payload(comuna, manzana, predio, wms_name)

    for attempt in range(MAX_RETRIES):
        try:
            r = session.post(URL_PREDIO_NACIONAL, json=payload,
                             timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                time.sleep(2 + attempt * 2)
                continue
            if r.status_code == 504:
                time.sleep(1)
                continue

            raw = r.json() if r.content else {}
            result = normalize(comuna, manzana, predio, raw)
            result["_status"] = r.status_code

            existe = _safe(raw, ["data", "existePredio"])
            result["_ok"] = (
                r.status_code == 200 and
                (existe in [1, "1", True] or result.get("nombreComuna") is not None)
            )
            return result

        except requests.exceptions.RequestException:
            if attempt == MAX_RETRIES - 1:
                return {
                    "comuna": comuna, "manzana": manzana, "predio": predio,
                    "rol": f"{manzana}-{predio}",
                    "_ok": False, "_status": 0, "_error": "max_retries",
                }
            time.sleep(1)

    return {
        "comuna": comuna, "manzana": manzana, "predio": predio,
        "rol": f"{manzana}-{predio}",
        "_ok": False, "_status": 0, "_error": "max_retries",
    }
