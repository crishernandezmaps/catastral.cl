#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Paso 0: Descarga masiva de datos prediales desde API SII (getPredioNacional).

Lee archivo TXT de roles semestrales (ancho fijo), consulta la API por cada
predio y guarda CSVs incrementales por comuna con recovery automático.

Optimizado para ~20 rps (probado seguro contra el SII).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from tqdm import tqdm
except Exception:
    tqdm = None


# =========================
# Endpoints SII
# =========================

SII_BASE = "https://www4.sii.cl"
URL_PREDIO_NACIONAL = f"{SII_BASE}/mapasui/services/data/mapasFacadeService/getPredioNacional"
URL_BOOTSTRAP = f"{SII_BASE}/mapasui/internet/"


# =========================
# Config
# =========================

@dataclass
class ClientConfig:
    workers: int = 24
    timeout_s: int = 30
    max_retries: int = 5
    backoff_factor: float = 0.6
    rps: float = 20.0
    jitter_s: float = 0.03
    bootstrap_cookies: bool = True

    # Servicios (payload): modo simple (AH) o un "generic full" (sin WMS comuna-specific)
    servicios_mode: str = "ah_only"  # "ah_only" | "full_generic"

    # Default AH (si usas ah_only)
    style_layer: str = "AH_MUESTRA_EAC_14_2022"
    eac: int = 14
    eacano: int = 2022

    # Recovery / checkpoint
    progress_every_s: float = 10.0
    write_every_n: int = 1000

    # CSV schema stability (para reanudar sin romper headers)
    extra_json_col: str = "_extra_json"

    # Skip rol_base assignment (for chunked batch mode)
    skip_rol_base: bool = False


# =========================
# Rate limiter thread-safe
# =========================

class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.time()


# =========================
# Interface-bound adapter (macOS IP_BOUND_IF for multi-VPN)
# =========================

import socket as _socket
import struct as _struct

_BIND_IFACE: str | None = None  # Set via --bind-iface CLI arg

class _BoundIfaceAdapter(HTTPAdapter):
    """HTTPAdapter that binds sockets to a specific network interface.
    macOS: IP_BOUND_IF (IPPROTO_IP, 25) with interface index.
    Linux: SO_BINDTODEVICE (SOL_SOCKET, 25) with interface name bytes.
    """
    def __init__(self, iface_name: str, **kwargs):
        self._iface_name = iface_name
        self._is_linux = sys.platform.startswith('linux')
        if not self._is_linux:
            self._iface_idx = _socket.if_nametoindex(iface_name)
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        super().init_poolmanager(*args, **kwargs)
        if self._is_linux:
            # Linux: SO_BINDTODEVICE
            sock_opt = (_socket.SOL_SOCKET, 25, self._iface_name.encode())
        else:
            # macOS: IP_BOUND_IF
            sock_opt = (_socket.IPPROTO_IP, 25, _struct.pack('I', self._iface_idx))
        orig = self.poolmanager.connection_pool_kw.get('socket_options', [])
        self.poolmanager.connection_pool_kw['socket_options'] = orig + [sock_opt]


# =========================
# Session (1 por thread)
# =========================

_thread_local = threading.local()

def _build_session(cfg: ClientConfig) -> requests.Session:
    s = requests.Session()

    retry = Retry(
        total=cfg.max_retries,
        connect=cfg.max_retries,
        read=cfg.max_retries,
        status=cfg.max_retries,
        backoff_factor=cfg.backoff_factor,
        status_forcelist=(429, 500, 502, 503),  # 504 handled at app level (fast retry)
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    if _BIND_IFACE:
        adapter = _BoundIfaceAdapter(
            iface_name=_BIND_IFACE,
            max_retries=retry,
            pool_connections=cfg.workers,
            pool_maxsize=cfg.workers,
        )
    else:
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=cfg.workers,
            pool_maxsize=cfg.workers,
        )
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    s.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": SII_BASE,
            "Referer": URL_BOOTSTRAP,
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
        }
    )

    cookie = os.getenv("SII_COOKIE")
    if cookie:
        s.headers["Cookie"] = cookie

    return s


def get_thread_session(cfg: ClientConfig) -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    booted = getattr(_thread_local, "booted", False)

    if sess is None:
        sess = _build_session(cfg)
        _thread_local.session = sess

    if cfg.bootstrap_cookies and not booted:
        try:
            sess.get(URL_BOOTSTRAP, timeout=cfg.timeout_s)
        except Exception:
            pass
        _thread_local.booted = True

    return sess


# =========================
# Parsing archivo ancho fijo
# =========================
def read_rol_semestral_txt(path: str, encoding: str = "latin-1", only_comuna: int = None) -> pd.DataFrame:
    """Lee TXT ancho fijo de roles. Si only_comuna, filtra durante lectura (ahorra RAM)."""
    only_comuna_s = str(only_comuna).zfill(5) if only_comuna is not None else None
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding=encoding, errors="replace") as f:
        for line_no, raw in enumerate(f, start=1):
            line = raw.rstrip("\n")
            if not line.strip():
                continue

            def sl(a: int, b: int) -> str:
                return line[a - 1 : b].strip()

            comuna = sl(1, 5)

            # Filtro temprano: skip lineas de otras comunas
            if only_comuna_s is not None and comuna != only_comuna_s:
                continue
            anio = sl(6, 9)
            semestre = sl(10, 10)
            ind_aseo = sl(11, 11)
            direccion = sl(18, 57)
            manzana = line[57:62]  # 58-62
            predio = line[62:67]   # 63-67
            serie = sl(68, 68)
            cuota = sl(69, 81)
            avaluo_total = sl(82, 96)
            avaluo_exento = sl(97, 111)
            anio_term_ex = sl(112, 115)
            cod_ubic = sl(116, 116)
            cod_dest = sl(117, 117)

            comuna_i = int(comuna) if comuna.isdigit() else None
            anio_i = int(anio) if anio.isdigit() else None
            sem_i = int(semestre) if semestre.isdigit() else None

            manzana_s = str(manzana).strip().replace(" ", "")
            predio_s = str(predio).strip().replace(" ", "")

            if comuna_i is None or not manzana_s or not predio_s:
                rows.append(
                    {
                        "_line": line_no,
                        "comuna": comuna_i,
                        "anio": anio_i,
                        "semestre": sem_i,
                        "indicador_aseo": ind_aseo or None,
                        "direccion": direccion or None,
                        "manzana": manzana_s or None,
                        "predio": predio_s or None,
                        "serie": serie or None,
                        "cuota_trimestral": cuota or None,
                        "avaluo_total": avaluo_total or None,
                        "avaluo_exento": avaluo_exento or None,
                        "anio_termino_exencion": anio_term_ex or None,
                        "codigo_ubicacion": cod_ubic or None,
                        "codigo_destino": cod_dest or None,
                        "v": None,
                        "_parse_ok": False,
                    }
                )
                continue

            manzana5 = manzana_s.zfill(5)
            predio5 = predio_s.zfill(5)
            rows.append(
                {
                    "_line": line_no,
                    "comuna": comuna_i,
                    "anio": anio_i,
                    "semestre": sem_i,
                    "indicador_aseo": ind_aseo or None,
                    "direccion": direccion or None,
                    "manzana": manzana5,
                    "predio": predio5,
                    "serie": serie or None,
                    "cuota_trimestral": cuota or None,
                    "avaluo_total": avaluo_total or None,
                    "avaluo_exento": avaluo_exento or None,
                    "anio_termino_exencion": anio_term_ex or None,
                    "codigo_ubicacion": cod_ubic or None,
                    "codigo_destino": cod_dest or None,
                    "v": f"{comuna_i}|{manzana5}|{predio5}",
                    "_parse_ok": True,
                }
            )

    return pd.DataFrame(rows)


# =========================
# Helpers JSON
# =========================
def safe_get(d: Dict[str, Any], path: List[Any], default: Any = None) -> Any:
    cur: Any = d
    for key in path:
        try:
            cur = cur[key]
        except Exception:
            return default
    return cur


def slug(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"[^\w\s-]+", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s\-]+", "_", s, flags=re.UNICODE)
    s = s.strip("_")
    return s[:120] if len(s) > 120 else s


def extract_valor_m2_terreno_clp_from_datoscapas(payload_json: Dict[str, Any]) -> Optional[str]:
    capas = safe_get(payload_json, ["data", "datosCapas"], default=[])
    if not isinstance(capas, list):
        return None

    last_value: Optional[str] = None
    for capa in capas:
        datos = safe_get(capa, ["datos"], default=[])
        if not isinstance(datos, list):
            continue
        for item in datos:
            et = safe_get(item, ["etiqueta"], default="")
            if str(et).strip().lower() in ["valor m² de terreno", "valor m2 de terreno", "valor m² de terreno "]:
                val = safe_get(item, ["valor"], default=None)
                if isinstance(val, str):
                    last_value = (
                        val.replace("$", "")
                        .replace(".", "")
                        .replace(",", "")
                        .strip()
                    )
                else:
                    last_value = str(val) if val is not None else None
    return last_value


def extract_observatorio_2025(payload_json: Dict[str, Any]) -> Dict[str, Any]:
    capas = safe_get(payload_json, ["data", "datosCapas"], default=[])
    if not isinstance(capas, list):
        return {}

    target = None
    for capa in capas:
        titulo = safe_get(capa, ["titulo"], default="")
        if isinstance(titulo, str) and "observatorio de mercado de suelo urbano 2025" in titulo.lower():
            target = capa
            break

    if not target:
        return {}

    out: Dict[str, Any] = {}
    datos = safe_get(target, ["datos"], default=[])
    if not isinstance(datos, list):
        return {}

    for item in datos:
        et = safe_get(item, ["etiqueta"], default=None)
        val = safe_get(item, ["valor"], default=None)
        if et is None:
            continue
        key = slug(et)

        rename = {
            "región": "obs_region",
            "region": "obs_region",
            "comuna": "obs_comuna",
            "código_área_homogénea": "obs_codigo_area_homogenea",
            "codigo_area_homogenea": "obs_codigo_area_homogenea",
            "valor_comercial_m²_de_suelo": "obs_valor_comercial_m2_suelo",
            "valor_comercial_m2_de_suelo": "obs_valor_comercial_m2_suelo",
            "valor_comercial_m²_de_suelo_": "obs_valor_comercial_m2_suelo",
            "valor_comercial_m²_de_suelo__": "obs_valor_comercial_m2_suelo",
        }

        if key.startswith("transacciones_"):
            out["obs_tx_" + key.replace("transacciones_", "")] = val
        elif key in rename:
            out[rename[key]] = val
        else:
            out["obs_" + key] = val

    return out


def flatten_datoscapas_generic(payload_json: Dict[str, Any]) -> Dict[str, Any]:
    capas = safe_get(payload_json, ["data", "datosCapas"], default=[])
    if not isinstance(capas, list):
        return {}

    out: Dict[str, Any] = {}
    for capa in capas:
        titulo = safe_get(capa, ["titulo"], default="")
        tslug = slug(titulo) if titulo else "sin_titulo"
        datos = safe_get(capa, ["datos"], default=[])
        if not isinstance(datos, list):
            continue
        for item in datos:
            et = safe_get(item, ["etiqueta"], default=None)
            val = safe_get(item, ["valor"], default=None)
            if et is None:
                continue
            k = f"cap__{tslug}__{slug(et)}"
            if k in out and out[k] != val:
                i = 2
                kk = f"{k}__{i}"
                while kk in out:
                    i += 1
                    kk = f"{k}__{i}"
                out[kk] = val
            else:
                out[k] = val
    return out


def normalize_result(c: int, m: str, p: str, payload_json: Dict[str, Any]) -> Dict[str, Any]:
    data = safe_get(payload_json, ["data"], default={})
    if not isinstance(data, dict):
        data = {}

    base = {
        "comuna": c,
        "manzana": m,
        "predio": p,
        "v": f"{c}|{m}|{p}",
        "rol": f"{m}-{p}",

        "eacs": safe_get(data, ["eacs"], default=None),
        "eacano": safe_get(data, ["eacano"], default=None),
        "eacsDescripcion": safe_get(data, ["eacsDescripcion"], default=None),

        "direccion_sii": safe_get(data, ["direccion"], default=None),
        "nombreComuna": safe_get(data, ["nombreComuna"], default=None),
        "destinoDescripcion": safe_get(data, ["destinoDescripcion"], default=None),
        "ubicacion": safe_get(data, ["ubicacion"], default=None),
        "tablaOrigen": safe_get(data, ["tablaOrigen"], default=None),
        "periodo": safe_get(data, ["periodo"], default=None),
        "existePredio": safe_get(data, ["existePredio"], default=None),

        "valorTotal": safe_get(data, ["valorTotal"], default=None),
        "valorAfecto": safe_get(data, ["valorAfecto"], default=None),
        "valorExento": safe_get(data, ["valorExento"], default=None),

        "supTerreno": safe_get(data, ["supTerreno"], default=None),
        "supConsMt2": safe_get(data, ["supConsMt2"], default=None),
        "supConsMt3": safe_get(data, ["supConsMt3"], default=None),
        "medidaSup": safe_get(data, ["medidaSup"], default=None),
        "medidaSupConst": safe_get(data, ["medidaSupConst"], default=None),

        "ah": safe_get(data, ["ah"], default=safe_get(data, ["datosAh", "codigoAh"], default=None)),
        "sector": safe_get(data, ["sector"], default=None),

        # OJO: ubicacionX es latitud, ubicacionY es longitud (invertido!)
        "lat": safe_get(data, ["ubicacionX"], default=None),
        "lon": safe_get(data, ["ubicacionY"], default=None),
    }

    datos_ah = safe_get(data, ["datosAh"], default={})
    if isinstance(datos_ah, dict):
        base.update(
            {
                "ah_rangoSuperficie": safe_get(datos_ah, ["rangoSuperficie"], default=None),
                "ah_valorUnitario": safe_get(datos_ah, ["valorUnitario"], default=None),
                "ah_numeroMuestras": safe_get(datos_ah, ["numeroMuestras"], default=None),
                "ah_coefVariacion": safe_get(datos_ah, ["coefVariacion"], default=None),
                "ah_mediana": safe_get(datos_ah, ["mediana"], default=None),
                "ah_eac": safe_get(datos_ah, ["eac"], default=None),
                "ah_eacano": safe_get(datos_ah, ["eacano"], default=None),
                "ah_utm_x": safe_get(datos_ah, ["ubicacionX"], default=None),
                "ah_utm_y": safe_get(datos_ah, ["ubicacionY"], default=None),
            }
        )
        if isinstance(base.get("ah_rangoSuperficie"), str):
            base["ah_rangoSuperficie"] = base["ah_rangoSuperficie"].strip()

    pp = safe_get(data, ["predioPublicado"], default={})
    if isinstance(pp, dict):
        base.update(
            {
                "predioPublicado_id": safe_get(pp, ["id"], default=None),
                "predioPublicado_comuna": safe_get(pp, ["comuna"], default=None),
                "predioPublicado_manzana": safe_get(pp, ["manzana"], default=None),
                "predioPublicado_predio": safe_get(pp, ["predio"], default=None),
                "predioPublicado_utm_x": safe_get(pp, ["ubicacionX"], default=None),
                "predioPublicado_utm_y": safe_get(pp, ["ubicacionY"], default=None),
            }
        )

    base["rangoSuperficie"] = base.get("ah_rangoSuperficie")
    base["valorComercial_clp_m2"] = extract_valor_m2_terreno_clp_from_datoscapas(payload_json)
    base.update(extract_observatorio_2025(payload_json))
    base.update(flatten_datoscapas_generic(payload_json))

    return base


# =========================
# Payload servicios
# =========================

def build_servicios(cfg: ClientConfig, c: int) -> List[Dict[str, Any]]:
    if cfg.servicios_mode == "full_generic":
        return [
            {"comuna": int(c), "layer": "sii:BR_CART_AH_MUESTRAS", "style": "AH_MUESTRA_EAC_15_2025", "eac": 15, "eacano": 2025},
            {"comuna": int(c), "layer": "sii:BR_CART_CSA_MUESTRAS", "style": "CSA_MUESTRA_EAC_16_2024", "eac": 16, "eacano": 2024},
            {"comuna": int(c), "layer": "sii:BR_CART_AH_MUESTRAS", "style": "AH_MUESTRA_EAC_15_2024", "eac": 15, "eacano": 2024},
            {"comuna": int(c), "layer": "sii:BR_CART_AH_MUESTRAS", "style": "AH_MUESTRA_EAC_15_2023", "eac": 15, "eacano": 2023},
            {"comuna": int(c), "layer": "sii:BR_CART_AH_MUESTRAS", "style": "AH_MUESTRA_EAC_14_2022", "eac": 14, "eacano": 2022},
            {"comuna": int(c), "layer": "sii:BR_CART_TEMATICA", "style": "DEST_EDUCACION", "eac": 0, "eacano": 0},
            {"comuna": int(c), "layer": "sii:BR_CART_TEMATICA", "style": "DESTINOS_02", "eac": 0, "eacano": 0},
            {"comuna": int(c), "layer": "sii:BR_CART_OBS_SUELO_15", "style": "OBS_SUELO_15_V2", "eac": 15, "eacano": 2025},
        ]

    if cfg.servicios_mode == "obs_ah":
        return [
            {"comuna": int(c), "layer": "sii:BR_CART_AH_MUESTRAS", "style": "AH_MUESTRA_EAC_15_2025", "eac": 15, "eacano": 2025},
            {"comuna": int(c), "layer": "sii:BR_CART_OBS_SUELO_15", "style": "OBS_SUELO_15_V2", "eac": 15, "eacano": 2025},
        ]

    return [
        {"comuna": int(c), "layer": "sii:BR_CART_AH_MUESTRAS", "style": cfg.style_layer, "eac": int(cfg.eac), "eacano": int(cfg.eacano)}
    ]


def build_payload_get_predio_nacional(cfg: ClientConfig, c: int, m: str, p: str) -> Dict[str, Any]:
    return {
        "metaData": {
            "namespace": "cl.sii.sdi.lob.bbrr.mapas.data.api.interfaces.MapasFacadeService/getPredioNacional",
            "conversationId": "UNAUTHENTICATED-CALL",
            "transactionId": f"tx-{int(time.time()*1000)}-{random.randint(1000,9999)}",
        },
        "data": {
            "predio": {"comuna": str(c), "manzana": str(int(m)), "predio": str(int(p))},
            "servicios": build_servicios(cfg, c),
        },
    }


def parse_v(v: str) -> Tuple[int, str, str]:
    parts = str(v).split("|")
    if len(parts) != 3:
        raise ValueError(f"Formato v invalido: {v!r}")
    c = int(parts[0])
    m = parts[1].strip()
    p = parts[2].strip()
    return c, m, p


# =========================
# Worker
# =========================

def fetch_one(cfg: ClientConfig, limiter: RateLimiter, v: str) -> Dict[str, Any]:
    c, m, p = parse_v(v)

    limiter.wait()
    if cfg.jitter_s:
        time.sleep(random.random() * cfg.jitter_s)

    session = get_thread_session(cfg)
    payload = build_payload_get_predio_nacional(cfg, c, m, p)

    max_504_retries = 2
    for attempt in range(1 + max_504_retries):
        try:
            r = session.post(URL_PREDIO_NACIONAL, json=payload, timeout=cfg.timeout_s)

            # Fast retry on 504: don't wait, just try again immediately
            if r.status_code == 504 and attempt < max_504_retries:
                continue

            try:
                data: Dict[str, Any] = r.json() if r.content else {}
            except Exception:
                data = {}

            out = normalize_result(c, m, p, data)
            out["_status"] = r.status_code

            existe = safe_get(data, ["data", "existePredio"], default=None)
            ok = (r.status_code == 200) and (existe in [1, "1", True] or out.get("nombreComuna") or out.get("valorTotal") is not None)
            out["_ok"] = bool(ok)

            if not out["_ok"]:
                out["_msg"] = safe_get(data, ["data", "mensaje"], default=safe_get(data, ["message"], default=None))

            return out

        except requests.exceptions.ReadTimeout:
            # Timeout: fast retry without backoff
            if attempt < max_504_retries:
                continue
            return {
                "comuna": c, "manzana": m, "predio": p,
                "v": f"{c}|{m}|{p}", "rol": f"{m}-{p}",
                "_ok": False, "_status": None, "_error": "ReadTimeout",
            }
        except Exception as e:
            return {
                "comuna": c, "manzana": m, "predio": p,
                "v": f"{c}|{m}|{p}", "rol": f"{m}-{p}",
                "_ok": False, "_status": None, "_error": repr(e),
            }

    # Should not reach here, but safety net
    return {
        "comuna": c, "manzana": m, "predio": p,
        "v": f"{c}|{m}|{p}", "rol": f"{m}-{p}",
        "_ok": False, "_status": 504, "_error": "max_504_retries",
    }


# =========================
# Recovery + CSV incremental robusto
# =========================

def _truthy(s: Any) -> bool:
    if s is None:
        return False
    return str(s).strip().lower() in {"true", "1", "t", "yes", "y", "si", "si"}


def _read_csv_header(out_csv: str) -> Optional[List[str]]:
    if not os.path.exists(out_csv):
        return None
    try:
        with open(out_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return None
            return [h.strip() for h in header]
    except UnicodeDecodeError:
        with open(out_csv, "r", encoding="latin-1", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return None
            return [h.strip() for h in header]
    except Exception:
        return None


def load_existing_ok_set(out_csv: str) -> Set[str]:
    if not os.path.exists(out_csv):
        return set()

    ok_set: Set[str] = set()
    for enc in ("utf-8", "latin-1"):
        try:
            with open(out_csv, "r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    return set()

                has_v = "v" in reader.fieldnames
                has_ok = "_ok" in reader.fieldnames
                can_build_v = {"comuna", "manzana", "predio"}.issubset(set(reader.fieldnames or []))

                if not has_ok:
                    return set()

                for row in reader:
                    if not row:
                        continue
                    if not _truthy(row.get("_ok")):
                        continue

                    if has_v:
                        v = (row.get("v") or "").strip()
                    elif can_build_v:
                        v = f"{(row.get('comuna') or '').strip()}|{(row.get('manzana') or '').strip()}|{(row.get('predio') or '').strip()}"
                    else:
                        continue

                    if v and "|" in v:
                        ok_set.add(v)
            return ok_set
        except UnicodeDecodeError:
            continue
        except Exception:
            break

    return ok_set


def _project_row_to_header(row: Dict[str, Any], header: List[str], extra_col: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    extras: Dict[str, Any] = {}

    header_set = set(header)
    for k, v in row.items():
        if k in header_set:
            out[k] = v
        else:
            if k != extra_col:
                extras[k] = v

    for k in header:
        out.setdefault(k, None)

    if extra_col in header_set:
        prev_extra = row.get(extra_col)
        merged = {}
        if isinstance(prev_extra, str) and prev_extra.strip():
            try:
                merged.update(json.loads(prev_extra))
            except Exception:
                pass
        merged.update(extras)

        out[extra_col] = json.dumps(merged, ensure_ascii=False) if merged else (prev_extra if prev_extra else "")
    return out


def append_results_incremental(out_csv: str, rows: List[Dict[str, Any]], cfg: ClientConfig) -> None:
    if not rows:
        return

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)

    header = _read_csv_header(out_csv)
    write_header = header is None

    if write_header:
        keys = set()
        for r in rows:
            keys.update(r.keys())
        keys.add(cfg.extra_json_col)

        preferred = [
            "v", "comuna", "manzana", "predio", "rol", "rol_base",
            "_ok", "_status", "_msg", "_error",
            "nombreComuna", "direccion_sii",
            "lat", "lon",
            "valorTotal", "valorAfecto", "valorExento",
            "supTerreno", "supConsMt2", "supConsMt3",
            "valorComercial_clp_m2",
            # Campos exclusivos del TXT
            "txt_direccion", "txt_avaluo_total", "txt_avaluo_exento",
            "txt_cuota_trimestral", "txt_serie", "txt_ind_aseo",
            "txt_anio_term_exencion", "txt_cod_ubicacion", "txt_cod_destino",
            cfg.extra_json_col,
        ]
        header = [k for k in preferred if k in keys] + sorted([k for k in keys if k not in set(preferred)])

    projected = [_project_row_to_header(r, header, cfg.extra_json_col) for r in rows]

    with open(out_csv, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=header,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
        )
        if write_header:
            w.writeheader()
        w.writerows(projected)


# =========================
# Run with incremental recovery
# =========================

def run_comuna_incremental(
    roles_df: pd.DataFrame,
    comuna: int,
    out_csv: str,
    cfg: ClientConfig,
) -> pd.DataFrame:
    df = roles_df.loc[roles_df["comuna"].astype(int) == int(comuna)].copy()
    if df.empty:
        raise ValueError(f"No hay filas para comuna={comuna}")

    if "v" not in df.columns:
        df["v"] = df["comuna"].astype(int).astype(str) + "|" + df["manzana"].astype(str) + "|" + df["predio"].astype(str)

    values_all = df["v"].astype(str).tolist()

    # Chunk support: process only every Nth predio (for multi-tunnel parallelism)
    _chunk = int(os.environ.get("SII_CHUNK", "1"))
    _chunk_offset = int(os.environ.get("SII_CHUNK_OFFSET", "0"))
    if _chunk > 1:
        values_all = [v for i, v in enumerate(values_all) if i % _chunk == _chunk_offset]
        print(f"[CHUNK] {_chunk_offset}/{_chunk}: {len(values_all)} predios (from {len(df)} total)")

    # Lookup campos TXT por v (para enriquecer resultado API)
    _TXT_FIELDS = {
        "direccion": "txt_direccion",
        "avaluo_total": "txt_avaluo_total",
        "avaluo_exento": "txt_avaluo_exento",
        "cuota_trimestral": "txt_cuota_trimestral",
        "serie": "txt_serie",
        "indicador_aseo": "txt_ind_aseo",
        "anio_termino_exencion": "txt_anio_term_exencion",
        "codigo_ubicacion": "txt_cod_ubicacion",
        "codigo_destino": "txt_cod_destino",
    }
    txt_lookup: Dict[str, Dict[str, Any]] = {}
    txt_cols_present = [c for c in _TXT_FIELDS if c in df.columns]
    if txt_cols_present:
        for _, row in df.iterrows():
            v_key = str(row["v"])
            txt_lookup[v_key] = {_TXT_FIELDS[c]: row[c] for c in txt_cols_present}

    done_ok = load_existing_ok_set(out_csv)
    values_pending = [v for v in values_all if v not in done_ok]

    print(f"[RECOVERY] comuna={comuna} total={len(values_all)} ya_ok={len(done_ok)} pendientes={len(values_pending)} -> {out_csv}")

    if not values_pending:
        try:
            return pd.read_csv(out_csv, low_memory=False)
        except Exception:
            return pd.DataFrame()

    limiter = RateLimiter(cfg.rps)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    lock = threading.Lock()
    stats = {"done": 0, "ok": 0, "fail": 0, "last_print": time.time(), "t0": time.time()}
    buffer: List[Dict[str, Any]] = []

    def maybe_print():
        now = time.time()
        if now - stats["last_print"] >= cfg.progress_every_s:
            elapsed = now - stats["t0"]
            with lock:
                rate = stats["done"] / max(elapsed, 1e-9)
                print(
                    f"[PROGRESS] comuna={comuna} done={stats['done']}/{len(values_pending)} "
                    f"ok={stats['ok']} fail={stats['fail']} rate={rate:.2f} r/s elapsed={elapsed:.1f}s"
                )
                stats["last_print"] = now

    submit_iter = values_pending
    if tqdm is not None:
        submit_iter = tqdm(values_pending, total=len(values_pending), desc=f"SII comuna {comuna}")

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        futs = [ex.submit(fetch_one, cfg, limiter, v) for v in submit_iter]

        for f in as_completed(futs):
            row = f.result()

            # Enriquecer con campos del TXT
            v_key = row.get("v")
            if v_key and v_key in txt_lookup:
                row.update(txt_lookup[v_key])

            with lock:
                stats["done"] += 1
                if row.get("_ok"):
                    stats["ok"] += 1
                    stats["_consec_fail"] = 0
                else:
                    stats["fail"] += 1
                    stats["_consec_fail"] = stats.get("_consec_fail", 0) + 1
                    # Detect rate-limited IP: 20 consecutive fails → exit 42
                    if stats["_consec_fail"] >= 20:
                        print(f"[RATE_LIMITED] {stats['_consec_fail']} consecutive fails, IP likely burned. Exiting with code 42.")
                        if buffer:
                            append_results_incremental(out_csv, buffer, cfg)
                        os._exit(42)

                buffer.append(row)

                if len(buffer) >= cfg.write_every_n:
                    append_results_incremental(out_csv, buffer, cfg)
                    buffer.clear()

            maybe_print()

    if buffer:
        append_results_incremental(out_csv, buffer, cfg)
        buffer.clear()

    try:
        out = pd.read_csv(out_csv, low_memory=False)
    except Exception:
        out = pd.DataFrame()

    elapsed = time.time() - stats["t0"]
    print(f"[DONE] comuna={comuna} elapsed={elapsed:.1f}s out={out_csv}")

    # Asignar rol_base si aun no tiene (skip si --skip-rol-base)
    if not getattr(cfg, 'skip_rol_base', False):
        if "rol_base" not in out.columns or out["rol_base"].isna().all() or (out["rol_base"] == "").all():
            assign_rol_base(out_csv, comuna, cfg)
            try:
                out = pd.read_csv(out_csv, low_memory=False)
            except Exception:
                pass

    return out


# =========================
# Asignacion de rol_base
# =========================

def assign_rol_base(csv_path: str, comuna: int, cfg: ClientConfig) -> None:
    """
    Post-proceso: asigna rol_base a cada predio en el CSV.
    - Lotes simples (1 predio por coordenada): rol_base = su propio rol
    - Multi-unidad (N predios misma coordenada): scan 90xxx inteligente
      (90000 + min_predio del grupo, no scan ciego 90001-90999)
    """
    df = pd.read_csv(csv_path, low_memory=False)

    if "rol_base" in df.columns and df["rol_base"].notna().any() and (df["rol_base"] != "").any():
        print(f"[ROL_BASE] ya asignado, saltando")
        return

    # Filtrar solo los OK con coordenadas
    mask_ok = df["_ok"].astype(str).str.lower().isin(["true", "1"])
    mask_geo = df["lat"].notna() & df["lon"].notna()
    geo = df[mask_ok & mask_geo].copy()

    if geo.empty:
        print(f"[ROL_BASE] sin predios geo, saltando")
        df["rol_base"] = ""
        df.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
        return

    # Redondear coordenadas para agrupar (6 decimales ~ 0.1m)
    geo["_coord_key"] = (
        geo["manzana"].astype(str) + "|" +
        geo["lat"].round(6).astype(str) + "|" +
        geo["lon"].round(6).astype(str)
    )

    # Contar predios por coordenada
    coord_counts = geo["_coord_key"].value_counts()

    # Clasificar
    simple_coords = set(coord_counts[coord_counts == 1].index)
    multi_coords = set(coord_counts[coord_counts > 1].index)

    n_simple = len(simple_coords)
    n_multi = len(multi_coords)
    print(f"[ROL_BASE] comuna={comuna} predios_geo={len(geo)} coords_simple={n_simple} coords_multi={n_multi}")

    # Inicializar rol_base
    df["rol_base"] = ""

    # 1) Lotes simples: rol_base = su propio rol
    for idx, row in geo.iterrows():
        if row["_coord_key"] in simple_coords:
            mz = str(row["manzana"]).strip().zfill(5)
            pr = str(row["predio"]).strip().zfill(5)
            df.at[idx, "rol_base"] = f"{mz}-{pr}"

    assigned_simple = (df["rol_base"] != "").sum()
    print(f"[ROL_BASE] simples asignados: {assigned_simple}")

    # 2) Multi-unidad: scan inteligente de 90xxx
    if not multi_coords:
        df.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
        print(f"[ROL_BASE] sin multi-unidad, guardado")
        return

    # Agrupar multi-unidad por manzana → coordenadas → min predio
    from collections import defaultdict
    mz_groups: Dict[str, List[Dict]] = defaultdict(list)  # manzana → [{coord_key, min_predio, lat, lon, indices}]

    for coord_key in multi_coords:
        group = geo[geo["_coord_key"] == coord_key]
        manzana = str(group["manzana"].iloc[0]).strip()
        predios_int = []
        for p in group["predio"]:
            try:
                predios_int.append(int(p))
            except (ValueError, TypeError):
                pass
        min_p = min(predios_int) if predios_int else 1
        lat_mean = group["lat"].mean()
        lon_mean = group["lon"].mean()
        indices = group.index.tolist()
        mz_groups[manzana].append({
            "coord_key": coord_key,
            "min_predio": min_p,
            "lat": lat_mean,
            "lon": lon_mean,
            "indices": indices,
            "n_predios": len(group),
        })

    # Construir candidatos 90xxx por manzana
    candidates: List[str] = []  # v values a consultar
    candidate_meta: Dict[str, Tuple[str, int]] = {}  # v → (manzana, predio_90xxx)

    for manzana, groups in mz_groups.items():
        seen_predios = set()
        for g in groups:
            # Candidato principal: 90000 + min_predio
            p90 = 90000 + g["min_predio"]
            seen_predios.add(p90)
            # Tambien probar +-2 alrededor
            for delta in range(-2, 3):
                seen_predios.add(p90 + delta)

        # Agregar 90001-90005 como fallback comun
        for p in range(90001, 90006):
            seen_predios.add(p)

        # Filtrar rango valido
        for p in sorted(seen_predios):
            if 90001 <= p <= 90999:
                v = f"{comuna}|{manzana}|{str(p).zfill(5)}"
                candidates.append(v)
                candidate_meta[v] = (manzana, p)

    print(f"[ROL_BASE] manzanas_multi={len(mz_groups)} candidatos_90xxx={len(candidates)}")

    if not candidates:
        df.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
        return

    # Fetch candidatos 90xxx en paralelo
    limiter = RateLimiter(cfg.rps)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    found_90xxx: List[Dict[str, Any]] = []
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        futs = {ex.submit(fetch_one, cfg, limiter, v): v for v in candidates}
        for f in as_completed(futs):
            result = f.result()
            if result.get("_ok") and result.get("lat") is not None and result.get("lon") is not None:
                found_90xxx.append(result)

    elapsed = time.time() - t0
    print(f"[ROL_BASE] scan 90xxx: {len(found_90xxx)} encontrados de {len(candidates)} candidatos ({elapsed:.1f}s)")

    # Matchear cada 90xxx encontrado a un grupo multi-unidad por coordenada
    assigned_multi = 0
    for r90 in found_90xxx:
        mz = str(r90["manzana"]).strip()
        lat90 = float(r90["lat"])
        lon90 = float(r90["lon"])
        pr90 = str(r90["predio"]).strip().zfill(5)
        rol90 = f"{mz.zfill(5)}-{pr90}"

        if mz not in mz_groups:
            continue

        # Encontrar el grupo mas cercano en esta manzana
        best_dist = float("inf")
        best_group = None
        for g in mz_groups[mz]:
            dist = ((lat90 - g["lat"])**2 + (lon90 - g["lon"])**2) ** 0.5
            if dist < best_dist:
                best_dist = dist
                best_group = g

        if best_group is not None and best_dist < 0.001:  # ~100m threshold
            for idx in best_group["indices"]:
                df.at[idx, "rol_base"] = rol90
            assigned_multi += len(best_group["indices"])
            # Marcar grupo como asignado para no reasignar
            best_group["_assigned"] = True

    total_assigned = (df["rol_base"] != "").sum()
    total_predios = mask_ok.sum()
    pct = total_assigned / max(total_predios, 1) * 100
    print(f"[ROL_BASE] multi asignados: {assigned_multi} predios")
    print(f"[ROL_BASE] TOTAL: {total_assigned}/{total_predios} ({pct:.1f}%) con rol_base")

    df.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
    print(f"[ROL_BASE] guardado en {csv_path}")


def run_all_comunas_incremental(
    roles_df: pd.DataFrame,
    out_dir: str,
    cfg: ClientConfig,
) -> None:
    os.makedirs(out_dir, exist_ok=True)

    comunas = sorted(roles_df["comuna"].dropna().astype(int).unique().tolist())
    print(f"[RUN] comunas={len(comunas)} out_dir={out_dir}")

    for c in comunas:
        out_csv = os.path.join(out_dir, f"comuna={c}.csv")
        try:
            run_comuna_incremental(roles_df, comuna=c, out_csv=out_csv, cfg=cfg)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[ERROR] comuna={c} err={repr(e)} (se continua con la siguiente)")


def consolidate_out_dir(out_dir: str, consolidated_csv: str) -> None:
    files = [os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.startswith("comuna=") and f.endswith(".csv")]
    if not files:
        raise ValueError(f"No hay archivos en {out_dir} para consolidar.")

    dfs = []
    for fp in sorted(files):
        try:
            dfs.append(pd.read_csv(fp, low_memory=False))
        except Exception:
            continue

    if not dfs:
        raise ValueError("No se pudieron leer los CSVs para consolidar.")

    out = pd.concat(dfs, ignore_index=True, sort=False)
    os.makedirs(os.path.dirname(consolidated_csv) or ".", exist_ok=True)
    out.to_csv(consolidated_csv, index=False)
    print(f"[CONSOLIDATE] {len(files)} archivos -> {consolidated_csv} filas={len(out)}")


# =========================
# CLI
# =========================

def main():
    ap = argparse.ArgumentParser(
        description="Paso 0: Descarga masiva de datos prediales desde API SII"
    )

    ap.add_argument("--roles-txt", required=True, help="TXT ancho fijo, ej: BRTMPNACROL_NAC_2025_2.txt")
    ap.add_argument("--encoding", default="latin-1")
    ap.add_argument("--comuna", type=int, default=None, help="Procesar solo una comuna")

    ap.add_argument("--out", default=None, help="Salida CSV unica (solo con --comuna)")
    ap.add_argument("--out-dir", default=None, help="Directorio checkpoint por comuna (recomendado)")
    ap.add_argument("--consolidate", default=None, help="Ruta CSV consolidado (con --out-dir)")
    ap.add_argument("--consolidate-only", action="store_true",
                     help="Solo consolidar CSVs existentes en --out-dir, sin descargar")

    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=5)
    ap.add_argument("--rps", type=float, default=20.0)
    ap.add_argument("--jitter", type=float, default=0.03)
    ap.add_argument("--no-bootstrap", action="store_true")

    ap.add_argument("--servicios-mode", default="ah_only", choices=["ah_only", "full_generic", "obs_ah"])
    ap.add_argument("--style", default="AH_MUESTRA_EAC_14_2022")
    ap.add_argument("--eac", type=int, default=14)
    ap.add_argument("--eacano", type=int, default=2022)

    ap.add_argument("--write-every", type=int, default=1000)
    ap.add_argument("--progress-every", type=float, default=10.0)
    ap.add_argument("--bind-iface", default=None,
                    help="Bind all requests to this network interface (macOS, e.g. utun4)")
    ap.add_argument("--skip-rol-base", action="store_true",
                    help="Skip rol_base assignment (useful in chunked batch mode)")

    args = ap.parse_args()

    if args.bind_iface:
        global _BIND_IFACE
        _BIND_IFACE = args.bind_iface

    # Consolidate-only mode
    if args.consolidate_only:
        if not args.out_dir or not args.consolidate:
            raise SystemExit("--consolidate-only requiere --out-dir y --consolidate")
        consolidate_out_dir(args.out_dir, args.consolidate)
        return

    cfg = ClientConfig(
        workers=args.workers,
        timeout_s=args.timeout,
        max_retries=args.retries,
        rps=args.rps,
        jitter_s=args.jitter,
        bootstrap_cookies=not args.no_bootstrap,
        servicios_mode=args.servicios_mode,
        style_layer=args.style,
        eac=args.eac,
        eacano=args.eacano,
        write_every_n=args.write_every,
        progress_every_s=args.progress_every,
        skip_rol_base=args.skip_rol_base,
    )

    roles_df = read_rol_semestral_txt(args.roles_txt, encoding=args.encoding, only_comuna=args.comuna)

    bad = roles_df.loc[~roles_df["_parse_ok"]]
    if not bad.empty:
        print(f"[WARN] {len(bad)} lineas no parseadas correctamente. Se omiten.")
    roles_df = roles_df.loc[roles_df["_parse_ok"]].copy()

    # Modo 1: una comuna
    if args.comuna is not None:
        if not args.out and not args.out_dir:
            raise SystemExit("Si usas --comuna, debes indicar --out o --out-dir.")
        if args.out_dir:
            out_csv = os.path.join(args.out_dir, f"comuna={int(args.comuna)}.csv")
        else:
            out_csv = args.out

        run_comuna_incremental(roles_df, comuna=int(args.comuna), out_csv=out_csv, cfg=cfg)

        if args.out_dir and args.consolidate:
            consolidate_out_dir(args.out_dir, args.consolidate)
        return

    # Modo 2: todas las comunas
    if not args.out_dir:
        raise SystemExit("Sin --comuna, debes usar --out-dir para checkpoint por comuna.")
    run_all_comunas_incremental(roles_df, out_dir=args.out_dir, cfg=cfg)

    if args.consolidate:
        consolidate_out_dir(args.out_dir, args.consolidate)


if __name__ == "__main__":
    main()
