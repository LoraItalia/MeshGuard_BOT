import logging
import os
from functools import lru_cache
from typing import Optional, Dict

import requests

logger = logging.getLogger("noise-guard-loraitalia")

API_BASE = os.environ.get("LORAITALIA_API_BASE", "https://api.loraitalia.it")
TIMEOUT  = float(os.environ.get("LORAITALIA_TIMEOUT", "5.0"))


@lru_cache(maxsize=1024)
def lookup_node(node_hex: str) -> Optional[Dict]:
    """
    Risolve un nodo Meshtastic tramite l’API pubblica di LoraItalia.
    Restituisce None in caso di errore o se il nodo non esiste.

    Parametro node_hex: identificatore esadecimale senza '!' e senza prefisso "0x".
    """
    node_hex = node_hex.lower()
    url = f"{API_BASE.rstrip('/')}/public/map/get/node/{node_hex}"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        if not r.ok:
            logger.warning("LoraItalia %s -> HTTP %s", url, r.status_code)
            return None
        data = r.json()
    except Exception:
        logger.exception("Errore chiamando LoraItalia per %s", node_hex)
        return None

    if not isinstance(data, dict):
        return None

    node_id = data.get("id")
    # Supportiamo sia camelCase che snake_case e, in ultima istanza, name/description.
    short_name = (
        data.get("short_name")
        or data.get("shortName")
        or data.get("name")
    )
    long_name = (
        data.get("long_name")
        or data.get("longName")
        or data.get("description")
    )

    return {
        "id": node_id,
        "short_name": short_name,
        "long_name": long_name,
