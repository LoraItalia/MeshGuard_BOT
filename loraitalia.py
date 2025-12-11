import logging
import os
from functools import lru_cache
from typing import Optional, Dict

import requests

logger = logging.getLogger("noise-guard-loraitalia")

API_BASE = os.environ.get("LORAITALIA_API_BASE", "https://api.loraitalia.it")
TIMEOUT = float(os.environ.get("LORAITALIA_TIMEOUT", "5.0"))


@lru_cache(maxsize=1024)
def lookup_node(node_hex: str) -> Optional[Dict]:
    """
    Risolve un nodo Meshtastic tramite API pubblica LoraItalia.

    Ritorna un dict con:
      - id
      - short_name
      - long_name
    oppure None se non trovato / errore.
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
    short_name = data.get("shortName") or data.get("name")
    long_name = data.get("longName") or data.get("description")

    return {
        "id": node_id,
        "short_name": short_name,
        "long_name": long_name,
    }
