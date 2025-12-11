import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt
import requests

import db
import loraitalia

# Import opzionale di meshtastic.protobuf per decodificare pacchetti binari
try:
    from meshtastic.protobuf import mqtt_pb2  # type: ignore
except Exception:
    mqtt_pb2 = None

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("noise-guard-watcher")

# Config MQTT
MQTT_HOST     = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT     = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME") or None
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD") or None
MQTT_TOPIC    = os.environ.get("MQTT_TOPIC", "msh/#")

NOISE_THRESHOLD = int(os.environ.get("NOISE_THRESHOLD", "100"))

# Minimo intervallo (in secondi) tra due notifiche per lo stesso nodo nella stessa ora.
NOTIFICATION_INTERVAL_SECONDS = int(os.environ.get("NOTIFICATION_INTERVAL_SECONDS", "60"))

# Limite massimo accettabile per hop limit; oltre a questo verrà inviato un avviso.
MAX_HOPS_ALLOWED = int(os.environ.get("MAX_HOPS_ALLOWED", "5"))

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")

# Regex per estrarre i token LINK
LINK_RE = re.compile(r"\bLINK\s+([0-9A-Fa-f]{6,12})\b")


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def normalize_node_num(node_id: str) -> str:
    """
    Converte l’ID di un nodo Meshtastic (decimale, esadecimale con/ senza '!') in esadecimale minuscolo.
    - Se l’ID è solo numeri, lo interpreta come decimale e lo converte in hex.
    - Se c’è il prefisso '!', lo rimuove.
    """
    if not node_id:
        return ""
    node_str = str(node_id).lstrip("!").strip().lower()
    if not node_str:
        return ""
    # se è tutto cifre → decimale
    if all(ch in "0123456789" for ch in node_str):
        try:
            return format(int(node_str, 10), "x")  # senza zeri iniziali
        except Exception:
            pass
    # altrimenti è già esadecimale
    return node_str


def classify_packet(m: dict) -> str:
    decoded = m.get("decoded") or {}
    port    = decoded.get("portnum") or m.get("portnum")
    if isinstance(port, str):
        port = port.upper()
    if port in ("POSITION_APP", 4, "POSITION"):
        return "position"
    if port in ("NODEINFO_APP", 3, "NODEINFO"):
        return "nodeinfo"
    if port in ("TELEMETRY_APP", 8, "TELEMETRY"):
        return "telemetry"
    if port in ("TEXT_MESSAGE_APP", 1, "TEXT"):
        return "text"
    return "other"


def extract_text(m: dict) -> str:
    decoded = m.get("decoded") or {}
    payload = decoded.get("payload") or {}
    if isinstance(payload, dict) and isinstance(payload.get("text"), str):
        return payload["text"]
    if isinstance(decoded.get("text"), str):
        return decoded["text"]
    return ""


def send_telegram_message(chat_id: int, text: str):
    """
    Invia un messaggio Telegram; se il token non è configurato logga un warning.
    """
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN non configurato, non posso inviare messaggi")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        if not resp.ok:
            logger.error(
                "Errore Telegram HTTP %s: %s", resp.status_code, resp.text[:200]
            )
    except Exception:
        logger.exception("Errore nell'invio del messaggio Telegram")


def handle_link_token(node_num: str, message: dict):
    """
    Gestisce un messaggio con token LINK:
    - Estrae il token.
    - Verifica/consuma il token nel DB.
    - Associa il nodo alla chat.
    - Recupera long/short name via API LoraItalia (senza prefisso '!').
    - Inserisce/aggiorna il nodo nel DB con queste info.
    - Invia conferma in chat usando come display_name: nome locale > long_name > short_name > id.
    """
    text = extract_text(message)
    if not text:
        return
    m = LINK_RE.search(text)
    if not m:
        return
    token = m.group(1).upper()
    logger.info("Trovato token LINK %s dal nodo %s", token, node_num)

    with db.get_db() as conn:
        chat_id = db.consume_link_token(conn, token)
        if not chat_id:
            logger.info("Token %s non valido o già utilizzato", token)
            return

        info = loraitalia.lookup_node(node_num.lower())
        short_name    = info.get("short_name") if info else None
        long_name     = info.get("long_name")  if info else None
        loraitalia_id = info.get("id")         if info else None

        db.upsert_node(conn, node_num, short_name, long_name, loraitalia_id)
        db.add_node_chat_mapping(conn, node_num, chat_id)

    hex_id_pretty = f"!{node_num.lower()}"
    display_name  = short_name or long_name or hex_id_pretty
    msg = (
        f"✅ Nodo {display_name} ({hex_id_pretty}) collegato correttamente al tuo account Telegram.\n\>
        "Da ora in poi riceverai gli avvisi se il nodo diventa rumoroso."
    )
    send_telegram_message(chat_id, msg)


def check_hop_limit(node_num: str, message: dict):
    """
    Controlla se il pacchetto contiene un hop_limit; se è maggiore di MAX_HOPS_ALLOWED,
    invia un avviso a tutte le chat che hanno associato il nodo.
    """
    hop_limit = message.get("hop_limit") or message.get("hopLimit")
    if hop_limit is None:
        return
    try:
        hop_val = int(hop_limit)
    except Exception:
        return
    if hop_val <= MAX_HOPS_ALLOWED:
        return
    with db.get_db() as conn:
        cur = conn.execute(
            "SELECT chat_id FROM node_chat_mappings WHERE node_num = ?", (node_num,)
        )
        chat_rows = cur.fetchall()
        if not chat_rows:
            return
        display_name = db.get_node_display_name(conn, node_num)
    node_pretty = f"!{node_num.lower()}"
    warning = (
        f"⚠️ Nodo {display_name} ({node_pretty}) ha impostato un hop limit troppo alto: {hop_val}.\n"
        f"Il valore consigliato è {MAX_HOPS_ALLOWED} o meno."
    )
    for cr in chat_rows:
        send_telegram_message(cr["chat_id"], warning)


def handle_noise_counters(node_num: str, message: dict):
    """
    Aggiorna le statistiche orarie del nodo e genera una notifica se la soglia viene superata,
    rispettando il rate limit definito da NOTIFICATION_INTERVAL_SECONDS.
    """
    now          = utc_now()
    window_start = now.replace(minute=0, second=0)
    window_end   = window_start + timedelta(hours=1)

    cat = classify_packet(message)

    with db.get_db() as conn:
        db.upsert_node(conn, node_num)
        db.increment_hourly_stats(conn, node_num, iso(window_start), iso(window_end), cat)

        cur = conn.execute(
            """
            SELECT total_count,
                   position_count, nodeinfo_count,
                   telemetry_count, text_count, other_count
              FROM hourly_stats
             WHERE node_num = ? AND window_start = ?
            """,
            (node_num, iso(window_start)),
        )
        row = cur.fetchone()
        if not row or row["total_count"] < NOISE_THRESHOLD:
            return

        cats = {
            "position":  row["position_count"],
            "nodeinfo":  row["nodeinfo_count"],
            "telemetry": row["telemetry_count"],
            "text":      row["text_count"],
            "other":     row["other_count"],
        }
        cats = {k: v for k, v in cats.items() if v}

        # Rate limit: nessuna nuova notifica se ce n'è una troppo recente
        from datetime import timedelta as _td
        min_time = now - _td(seconds=NOTIFICATION_INTERVAL_SECONDS)
        cur2 = conn.execute(
            "SELECT 1 FROM notifications WHERE node_num = ? AND window_start = ? AND created_at >= ? L>
            (node_num, iso(window_start), iso(min_time)),
        )
        if cur2.fetchone():
            return

        import json as _json
        db.create_notification(
            conn,
            node_num,
            iso(window_start),
            iso(window_end),
            row["total_count"],
            NOISE_THRESHOLD,
            _json.dumps(cats),
        )


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connesso al broker MQTT %s:%s", MQTT_HOST, MQTT_PORT)
        client.subscribe(MQTT_TOPIC)
        logger.info("Sottoscritto al topic %s", MQTT_TOPIC)
    else:
        logger.error("Connessione MQTT fallita, rc=%s", rc)


def on_message(client, userdata, msg):
    """
    Elabora i messaggi MQTT:
    - tenta prima di decodificarli come JSON,
    - se fallisce, prova con protobuf Meshtastic (ServiceEnvelope),
    - normalizza l’ID del mittente,
    - gestisce token LINK, contatori di rumore e hop limit.
    """
    try:
        payload = msg.payload.decode("utf-8", errors="ignore")
        m = json.loads(payload)
    except Exception:
        m = None
        if mqtt_pb2 is not None:
            try:
                se = mqtt_pb2.ServiceEnvelope()
                se.ParseFromString(msg.payload)
                mp = se.packet
                m = {}
                try:
                    sender = getattr(mp, 'from')
                except Exception:
                    sender = None
                if sender is not None:
                    m['from'] = sender
                    m['sender'] = sender
                try:
                    if mp.HasField('hop_limit'):
                        m['hop_limit'] = mp.hop_limit
                except Exception:
                    pass
                portnum = None
                if mp.HasField('decoded'):
                    portnum = mp.decoded.portnum
                if portnum is None and hasattr(mp, 'portnum'):
                    portnum = getattr(mp, 'portnum')
                if portnum is not None:
                    m['portnum'] = portnum
                    decoded = {'portnum': portnum}
                    text_val = None
                    if mp.HasField('decoded') and mp.decoded.payload:
                        try:
                            text_val = mp.decoded.payload.decode('utf-8', errors='ignore')
                        except Exception:
                            text_val = None
                    if text_val:
                        decoded['payload'] = {'text': text_val}
                        decoded['text']    = text_val
                    m['decoded'] = decoded
            except Exception:
                m = None
        if not m:
            logger.debug('Messaggio MQTT non valido')
            return

    node_id  = m.get("from") or m.get("sender")
    node_num = normalize_node_num(str(node_id) if node_id is not None else "")
    if not node_num:
        logger.debug("Messaggio senza 'from': %s", str(msg.payload)[:200])
        return

    # 1. Gestione token LINK
    handle_link_token(node_num, m)
    # 2. Aggiornamento statistiche e notifica rumore
    handle_noise_counters(node_num, m)
    # 3. Controllo hop limit
    check_hop_limit(node_num, m)


def main():
    db.init_db()

    client = mqtt.Client()
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT, 60)
    logger.info("Avvio loop MQTT...")
    client.loop_forever()


if __name__ == "__main__":
    main()
