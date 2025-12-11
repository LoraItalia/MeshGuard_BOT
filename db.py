import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional, List

logger = logging.getLogger("noise-guard-db")

# Percorso del database; può essere sovrascritto da variabile d'ambiente
DB_PATH = os.environ.get("DB_PATH", "/data/noise_guard.db")


def utc_now_iso() -> str:
    """Restituisce l'orario attuale in formato ISO8601, senza microsecondi, in UTC."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@contextmanager
def get_db():
    """
    Gestore di contesto per accedere al DB SQLite con row_factory impostata su sqlite3.Row.
    Esegue automaticamente commit/rollback a fine transazione.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Crea tutte le tabelle del DB se non esistono già."""
    with get_db() as conn:
        c = conn.cursor()

        # Tabella delle chat Telegram
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS chats (
                chat_id     INTEGER PRIMARY KEY,
                type        TEXT NOT NULL,
                title       TEXT,
                username    TEXT,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            )
            """
        )

        # Tabella dei nodi Meshtastic
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS nodes (
                node_num      TEXT PRIMARY KEY,  -- id esadecimale (senza '!')
                short_name    TEXT,
                long_name     TEXT,
                loraitalia_id INTEGER,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            )
            """
        )

        # Mappatura nodo ↔ chat (con eventuale nome locale)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS node_chat_mappings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                node_num    TEXT NOT NULL,
                chat_id     INTEGER NOT NULL,
                local_name  TEXT,
                created_at  TEXT NOT NULL,
                verified_at TEXT NOT NULL,
                UNIQUE(node_num, chat_id),
                FOREIGN KEY(node_num) REFERENCES nodes(node_num) ON DELETE CASCADE,
                FOREIGN KEY(chat_id)  REFERENCES chats(chat_id)  ON DELETE CASCADE
            )
            """
        )

        # Token generati dal comando /link
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS link_tokens (
                token       TEXT PRIMARY KEY,
                chat_id     INTEGER NOT NULL,
                created_at  TEXT NOT NULL,
                consumed_at TEXT,
                FOREIGN KEY(chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
            )
            """
        )

        # Statistiche orarie per ogni nodo
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS hourly_stats (
                node_num        TEXT NOT NULL,
                window_start    TEXT NOT NULL,
                window_end      TEXT NOT NULL,
                total_count     INTEGER NOT NULL DEFAULT 0,
                position_count  INTEGER NOT NULL DEFAULT 0,
                nodeinfo_count  INTEGER NOT NULL DEFAULT 0,
                telemetry_count INTEGER NOT NULL DEFAULT 0,
                text_count      INTEGER NOT NULL DEFAULT 0,
                other_count     INTEGER NOT NULL DEFAULT 0,
                last_updated_at TEXT NOT NULL,
                PRIMARY KEY (node_num, window_start),
                FOREIGN KEY(node_num) REFERENCES nodes(node_num) ON DELETE CASCADE
            )
            """
        )

        # Notifiche da inviare per nodi rumorosi
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                node_num        TEXT NOT NULL,
                window_start    TEXT NOT NULL,
                window_end      TEXT NOT NULL,
                packet_count    INTEGER NOT NULL,
                threshold       INTEGER NOT NULL,
                categories_json TEXT NOT NULL,
                created_at      TEXT NOT NULL,
                processed       INTEGER NOT NULL DEFAULT 0,
                error           TEXT,
                FOREIGN KEY(node_num) REFERENCES nodes(node_num) ON DELETE CASCADE
            )
            """
        )

        conn.commit()
        logger.info("DB inizializzato in %s", DB_PATH)


def ensure_chat(conn, chat_id: int, chat_type: str,
                title: Optional[str], username: Optional[str]):
    """Inserisce o aggiorna una chat Telegram nel DB."""
    now = utc_now_iso()
    cur = conn.execute(
        "SELECT chat_id FROM chats WHERE chat_id = ?", (chat_id,)
    )
    row = cur.fetchone()
    if row:
        conn.execute(
            """
            UPDATE chats
               SET type = ?, title = ?, username = ?, updated_at = ?
             WHERE chat_id = ?
            """,
            (chat_type, title, username, now, chat_id),
        )
    else:
        conn.execute(
            """
            INSERT INTO chats (chat_id, type, title, username, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (chat_id, chat_type, title, username, now, now),
        )


def upsert_node(conn, node_num: str,
                short_name: Optional[str] = None,
                long_name: Optional[str] = None,
                loraitalia_id: Optional[int] = None):
    """Inserisce o aggiorna i dati di un nodo nel DB."""
    now = utc_now_iso()
    cur = conn.execute(
        "SELECT node_num FROM nodes WHERE node_num = ?", (node_num,)
    )
    row = cur.fetchone()
    if row:
        conn.execute(
            """
            UPDATE nodes
               SET short_name    = COALESCE(?, short_name),
                   long_name     = COALESCE(?, long_name),
                   loraitalia_id = COALESCE(?, loraitalia_id),
                   updated_at    = ?
             WHERE node_num = ?
            """,
            (short_name, long_name, loraitalia_id, now, node_num),
        )
    else:
        conn.execute(
            """
            INSERT INTO nodes (node_num, short_name, long_name, loraitalia_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (node_num, short_name, long_name, loraitalia_id, now, now),
        )


def create_link_token(chat_id: int) -> str:
    """
    Genera un token univoco (8 caratteri esadecimali, in maiuscolo) e lo salva nel DB
    associandolo alla chat.
    """
    import secrets
    token = secrets.token_hex(4).upper()
    with get_db() as conn:
        now = utc_now_iso()
        conn.execute(
            "INSERT INTO link_tokens (token, chat_id, created_at) VALUES (?, ?, ?)",
            (token, chat_id, now),
        )
    return token


def consume_link_token(conn, token: str) -> Optional[int]:
    """
    Consuma un token: restituisce l'ID della chat associata se il token esiste
    ed è ancora valido, altrimenti None.
    """
    cur = conn.execute(
        "SELECT token, chat_id, consumed_at FROM link_tokens WHERE token = ?",
        (token,),
    )
    row = cur.fetchone()
    if not row or row["consumed_at"]:
        return None

    now = utc_now_iso()
    conn.execute(
        "UPDATE link_tokens SET consumed_at = ? WHERE token = ?", (now, token)
    )
    return int(row["chat_id"])


def add_node_chat_mapping(conn, node_num: str,
                          chat_id: int, local_name: Optional[str] = None):
    """
    Associa un nodo ad una chat. Se esiste già, aggiorna solo il nome locale (se fornito)
    e la data di verifica.
    """
    now = utc_now_iso()
    cur = conn.execute(
        """
        SELECT id, local_name FROM node_chat_mappings
         WHERE node_num = ? AND chat_id = ?
        """,
        (node_num, chat_id),
    )
    row = cur.fetchone()
    if row:
        conn.execute(
            """
            UPDATE node_chat_mappings
               SET local_name  = COALESCE(?, local_name),
                   verified_at = ?
             WHERE id = ?
            """,
            (local_name, now, row["id"]),
        )
    else:
        conn.execute(
            """
            INSERT INTO node_chat_mappings (node_num, chat_id, local_name, created_at, verified_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (node_num, chat_id, local_name, now, now),
        )


def get_nodes_for_chat(chat_id: int) -> List[sqlite3.Row]:
    """
    Restituisce la lista dei nodi associati a una chat, insieme a:
    - display_name: priorità locale → long_name → short_name → ID.
    - short_name / long_name / loraitalia_id per eventuali usi.
    Ordina per display_name.
    """
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT m.node_num,
                   COALESCE(m.local_name, n.long_name, n.short_name, m.node_num) AS display_name,
                   n.short_name,
                   n.long_name,
                   n.loraitalia_id,
                   m.verified_at
              FROM node_chat_mappings m
              JOIN nodes n ON n.node_num = m.node_num
             WHERE m.chat_id = ?
             ORDER BY display_name
            """,
            (chat_id,),
        )
        return cur.fetchall()


def update_local_name(chat_id: int, node_num: str, local_name: str) -> bool:
    """
    Aggiorna il nome locale di un nodo per una specifica chat.
    Ritorna True se almeno una riga è stata modificata, False altrimenti.
    """
    with get_db() as conn:
        cur = conn.execute(
            """
            UPDATE node_chat_mappings
               SET local_name = ?, verified_at = ?
             WHERE chat_id = ? AND node_num = ?
            """,
            (local_name, utc_now_iso(), chat_id, node_num),
        )
        return cur.rowcount > 0


def delete_node_mapping(chat_id: int, node_num: str) -> bool:
    """Scollega un nodo da una chat."""
    with get_db() as conn:
        cur = conn.execute(
            "DELETE FROM node_chat_mappings WHERE chat_id = ? AND node_num = ?",
            (chat_id, node_num),
        )
        return cur.rowcount > 0


def increment_hourly_stats(conn, node_num: str,
                           window_start: str, window_end: str,
                           category: str):
    """
    Aggiorna (o inserisce) le statistiche orarie per un nodo, incrementando il contatore
    totale e quello della categoria specifica (position, nodeinfo, telemetry, text, other).
    """
    now = utc_now_iso()
    cur = conn.execute(
        """
        SELECT node_num, window_start, total_count,
               position_count, nodeinfo_count,
               telemetry_count, text_count, other_count
          FROM hourly_stats
         WHERE node_num = ? AND window_start = ?
        """,
        (node_num, window_start),
    )
    row = cur.fetchone()
    if row:
        total     = row["total_count"] + 1
        position  = row["position_count"]
        nodeinfo  = row["nodeinfo_count"]
        telemetry = row["telemetry_count"]
        text      = row["text_count"]
        other     = row["other_count"]
    else:
        total = 1
        position = nodeinfo = telemetry = text = other = 0

    # Incrementa il contatore appropriato
    if category == "position":
        position += 1
    elif category == "nodeinfo":
        nodeinfo += 1
    elif category == "telemetry":
        telemetry += 1
    elif category == "text":
        text += 1
    else:
        other += 1

    conn.execute(
        """
        INSERT INTO hourly_stats (
            node_num, window_start, window_end,
            total_count, position_count, nodeinfo_count,
            telemetry_count, text_count, other_count,
            last_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(node_num, window_start) DO UPDATE SET
            total_count     = excluded.total_count,
            position_count  = excluded.position_count,
            nodeinfo_count  = excluded.nodeinfo_count,
            telemetry_count = excluded.telemetry_count,
            text_count      = excluded.text_count,
            other_count     = excluded.other_count,
            last_updated_at = excluded.last_updated_at
        """,
        (
            node_num, window_start, window_end,
            total, position, nodeinfo, telemetry, text, other,
            now,
        ),
    )


def create_notification(conn, node_num: str,
                        window_start: str, window_end: str,
                        packet_count: int, threshold: int,
                        categories_json: str):
    """Inserisce una notifica da elaborare per un nodo rumoroso."""
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO notifications (
            node_num, window_start, window_end,
            packet_count, threshold, categories_json,
            created_at, processed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """,
        (
            node_num, window_start, window_end,
            packet_count, threshold, categories_json,
            now,
        ),
    )


def get_pending_notifications(conn):
    """Ritorna le notifiche in attesa di essere processate."""
    cur = conn.execute(
        """
        SELECT id, node_num, window_start, window_end,
               packet_count, threshold, categories_json
          FROM notifications
         WHERE processed = 0
         ORDER BY created_at
        """
    )
    return cur.fetchall()


def mark_notification_processed(conn, notif_id: int,
                                error: Optional[str] = None):
    """Segna una notifica come processata, memorizzando eventuale errore."""
    conn.execute(
        """
        UPDATE notifications
           SET processed = 1,
               error = ?
         WHERE id = ?
        """,
        (error, notif_id),
    )


def get_node_display_name(conn, node_num: str) -> str:
    """
    Restituisce il miglior nome disponibile per il nodo:
    1. long_name (se presente)
    2. short_name
    3. l'ID esadecimale
    """
    cur = conn.execute(
        """
        SELECT COALESCE(long_name, short_name, node_num) AS name
          FROM nodes
         WHERE node_num = ?
        """,
        (node_num,),
    )
    row = cur.fetchone()
    return row["name"] if row else node_num
