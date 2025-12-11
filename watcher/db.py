import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional, List

logger = logging.getLogger("noise-guard-db")

DB_PATH = os.environ.get("DB_PATH", "/data/noise_guard.db")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@contextmanager
def get_db():
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
    with get_db() as conn:
        c = conn.cursor()
        # chat, nodes, mappings, link_tokens, hourly_stats e notifications
        # (identiche a quelle del db.py principale)
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
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS nodes (
                node_num      TEXT PRIMARY KEY,
                short_name    TEXT,
                long_name     TEXT,
                loraitalia_id INTEGER,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            )
            """
        )
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
    cur = conn.execute(
        "SELECT token, chat_id, consumed_at FROM link_tokens WHERE token = ?",
        (token,),
    )
    row = cur.fetchone()
    if not row or row["consumed_at"]:
        return None

    now = utc_now_iso()
    conn.execute(
        "UPDATE link_tokens SET consumed_at = ? WHERE token = ?",
        (now, token),
    )
    return int(row["chat_id"])


def add_node_chat_mapping(conn, node_num: str,
                          chat_id: int, local_name: Optional[str] = None):
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
    Identico alla versione nel DB principale, ma viene usato dal watcher solo in casi limitati.
    Ordina per nome locale → long_name → short_name → id.
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
    with get_db() as conn:
        cur = conn.execute(
            "DELETE FROM node_chat_mappings WHERE chat_id = ? AND node_num = ?",
            (chat_id, node_num),
        )
        return cur.rowcount > 0


def increment_hourly_stats(conn, node_num: str,
                           window_start: str, window_end: str,
                           category: str):
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
        total = row["total_count"] + 1
        position  = row["position_count"]
        nodeinfo  = row["nodeinfo_count"]
        telemetry = row["telemetry_count"]
        text      = row["text_count"]
        other     = row["other_count"]
    else:
        total = 1
        position = nodeinfo = telemetry = text = other = 0

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
    Restituisce long_name, altrimenti short_name, altrimenti l'ID esadecimale.
    È usato dallo watcher per comporre messaggi di avviso.
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
