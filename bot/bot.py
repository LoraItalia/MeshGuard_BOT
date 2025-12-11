import json
import logging
import os
import pytz
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ParseMode,
    Update,
)
from telegram.ext import (
    Updater,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    Filters,
    MessageHandler,
)

import db

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("noise-guard-bot")

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
NOISE_THRESHOLD = int(os.environ.get("NOISE_THRESHOLD", "100"))

# Fuso orario di riferimento per la scheduler
TZ_ROME = pytz.timezone("Europe/Rome")

# Stati per le conversazioni /setname e /deletenode
SETNAME_WAIT_NODE, SETNAME_WAIT_NAME, DELNODE_WAIT_NODE = range(3)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def upsert_chat_record(chat):
    with db.get_db() as conn:
        db.ensure_chat(conn, chat.id, chat.type,
                       getattr(chat, 'title', None),
                       getattr(chat, 'username', None))


def format_nodes_list(chat_id: int) -> str:
    rows = db.get_nodes_for_chat(chat_id)
    if not rows:
        return "Non ci sono nodi attualmente associati a questa chat. Usa /link per collegarne uno."

    lines = ["Nodi collegati a questa chat:\n"]
    for idx, row in enumerate(rows, start=1):
        node_hex  = row["node_num"]
        display   = row["display_name"]
        node_pretty = f"!{node_hex.lower()}"
        lines.append(f"{idx}. {display} ({node_pretty})")
    return "\n".join(lines)


def cmd_start(update: Update, context: CallbackContext):
    chat = update.effective_chat
    upsert_chat_record(chat)
    text = (
        "Ti aiuto a tenere sotto controllo i nodi Meshtastic rumorosi.\n\n"
        "Comandi disponibili:\n"
        "/link – collega un nodo a questa chat tramite token\n"
        "/mynodes – mostra i nodi collegati\n"
        "/setname – imposta un nome locale per un nodo\n"
        "/deletenode – scollega un nodo da questa chat\n\n"
        "Per iniziare: manda /link."
    )
    context.bot.send_message(chat_id=chat.id, text=text)


def cmd_link(update: Update, context: CallbackContext):
    chat = update.effective_chat
    upsert_chat_record(chat)
    token = db.create_link_token(chat.id)
    logger.info("Creato token %s per chat_id=%s", token, chat.id)

    instr_text = (
        "🔗 Ho generato un token di collegamento per questa chat.\n\n"
        "Ora fai così:\n"
        "1️⃣ Copia il messaggio che ti invio dopo, con scritto `LINK XXXX`\n"
        "2️⃣ Invia quel testo da un tuo nodo Meshtastic (come *messaggio normale*)\n\n"
        "Quando il nodo invia il messaggio, lo associo a questa chat."
    )
    context.bot.send_message(chat_id=chat.id, text=instr_text, parse_mode=ParseMode.MARKDOWN)

    link_text = f"`LINK {token}`"
    context.bot.send_message(chat_id=chat.id, text=link_text, parse_mode=ParseMode.MARKDOWN)


def cmd_mynodes(update: Update, context: CallbackContext):
    chat = update.effective_chat
    upsert_chat_record(chat)
    text = format_nodes_list(chat.id)
    if "Non ci sono nodi" not in text:
        text += (
            "\n\n"
            "/setname – imposta un nome locale per un nodo\n"
            "/deletenode – scollega un nodo da questa chat"
        )
    context.bot.send_message(chat_id=chat.id, text=text, parse_mode=ParseMode.MARKDOWN)


def cmd_setname(update: Update, context: CallbackContext):
    chat = update.effective_chat
    upsert_chat_record(chat)

    rows = db.get_nodes_for_chat(chat.id)
    if not rows:
        context.bot.send_message(
            chat_id=chat.id,
            text="Non ci sono nodi collegati a questa chat. Prima usa /link.",
        )
        return ConversationHandler.END

    keyboard = []
    for row in rows:
        node_num = row["node_num"]
        label    = row["display_name"]
        keyboard.append(
            [InlineKeyboardButton(label, callback_data=f"setname:{node_num}")]
        )

    reply_markup = InlineKeyboardMarkup(keyboard)
    context.bot.send_message(
        chat_id=chat.id,
        text="Scegli il nodo a cui vuoi assegnare un nome locale:",
        reply_markup=reply_markup,
    )
    return SETNAME_WAIT_NODE


def on_setname_node_choice(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    data = query.data or ""
    _, node_num = data.split(":", 1)
    context.user_data["setname_node_num"] = node_num

    query.edit_message_text(
        text=f"Inviami il nuovo nome locale per il nodo `{node_num}`:",
        parse_mode=ParseMode.MARKDOWN,
    )
    return SETNAME_WAIT_NAME


def on_setname_receive_name(update: Update, context: CallbackContext):
    chat     = update.effective_chat
    node_num = context.user_data.get("setname_node_num")
    if not node_num:
        update.message.reply_text("Qualcosa è andato storto, riprova con /setname.")
        return ConversationHandler.END

    local_name = update.message.text.strip()
    if not local_name:
        update.message.reply_text("Il nome non può essere vuoto.")
        return SETNAME_WAIT_NAME

    ok = db.update_local_name(chat.id, node_num, local_name)
    if not ok:
        update.message.reply_text(
            "Non ho trovato quel nodo collegato a questa chat. Riprova con /setname."
        )
    else:
        update.message.reply_text(
            f"Nome locale aggiornato: `{node_num}` → *{local_name}*",
            parse_mode=ParseMode.MARKDOWN,
        )
    return ConversationHandler.END


def cmd_deletenode(update: Update, context: CallbackContext):
    chat = update.effective_chat
    upsert_chat_record(chat)
    rows = db.get_nodes_for_chat(chat.id)
    if not rows:
        context.bot.send_message(
            chat_id=chat.id, text="Non ci sono nodi collegati a questa chat."
        )
        return ConversationHandler.END

    keyboard = []
    for row in rows:
        node_num = row["node_num"]
        label    = row["display_name"]
        keyboard.append(
            [InlineKeyboardButton(label, callback_data=f"delnode:{node_num}")]
        )

    reply_markup = InlineKeyboardMarkup(keyboard)
    context.bot.send_message(
        chat_id=chat.id,
        text="Seleziona il nodo da scollegare da questa chat:",
        reply_markup=reply_markup,
    )
    return DELNODE_WAIT_NODE


def on_deletenode_choice(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    data = query.data or ""
    _, node_num = data.split(":", 1)
    chat_id = query.message.chat_id

    ok = db.delete_node_mapping(chat_id, node_num)
    if ok:
        query.edit_message_text(
            text=f"✅ Nodo `{node_num}` scollegato da questa chat.",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        query.edit_message_text(
            text="Non ho trovato quel nodo collegato a questa chat.",
        )
    return ConversationHandler.END


def cmd_cancel(update: Update, context: CallbackContext):
    update.message.reply_text("Operazione annullata.")
    return ConversationHandler.END


def job_process_notifications(bot):
    """
    Elabora le notifiche pendenti per i nodi rumorosi e le invia alle relative chat,
    utilizzando il nome locale o il long_name/short_name memorizzato.
    """
    with db.get_db() as conn:
        rows = db.get_pending_notifications(conn)
        if not rows:
            return
        logger.info("Trovate %d notifiche pendenti", len(rows))

        import json as _json

        for row in rows:
            notif_id    = row["id"]
            node_num    = row["node_num"]
            window_start = row["window_start"]
            window_end   = row["window_end"]
            count       = row["packet_count"]
            threshold   = row["threshold"]
            cats        = _json.loads(row["categories_json"])

            # Recupera tutte le chat collegate a questo nodo con la loro display_name preferita
            cur = conn.execute(
                """
                SELECT m.chat_id,
                       COALESCE(m.local_name, n.long_name, n.short_name, m.node_num) AS display_name
                  FROM node_chat_mappings m
                  JOIN nodes n ON n.node_num = m.node_num
                 WHERE m.node_num = ?
                """,
                (node_num,),
            )
            chat_rows = cur.fetchall()
            if not chat_rows:
                db.mark_notification_processed(conn, notif_id, "no_chat_for_node")
                continue

            node_pretty = f"!{node_num.lower()}"
            cats_parts  = [f"{k}: {v}" for k, v in sorted(cats.items(), key=lambda kv: kv[1], reverse=>
            cats_str    = ", ".join(cats_parts)

            send_errors = []
            for cr in chat_rows:
                chat_id      = cr["chat_id"]
                chat_display = cr["display_name"]
                text = (
                    f"⚠️ Nodo *{chat_display}* ({node_pretty}) rumoroso.\n"
                    f"Finestra: `{window_start}` – `{window_end}`\n"
                    f"Pacchetti: *{count}* (soglia {threshold})\n"
                    f"Dettaglio per tipo: {cats_str}"
                )
                try:
                    bot.send_message(
                        chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN
                    )
                except Exception as e:
                    logger.exception(
                        "Errore nell'invio della notifica per nodo %s a chat %s",
                        node_num,
                        chat_id,
                    )
                    send_errors.append(str(e))
            error_msg = "; ".join(send_errors) if send_errors else None
            db.mark_notification_processed(conn, notif_id, error_msg)


def main():
    db.init_db()

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", cmd_start))
    dp.add_handler(CommandHandler("link", cmd_link))
    dp.add_handler(CommandHandler("mynodes", cmd_mynodes))

    setname_conv = ConversationHandler(
        entry_points=[CommandHandler("setname", cmd_setname)],
        states={
            SETNAME_WAIT_NODE: [CallbackQueryHandler(on_setname_node_choice, pattern=r"^setname:")],
            SETNAME_WAIT_NAME: [MessageHandler(Filters.text & ~Filters.command, on_setname_receive_nam>
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )
    dp.add_handler(setname_conv)

    delnode_conv = ConversationHandler(
        entry_points=[CommandHandler("deletenode", cmd_deletenode)],
        states={
            DELNODE_WAIT_NODE: [CallbackQueryHandler(on_deletenode_choice, pattern=r"^delnode:")],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )
    dp.add_handler(delnode_conv)

    scheduler = BackgroundScheduler(timezone=TZ_ROME)
    scheduler.add_job(
        job_process_notifications,
        "interval",
        seconds=30,
        args=(updater.bot,),
        id="job_process_notifications",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler avviato")

    updater.start_polling()
    logger.info("Noise Guard bot avviato")
    updater.idle()


if __name__ == "__main__":
    main()
