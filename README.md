
# MeshGuard_BOT

MeshGuard_BOT è un bot Telegram pensato per aiutare a tenere pulita la rete Meshtastic, segnalando in automatico i nodi che:

- generano troppo traffico (spam di messaggi / beacon),
- sono configurati male,
- oppure sembrano “impazziti” e rovinano l’esperienza agli altri utenti.

Il bot si integra con un broker MQTT che riceve il traffico Meshtastic e, quando possibile, con le API di LoraItalia per arricchire le informazioni sui nodi.

---

## Funzionalità principali

- Monitoraggio continuo del traffico Meshtastic via MQTT.
- Aggregazione dei messaggi per nodo e per categoria.
- Rilevamento dei nodi “rumorosi” in base a una soglia configurabile (`NOISE_THRESHOLD`).
- Notifiche su Telegram in un gruppo o topic dedicato.
- Comandi Telegram per interrogare lo stato dei nodi.
- Integrazione con LoraItalia (se configurata) per recuperare informazioni aggiuntive.

> **Nota:** il set di funzionalità è in evoluzione. Il bot è pensato per essere uno strumento a supporto della community, non un “dito puntato” contro i singoli utenti.

---

## Architettura

Il progetto è diviso in due componenti principali:

1. **Watcher MQTT**  
   - Ascolta il broker MQTT sul topic Meshtastic (es. `msh/#`).
   - Normalizza i messaggi e li salva in un database (es. SQLite).
   - Calcola i contatori per nodo/categoria/finestra temporale.

2. **Bot Telegram**  
   - Espone i comandi Telegram agli utenti.
   - Interroga il database per produrre statistiche e segnalazioni.
   - Invia notifiche automatiche quando un nodo supera la soglia di rumore.

I due componenti possono girare come processi separati o come container separati nello stesso `docker-compose`.

---

## Requisiti

- Python 3.11+ (consigliato) **oppure** Docker / Docker Compose.
- Un broker MQTT che riceve il traffico Meshtastic.
- Un bot Telegram registrato tramite [@BotFather](https://t.me/BotFather).
- (Opzionale) Accesso alle API di LoraItalia.

---

## Configurazione

La configurazione avviene tramite un file `.env`.  
Nel repository trovi un file di esempio: **`.env.example`**.

1. Copia il file:

   ```bash
   cp .env.example .env
   ```

2. Modifica `.env` e imposta i valori reali:

   - `TELEGRAM_BOT_TOKEN` – token del bot ottenuto da BotFather.
   - `TELEGRAM_GROUP_ID` – ID del gruppo in cui il bot deve scrivere.
   - `TELEGRAM_TOPIC_ID` – ID del topic/thread (0 se non usato).
   - `MQTT_HOST`, `MQTT_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD` – credenziali del broker MQTT.
   - `MQTT_TOPIC` – topic da monitorare (es. `msh/#`).
   - `NOISE_THRESHOLD` – soglia di messaggi/ora oltre la quale un nodo è considerato rumoroso.
   - `LORAITALIA_*` – parametri per l’accesso all’API LoraItalia (opzionali).
   - `LOG_LEVEL` – livello di log (`DEBUG`, `INFO`, `WARNING`, `ERROR`).

Il file `.env` **non** viene commitato grazie al `.gitignore`.

---

## Installazione (senza Docker)

```bash
git clone https://github.com/LoraItalia/MeshGuard_BOT.git
cd MeshGuard_BOT

# (opzionale ma consigliato) crea un virtualenv
python -m venv .venv
source .venv/bin/activate  # su Windows: .venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# modifica .env con i tuoi parametri
```

### Avvio dei servizi

Watcher MQTT:

```bash
python watcher.py
```

Bot Telegram:

```bash
python bot.py
```

I nomi degli script possono variare in base all’evoluzione del progetto; fare riferimento alla struttura attuale della repo.

---

## Esecuzione con Docker

Se nel repository è presente un `docker-compose.yml`, l’avvio tipico sarà:

```bash
cp .env.example .env
# modifica .env con i tuoi parametri

docker compose up -d
```

Questo di solito crea due container:

- uno per il watcher MQTT,
- uno per il bot Telegram.

---

## Comandi Telegram (indicativi)

L’elenco preciso dei comandi può evolvere. Alcuni comandi previsti / tipici:

- `/start` – mostra un messaggio di benvenuto e una breve spiegazione.
- `/help` – riepilogo dei comandi disponibili.
- `/stats` – mostra un riepilogo dei nodi più rumorosi in un certo intervallo.
- `/node <id>` – mostra i dettagli di un singolo nodo (traffico recente, eventuali note).
- `/setname` – procedura guidata per collegare un utente Telegram a uno o più nodi Meshtastic.

I comandi rapidi possono essere configurati nel BotFather alla voce **Edit Commands**.

---

## Linee guida d’uso

- Lo scopo del bot è **migliorare la rete**, non attaccare i singoli utenti.
- Le segnalazioni che emergono dal bot dovrebbero essere usate per:
  - contattare chi gestisce il nodo,
  - proporre configurazioni migliori,
  - evitare spam e configurazioni che disturbano la rete.

Suggerimenti, bug e miglioramenti sono benvenuti tramite issue su GitHub o nel gruppo Telegram dedicato.

---

## Licenza

Questo progetto è rilasciato sotto licenza **MIT**.  
Vedi il file [`LICENSE`](LICENSE) per i dettagli.
