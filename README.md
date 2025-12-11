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
