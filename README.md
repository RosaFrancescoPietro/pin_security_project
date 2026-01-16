# 🔐 PIN Security Analyzer - Big Data Pipeline

Un sistema di **Big Data Analytics in Real-Time** che analizza la sicurezza dei PIN inviati via Telegram, geolocalizza le minacce e visualizza i risultati su una mappa interattiva.



## 🏗️ Architettura
Il progetto implementa una pipeline Lambda Architecture semplificata:
1.  **Ingestion:** Telegram Bot (Python) raccoglie PIN e Coordinate GPS.
2.  **Streaming:** Apache Kafka (KRaft mode) gestisce i messaggi in tempo reale.
3.  **Processing:** Apache Spark (Structured Streaming) elabora i dati, calcola un "Safety Score" e aggiunge timestamp.
4.  **Storage & Viz:** Elasticsearch indicizza i dati arricchiti; Kibana li visualizza su Mappe e Dashboard.

## 🚀 Come Avviare il Progetto

### Prerequisiti
* Docker & Docker Compose installati.
* Un Token per Bot Telegram (da `@BotFather`).

### Installazione
1.  Clona la repository:
    ```bash
    git clone [https://github.com/TUO_USERNAME/pin-security-project.git](https://github.com/TUO_USERNAME/pin-security-project.git)
    cd pin-security-project
    ```

2.  Crea un file `.env` nella root e inserisci il tuo token (opzionale, o inseriscilo nel docker-compose):
    ```env
    TELEGRAM_TOKEN=il_tuo_token_qui
    ```

3.  Avvia i container:
    ```bash
    docker compose up --build
    ```

4.  **Inizializza il Database (Passaggio Cruciale):**
    Apri un terminale e lancia questo comando per configurare la mappa:
    ```bash
    curl -X PUT "localhost:9200/pin-security-events" -H 'Content-Type: application/json' -d'
    {
      "mappings": {
        "properties": {
          "location": { "type": "geo_point" },
          "timestamp": { "type": "date" },
          "score": { "type": "integer" },
          "status": { "type": "keyword" },
          "pin": { "type": "keyword" }
        }
      }
    }'
    ```

## 📊 Utilizzo
1.  Apri il bot su Telegram.
2.  Invia la tua **Posizione** (📎 -> Location).
3.  Scrivi un **PIN** (es. `1234`).
4.  Apri Kibana a `http://localhost:5601` per vedere l'analisi in tempo reale.

## 🛠️ Tecnologie
* **Python 3.9**
* **Apache Kafka 3.7** (KRaft)
* **Apache Spark 3.4.2**
* **Elastic Stack 8.11** (Elasticsearch + Kibana)
* **Docker Compose**# pin_security_project

