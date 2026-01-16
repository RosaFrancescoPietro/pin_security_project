import os
import json
import asyncio
import threading
import re
import warnings
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, Application
from kafka import KafkaProducer, KafkaConsumer

warnings.filterwarnings("ignore", category=UserWarning)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

MESSAGES_TEMPLATE = {
    "start": "Ciao! Scrivimi un PIN o una frase che lo contiene.\n📍 Se vuoi, invia prima la tua posizione (graffetta -> Posizione) per la mappa in Kibana!",
    "help": "Criteri: Lunghezza, ripetizioni, sequenze e geolocalizzazione.",
    "wait": "⏳ Elaborazione in corso...",
    "found": "🔍 PIN individuato: {pin}\nAnalisi avviata...",
    "loc_received": "📍 Posizione salvata! Ora invia il PIN da analizzare.",
    "error_no_pin": "❌ Nessun numero valido (min 4 cifre). Riprova!",
}

# --- KAFKA SETUP ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    'enriched-pins',
    bootstrap_servers=KAFKA_BROKER,
    group_id='bot_geo_group_v1',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# --- HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(MESSAGES_TEMPLATE["start"])

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(MESSAGES_TEMPLATE["help"])

async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Salva la posizione temporaneamente nella sessione utente"""
    location = update.message.location
    if location:
        context.user_data['lat'] = location.latitude
        context.user_data['lon'] = location.longitude
        print(f"[DEBUG] 📍 Posizione salvata: {location.latitude}, {location.longitude}")
        await update.message.reply_text(MESSAGES_TEMPLATE["loc_received"])

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    pin_to_check = re.sub(r"\D", "", update.message.text)
    
    if not pin_to_check or len(pin_to_check) < 4:
        await update.message.reply_text(MESSAGES_TEMPLATE["error_no_pin"])
        return

    await update.message.reply_text(MESSAGES_TEMPLATE["wait"]) # Feedback immediato (senza edit per semplicità)
    
    # Recupero coordinate (se presenti)
    lat = context.user_data.get('lat')
    lon = context.user_data.get('lon')

    data = {
        "chat_id": str(chat_id), 
        "pin": str(pin_to_check),
        "language": "it",
        # Struttura per Elastic (se lat/lon esistono, altrimenti None)
        "location": { "lat": lat, "lon": lon } if lat and lon else None
    }
    
    producer.send('input-pins', value=data)
    producer.flush()
    print(f"[DEBUG] 📤 Inviato a Kafka: {pin_to_check} (Geo: {lat},{lon})", flush=True)

# --- LISTENER FEEDBACK ---
def kafka_listener(application, loop):
    print("👂 [INIT] Listener attivo.", flush=True)
    for msg in consumer:
        try:
            data = msg.value 
            chat_id = data.get('chat_id')
            status = data.get('status') 
            score = data.get('score')
            pin = data.get('pin')

            icons = {"VERY_WEAK": "🔴", "WEAK": "🟡", "MEDIUM": "🟠", "STRONG": "🟢"}
            icon = icons.get(status, "⚪")

            text = (
                f"{icon} <b>RISULTATO ANALISI</b>\n"
                f"PIN: <code>{pin}</code>\n"
                f"Status: <b>{status}</b>\n"
                f"Score: <code>{score}/100</code>"
            )
            asyncio.run_coroutine_threadsafe(
                application.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML"), 
                loop
            )
        except Exception as e:
            print(f"❌ Errore Listener: {e}", flush=True)

# --- MAIN ---
async def post_init(application: Application):
    loop = asyncio.get_running_loop()
    threading.Thread(target=kafka_listener, args=(application, loop), daemon=True).start()

if __name__ == '__main__':
    if not TELEGRAM_TOKEN:
        print("❌ ERRORE: TELEGRAM_TOKEN mancante!")
    else:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(post_init).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(MessageHandler(filters.LOCATION, handle_location)) # Handler posizione
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
        
        print("🚀 Bot Online (Geo Enabled).")
        app.run_polling()