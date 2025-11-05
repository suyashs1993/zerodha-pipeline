import asyncio
import json
import logging
import os
from datetime import datetime, UTC
from kiteconnect import KiteTicker, KiteConnect
from aiokafka import AIOKafkaProducer
from src.clickhouse.clickhouse_loader import insert_ticks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
REDPANDA_TOPIC = os.getenv("REDPANDA_TOPIC", "ticks")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", "20000"))

tick_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
producer: AIOKafkaProducer | None = None


def build_instrument_tokens():
    kite = KiteConnect(KITE_API_KEY)
    kite.set_access_token(KITE_ACCESS_TOKEN)
    try:
        instruments = kite.instruments("NSE")
    except Exception as exc:
        logging.error("Failed to fetch instruments: %s", exc)
        return []

    tokens = []
    for inst in instruments:
        if inst.get("exchange") == "NSE" and inst.get("segment", "").startswith("NSE"):
            tokens.append(inst.get("instrument_token"))
    if len(tokens) >= 2999:
        tokens = tokens[:2999]
    else:
        logging.warning("Only %d symbols resolved to tokens; expected at least 100.", len(tokens))
    return tokens


# ----------------------------
# WebSocket callbacks
# ----------------------------

def on_connect(ws, response):
    logging.info("✅ Connected to Kite WebSocket. Subscribing...")
    tokens = build_instrument_tokens()  # Example tokens
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)


def on_ticks(ws, ticks):
    logging.debug("Ticks: {}".format(ticks))
    for tick in ticks:
        tick_data = {
            "ts": datetime.now(UTC).isoformat(),
            "instrument_token": tick.get("instrument_token"),
            "avg_traded_price": tick.get("average_traded_price", 0.0),
            "last_price": tick.get("last_price", 0.0),
            "volume": tick.get("volume_traded", 0),
            "buy_qty": tick.get("total_buy_quantity", 0),
            "sell_qty": tick.get("total_sell_quantity", 0),
            "depth": json.dumps(tick.get("depth"))
        }
        try:
            tick_queue.put_nowait(tick_data)
        except asyncio.QueueFull:
            logging.warning("⚠️ Tick queue full — dropping tick.")


def on_close(ws, code, reason):
    logging.info(f"❌ WebSocket closed: {code}, {reason}")


# Callback when connection closed with error.
def on_error(ws, code, reason):
    logging.error("Connection error: {code} - {reason}".format(code=code, reason=reason))


# Callback when reconnect is on progress
def on_reconnect(ws, attempts_count):
    logging.info("Reconnecting: {}".format(attempts_count))


# Callback when all reconnect failed (exhausted max retries)
def on_noreconnect(ws):
    logging.error("Reconnect failed, stopping the connection.")
    if producer:
        producer.stop()


# ----------------------------
# Tick processor
# ----------------------------
async def process_ticks():
    """Consume ticks, insert into ClickHouse, and publish to Redpanda."""
    global producer
    batch = []

    while True:
        tick = await tick_queue.get()
        batch.append(tick)

        if len(batch) >= BATCH_SIZE:

            try:
                start = datetime.now()
                logging.info(f"🚀 Inserting {len(batch)} ticks into ClickHouse...")
                await asyncio.to_thread(insert_ticks, batch)
                logging.info("✅ ClickHouse insert successful.")
                # Publish to Redpanda (Kafka compatible)
                publish_tasks = [
                    producer.send(REDPANDA_TOPIC, json.dumps(t).encode("utf-8"))
                    for t in batch
                ]
                await asyncio.gather(*publish_tasks)
                await producer.flush()
                elapsed = (datetime.now() - start).total_seconds()
                logging.info(f"🚀 Batch of {len(batch)} processed + sent in {elapsed:.3f}s")
                batch.clear()
            except Exception as e:
                logging.exception(f"❌ Error processing batch: {e}")
            finally:
                batch.clear()
                await asyncio.sleep(0.01)


# ----------------------------
# Main orchestrator
# ----------------------------
async def main():

    global producer

    logging.info(f"🚀 Initializing Redpanda producer → {REDPANDA_BROKER}")
    producer = AIOKafkaProducer(bootstrap_servers=REDPANDA_BROKER)
    await producer.start()

    asyncio.create_task(process_ticks())

    logging.info(f" batch size {BATCH_SIZE}")
    kws = KiteTicker(KITE_API_KEY, KITE_ACCESS_TOKEN)

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_reconnect = on_reconnect
    kws.on_noreconnect = on_noreconnect

    logging.info("🔌 Connecting to Zerodha WebSocket...")
    kws.connect(threaded=True)

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logging.info("🛑 Shutting down...")
    finally:
        if producer:
            await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
