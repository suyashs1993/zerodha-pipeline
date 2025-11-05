"""
signal_generator.py
--------------------------
Generates BUY/SELL/NEUTRAL trading signals based on order book imbalance.
Includes confidence scoring and metadata for downstream analytics.
"""
import os
from typing import Dict, List
import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
import datetime



# --- Logging Setup (with ms precision) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
REDPANDA_TOPIC = os.getenv("REDPANDA_TOPIC", "ticks")
OUTPUT_FILE = os.getenv("OUTPUT_FILE")


async def generate_signals():
    """Consume tick data from Redpanda, generate signals in log."""
    loop = asyncio.get_event_loop()

    # Initialize consumer inside the running event loop
    consumer = AIOKafkaConsumer(
        REDPANDA_TOPIC,
        bootstrap_servers=REDPANDA_BROKER,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        group_id="signal_group_v2",  # new group id
    )
    try:
        await consumer.start()
        logging.info(f"✅ Connected to Redpanda broker at {REDPANDA_BROKER}")

    except Exception as e:
        logging.warning(f"⚠️ Error while connecting to Redpanda {e}")


    try:
        async for msg in consumer:
            tick = json.loads(msg.value.decode())
            token = tick.get("instrument_token")
            depth_raw = tick.get("depth")
            if depth_raw is None or depth_raw == "null":
                continue

            depth = json.loads(depth_raw)
            bids = depth.get("buy", [])
            asks = depth.get("sell", [])
            if not bids or not asks:
                continue
            bid_qty = sum(b["quantity"] for b in bids)
            ask_qty = sum(a["quantity"] for a in asks)
            if bid_qty + ask_qty == 0:
                continue
            signal = generate_signal(token, bid_qty, ask_qty)
            write_signal(signal, OUTPUT_FILE)
            logging.info(f"signal {signal}")

    finally:
        logging.info("🛑 Stopping Redpanda consumer...")
        await consumer.stop()

def generate_signal(token: int, bid_qty: int, ask_qty: int) -> Dict:

        imbalance_ratio = (bid_qty - ask_qty)/ (bid_qty +ask_qty)
        signal_type = "NEUTRAL"
        confidence = 0.0

        if imbalance_ratio > 0.3:
            signal_type = "BUY"
            confidence = min(1.0, (imbalance_ratio - 0.3) / 0.7)  # scale 0.3→1.0
        elif imbalance_ratio < -0.3:
            signal_type = "SELL"
            confidence = min(1.0, (abs(imbalance_ratio) - 0.3) / 0.7)
        else:
            signal_type = "NEUTRAL"
            confidence = abs(imbalance_ratio) / 0.3  # low-confidence neutral

        # --- Compose signal record ---
        signal = {
            "timestamp":datetime.datetime.now(datetime.UTC).isoformat(timespec="microseconds"),
            "instrument_token": token,
            "signal": signal_type,
            "confidence": round(confidence, 3),
            "imbalance_ratio": round(imbalance_ratio, 4),
        }
        return signal

def write_signal(signal: dict, file_path: str):
    """
    Efficiently appends a JSON signal entry to a log file.
    Each line = one JSON object.
    """
    try:
        # Serialize once and append directly (fast I/O)
        logging.info("🛑 Writing to file")
        with open(file_path, "a", buffering=1) as f:  # line-buffered for low latency
            json.dump(signal, f, separators=(',', ':'))  # compact JSON
            f.write("\n")

    except Exception as e:
        print(f"⚠️ Error writing signal to file: {e}")



if __name__ == "__main__":
    logging.info("📡 Signal generation started")
    asyncio.run(generate_signals())






