import asyncio
import json
import os
import logging
from aiokafka import AIOKafkaConsumer
from datetime import datetime, timedelta, UTC
from src.clickhouse.clickhouse_loader import insert_snapshots

# --- Logging Setup (with ms precision) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
REDPANDA_TOPIC = os.getenv("REDPANDA_TOPIC", "ticks")
SNAPSHOT_INTERVAL = int(os.getenv("SNAPSHOT_INTERVAL", 1))

async def process_snapshots():

    logging.info("✅ Connected to ClickHouse for snapshot processing")

    # Initialize consumer inside the running event loop
    consumer = AIOKafkaConsumer(
        REDPANDA_TOPIC,
        bootstrap_servers=REDPANDA_BROKER,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        group_id="snapshot_group_v4",  # new group id
    )
    try:
        await consumer.start()
        logging.info(f"✅ Connected to Redpanda broker at {REDPANDA_BROKER}")

    except Exception as e:
        logging.warning(f"⚠️ Redpanda not ready: {e}")

    buffer = {}
    last_snapshot_time = datetime.now(UTC)
    try:
        async for msg in consumer:
            try:
                tick = json.loads(msg.value)
                token = tick.get("instrument_token")

                if not token:
                    logging.info("no token")
                    continue

                depth_raw = tick.get("depth")
                if depth_raw is None or depth_raw == "null":
                    continue

                depth = json.loads(depth_raw)

                bids = depth.get("buy", [])
                asks = depth.get("sell", [])
                if not bids or not asks:
                    continue

                # --- Compute Order Book Metrics ---
                best_bid = bids[0]["price"]
                best_ask = asks[0]["price"]
                bid_ask_spread = best_ask - best_bid
                mid_price = (best_bid + best_ask) / 2

                bid_qty = sum(b["quantity"] for b in bids)
                ask_qty = sum(a["quantity"] for a in asks)
                bid_orders =sum(b["orders"] for b in bids)
                ask_orders =sum(a["orders"] for a in asks)

                weighted_mid_price = (
                    (best_bid * ask_qty + best_ask * bid_qty) / (bid_qty + ask_qty)
                    if (bid_qty + ask_qty) > 0 else mid_price
                )
                order_count_imbalance = (
                    (bid_orders - ask_orders) / (bid_orders+ ask_orders)
                    if (bid_orders+ ask_orders) > 0 else 0
                )
                order_book_imbalance_ratio = (
                    (bid_qty - ask_qty) / (bid_qty + ask_qty)
                    if (bid_qty + ask_qty) > 0 else 0
                )

                # --- Store latest snapshot ---
                buffer[token] = {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "imbalance_ratio": order_book_imbalance_ratio,
                    "bid_ask_spread": bid_ask_spread,
                    "mid_price": mid_price,
                    "weighted_mid_price": weighted_mid_price,
                    "order_count_imbalance": order_count_imbalance,
                    "depth_json": depth,
                }

                # --- Flush every 1 second ---
                now = datetime.now(UTC)
                if now - last_snapshot_time >= timedelta(seconds= SNAPSHOT_INTERVAL):
                    snapshot_time = now
                    batch = buffer.copy()
                    last_snapshot_time = snapshot_time

                    logging.info(f"🚀 Flushing {len(batch)} snapshots..")
                    insert_snapshots(now, batch)
                    buffer.clear()

            except Exception as e:
                logging.error(f"🛑 Error while processing orderbook {e}...")
    finally:
        logging.info("🛑 Stopping Redpanda consumer...")
        await consumer.stop()


if __name__ == "__main__":
    logging.info("📡 Snapshot job started")
    asyncio.run(process_snapshots())
