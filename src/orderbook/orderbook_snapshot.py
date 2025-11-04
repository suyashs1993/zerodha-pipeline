import asyncio
import json
import os
import logging
from json import JSONDecodeError
from aiokafka import AIOKafkaConsumer
from src.clickhouse.clickhouse_client import get_clickhouse_client
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

# --- Validate env vars ---
if not REDPANDA_BROKER:
    logging.error("❌ REDPANDA_BROKER not set in environment!")
if not REDPANDA_TOPIC:
    logging.error("❌ REDPANDA_TOPIC not set in environment!")


async def process_snapshots():
    """Consume tick data from Redpanda, compute orderbook snapshot, and store in ClickHouse."""
    client = get_clickhouse_client()
    logging.info("✅ Connected to ClickHouse for snapshot processing")

    loop = asyncio.get_event_loop()

    # Initialize consumer inside the running event loop
    consumer = AIOKafkaConsumer(
        REDPANDA_TOPIC,
        bootstrap_servers=REDPANDA_BROKER,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        group_id="snapshot_group_v4",  # new group id
    )

    # Retry connecting to Redpanda
    for attempt in range(5):
        try:
            await consumer.start()
            logging.info(f"✅ Connected to Redpanda broker at {REDPANDA_BROKER}")
            break
        except Exception as e:
            logging.warning(f"⚠️ Redpanda not ready (attempt {attempt+1}/5): {e}")
            await asyncio.sleep(5)
    else:
        logging.error("❌ Failed to connect to Redpanda after 5 attempts — exiting.")
        return

    buffer = {}
    last_snapshot_time = datetime.now(UTC)
    try:
        async for msg in consumer:
            tick = json.loads(msg.value.decode())
            token = tick.get("instrument_token")

            if not token:
                logging.info("no token")
                continue

            depth_raw = tick.get("depth")
            if depth_raw is None or depth_raw == "null":
                continue
            try:
                depth = json.loads(depth_raw)
            except JSONDecodeError or TypeError as e:
                logging.info(f"exception {e}")
                continue

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
            num_bids = len(bids)
            num_asks = len(asks)

            weighted_mid_price = (
                (best_bid * ask_qty + best_ask * bid_qty) / (bid_qty + ask_qty)
                if (bid_qty + ask_qty) > 0 else mid_price
            )
            order_count_imbalance = (
                (num_bids - num_asks) / (num_bids + num_asks)
                if (num_bids + num_asks) > 0 else 0
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
            if datetime.now(UTC) - last_snapshot_time >= timedelta(seconds=1):
                logging.info( f" inserting {len(buffer)} snapshots in clickhouse ")
                try:
                    insert_snapshots(datetime.now(UTC), buffer)
                except Exception as e:
                    logging.error(f"❌ Failed to insert snapshots: {e}")
                buffer.clear()
                last_snapshot_time = datetime.now(UTC)

    finally:
        logging.info("🛑 Stopping Redpanda consumer...")
        await consumer.stop()


if __name__ == "__main__":
    logging.info("📡 Snapshot job started")
    asyncio.run(process_snapshots())
