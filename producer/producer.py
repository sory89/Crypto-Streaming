"""
Crypto Producer
Fetches real-time prices from CoinGecko API → Kafka topic: crypto-prices
Handles 429 rate-limit with exponential backoff.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP        = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC                  = os.getenv("TOPIC", "crypto-prices")
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "30"))  # ← 30s pour éviter 429

COINS = [
    "bitcoin", "ethereum", "binancecoin", "solana", "ripple",
    "cardano", "dogecoin", "avalanche-2", "polkadot", "chainlink",
]

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids={ids}&vs_currencies=usd"
    "&include_24hr_change=true&include_24hr_vol=true"
    "&include_market_cap=true"
)


def build_producer() -> KafkaProducer:
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                linger_ms=100,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return p
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying in 3s…")
            time.sleep(3)


def fetch_prices(retry: int = 0) -> list[dict]:
    """Fetch with exponential backoff on 429."""
    url = COINGECKO_URL.format(ids=",".join(COINS))
    try:
        resp = requests.get(url, timeout=15, headers={"Accept": "application/json"})

        if resp.status_code == 429:
            wait = 60 * (2 ** retry)   # 60s, 120s, 240s …
            log.warning("Rate limited (429). Waiting %ds before retry %d…", wait, retry + 1)
            time.sleep(wait)
            return fetch_prices(retry + 1)

        resp.raise_for_status()

    except requests.exceptions.Timeout:
        log.warning("Request timed out, retrying in 15s…")
        time.sleep(15)
        return fetch_prices(retry)

    data = resp.json()
    now  = datetime.now(timezone.utc).isoformat()

    records = []
    for coin_id, info in data.items():
        records.append({
            "coin_id":        coin_id,
            "price_usd":      float(info.get("usd", 0.0)),
            "market_cap_usd": float(info.get("usd_market_cap", 0.0)),
            "volume_24h_usd": float(info.get("usd_24h_vol", 0.0)),
            "change_24h_pct": float(info.get("usd_24h_change", 0.0)),
            "fetched_at":     now,
        })
    return records


def main():
    producer = build_producer()
    log.info("Producer started — topic=%s interval=%ss", TOPIC, FETCH_INTERVAL_SECONDS)

    while True:
        try:
            records = fetch_prices()
            for rec in records:
                producer.send(TOPIC, key=rec["coin_id"], value=rec)
                log.info("→ %s  $%.2f  (%.2f%%)",
                         rec["coin_id"], rec["price_usd"], rec["change_24h_pct"])
            producer.flush()

        except Exception as e:
            log.error("Unexpected error: %s", e)

        time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
