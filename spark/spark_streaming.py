"""
Spark Structured Streaming — Crypto Pipeline
Kafka → parse → enrich → alerts → PostgreSQL

Tables written:
  - crypto_prices   : every incoming tick
  - crypto_alerts   : rows that triggered an alert rule
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [spark] %(message)s")
log = logging.getLogger("spark-streaming")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "kafka:9092")
TOPIC           = os.getenv("TOPIC",            "crypto-prices")
PG_URL          = os.getenv("PG_URL",           "jdbc:postgresql://postgres:5432/crypto")
PG_USER         = os.getenv("PG_USER",          "crypto")
PG_PASSWORD     = os.getenv("PG_PASSWORD",      "crypto")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR",   "/tmp/spark-checkpoints")

PG_PROPS = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── Alert thresholds ──────────────────────────────────────────────────────────
ALERT_RULES = {
    "DROP_24H":    ("change_24h_pct",  "< -10",  "Drop > 10% in 24h"),
    "SURGE_24H":   ("change_24h_pct",  "> 15",   "Surge > 15% in 24h"),
    "HIGH_VOLUME": ("volume_24h_usd",  "> 5e10", "Unusual 24h volume (> $50B)"),
    "BTC_CRASH":   ("price_usd",       "< 20000","BTC below $20,000"),
}

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA = StructType([
    StructField("coin_id",        StringType(),    True),
    StructField("price_usd",      DoubleType(),    True),
    StructField("market_cap_usd", DoubleType(),    True),
    StructField("volume_24h_usd", DoubleType(),    True),
    StructField("change_24h_pct", DoubleType(),    True),
    StructField("fetched_at",     StringType(),    True),
])


def write_to_postgres(batch_df, batch_id, table: str):
    """Write a micro-batch to PostgreSQL using JDBC (foreachBatch sink)."""
    if batch_df.isEmpty():
        return
    log.info("Batch %d → table=%s  rows=%d", batch_id, table, batch_df.count())
    (
        batch_df.write
        .jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)
    )


def apply_alert_rules(df):
    """
    Apply all alert rules and return a DataFrame with alert metadata columns.
    Each rule that matches creates a row with alert_type and alert_message.
    """
    alert_frames = []

    for alert_type, (col_name, condition, message) in ALERT_RULES.items():
        rule_df = (
            df.filter(f"{col_name} {condition}")
              .withColumn("alert_type",    F.lit(alert_type))
              .withColumn("alert_message", F.lit(message))
              .withColumn("alert_value",   F.col(col_name).cast(DoubleType()))
              .withColumn("alerted_at",    F.current_timestamp())
        )
        alert_frames.append(rule_df)

    if not alert_frames:
        return None

    from functools import reduce
    return reduce(lambda a, b: a.union(b), alert_frames)


def main():
    spark = (
        SparkSession.builder
        .appName("CryptoStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created")

    # ── Read from Kafka ───────────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON payload ────────────────────────────────────────────────────
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("fetched_at", F.to_timestamp("fetched_at"))
        .withColumn("ingested_at", F.current_timestamp())
        .filter(F.col("coin_id").isNotNull())
    )

    # ── Enrich: add moving average placeholder & USD formatted columns ───────
    enriched = (
        parsed
        .withColumn("price_formatted", F.format_number("price_usd", 2))
        .withColumn("change_direction",
                    F.when(F.col("change_24h_pct") >= 0, "UP").otherwise("DOWN"))
    )

    # ── Sink 1: write all prices to PostgreSQL ────────────────────────────────
    prices_query = (
        enriched.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "crypto_prices"))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/prices")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Sink 2: alerts ────────────────────────────────────────────────────────
    def process_alerts(batch_df, batch_id):
        alerts_df = apply_alert_rules(batch_df)
        if alerts_df is not None and not alerts_df.isEmpty():
            alert_cols = [
                "coin_id", "price_usd", "change_24h_pct", "volume_24h_usd",
                "alert_type", "alert_message", "alert_value", "alerted_at",
            ]
            write_to_postgres(alerts_df.select(alert_cols), batch_id, "crypto_alerts")

    alerts_query = (
        enriched.writeStream
        .foreachBatch(process_alerts)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/alerts")
        .trigger(processingTime="10 seconds")
        .start()
    )

    log.info("Streaming queries started — waiting for termination")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
