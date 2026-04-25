# Crypto Streaming Pipeline

```
CoinGecko API → Kafka → Spark Structured Streaming → PostgreSQL → Streamlit
                                    ↓
                            Alerting (rules engine)
```

## Structure

```
crypto-streaming/
├── producer/
│   ├── producer.py         # Fetches CoinGecko prices → Kafka
│   ├── requirements.txt
│   └── Dockerfile
├── spark/
│   ├── spark_streaming.py  # PySpark Structured Streaming
│   ├── submit.sh           # spark-submit command
│   └── Dockerfile
├── streamlit/
│   ├── app.py              # Dark dashboard
│   ├── requirements.txt
│   ├── .streamlit/config.toml
│   └── Dockerfile
├── postgres/
│   └── init/
│       └── 01_schema.sql   # crypto_prices + crypto_alerts tables
└── docker-compose.yml
```

## Start

```bash
docker compose up --build
```

| Service    | URL                        |
|------------|---------------------------|
| Dashboard  | http://localhost:8501      |
| PostgreSQL | localhost:5432 db=crypto   |
| Kafka      | localhost:9094             |

## Alert Rules (in spark_streaming.py)

| Rule        | Condition                  | Description            |
|-------------|----------------------------|------------------------|
| DROP_24H    | change_24h_pct < -10       | Drop > 10% in 24h      |
| SURGE_24H   | change_24h_pct > 15        | Surge > 15% in 24h     |
| HIGH_VOLUME | volume_24h_usd > 50B       | Unusual 24h volume     |
| BTC_CRASH   | price_usd < 20000          | BTC below $20,000      |

## Dashboard Tabs

- **💰 Live Prices** — latest tick per coin + 24h change bar chart
- **📈 History** — price lines last 1h + volatility ranking
- **🚨 Alerts** — alert log + rule summary + counts chart
- **⚡ Spark Pipeline** — ingestion rate, tick count, raw data

## Add a custom alert rule

Edit `spark/spark_streaming.py` → `ALERT_RULES` dict:

```python
ALERT_RULES = {
    "MY_RULE": ("price_usd", "> 100000", "BTC above $100k!"),
    ...
}
```

Then rebuild: `docker compose up --build spark`
# Crypto-Streaming
