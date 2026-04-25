-- ── Crypto prices (raw tick data) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crypto_prices (
    id               SERIAL PRIMARY KEY,
    coin_id          TEXT        NOT NULL,
    price_usd        NUMERIC(20, 8),
    market_cap_usd   NUMERIC(24, 2),
    volume_24h_usd   NUMERIC(24, 2),
    change_24h_pct   NUMERIC(10, 4),
    price_formatted  TEXT,
    change_direction TEXT,
    fetched_at       TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prices_coin    ON crypto_prices(coin_id);
CREATE INDEX IF NOT EXISTS idx_prices_fetched ON crypto_prices(fetched_at DESC);

-- ── Crypto alerts ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crypto_alerts (
    id            SERIAL PRIMARY KEY,
    coin_id       TEXT        NOT NULL,
    price_usd     NUMERIC(20, 8),
    change_24h_pct NUMERIC(10, 4),
    volume_24h_usd NUMERIC(24, 2),
    alert_type    TEXT        NOT NULL,
    alert_message TEXT        NOT NULL,
    alert_value   NUMERIC(24, 4),
    alerted_at    TIMESTAMPTZ DEFAULT now(),
    acknowledged  BOOLEAN     DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_alerts_coin      ON crypto_alerts(coin_id);
CREATE INDEX IF NOT EXISTS idx_alerts_type      ON crypto_alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_alerted   ON crypto_alerts(alerted_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_ack       ON crypto_alerts(acknowledged);
