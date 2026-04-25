"""
Crypto Streaming Dashboard — Streamlit
Reads from PostgreSQL written by Spark Structured Streaming
"""

import os
import time
from decimal import Decimal

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Dark theme ────────────────────────────────────────────────────────────────
DARK_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Inter:wght@300;400;500;600;700&display=swap');
:root { --bg:#0d1117; --bg2:#161b22; --border:#21262d; --green:#23c45e; --red:#f85149; --yellow:#e3b341; --blue:#388bfd; --text:#e6edf3; --muted:#8b949e; }
html,body{background-color:var(--bg)!important;color:var(--text)!important;}
.stApp,[data-testid="stAppViewContainer"],[data-testid="stAppViewBlockContainer"],[data-testid="stVerticalBlock"],.main,.main>div{background-color:var(--bg)!important;color:var(--text)!important;}
.main .block-container{background-color:var(--bg)!important;padding:1.5rem 2rem!important;max-width:100%!important;}
.main p,.main span,.main div,.main label{color:var(--text)!important;}
#MainMenu,footer,header,.stDeployButton{visibility:hidden!important;display:none!important;}
section[data-testid="stSidebar"],section[data-testid="stSidebar"]>div{background-color:var(--bg)!important;border-right:1px solid var(--border)!important;}
section[data-testid="stSidebar"] *{color:var(--text)!important;}
h1{font-size:1.9rem!important;font-weight:700!important;color:var(--text)!important;letter-spacing:-.04em!important;}
h2,h3{color:var(--text)!important;font-weight:600!important;}
h2{font-size:1.05rem!important;}
[data-testid="metric-container"]{background:var(--bg2)!important;border:1px solid var(--border)!important;border-radius:8px!important;padding:1rem 1.2rem!important;}
[data-testid="stMetricLabel"]{font-size:.72rem!important;color:var(--muted)!important;text-transform:uppercase;letter-spacing:.06em;}
[data-testid="stMetricValue"]{font-size:1.5rem!important;font-weight:700!important;font-family:'JetBrains Mono',monospace!important;color:var(--text)!important;}
.stTabs [data-baseweb="tab-list"]{background:transparent!important;border-bottom:1px solid var(--border)!important;gap:0!important;}
.stTabs [data-baseweb="tab"]{background:transparent!important;color:var(--muted)!important;border:none!important;border-bottom:2px solid transparent!important;padding:.55rem 1rem!important;font-size:.84rem!important;font-weight:500!important;}
.stTabs [aria-selected="true"]{color:var(--green)!important;border-bottom-color:var(--green)!important;background:transparent!important;}
.stTabs [data-baseweb="tab-panel"]{padding-top:1.2rem!important;}
[data-testid="stDataFrame"]{border:1px solid var(--border)!important;border-radius:8px!important;overflow:hidden;}
.stButton>button{background:transparent!important;border:1px solid var(--border)!important;color:var(--text)!important;border-radius:6px!important;}
.stButton>button:hover{border-color:var(--green)!important;background:rgba(35,196,94,.08)!important;}
hr{border-color:var(--border)!important;margin:1rem 0!important;}
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-track{background:var(--bg);}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
</style>
"""

PLOTLY = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#161b22",
    font_color="#e6edf3",
    font_family="Inter",
    margin=dict(l=0, r=0, t=36, b=0),
    title_font_size=13,
)

GREEN  = "#23c45e"
RED    = "#f85149"
YELLOW = "#e3b341"
BLUE   = "#388bfd"

st.set_page_config(page_title="Crypto Stream", page_icon="₿",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(DARK_CSS, unsafe_allow_html=True)

# ── DB ────────────────────────────────────────────────────────────────────────
DB_DSN = os.getenv("DB_DSN", "host=postgres port=5432 dbname=crypto user=crypto password=crypto")


def get_conn():
    return psycopg2.connect(DB_DSN)


def _cast_floats(df: pd.DataFrame) -> pd.DataFrame:
    """Convert any decimal.Decimal columns returned by psycopg2 to float."""
    for col in df.columns:
        if not df.empty and isinstance(df[col].dropna().iloc[0] if not df[col].dropna().empty else None, Decimal):
            df[col] = df[col].apply(lambda x: float(x) if x is not None else None)
    return df


@st.cache_data(ttl=10, show_spinner=False)
def run_query(sql: str, params=None) -> pd.DataFrame:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            df = pd.DataFrame(cur.fetchall())
    return _cast_floats(df) if not df.empty else df


def test_db() -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception:
        return False


# ── SQL ───────────────────────────────────────────────────────────────────────
SQL_LATEST = """
    SELECT DISTINCT ON (coin_id)
        coin_id, price_usd, change_24h_pct, market_cap_usd,
        volume_24h_usd, change_direction, fetched_at
    FROM crypto_prices ORDER BY coin_id, fetched_at DESC
"""
SQL_ALL_HISTORY = """
    SELECT coin_id, price_usd, change_24h_pct, fetched_at
    FROM crypto_prices
    WHERE fetched_at >= now() - INTERVAL '1 hour'
    ORDER BY fetched_at ASC
"""
SQL_ALERTS = """
    SELECT coin_id, alert_type, alert_message, price_usd,
           change_24h_pct, alert_value, alerted_at, acknowledged
    FROM crypto_alerts ORDER BY alerted_at DESC LIMIT 100
"""
SQL_ALERT_COUNTS = """
    SELECT alert_type, COUNT(*) as n FROM crypto_alerts
    WHERE alerted_at >= now() - INTERVAL '24 hours'
    GROUP BY alert_type ORDER BY n DESC
"""
SQL_TOP_VOLATILE = """
    SELECT coin_id,
           MAX(price_usd) as price_max, MIN(price_usd) as price_min,
           MAX(price_usd) - MIN(price_usd) as price_range,
           AVG(price_usd) as price_avg, COUNT(*) as ticks
    FROM crypto_prices
    WHERE fetched_at >= now() - INTERVAL '1 hour'
    GROUP BY coin_id
    ORDER BY (MAX(price_usd) - MIN(price_usd)) / NULLIF(AVG(price_usd), 0) DESC
"""

# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.markdown('<div style="font-family:JetBrains Mono,monospace;font-size:1rem;font-weight:600;padding:0 0 .8rem">₿ Crypto <span style="color:#23c45e">Stream</span></div>',
                    unsafe_allow_html=True)
        connected = test_db()
        color = "#23c45e" if connected else "#f85149"
        bg    = "#0F6E56" if connected else "#791F1F"
        label = "● Connected" if connected else "✕ DB unreachable"
        st.markdown(f'<div style="background:{bg};color:{color};font-weight:600;font-size:.8rem;border-radius:6px;padding:.4rem 1rem;margin:.5rem 0;text-align:center">{label}</div>',
                    unsafe_allow_html=True)
        st.markdown('<div style="font-family:JetBrains Mono,monospace;font-size:.7rem;color:#23c45e;background:rgba(35,196,94,.08);border:1px solid rgba(35,196,94,.2);border-radius:4px;padding:.25rem .5rem;margin-top:.25rem">postgres:5432/crypto</div>',
                    unsafe_allow_html=True)
        st.divider()
        if st.button("↺  Refresh now", use_container_width=True):
            st.cache_data.clear(); st.rerun()
        auto = st.checkbox("Auto-refresh every 30s")
        if auto:
            time.sleep(30)
            st.cache_data.clear()
            st.rerun()
        st.divider()
        st.markdown('<p style="font-size:.72rem;color:#6e7681">Pipeline</p>', unsafe_allow_html=True)
        st.markdown("""
        <div style="font-size:.7rem;color:#8b949e;line-height:2">
        CoinGecko API<br>↓ <span style="color:#23c45e">Kafka</span> crypto-prices<br>
        ↓ <span style="color:#388bfd">Spark</span> Structured Streaming<br>
        ↓ <span style="color:#e3b341">PostgreSQL</span><br>
        ↓ <span style="color:#f85149">Alerts</span> engine<br>
        ↓ <span style="color:#23c45e">Streamlit</span> dashboard
        </div>""", unsafe_allow_html=True)
    return connected

# ── Overview ──────────────────────────────────────────────────────────────────
def render_overview():
    df = run_query(SQL_LATEST)
    if df.empty:
        st.info("Waiting for data from Spark… make sure all services are running.")
        return df

    def get_metric(coin):
        row = df[df["coin_id"] == coin]
        if row.empty: return "—", "—"
        price = f"${float(row.iloc[0]['price_usd']):,.2f}"
        delta = f"{float(row.iloc[0]['change_24h_pct']):+.2f}%"
        return price, delta

    cols = st.columns(5)
    for i, coin in enumerate(["bitcoin", "ethereum", "solana"]):
        p, d = get_metric(coin)
        cols[i].metric(coin.capitalize(), p, d)

    alerts_n = run_query("SELECT COUNT(*) as n FROM crypto_alerts WHERE acknowledged=false AND alerted_at >= now()-interval '1 hour'")
    ticks_n  = run_query("SELECT COUNT(*) as n FROM crypto_prices WHERE ingested_at >= now()-interval '1 hour'")
    cols[3].metric("🚨 Active Alerts", int(alerts_n.iloc[0]["n"]) if not alerts_n.empty else 0)
    cols[4].metric("📊 Ticks (1h)",    int(ticks_n.iloc[0]["n"])  if not ticks_n.empty  else 0)
    return df

# ── Tab: Live Prices ──────────────────────────────────────────────────────────
def tab_prices(latest_df):
    st.markdown("#### Live Prices")
    if latest_df.empty:
        st.info("No data yet."); return

    display = latest_df.copy()
    # Safe cast — PostgreSQL NUMERIC arrives as Decimal
    for col in ["price_usd", "market_cap_usd", "volume_24h_usd", "change_24h_pct"]:
        display[col] = pd.to_numeric(display[col], errors="coerce").fillna(0.0)

    display["price_usd"]      = display["price_usd"].apply(lambda x: f"${x:,.4f}")
    display["market_cap_usd"] = display["market_cap_usd"].apply(lambda x: f"${x/1e9:,.2f}B")
    display["volume_24h_usd"] = display["volume_24h_usd"].apply(lambda x: f"${x/1e9:,.2f}B")
    display["change_24h_pct"] = display["change_24h_pct"].apply(lambda x: f"{x:+.2f}%")

    st.dataframe(
        display[["coin_id","price_usd","change_24h_pct","market_cap_usd","volume_24h_usd","change_direction","fetched_at"]],
        use_container_width=True, hide_index=True,
        column_config={
            "coin_id": "Coin", "price_usd": "Price (USD)", "change_24h_pct": "24h Change",
            "market_cap_usd": "Market Cap", "volume_24h_usd": "Volume 24h",
            "change_direction": "Direction", "fetched_at": "Last seen",
        }
    )

    st.divider()
    st.markdown("#### 24h Change by Coin")
    raw = run_query(SQL_LATEST)
    if not raw.empty:
        raw["change_24h_pct"] = pd.to_numeric(raw["change_24h_pct"], errors="coerce").fillna(0.0)
        raw["color"] = raw["change_24h_pct"].apply(lambda x: GREEN if x >= 0 else RED)
        fig = go.Figure(go.Bar(
            x=raw["coin_id"], y=raw["change_24h_pct"],
            marker_color=raw["color"].tolist(), marker_line_width=0,
        ))
        fig.update_layout(**PLOTLY, title="24h price change (%)", height=300)
        fig.add_hline(y=0, line_color="#21262d", line_width=1)
        st.plotly_chart(fig, use_container_width=True)

# ── Tab: History ──────────────────────────────────────────────────────────────
def tab_history():
    st.markdown("#### Price History (last 1 hour)")
    df = run_query(SQL_ALL_HISTORY)
    if df.empty:
        st.info("No history yet."); return

    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
    coins    = sorted(df["coin_id"].unique())
    selected = st.multiselect("Select coins", coins, default=coins[:4])
    filtered = df[df["coin_id"].isin(selected)]
    if filtered.empty:
        st.warning("No data for selected coins."); return

    fig = px.line(filtered, x="fetched_at", y="price_usd", color="coin_id",
                  title="Price over time (USD)",
                  color_discrete_sequence=[GREEN, BLUE, YELLOW, RED, "#a78bfa", "#fb923c"])
    fig.update_layout(**PLOTLY, height=380)
    fig.update_traces(line_width=1.5)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### Volatility (last 1h)")
    vol_df = run_query(SQL_TOP_VOLATILE)
    if not vol_df.empty:
        for col in ["price_range", "price_avg"]:
            vol_df[col] = pd.to_numeric(vol_df[col], errors="coerce")
        vol_df["volatility_pct"] = (vol_df["price_range"] / vol_df["price_avg"] * 100).round(4)
        fig2 = px.bar(vol_df, x="coin_id", y="volatility_pct",
                      color="volatility_pct", color_continuous_scale="Reds",
                      title="Volatility % (price range / avg)")
        fig2.update_layout(**PLOTLY, coloraxis_showscale=False, height=280)
        fig2.update_traces(marker_line_width=0)
        st.plotly_chart(fig2, use_container_width=True)

# ── Tab: Alerts ───────────────────────────────────────────────────────────────
def tab_alerts():
    st.markdown("#### 🚨 Alert Rules")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        <div style="background:#161b22;border:1px solid #21262d;border-radius:8px;padding:14px 18px;font-size:13px">
        <div style="color:#8b949e;font-size:11px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:10px">Active Rules</div>
        <div style="margin-bottom:6px">🔴 <b>DROP_24H</b> — change_24h_pct &lt; -10%</div>
        <div style="margin-bottom:6px">🟢 <b>SURGE_24H</b> — change_24h_pct &gt; 15%</div>
        <div style="margin-bottom:6px">🟡 <b>HIGH_VOLUME</b> — volume_24h &gt; $50B</div>
        <div>🔵 <b>BTC_CRASH</b> — BTC price &lt; $20,000</div>
        </div>""", unsafe_allow_html=True)

    with col2:
        counts = run_query(SQL_ALERT_COUNTS)
        if not counts.empty:
            color_map = {"DROP_24H": RED, "SURGE_24H": GREEN, "HIGH_VOLUME": YELLOW, "BTC_CRASH": BLUE}
            colors = [color_map.get(t, "#8b949e") for t in counts["alert_type"]]
            fig = go.Figure(go.Bar(x=counts["alert_type"], y=counts["n"],
                                   marker_color=colors, marker_line_width=0))
            fig.update_layout(**PLOTLY, title="Alerts (last 24h)", height=220)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### Alert log")
    alerts = run_query(SQL_ALERTS)
    if alerts.empty:
        st.success("✅ No alerts triggered yet."); return

    unacked = alerts[alerts["acknowledged"] == False]  # noqa
    if not unacked.empty:
        st.warning(f"⚠️ **{len(unacked)}** unacknowledged alert(s)")

    for col in ["price_usd", "change_24h_pct", "alert_value"]:
        if col in alerts.columns:
            alerts[col] = pd.to_numeric(alerts[col], errors="coerce")

    st.dataframe(alerts, use_container_width=True, hide_index=True,
                 column_config={
                     "acknowledged":   st.column_config.CheckboxColumn("Acked"),
                     "price_usd":      st.column_config.NumberColumn("Price", format="$%.4f"),
                     "change_24h_pct": st.column_config.NumberColumn("24h %", format="%.2f%%"),
                 })

# ── Tab: Spark Pipeline ───────────────────────────────────────────────────────
def tab_spark():
    st.markdown("#### Pipeline status")
    col1, col2, col3 = st.columns(3)
    total = run_query("SELECT COUNT(*) as n FROM crypto_prices")
    last  = run_query("SELECT MAX(ingested_at) as last FROM crypto_prices")
    coins = run_query("SELECT COUNT(DISTINCT coin_id) as n FROM crypto_prices")
    col1.metric("Total ticks stored", int(total.iloc[0]["n"]) if not total.empty else 0)
    col2.metric("Last ingestion",     str(last.iloc[0]["last"])[:19] if not last.empty else "—")
    col3.metric("Coins tracked",      int(coins.iloc[0]["n"])  if not coins.empty  else 0)

    st.divider()
    st.markdown("#### Ingestion rate (ticks/minute)")
    rate_df = run_query("""
        SELECT date_trunc('minute', ingested_at) as minute, COUNT(*) as ticks
        FROM crypto_prices
        WHERE ingested_at >= now() - interval '30 minutes'
        GROUP BY 1 ORDER BY 1
    """)
    if not rate_df.empty:
        fig = px.area(rate_df, x="minute", y="ticks", title="Ticks per minute",
                      color_discrete_sequence=[GREEN])
        fig.update_layout(**PLOTLY, height=280)
        fig.update_traces(line_color=GREEN, fillcolor="rgba(35,196,94,0.1)")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### Last 20 raw ticks")
    raw = run_query("SELECT * FROM crypto_prices ORDER BY ingested_at DESC LIMIT 20")
    st.dataframe(raw, use_container_width=True, hide_index=True)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    connected = render_sidebar()
    st.title("₿ Crypto Streaming Dashboard")
    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    if not connected:
        st.error("Cannot reach PostgreSQL. Make sure all services are running.")
        st.stop()

    latest_df = render_overview()
    st.markdown("<div style='height:.8rem'></div>", unsafe_allow_html=True)

    t1, t2, t3, t4 = st.tabs([
        "💰 Live Prices", "📈 History", "🚨 Alerts", "⚡ Spark Pipeline"
    ])
    with t1: tab_prices(latest_df)
    with t2: tab_history()
    with t3: tab_alerts()
    with t4: tab_spark()


if __name__ == "__main__":
    main()
