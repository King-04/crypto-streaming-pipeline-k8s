"""
Crypto Pipeline Dashboard

Reads live data written by the consumer and renders:
- KPI cards per coin (latest price, 24h change, moving avg, volatility)
- Live price history charts
- Raw ticks table

Auto-refreshes on a configurable interval.
"""

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crypto")
PG_USER = os.getenv("PG_USER", "crypto")
PG_PASSWORD = os.getenv("PG_PASSWORD", "crypto")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))

st.set_page_config(
    page_title="Crypto Pipeline | Live Analytics",
    page_icon="📈",
    layout="wide",
)


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
    )


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    conn.commit()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
st.title("📈 Real-Time Crypto Market Analytics")
st.caption(
    "Live data: CoinGecko → Kafka → Python Consumer → PostgreSQL → Streamlit. "
    "Running on Kubernetes."
)

with st.sidebar:
    st.header("Settings")
    refresh = st.slider("Auto-refresh (seconds)", 5, 60, REFRESH_SECONDS)
    lookback = st.slider("History window (minutes)", 5, 240, 60)
    st.markdown("---")
    st.markdown("**Architecture**")
    st.code(
        "Producer → Kafka → Consumer → Postgres → Dashboard",
        language="text",
    )

try:
    analytics = query("SELECT * FROM price_analytics ORDER BY coin;")
except psycopg2.Error as exc:
    st.error(f"Could not query analytics table: {exc}")
    st.stop()

if analytics.empty:
    st.warning("No data yet. Waiting for the consumer to process the first ticks...")
    st.stop()

# KPI row
st.subheader("Latest Snapshot")
cols = st.columns(len(analytics))
for col, (_, row) in zip(cols, analytics.iterrows()):
    with col:
        st.metric(
            label=row["coin"].capitalize(),
            value=f"${float(row['last_price']):,.2f}",
            delta=f"{float(row['volatility']):.4f} σ",
        )
        st.caption(f"MA({int(row['window_size'])}): "
                   f"${float(row['moving_avg']):,.2f}")

# Time series
st.subheader(f"Price History (last {lookback} min)")
history = query(
    """
    SELECT coin, ts, price
    FROM price_ticks
    WHERE ts >= NOW() - (%s || ' minutes')::interval
    ORDER BY ts;
    """,
    (str(lookback),),
)

if history.empty:
    st.info("No ticks in the selected window yet.")
else:
    fig = px.line(
        history, x="ts", y="price", color="coin",
        labels={"ts": "Time", "price": "Price (USD)", "coin": "Coin"},
    )
    fig.update_layout(height=450, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

# Raw recent ticks
with st.expander("Recent raw ticks (last 50)"):
    recent = query(
        "SELECT coin, price, change_24h, volume_24h, ts "
        "FROM price_ticks ORDER BY ts DESC LIMIT 50;"
    )
    st.dataframe(recent, use_container_width=True)

st.caption(f"Last refreshed: {datetime.now(timezone.utc).isoformat()}")

# Auto-refresh
import time
time.sleep(refresh)
st.rerun()
