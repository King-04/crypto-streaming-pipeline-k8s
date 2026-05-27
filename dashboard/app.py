"""
Crypto Pipeline Dashboard

Modern dark-themed real-time dashboard:
- Brand-coloured KPI cards with 24h change and rolling volatility
- Performance comparison (normalised % change OR log-scale absolute)
- Top movers (sorted 24h change with red/green coding)
- Volume + volatility side-by-side rankings
- Per-coin deep dive with moving average overlay and volume sub-chart
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import streamlit as st

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crypto")
PG_USER = os.getenv("PG_USER", "crypto")
PG_PASSWORD = os.getenv("PG_PASSWORD", "crypto")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))

# Brand colours (official-ish hex for each coin)
COIN_COLORS = {
    "bitcoin":  "#F7931A",
    "ethereum": "#627EEA",
    "solana":   "#14F195",
    "cardano":  "#3CC8C8",
    "ripple":   "#00AAE4",
}
COIN_SYMBOLS = {
    "bitcoin":  "BTC",
    "ethereum": "ETH",
    "solana":   "SOL",
    "cardano":  "ADA",
    "ripple":   "XRP",
}
GRID = "#2d3548"
MUTED = "#6b7280"
UP = "#14f195"
DOWN = "#f43f5e"


def color_for(coin: str) -> str:
    return COIN_COLORS.get(coin.lower(), "#9CA3AF")


def symbol_for(coin: str) -> str:
    return COIN_SYMBOLS.get(coin.lower(), coin.upper()[:3])


def format_price(p) -> str:
    p = float(p)
    if p >= 1000:
        return f"${p:,.2f}"
    if p >= 1:
        return f"${p:,.4f}"
    return f"${p:.6f}"


def hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str:
    """Convert '#RRGGBB' to 'rgba(r, g, b, a)' — Plotly rejects 8-digit hex."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha})"


def format_volume(v) -> str:
    v = float(v)
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    if v >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:.0f}"


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Crypto Pipeline | Live Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS — KPI cards, header gradient, tab styling
st.markdown("""
<style>
    .block-container { padding-top: 5rem; padding-bottom: 2rem; }

    .metric-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #232838 100%);
        border-radius: 14px;
        padding: 18px 20px;
        border: 1px solid #2d3548;
        height: 100%;
        transition: transform 0.15s ease, border-color 0.15s ease;
    }
    .metric-card:hover { border-color: #4a5568; transform: translateY(-2px); }
    .metric-symbol { font-size: 13px; font-weight: 700; letter-spacing: 1.2px; }
    .metric-name {
        font-size: 11px; color: #9ca3af;
        text-transform: uppercase; letter-spacing: 0.6px; margin-top: 2px;
    }
    .metric-price { font-size: 26px; font-weight: 700; margin: 10px 0 2px 0; }
    .metric-up { color: #14f195; font-weight: 600; }
    .metric-down { color: #f43f5e; font-weight: 600; }
    .metric-muted { color: #6b7280; font-weight: 400; font-size: 12px; }
    .metric-detail { font-size: 11px; color: #6b7280; margin-top: 6px; }

    .header-title {
        background: linear-gradient(90deg, #F7931A 0%, #627EEA 50%, #14F195 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-size: 38px; font-weight: 800; margin: 0; line-height: 1.1;
    }
    .header-sub { color: #9ca3af; font-size: 13px; margin: 4px 0 18px 0; }

    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 8px;
        padding: 8px 16px;
    }
    .stTabs [aria-selected="true"] {
        background: #1a1f2e !important;
        border: 1px solid #2d3548;
    }

    #MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
    )
    conn.autocommit = True
    return conn


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def tables_ready() -> bool:
    df = query("""
        SELECT COUNT(*) AS n FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ('price_ticks', 'price_analytics');
    """)
    return int(df.iloc[0]["n"]) == 2


# ---------------------------------------------------------------------------
# Plotly helpers
# ---------------------------------------------------------------------------
def style_fig(fig, height: int = 420, show_legend: bool = True) -> go.Figure:
    """Apply the project-wide dark-transparent theme to a Plotly figure."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=height,
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        showlegend=show_legend,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1, bgcolor="rgba(0,0,0,0)",
        ),
        font=dict(family="sans-serif", size=12),
    )
    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID)
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=GRID)
    return fig


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown('<div class="header-title">📈 Crypto Pipeline</div>',
            unsafe_allow_html=True)
st.markdown(
    '<div class="header-sub">Real-time market analytics · '
    'CoinGecko → Kafka → PostgreSQL → Streamlit · running on Kubernetes</div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### ⚙️ Controls")
    refresh = st.slider("Auto-refresh (sec)", 5, 60, REFRESH_SECONDS)
    lookback = st.slider("History window (min)", 5, 240, 60)
    st.markdown("---")
    st.markdown("### 🏗️ Architecture")
    st.code(
        "CoinGecko API\n"
        "      ↓\n"
        "  Producer (Deployment)\n"
        "      ↓\n"
        "  Kafka (StatefulSet)\n"
        "      ↓\n"
        "  Consumer (Deployment)\n"
        "      ↓\n"
        "  Postgres (StatefulSet)\n"
        "      ↓\n"
        "  Dashboard (Deployment)",
        language="text",
    )


# ---------------------------------------------------------------------------
# Bootstrap state
# ---------------------------------------------------------------------------
try:
    if not tables_ready():
        st.info("⏳ Waiting for the consumer to initialise the database schema "
                "and process the first ticks (usually ~30 seconds).")
        time.sleep(refresh)
        st.rerun()
except psycopg2.Error as exc:
    st.error(f"Could not reach Postgres: {exc}")
    st.stop()

analytics = query("SELECT * FROM price_analytics ORDER BY coin;")
if analytics.empty:
    st.warning("Schema ready, no ticks processed yet. Hold on...")
    time.sleep(refresh)
    st.rerun()

# Latest tick per coin (for 24h change and volume)
latest = query("""
    SELECT DISTINCT ON (coin) coin, price, change_24h, volume_24h, ts
    FROM price_ticks
    ORDER BY coin, ts DESC;
""")

# Combined snapshot for KPI cards
snapshot = analytics.merge(latest, on="coin", how="left", suffixes=("", "_t"))

# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------
st.markdown("#### Market Snapshot")
cols = st.columns(len(snapshot))
for col, (_, row) in zip(cols, snapshot.iterrows()):
    coin = row["coin"]
    color = color_for(coin)
    price = float(row["last_price"])
    change = float(row["change_24h"]) if pd.notna(row["change_24h"]) else 0.0
    mavg = float(row["moving_avg"]) if pd.notna(row["moving_avg"]) else 0.0
    vol = float(row["volatility"]) if pd.notna(row["volatility"]) else 0.0
    window_n = int(row["window_size"]) if pd.notna(row["window_size"]) else 0

    klass = "metric-up" if change >= 0 else "metric-down"
    arrow = "▲" if change >= 0 else "▼"

    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <span class="metric-symbol" style="color:{color};">{symbol_for(coin)}</span>
                <span style="width:10px;height:10px;background:{color};
                             border-radius:50%;display:inline-block;
                             box-shadow:0 0 10px {color};"></span>
            </div>
            <div class="metric-name">{coin.capitalize()}</div>
            <div class="metric-price">{format_price(price)}</div>
            <div>
                <span class="{klass}">{arrow} {abs(change):.2f}%</span>
                <span class="metric-muted">  24h</span>
            </div>
            <div class="metric-detail">
                MA({window_n}): {format_price(mavg)} · σ {vol:.4f}
            </div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("&nbsp;")  # spacer

# ---------------------------------------------------------------------------
# History data (one query, reused across tabs)
# ---------------------------------------------------------------------------
history = query("""
    SELECT coin, ts, price, volume_24h
    FROM price_ticks
    WHERE ts >= NOW() - (%s || ' minutes')::interval
    ORDER BY ts;
""", (str(lookback),))
if not history.empty:
    history["price"] = history["price"].astype(float)
    history["volume_24h"] = history["volume_24h"].astype(float)

# ---------------------------------------------------------------------------
# Tabbed analytical views
# ---------------------------------------------------------------------------
tab_compare, tab_movers, tab_vv, tab_deep = st.tabs([
    "📊 Performance Comparison",
    "🏆 Top Movers (24h)",
    "💰 Volume & Volatility",
    "🔍 Coin Deep Dive",
])

# ----- Tab 1: Comparison -----
with tab_compare:
    if history.empty:
        st.info("No ticks in the selected window yet.")
    else:
        mode = st.radio(
            "View",
            ["Normalised (% change)", "Absolute (log scale)"],
            horizontal=True, index=0,
            help=("Normalised: every coin starts at 0% — directly compares "
                  "performance. Log scale: real prices on a logarithmic axis "
                  "so a $70k coin and a $0.50 coin both fit on one chart."),
        )

        if mode.startswith("Normalised"):
            df = history.sort_values(["coin", "ts"]).copy()
            df["base"] = df.groupby("coin")["price"].transform("first")
            df["pct"] = (df["price"] / df["base"] - 1) * 100

            fig = px.line(
                df, x="ts", y="pct", color="coin",
                color_discrete_map=COIN_COLORS,
                labels={"ts": "", "pct": "% change from start", "coin": ""},
            )
            fig.add_hline(y=0, line_dash="dash", line_color=MUTED, opacity=0.5)
            style_fig(fig, height=460)
            fig.update_yaxes(ticksuffix="%")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                "All coins indexed to 0% at the start of the window — "
                "directly comparable performance regardless of absolute price."
            )
        else:
            fig = px.line(
                history, x="ts", y="price", color="coin",
                color_discrete_map=COIN_COLORS,
                log_y=True,
                labels={"ts": "", "price": "Price (USD, log)", "coin": ""},
            )
            style_fig(fig, height=460)
            fig.update_yaxes(tickprefix="$")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Logarithmic Y-axis — every order of magnitude is "
                       "equal vertical space, so a $0.50 coin and a $70k "
                       "coin are both readable.")

# ----- Tab 2: Movers -----
with tab_movers:
    movers = latest.copy()
    movers["change_24h"] = movers["change_24h"].astype(float)
    movers = movers.sort_values("change_24h")
    bar_colors = [UP if c >= 0 else DOWN for c in movers["change_24h"]]

    fig = go.Figure(go.Bar(
        x=movers["change_24h"],
        y=[symbol_for(c) for c in movers["coin"]],
        orientation="h",
        marker=dict(color=bar_colors, line=dict(width=0)),
        text=[f"{x:+.2f}%" for x in movers["change_24h"]],
        textposition="outside",
        textfont=dict(size=13, color="#FAFAFA"),
        hovertemplate="<b>%{y}</b>: %{x:+.2f}%<extra></extra>",
    ))
    fig.add_vline(x=0, line_color=MUTED, line_width=1)
    fig.update_layout(title="24-Hour Price Change")
    style_fig(fig, height=400, show_legend=False)
    fig.update_xaxes(ticksuffix="%")
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Green = gainers, red = decliners over the last 24 hours.")

# ----- Tab 3: Volume + Volatility -----
with tab_vv:
    c1, c2 = st.columns(2)

    with c1:
        vdf = latest.copy()
        vdf["volume_24h"] = vdf["volume_24h"].astype(float)
        vdf = vdf.sort_values("volume_24h")

        fig = go.Figure(go.Bar(
            x=vdf["volume_24h"],
            y=[symbol_for(c) for c in vdf["coin"]],
            orientation="h",
            marker=dict(color=[color_for(c) for c in vdf["coin"]],
                        line=dict(width=0)),
            text=[format_volume(v) for v in vdf["volume_24h"]],
            textposition="outside",
            textfont=dict(size=12, color="#FAFAFA"),
            hovertemplate="<b>%{y}</b>: $%{x:,.0f}<extra></extra>",
        ))
        fig.update_layout(title="24h Trading Volume (log scale)")
        style_fig(fig, height=380, show_legend=False)
        fig.update_xaxes(type="log", tickprefix="$")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        adf = analytics.copy()
        adf["volatility"] = adf["volatility"].astype(float)
        adf = adf.sort_values("volatility")
        window_n = int(adf["window_size"].iloc[0]) if not adf.empty else 0

        fig = go.Figure(go.Bar(
            x=adf["volatility"],
            y=[symbol_for(c) for c in adf["coin"]],
            orientation="h",
            marker=dict(color=[color_for(c) for c in adf["coin"]],
                        line=dict(width=0)),
            text=[f"{v:.4f}" for v in adf["volatility"]],
            textposition="outside",
            textfont=dict(size=12, color="#FAFAFA"),
            hovertemplate="<b>%{y}</b>: σ %{x:.4f}<extra></extra>",
        ))
        fig.update_layout(title=f"Rolling Volatility (σ, last {window_n} ticks)")
        style_fig(fig, height=380, show_legend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.caption("Volume uses a log scale because BTC's volume dwarfs the "
               "rest. Volatility is the standard deviation of the "
               "rolling-window prices — higher = more turbulent recently.")

# ----- Tab 4: Deep dive -----
with tab_deep:
    if history.empty:
        st.info("No ticks in the selected window yet.")
    else:
        coins_available = sorted(history["coin"].unique())
        selected = st.selectbox(
            "Coin",
            options=coins_available,
            format_func=lambda c: f"{symbol_for(c)} — {c.capitalize()}",
        )

        coin_df = history[history["coin"] == selected].sort_values("ts").copy()
        coin_df = coin_df.reset_index(drop=True)
        coin_df["ma"] = coin_df["price"].rolling(window=20, min_periods=1).mean()
        c = color_for(selected)

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.72, 0.28],
            subplot_titles=("Price & Moving Average", "24h Volume"),
        )

        fig.add_trace(
            go.Scatter(
                x=coin_df["ts"], y=coin_df["price"],
                mode="lines", name="Price",
                line=dict(color=c, width=2.5),
                fill="tozeroy", fillcolor=hex_to_rgba(c, 0.13),
                hovertemplate="$%{y:,.4f}<extra></extra>",
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=coin_df["ts"], y=coin_df["ma"],
                mode="lines", name="MA(20)",
                line=dict(color="#FAFAFA", width=1.3, dash="dash"),
                hovertemplate="MA: $%{y:,.4f}<extra></extra>",
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Bar(
                x=coin_df["ts"], y=coin_df["volume_24h"],
                name="Volume", marker_color=c, opacity=0.55,
                hovertemplate="$%{y:,.0f}<extra></extra>",
            ),
            row=2, col=1,
        )

        style_fig(fig, height=560)
        fig.update_yaxes(tickprefix="$", row=1, col=1)
        fig.update_yaxes(tickformat=".2s", row=2, col=1)
        # subplot titles are annotations — recolour them
        for a in fig["layout"]["annotations"]:
            a["font"] = dict(size=12, color="#9ca3af")
        st.plotly_chart(fig, use_container_width=True)

        last = coin_df["price"].iloc[-1]
        high = coin_df["price"].max()
        low = coin_df["price"].min()
        avg = coin_df["price"].mean()
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Latest", format_price(last))
        s2.metric(f"High ({lookback}m)", format_price(high))
        s3.metric(f"Low ({lookback}m)", format_price(low))
        s4.metric(f"Average ({lookback}m)", format_price(avg))

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
with st.expander("📋 Recent raw ticks (last 50)"):
    recent = query(
        "SELECT coin, price, change_24h, volume_24h, ts "
        "FROM price_ticks ORDER BY ts DESC LIMIT 50;"
    )
    st.dataframe(recent, use_container_width=True, hide_index=True)

st.markdown(
    f'<div style="text-align:right;color:{MUTED};font-size:11px;margin-top:12px;">'
    f'Last refreshed: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")} · '
    f'Next refresh in {refresh}s</div>',
    unsafe_allow_html=True,
)

time.sleep(refresh)
st.rerun()