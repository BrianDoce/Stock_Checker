# dashboard/app.py
import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.graph_objects as go
from datetime import datetime

# ------------------------------
# Configuration
# ------------------------------
STOCK_SYMBOLS = [
    "SPY", "AAPL", "MSFT", "AMZN", "GOOGL", "NVDA",
    "META", "TSLA", "BRK.B", "JPM", "UNH"
]

# Load DB credentials from Streamlit secrets
db_user = st.secrets["connections"]["postgresql"]["username"]
db_pass = st.secrets["connections"]["postgresql"]["password"]
db_host = st.secrets["connections"]["postgresql"]["host"]
db_port = st.secrets["connections"]["postgresql"]["port"]
db_name = st.secrets["connections"]["postgresql"]["database"]

# ------------------------------
# Data Loading Function
# ------------------------------
@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass
    )
    query = "SELECT * FROM stock_trades;"
    df = pd.read_sql(query, conn)
    conn.close()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

df = load_data()

# ------------------------------
# Sidebar: Filters
# ------------------------------
st.sidebar.title("Filters & Controls")

# On-demand refresh button
if st.sidebar.button("Refresh Data"):
    st.experimental_rerun()

# Date range filter
min_date, max_date = df["timestamp"].min(), df["timestamp"].max()
date_range = st.sidebar.date_input(
    "Date Range",
    [min_date.date(), max_date.date()],
    min_value=min_date.date(),
    max_value=max_date.date()
)

# Stock symbol checkboxes
st.sidebar.subheader("Select Stock Symbols")
selected_symbols = []
cols = st.sidebar.columns(2)
for i, symbol in enumerate(STOCK_SYMBOLS):
    if cols[i % 2].checkbox(symbol, value=(symbol == "SPY")):
        selected_symbols.append(symbol)

# ------------------------------
# Data Filtering
# ------------------------------
filtered_df = df.copy()

if selected_symbols:
    filtered_df = filtered_df[filtered_df["symbol"].isin(selected_symbols)]

if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    filtered_df = filtered_df[
        (filtered_df["timestamp"] >= start_date) & 
        (filtered_df["timestamp"] <= end_date)
    ]

# ------------------------------
# KPIs
# ------------------------------
st.header("📈 Key Metrics")
if not filtered_df.empty:
    for sym in selected_symbols:
        stock_data = filtered_df[filtered_df["symbol"] == sym]
        if stock_data.empty:
            continue

        latest_price = stock_data["price"].iloc[-1]
        first_price = stock_data["price"].iloc[0]
        pct_change = ((latest_price - first_price) / first_price) * 100
        avg_volume = stock_data["volume"].mean() if "volume" in stock_data else None

        kpi_cols = st.columns(3)
        kpi_cols[0].metric(label=f"{sym} Latest Price", value=f"${latest_price:.2f}")
        kpi_cols[1].metric(label=f"{sym} % Change", value=f"{pct_change:.2f}%")
        if avg_volume:
            kpi_cols[2].metric(label=f"{sym} Avg Volume", value=f"{avg_volume:,.0f}")

# ------------------------------
# Line Charts with SMA & EMA
# ------------------------------
st.header("📊 Stock Prices with SMA & EMA")
if not filtered_df.empty:
    fig = go.Figure()
    for sym in selected_symbols:
        stock_data = filtered_df[filtered_df["symbol"] == sym].copy()
        stock_data = stock_data.set_index("timestamp").sort_index()

        # SMA & EMA
        stock_data["SMA_10"] = stock_data["price"].rolling(10).mean()
        stock_data["EMA_10"] = stock_data["price"].ewm(span=10, adjust=False).mean()

        fig.add_trace(go.Scatter(
            x=stock_data.index, y=stock_data["price"], mode="lines", name=f"{sym} Price"
        ))
        fig.add_trace(go.Scatter(
            x=stock_data.index, y=stock_data["SMA_10"], mode="lines", name=f"{sym} SMA(10)", line=dict(dash="dot")
        ))
        fig.add_trace(go.Scatter(
            x=stock_data.index, y=stock_data["EMA_10"], mode="lines", name=f"{sym} EMA(10)", line=dict(dash="dash")
        ))
    fig.update_layout(xaxis_title="Timestamp", yaxis_title="Price ($)", height=500)
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------
# Correlation Heatmap
# ------------------------------
st.header("📊 Correlation Heatmap of Returns")
if len(selected_symbols) > 1:
    pivot_df = filtered_df.pivot(index="timestamp", columns="symbol", values="price")
    returns_df = pivot_df.pct_change().dropna()
    corr_matrix = returns_df.corr()

    fig_corr = go.Figure(
        go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.columns,
            colorscale="Viridis",
            zmin=-1, zmax=1,
            colorbar=dict(title="Correlation")
        )
    )
    fig_corr.update_layout(title="Correlation Heatmap of Stock Returns")
    st.plotly_chart(fig_corr, use_container_width=True)
else:
    st.info("Select at least two stocks for correlation heatmap.")

# ------------------------------
# Portfolio Performance Simulator
# ------------------------------
st.header("💼 Portfolio Performance Simulator")
if len(selected_symbols) > 1:
    st.info("Allocate weights to each stock. Total will be normalized to 1.")

    # Default equal weights
    default_weights = [round(1/len(selected_symbols), 2)] * len(selected_symbols)
    weights = []
    for i, sym in enumerate(selected_symbols):
        w = st.number_input(
            f"Weight for {sym}", min_value=0.0, max_value=1.0, value=float(default_weights[i]), step=0.05, key=f"weight_{sym}"
        )
        weights.append(w)

    total_weight = sum(weights)
    if total_weight != 1 and total_weight > 0:
        weights = [w / total_weight for w in weights]
        st.warning(f"Weights normalized to sum to 1 (Total weight was {total_weight:.2f})")

    pivot_df = filtered_df.pivot(index="timestamp", columns="symbol", values="price").dropna()
    returns_df = pivot_df.pct_change().fillna(0)
    portfolio_returns = (returns_df * weights).sum(axis=1)
    portfolio_value = 100 * (1 + portfolio_returns).cumprod()

    fig_portfolio = go.Figure()
    fig_portfolio.add_trace(go.Scatter(
        x=portfolio_value.index, y=portfolio_value.values, mode="lines", name="Portfolio Value"
    ))
    fig_portfolio.update_layout(xaxis_title="Date", yaxis_title="Portfolio Value ($)")
    st.plotly_chart(fig_portfolio, use_container_width=True)
else:
    st.info("Select at least two stocks to simulate portfolio performance.")

# ------------------------------
# Download filtered data
# ------------------------------
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download filtered data as CSV",
    data=csv,
    file_name="filtered_stocks.csv",
    mime="text/csv"
)
