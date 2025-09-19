from ml.analytics import compute_returns, cluster_stocks, summarize_clusters
import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt
import os

STOCK_SYMBOLS = [ 
    "SPY",  # S&P 500 ETF
    "AAPL", "MSFT", "AMZN", "GOOGL", "NVDA",
    "META", "TSLA", "BRK.B", "JPM", "UNH"
]

db_user = os.environ.get("POSTGRES_USER", "postgres")
db_pass = os.environ.get("POSTGRES_PASSWORD", "root")
db_host = os.environ.get("POSTGRES_HOST", "localhost")  # fallback to localhost
db_port = os.environ.get("POSTGRES_PORT", 5432)
db_name = os.environ.get("POSTGRES_DB", "stocks_db")

@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass
    )
    query = "SELECT * FROM stock_trades ORDER BY timestamp ASC;" 
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.title("Stock Dashboard")

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

df = load_data()
if df.empty:
    st.warning("No stock trade data available in the database yet.")
    st.stop()

df["timestamp"] = pd.to_datetime(df["timestamp"])

today = pd.Timestamp.today().normalize()
default_start = today
default_end = today + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

min_date, max_date = df["timestamp"].min(), df["timestamp"].max()

st.subheader("Select Date Range")
date_range = st.date_input(
    "Date Range",
    [default_start.date(), default_end.date()],
    min_value=min_date.date(),
    max_value=max_date.date()
)


st.subheader("Select Stock Symbols")
# Dynamically get available symbols from the DB
available_symbols = df["symbol"].unique().tolist()

selected_symbols = []
cols = st.columns(6)
clear_all = st.button("Clear All Symbols")

# On first load, select all available symbols by default
for i, symbol in enumerate(available_symbols):
    default_value = not clear_all  # checked by default unless Clear All pressed
    if cols[i % 6].checkbox(symbol, value=default_value, key=f"chk_{symbol}"):
        selected_symbols.append(symbol)

filtered_df = df.copy()
if selected_symbols:
    filtered_df = filtered_df[filtered_df["symbol"].isin(selected_symbols)]

if len(date_range) == 2:
    start_date = pd.to_datetime(date_range[0])
    # include the whole day by adding 1 day - 1 second
    end_date = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    filtered_df = filtered_df[
        (filtered_df["timestamp"] >= start_date) & 
        (filtered_df["timestamp"] <= end_date)
    ]
else:
    filtered_df = df

if filtered_df.empty:
    st.info("No data found for the selected range — showing all available data.")
    filtered_df = df.copy()

st.subheader("Correlation Heatmap of Returns")

if len(selected_symbols) > 1:
    # Use pivot_table to handle duplicates
    pivot_df = filtered_df.pivot_table(
        index="timestamp",
        columns="symbol",
        values="price",
        aggfunc="mean"  # or "last", "first", etc.
    )

    returns_df = pivot_df.pct_change().dropna()
    
    corr_matrix = returns_df.corr()
    
    fig_corr = go.Figure(
        data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.index if hasattr(corr_matrix, "index") else corr_matrix.columns,
            colorscale="Viridis",
            zmin=-1, zmax=1,
            colorbar=dict(title="Correlation")
        )
    )
    fig_corr.update_layout(
        title="Correlation Heatmap of Stock Returns",
        xaxis_title="Stock",
        yaxis_title="Stock"
    )
    st.plotly_chart(fig_corr, use_container_width=True)
else:
    st.info("Select at least two stock symbols to view correlation heatmap.")


st.subheader("Key Metrics")
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

st.write("### Latest Stock Data", filtered_df.tail(20))

if not filtered_df.empty:
    st.line_chart(
        filtered_df.pivot_table(
            index="timestamp",
            columns="symbol",
            values="price",
            aggfunc="mean"  # or "last" if you prefer
        )
    )


csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download filtered data as CSV",
    data=csv,
    file_name="filtered_stocks.csv",
    mime="text/csv",
)

returns_df = compute_returns(filtered_df)

cluster_df, corr_matrix = cluster_stocks(returns_df, n_clusters=3)
st.subheader("Stock Clusters & Correlation")

if cluster_df is None or corr_matrix is None:
    st.info("Select at least 3 stocks to view clustering and correlation.")
else:
    st.write("### Clusters")
    st.dataframe(cluster_df)

    st.write("### Correlation Heatmap")

    fig, ax = plt.subplots()
    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", ax=ax)
    st.pyplot(fig)

st.subheader("Market Summary (AI)")
if cluster_df is None or corr_matrix is None:
    st.info("No clusters available for summary. Please select at least 3 stocks to generate a market summary.")
else:
    summary_text = summarize_clusters(cluster_df, corr_matrix)
    st.write(summary_text)