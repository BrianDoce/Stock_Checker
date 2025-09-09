from ml.analytics import compute_returns, cluster_stocks, summarize_clusters
import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go

STOCK_SYMBOLS = [ 
    "SPY",  # S&P 500 ETF
    "AAPL", "MSFT", "AMZN", "GOOGL", "NVDA",
    "META", "TSLA", "BRK.B", "JPM", "UNH"
]

db_user = st.secrets["connections"]["postgresql"]["username"]
db_pass = st.secrets["connections"]["postgresql"]["password"]
db_host = st.secrets["connections"]["postgresql"]["host"]
db_port = st.secrets["connections"]["postgresql"]["port"]
db_name = st.secrets["connections"]["postgresql"]["database"]

@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass
    )
    query = "SELECT * FROM stock_trades;"  # adjust if needed
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.title("Stock Dashboard")

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

df = load_data()
df["timestamp"] = pd.to_datetime(df["timestamp"])

st.subheader("Select Date Range")
min_date, max_date = df["timestamp"].min(), df["timestamp"].max()
date_range = st.date_input(
    "Date Range",
    [min_date.date(), max_date.date()],
    min_value=min_date.date(),
    max_value=max_date.date()
)

st.subheader("Select Stock Symbols")
selected_symbols = []
cols = st.columns(6)
clear_all = st.button("Clear All Symbols")
for i, symbol in enumerate(STOCK_SYMBOLS):
    default_value = (symbol == "SPY") and not clear_all
    if cols[i % 6].checkbox(symbol, value=default_value, key=f"chk_{symbol}"):
        selected_symbols.append(symbol)

filtered_df = df.copy()
if selected_symbols:
    filtered_df = filtered_df[filtered_df["symbol"].isin(selected_symbols)]

if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    filtered_df = filtered_df[
        (filtered_df["timestamp"] >= start_date) & 
        (filtered_df["timestamp"] <= end_date)
    ]

st.subheader("Correlation Heatmap of Returns")

if len(selected_symbols) > 1:
    pivot_df = filtered_df.pivot(index="timestamp", columns="symbol", values="price")
    
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
        filtered_df.pivot(index="timestamp", columns="symbol", values="price")
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

st.subheader("Stock Clusters")
st.dataframe(cluster_df)

st.subheader("Market Summary (AI)")
summary_text = summarize_clusters(cluster_df, corr_matrix)
st.write(summary_text)