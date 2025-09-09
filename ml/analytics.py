import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from google import genai
from dotenv import load_dotenv
import os

load_dotenv()

client = genai.Client(api_key=os.getenv("GENAI_API_KEY"))

def compute_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily returns per symbol.
    Expects df columns: ['timestamp', 'symbol', 'price']
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["symbol", "timestamp"])
    df["return"] = df.groupby("symbol")["price"].pct_change()
    df.dropna(subset=["return"], inplace=True)
    return df

def cluster_stocks(df: pd.DataFrame, n_clusters=3) -> pd.DataFrame:
    """
    Cluster stocks based on returns correlation.
    Returns DataFrame mapping symbols to cluster labels.
    """
    returns_df = df.pivot(index="timestamp", columns="symbol", values="return").fillna(0)
    corr = returns_df.corr()  

    scaler = StandardScaler()
    X = scaler.fit_transform(corr) 

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    labels = kmeans.fit_predict(X)

    cluster_df = pd.DataFrame({
        "symbol": corr.index,
        "cluster": labels
    })

    return cluster_df, corr

# -----------------------------
# LLM Summary Function
# -----------------------------
def summarize_clusters(cluster_df: pd.DataFrame, corr_matrix: pd.DataFrame) -> str:
    """
    Generate a plain-language summary using OpenAI LLM.
    """
    summary_text = ""
    for c in sorted(cluster_df["cluster"].unique()):
        symbols_in_cluster = cluster_df[cluster_df["cluster"] == c]["symbol"].tolist()
        # Compute average pairwise correlation within cluster
        subset = corr_matrix.loc[symbols_in_cluster, symbols_in_cluster]
        avg_corr = subset.values[np.triu_indices_from(subset.values, k=1)].mean()
        summary_text += f"Cluster {c}: {', '.join(symbols_in_cluster)} (avg corr={avg_corr:.2f})\n"

    prompt = (
        "Given these stock clusters and average correlations, "
        "write a concise, professional market summary in plain language:\n\n"
        f"{summary_text}"
    )

    response = client.models.generate_content(
    model="gemini-2.5-flash-lite", contents=prompt)

    return response.choices[0].message.content.strip()
