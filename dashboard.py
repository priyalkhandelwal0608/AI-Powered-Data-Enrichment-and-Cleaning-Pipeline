import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="NYC Airbnb Dashboard", layout="wide")

DB_PATH = "data/airbnb.duckdb"

@st.cache_data
def load_data():
    # READ_ONLY mode is key to avoiding 'process access' errors
    with duckdb.connect(DB_PATH, read_only=True) as con:
        df = con.execute("SELECT * FROM listings").df()
    return df

st.title(" NYC Airbnb ETL Dashboard")

try:
    df = load_data()
    
    # Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Listings", len(df))
    col2.metric("Avg Price", f"${df['price'].mean():.2f}")
    col3.metric("Avg Reviews", f"{df['number_of_reviews'].mean():.1f}")

    # Sidebar Filter
    neighborhood = st.sidebar.multiselect("Select Borough", options=df['neighbourhood_group'].unique())
    if neighborhood:
        df = df[df['neighbourhood_group'].isin(neighborhood)]

    # Map
    st.subheader("Map of Listings")
    st.map(df[['latitude', 'longitude']])

    # Table
    st.subheader("Raw Data Preview")
    st.dataframe(df.head(100))

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure you have run 'python etl_pipeline.py' first!")
