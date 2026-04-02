# pipeline.py
import pandas as pd
import duckdb

# -------------------------------
# ETL Functions
# -------------------------------
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with missing essential columns or invalid price"""
    df = df.dropna(subset=['id', 'name', 'host_id', 'neighbourhood_group', 'latitude', 'longitude', 'price'])
    df = df[(df['price'] > 0) & (df['price'] < 10000)]
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicates, unnecessary columns, and strip strings"""
    df = df.drop_duplicates(subset=['id'])
    columns_to_drop = ['last_review', 'reviews_per_month']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    df['name'] = df['name'].str.strip()
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Add a price category column"""
    df['price_category'] = df['price'].apply(lambda p: 'Low' if p < 100 else 'Medium' if p < 300 else 'High')
    return df

def run_etl_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the full ETL pipeline:
    1. Validate
    2. Clean
    3. Transform
    4. Store in in-memory DuckDB (returns final DataFrame)
    """
    df = validate_data(df)
    df = clean_data(df)
    df = transform_data(df)
    
    con = duckdb.connect(":memory:")  # in-memory DB avoids file lock
    try:
        con.execute("DROP TABLE IF EXISTS listings")
        con.execute("CREATE TABLE listings AS SELECT * FROM df")
        df_from_db = con.execute("SELECT * FROM listings").fetchdf()
    finally:
        con.close()
    return df_from_db
