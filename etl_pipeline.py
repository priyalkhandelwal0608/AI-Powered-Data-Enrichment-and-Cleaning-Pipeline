import duckdb
import os

DB_PATH = "data/airbnb.duckdb"
CSV_PATH = "data/AB_NYC_2019.csv"

def run_etl():
    print(" Starting ETL Process...")
    os.makedirs("data", exist_ok=True)

    with duckdb.connect(DB_PATH) as con:
        # 1. Drop existing table to avoid schema conflicts
        con.execute("DROP TABLE IF EXISTS listings")
        
        print(f" Loading {CSV_PATH}...")
        # 2. Use read_csv_auto which is great at detecting headers
        con.execute(f"CREATE TABLE listings AS SELECT * FROM read_csv_auto('{CSV_PATH}')")
        
        # 3. Debug: Print columns to verify names
        columns = [col[0] for col in con.execute("DESCRIBE listings").fetchall()]
        print(f" Columns found: {columns}")

        # 4. Conditional Update: Only run if the column exists
        target_col = "reviews_per_month"
        if target_col in columns:
            print(f" Cleaning {target_col}...")
            con.execute(f"UPDATE listings SET {target_col} = 0 WHERE {target_col} IS NULL")
        else:
            print(f" Warning: '{target_col}' not found. Check if the CSV header matches exactly.")
        
        count = con.execute("SELECT count(*) FROM listings").fetchone()[0]
        print(f" Success! {count} rows ready.")

if __name__ == "__main__":
    run_etl()
