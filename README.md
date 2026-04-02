#  AI-Powered ETL Pipeline (Apache Beam + DuckDB)

##  Project Overview

This project implements a **scalable ETL (Extract, Transform, Load) pipeline** using Apache Beam, Pandas, and DuckDB. It processes Airbnb listing data, performs cleaning and transformation, and stores the final structured data in a database for analytics and visualization.

---

##  ETL Workflow

### 1. Extract

* Reads raw CSV data using Pandas
* Handles missing or inconsistent column formatting

### 2. Transform

* Cleans data (removes unnecessary columns, trims text)
* Performs feature engineering:

  * Categorizes listings into **Low, Medium, High price segments**

### 3. Load

* Saves processed data as CSV
* Stores final dataset in DuckDB for fast querying

---

##  Tech Stack

* **Apache Beam** → Data processing pipeline
* **Pandas** → Data manipulation
* **DuckDB** → Lightweight analytical database
* **Python** → Core programming language

---

##  Project Structure

```
ETL-Pipeline/
│── app.py                # Streamlit dashboard (optional)
│── pipeline.py           # ETL pipeline logic
│── data/
│   ├── AB_NYC_2019.csv  # Input dataset
│   ├── output.csv       # Processed output
│   └── airbnb.duckdb    # Database file
│── requirements.txt
│── README.md
```

---

##  Installation and run
- pip install -r requirements.txt
- python pipeline.py
- streamlit run app.py

---

##  Key Features

*  Modular ETL design
*  Scalable pipeline using Apache Beam
*  Data cleaning & preprocessing
*  Feature engineering (price categorization)
*  Efficient storage with DuckDB
*  Ready for dashboard integration

---




